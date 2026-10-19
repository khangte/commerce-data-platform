-- 기본 창 3시간, 실행 간격 10분. 스케줄 변경 시 두 값을 함께 조정한다.
CREATE OR REPLACE TEMP VIEW run_report_window AS
SELECT interval '3 hours' AS window_size, interval '10 minutes' AS slot_size,
       date_bin(interval '10 minutes', CURRENT_TIMESTAMP, timestamptz '2000-01-01') AS window_end;

CREATE OR REPLACE TEMP VIEW run_report_slots AS
SELECT generate_series(window_end - window_size, window_end - slot_size, slot_size) AS logical_date
FROM run_report_window;

CREATE OR REPLACE TEMP VIEW run_reports_in_window AS
SELECT r.* FROM warehouse_run_reports r CROSS JOIN run_report_window w
WHERE r.logical_date >= w.window_end - w.window_size AND r.logical_date < w.window_end;

-- 실행 슬롯의 존재 여부는 시각이 정확히 같은 기록으로 판정한다.
WITH totals AS (
    SELECT count(*) AS reported, count(*) FILTER (WHERE ok) AS ok_runs,
           count(*) FILTER (WHERE NOT ok) AS not_ok_runs,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY (report->>'dag_elapsed_s')::double precision) AS p95_dag_s,
           max((report->>'dag_elapsed_s')::double precision) AS max_dag_s,
           max((report->>'start_lag_s')::double precision) AS max_start_lag_s,
           max((report->>'failed_builds_count')::bigint) AS failed_builds_max,
           min((report->>'disk_free_bytes')::bigint) AS disk_free_min
    FROM run_reports_in_window
), slots AS (
    SELECT count(*) AS expected_slots,
           count(*) FILTER (WHERE NOT EXISTS (
               SELECT 1 FROM run_reports_in_window r WHERE r.logical_date = s.logical_date
           )) AS missing_slots
    FROM run_report_slots s
), codes AS (
    SELECT code, count(*) AS count FROM run_reports_in_window r
    CROSS JOIN LATERAL jsonb_array_elements_text(r.failures) AS f(code) GROUP BY code
)
SELECT expected_slots, reported, ok_runs, not_ok_runs, missing_slots,
       p95_dag_s, max_dag_s, max_start_lag_s,
       COALESCE((SELECT jsonb_object_agg(code, count) FROM codes), '{}'::jsonb) AS failure_counts,
       (SELECT (report->>'exports_count')::bigint FROM run_reports_in_window
        ORDER BY logical_date DESC, recorded_at DESC, dag_run_id DESC LIMIT 1) AS last_exports_count,
       failed_builds_max, disk_free_min,
       CASE WHEN missing_slots = 0 AND not_ok_runs = 0 AND p95_dag_s <= 300
            THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM totals CROSS JOIN slots;

SELECT dag_run_id, failures, report->'probe_errors' AS probe_errors
FROM run_reports_in_window WHERE NOT ok ORDER BY logical_date, dag_run_id;

SELECT s.logical_date, g.status AS generator_status, g.error_message
FROM run_report_slots s
LEFT JOIN generator_runs g ON g.logical_date = s.logical_date
WHERE NOT EXISTS (SELECT 1 FROM run_reports_in_window r WHERE r.logical_date = s.logical_date)
ORDER BY s.logical_date, g.started_at;
