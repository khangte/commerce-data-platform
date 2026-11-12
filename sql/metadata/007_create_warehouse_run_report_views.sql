-- Metabase 운영 Dashboard가 읽는 Run report View 2개. 원본 Table과 report JSON 전체는 노출하지 않는다.
-- View는 소유자(pipeline 계정) 권한으로 읽으므로 metabase_ops_ro에는 View SELECT만 준다.
CREATE OR REPLACE VIEW warehouse_run_report_status AS
WITH base AS (
    SELECT
        r.dag_run_id,
        r.logical_date,
        r.recorded_at,
        r.ok,
        r.report->>'publish_status' AS publish_status,
        r.report->>'publish_error_type' AS publish_error_type,
        r.report->>'serving_export_status' AS serving_export_status,
        r.report->>'metabase_repoint_status' AS metabase_repoint_status,
        (r.report->>'fresh')::boolean AS fresh,
        (SELECT string_agg(code, ', ' ORDER BY position)
           FROM jsonb_array_elements_text(r.failures) WITH ORDINALITY AS f(code, position)
        ) AS failure_codes,
        -- 접두사는 src/observability/run_report.py ALERT_FAILURE_PREFIXES와 같아야 한다.
        (SELECT string_agg(code, ', ' ORDER BY position)
           FROM jsonb_array_elements_text(r.failures) WITH ORDINALITY AS f(code, position)
          WHERE starts_with(code, 'PUBLISH_PARTIAL:')
             OR starts_with(code, 'DATA_STALE:')
             OR starts_with(code, 'PUBLISH_FAILED')
             OR starts_with(code, 'ROW_COUNT_DROP:')
        ) AS alert_codes,
        (SELECT string_agg(domain, ', ' ORDER BY position)
           FROM jsonb_array_elements_text(
                CASE WHEN jsonb_typeof(r.report->'held_domains') = 'array'
                     THEN r.report->'held_domains' END
           ) WITH ORDINALITY AS h(domain, position)
        ) AS held_domains,
        (r.report->>'data_lag_s')::double precision AS data_lag_s,
        (r.report->>'stale_after_s')::double precision AS stale_after_s,
        (r.report->>'dag_elapsed_s')::double precision AS dag_elapsed_s,
        (r.report->>'tests_warned')::integer AS tests_warned,
        (r.report->>'full_refresh')::boolean AS full_refresh,
        extract(epoch FROM now() - r.recorded_at)::double precision AS record_age_s
    FROM warehouse_run_reports r
)
SELECT
    base.*,
    CASE
        WHEN stale_after_s IS NULL OR record_age_s > stale_after_s THEN '확인 불가'
        WHEN alert_codes IS NOT NULL THEN '알림 대상'
        WHEN NOT ok THEN '판정 있음'
        ELSE '정상'
    END AS status
FROM base;

CREATE OR REPLACE VIEW warehouse_run_report_domain_status AS
SELECT
    r.dag_run_id,
    r.recorded_at,
    e.domain,
    (e.value #>> '{}')::double precision AS lag_s,
    (r.report->'domain_refreshed_at'->>e.domain)::timestamptz AS refreshed_at,
    (r.report->'domain_as_of'->>e.domain)::timestamptz AS as_of,
    (r.report->'consecutive_holds'->>e.domain)::integer AS consecutive_holds,
    CASE WHEN jsonb_typeof(r.report->'held_domains') = 'array'
         THEN r.report->'held_domains' ? e.domain END AS held,
    (r.report->>'stale_after_s')::double precision AS stale_after_s
FROM warehouse_run_reports r
-- 도메인 값이 없거나 JSON null인 기록(Build 전 실패, 이전 형식)은 행을 만들지 않는다.
CROSS JOIN LATERAL jsonb_each(
    CASE WHEN jsonb_typeof(r.report->'domain_lag_s') = 'object'
         THEN r.report->'domain_lag_s' END
) AS e(domain, value);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_ops_ro') THEN
        EXECUTE format('GRANT CONNECT ON DATABASE %I TO metabase_ops_ro', current_database());
        GRANT USAGE ON SCHEMA public TO metabase_ops_ro;
        GRANT SELECT ON warehouse_run_report_status, warehouse_run_report_domain_status
            TO metabase_ops_ro;
    END IF;
END
$$;
