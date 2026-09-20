-- Batch별 Ingestion 결과와 연결된 최신 Warehouse Publish 결과를 한 번에 조회한다.
-- run_outcome 우선순위: FAILED > RUNNING > RERUN > EMPTY > SUCCESS.
with ranked_runs as (
    select
        pipeline_runs.*,
        row_number() over (
            partition by pipeline_name, batch_id, source_table
            order by attempt_number desc, started_at desc
        ) as attempt_rank,
        max(attempt_number) over (
            partition by pipeline_name, batch_id
        ) as batch_max_attempt_number
    from pipeline_runs
),
latest_runs as (
    select * from ranked_runs where attempt_rank = 1
),
batch_runs as (
    select
        pipeline_name, batch_id, min(logical_date) as logical_date, count(*) as table_count,
        max(batch_max_attempt_number) as max_attempt_number,
        count(*) filter (where status = 'FAILED') as failed_tables,
        count(*) filter (where status = 'RUNNING') as running_tables,
        count(*) filter (where status = 'SKIPPED_ALREADY_COMMITTED') as skipped_tables,
        count(*) filter (where status = 'SUCCESS_NO_DATA') as empty_tables,
        sum(rows_extracted) as rows_extracted, sum(rows_rejected) as rows_rejected,
        sum(rows_loaded) as rows_loaded,
        string_agg(distinct error_type, ',' order by error_type) as ingestion_error_types,
        min(started_at) as started_at, max(finished_at) as finished_at
    from latest_runs
    group by pipeline_name, batch_id
),
latest_publish as (
    select distinct on (batch_id) batch_id, publish_run_id, status, error_type
    from mart_publish_runs
    where batch_id is not null
    order by batch_id, started_at desc
)
select
    batch_runs.pipeline_name, batch_runs.batch_id, batch_runs.logical_date,
    case
        when batch_runs.failed_tables > 0 or latest_publish.status = 'FAILED' then 'FAILED'
        when batch_runs.running_tables > 0 or latest_publish.status in ('BUILDING', 'PUBLISHING') then 'RUNNING'
        when batch_runs.max_attempt_number > 1 or batch_runs.skipped_tables > 0 then 'RERUN'
        when batch_runs.empty_tables = batch_runs.table_count then 'EMPTY'
        else 'SUCCESS'
    end as run_outcome,
    batch_runs.table_count, batch_runs.max_attempt_number, batch_runs.rows_extracted,
    batch_runs.rows_rejected, batch_runs.rows_loaded, batch_runs.ingestion_error_types,
    latest_publish.publish_run_id, latest_publish.status as publish_status,
    latest_publish.error_type as publish_error_type, batch_runs.started_at, batch_runs.finished_at
from batch_runs
left join latest_publish using (batch_id)
order by batch_runs.started_at desc
