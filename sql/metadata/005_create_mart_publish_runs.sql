CREATE TABLE IF NOT EXISTS mart_publish_runs (
    publish_run_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(128) NOT NULL CHECK (length(pipeline_name) > 0),
    batch_id VARCHAR(192),
    dag_run_id VARCHAR(250),
    dbt_invocation_id VARCHAR(64),
    status VARCHAR(16) NOT NULL
        CHECK (status IN ('BUILDING', 'PUBLISHING', 'PUBLISHED', 'FAILED')),
    error_type VARCHAR(64),
    error_message TEXT,
    previous_publish_run_id UUID
        REFERENCES mart_publish_runs (publish_run_id) ON DELETE SET NULL,
    mart_hashes JSONB,
    mart_row_counts JSONB,
    tests_passed INTEGER NOT NULL DEFAULT 0 CHECK (tests_passed >= 0),
    tests_failed INTEGER NOT NULL DEFAULT 0 CHECK (tests_failed >= 0),
    failed_path TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    CONSTRAINT mart_publish_runs_finished_check CHECK (
        (status IN ('BUILDING', 'PUBLISHING')) = (finished_at IS NULL)
    ),
    CONSTRAINT mart_publish_runs_error_check CHECK (
        (status <> 'FAILED' AND error_type IS NULL AND error_message IS NULL)
        OR (status = 'FAILED' AND error_type IS NOT NULL AND length(error_type) > 0)
    ),
    CONSTRAINT mart_publish_runs_hash_check CHECK (
        status NOT IN ('PUBLISHING', 'PUBLISHED')
        OR (
            mart_hashes IS NOT NULL
            AND jsonb_typeof(mart_hashes) = 'object'
            AND mart_row_counts IS NOT NULL
            AND jsonb_typeof(mart_row_counts) = 'object'
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS mart_publish_runs_single_active_idx
    ON mart_publish_runs ((true))
    WHERE status IN ('BUILDING', 'PUBLISHING');

CREATE INDEX IF NOT EXISTS mart_publish_runs_batch_started_idx
    ON mart_publish_runs (batch_id, started_at);
