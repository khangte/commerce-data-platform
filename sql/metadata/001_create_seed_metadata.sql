CREATE TABLE IF NOT EXISTS seed_runs (
    seed_run_id UUID PRIMARY KEY,
    raw_checksum CHAR(64) NOT NULL,
    seeded_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    table_row_counts JSONB,
    table_content_hashes JSONB,
    status VARCHAR(16) NOT NULL,
    error_message TEXT,
    CONSTRAINT seed_runs_status_check CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT seed_runs_finished_at_check CHECK (
        (status = 'RUNNING' AND finished_at IS NULL)
        OR (status IN ('SUCCESS', 'FAILED') AND finished_at IS NOT NULL)
    )
);
