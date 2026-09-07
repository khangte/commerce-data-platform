CREATE TABLE IF NOT EXISTS generator_runs (
    generator_run_id UUID PRIMARY KEY,
    source_snapshot_id VARCHAR(256) NOT NULL,
    random_seed BIGINT NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    order_count INTEGER NOT NULL CHECK (order_count >= 0),
    anomaly_profile VARCHAR(64) NOT NULL,
    generator_version VARCHAR(32) NOT NULL,
    result_counts JSONB,
    logical_hash CHAR(64),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status VARCHAR(16) NOT NULL,
    error_message TEXT,
    CONSTRAINT generator_runs_status_check CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT generator_runs_finished_at_check CHECK (
        (status = 'RUNNING' AND finished_at IS NULL)
        OR (status IN ('SUCCESS', 'FAILED') AND finished_at IS NOT NULL)
    ),
    CONSTRAINT generator_runs_success_evidence_check CHECK (
        status <> 'SUCCESS' OR (result_counts IS NOT NULL AND logical_hash ~ '^[0-9a-f]{64}$')
    )
);

ALTER TABLE generator_runs DROP CONSTRAINT IF EXISTS generator_runs_deterministic_input_unique;

CREATE UNIQUE INDEX IF NOT EXISTS generator_runs_success_input_unique
    ON generator_runs (
        source_snapshot_id, random_seed, logical_date, order_count, anomaly_profile, generator_version
    )
    WHERE status = 'SUCCESS';

CREATE INDEX IF NOT EXISTS generator_runs_status_finished_at_idx
    ON generator_runs (status, finished_at DESC);
