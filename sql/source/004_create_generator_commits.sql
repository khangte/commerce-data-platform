CREATE TABLE IF NOT EXISTS generator_commits (
    generator_run_id UUID PRIMARY KEY,
    source_snapshot_id VARCHAR(256) NOT NULL,
    random_seed BIGINT NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    order_count INTEGER NOT NULL,
    anomaly_profile VARCHAR(64) NOT NULL,
    generator_version VARCHAR(32) NOT NULL,
    result_counts JSONB NOT NULL,
    logical_hash CHAR(64) NOT NULL,
    UNIQUE (source_snapshot_id, random_seed, logical_date, order_count, anomaly_profile, generator_version)
);
