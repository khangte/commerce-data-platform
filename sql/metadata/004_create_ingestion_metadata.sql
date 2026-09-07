CREATE TABLE IF NOT EXISTS watermarks (
    pipeline_name VARCHAR(128) NOT NULL,
    source_table VARCHAR(64) NOT NULL,
    watermark_timestamp TIMESTAMPTZ,
    watermark_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    lease_owner UUID,
    lease_expires_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pipeline_name, source_table),
    CONSTRAINT watermarks_pipeline_name_check CHECK (length(trim(pipeline_name)) > 0),
    CONSTRAINT watermarks_source_table_check CHECK (length(trim(source_table)) > 0),
    CONSTRAINT watermarks_keys_array_check CHECK (jsonb_typeof(watermark_keys) = 'array'),
    CONSTRAINT watermarks_initial_cursor_check CHECK (
        (watermark_timestamp IS NULL AND watermark_keys = '[]'::jsonb)
        OR (watermark_timestamp IS NOT NULL AND jsonb_array_length(watermark_keys) > 0)
    ),
    CONSTRAINT watermarks_lease_check CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT watermarks_version_check CHECK (version >= 0)
);

CREATE INDEX IF NOT EXISTS watermarks_active_lease_idx
    ON watermarks (lease_expires_at)
    WHERE lease_owner IS NOT NULL;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID NOT NULL,
    source_table VARCHAR(64) NOT NULL,
    pipeline_name VARCHAR(128) NOT NULL,
    batch_id VARCHAR(192) NOT NULL,
    dag_id VARCHAR(250),
    logical_date TIMESTAMPTZ NOT NULL,
    attempt_number INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    watermark_before JSONB NOT NULL,
    extract_upper_bound JSONB,
    rows_extracted BIGINT NOT NULL DEFAULT 0,
    rows_valid BIGINT NOT NULL DEFAULT 0,
    rows_rejected BIGINT NOT NULL DEFAULT 0,
    rows_loaded BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    error_type VARCHAR(64),
    error_message TEXT,
    PRIMARY KEY (run_id, source_table),
    CONSTRAINT pipeline_runs_attempt_number_check CHECK (attempt_number > 0),
    CONSTRAINT pipeline_runs_watermark_before_object_check CHECK (
        jsonb_typeof(watermark_before) = 'object'
    ),
    CONSTRAINT pipeline_runs_extract_upper_bound_object_check CHECK (
        extract_upper_bound IS NULL OR jsonb_typeof(extract_upper_bound) = 'object'
    ),
    CONSTRAINT pipeline_runs_count_check CHECK (
        rows_extracted >= 0 AND rows_valid >= 0 AND rows_rejected >= 0 AND rows_loaded >= 0
    ),
    CONSTRAINT pipeline_runs_status_check CHECK (
        status IN ('RUNNING', 'SUCCESS', 'SUCCESS_NO_DATA', 'SKIPPED_ALREADY_COMMITTED', 'FAILED')
    ),
    CONSTRAINT pipeline_runs_finished_at_check CHECK (
        (status = 'RUNNING' AND finished_at IS NULL)
        OR (status <> 'RUNNING' AND finished_at IS NOT NULL)
    ),
    CONSTRAINT pipeline_runs_error_check CHECK (
        (status <> 'FAILED' AND error_type IS NULL AND error_message IS NULL)
        OR (status = 'FAILED' AND error_type IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS pipeline_runs_batch_status_idx
    ON pipeline_runs (pipeline_name, batch_id, source_table, status);

CREATE TABLE IF NOT EXISTS bronze_objects (
    table_batch_id VARCHAR(320) PRIMARY KEY,
    source_table VARCHAR(64) NOT NULL,
    batch_id VARCHAR(192) NOT NULL,
    object_key TEXT NOT NULL UNIQUE,
    manifest_key TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    row_count BIGINT NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    logical_hash CHAR(64) NOT NULL,
    watermark_before JSONB NOT NULL,
    watermark_after JSONB NOT NULL,
    status VARCHAR(16) NOT NULL,
    committed_at TIMESTAMPTZ,
    CONSTRAINT bronze_objects_schema_version_check CHECK (schema_version > 0),
    CONSTRAINT bronze_objects_row_count_check CHECK (row_count >= 0),
    CONSTRAINT bronze_objects_content_sha256_check CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT bronze_objects_logical_hash_check CHECK (logical_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT bronze_objects_watermark_before_object_check CHECK (
        jsonb_typeof(watermark_before) = 'object'
    ),
    CONSTRAINT bronze_objects_watermark_after_object_check CHECK (
        jsonb_typeof(watermark_after) = 'object'
    ),
    CONSTRAINT bronze_objects_status_check CHECK (status IN ('STAGED', 'COMMITTED', 'ORPHANED')),
    CONSTRAINT bronze_objects_committed_at_check CHECK (
        (status = 'COMMITTED' AND committed_at IS NOT NULL)
        OR (status = 'STAGED' AND committed_at IS NULL)
        OR status = 'ORPHANED'
    )
);

CREATE INDEX IF NOT EXISTS bronze_objects_catalog_idx
    ON bronze_objects (source_table, batch_id)
    WHERE status = 'COMMITTED';

CREATE TABLE IF NOT EXISTS quarantine_batches (
    table_batch_id VARCHAR(320) PRIMARY KEY,
    object_key TEXT NOT NULL UNIQUE,
    row_count BIGINT NOT NULL,
    error_counts JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT quarantine_batches_row_count_check CHECK (row_count >= 0),
    CONSTRAINT quarantine_batches_error_counts_object_check CHECK (
        jsonb_typeof(error_counts) = 'object'
    )
);
