CREATE TABLE IF NOT EXISTS warehouse_run_reports (
    dag_run_id VARCHAR(250) PRIMARY KEY,
    batch_id VARCHAR(192) NOT NULL,
    logical_date TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL,
    ok BOOLEAN NOT NULL,
    failures JSONB NOT NULL CHECK (jsonb_typeof(failures) = 'array'),
    report JSONB NOT NULL CHECK (jsonb_typeof(report) = 'object')
);
