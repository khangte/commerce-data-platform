CREATE TABLE IF NOT EXISTS generator_commits (
    generator_run_id UUID PRIMARY KEY,
    source_snapshot_id VARCHAR(256) NOT NULL,
    random_seed BIGINT NOT NULL,
    logical_date TIMESTAMPTZ NOT NULL,
    order_count INTEGER NOT NULL,
    anomaly_profile VARCHAR(64) NOT NULL,
    generator_version VARCHAR(32) NOT NULL,
    progress_time_scale INTEGER NOT NULL DEFAULT 1 CHECK (progress_time_scale >= 1),
    result_counts JSONB NOT NULL,
    logical_hash CHAR(64) NOT NULL
);

ALTER TABLE generator_commits
    ADD COLUMN IF NOT EXISTS progress_time_scale INTEGER NOT NULL DEFAULT 1
        CHECK (progress_time_scale >= 1);

-- 배율 이전의 이름 자동 생성 UNIQUE 제약을 제거한다.
DO $$
DECLARE
    legacy_constraint text;
BEGIN
    FOR legacy_constraint IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'generator_commits'::regclass AND contype = 'u'
    LOOP
        EXECUTE format('ALTER TABLE generator_commits DROP CONSTRAINT %I', legacy_constraint);
    END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS generator_commits_input_unique
    ON generator_commits (
        source_snapshot_id, random_seed, logical_date, order_count, anomaly_profile,
        generator_version, progress_time_scale
    );
