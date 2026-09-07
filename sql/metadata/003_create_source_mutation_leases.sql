CREATE TABLE IF NOT EXISTS source_mutation_leases (
    resource_name VARCHAR(64) PRIMARY KEY,
    owner_type VARCHAR(16),
    owner_id UUID,
    lease_expires_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT source_mutation_leases_resource_check CHECK (resource_name = 'commerce_source'),
    CONSTRAINT source_mutation_leases_owner_check CHECK (
        (owner_type IS NULL AND owner_id IS NULL AND lease_expires_at IS NULL)
        OR (
            owner_type IN ('GENERATOR', 'WAREHOUSE')
            AND owner_id IS NOT NULL
            AND lease_expires_at IS NOT NULL
        )
    )
);
