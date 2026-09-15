DO $$
DECLARE
    expected_columns TEXT[] := ARRAY[
        'order_id',
        'payment_sequential',
        'payment_type',
        'payment_installments',
        'payment_value',
        'payment_status',
        'payment_initiated_at',
        'payment_completed_at',
        'payment_failed_at',
        'payment_refunded_at',
        'created_at',
        'updated_at'
    ];
    current_columns TEXT[];
    original_row_count BIGINT;
    replacement_row_count BIGINT;
BEGIN
    SELECT ARRAY_AGG(column_name ORDER BY ordinal_position)
    INTO current_columns
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'order_payments';

    IF current_columns IS DISTINCT FROM expected_columns THEN
        LOCK TABLE order_payments IN ACCESS EXCLUSIVE MODE;
        SELECT COUNT(*) INTO original_row_count FROM order_payments;

        CREATE TABLE order_payments_reordered (
            order_id VARCHAR(64) COLLATE "C" NOT NULL,
            payment_sequential INTEGER NOT NULL,
            payment_type VARCHAR(32) NOT NULL,
            payment_installments INTEGER,
            payment_value NUMERIC(14, 2) NOT NULL,
            payment_status VARCHAR(16) NOT NULL,
            payment_initiated_at TIMESTAMPTZ,
            payment_completed_at TIMESTAMPTZ,
            payment_failed_at TIMESTAMPTZ,
            payment_refunded_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );

        INSERT INTO order_payments_reordered (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value,
            payment_status,
            payment_initiated_at,
            payment_completed_at,
            payment_failed_at,
            payment_refunded_at,
            created_at,
            updated_at
        )
        SELECT
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value,
            payment_status,
            payment_initiated_at,
            payment_completed_at,
            payment_failed_at,
            payment_refunded_at,
            created_at,
            updated_at
        FROM order_payments;

        SELECT COUNT(*) INTO replacement_row_count FROM order_payments_reordered;
        IF replacement_row_count <> original_row_count THEN
            RAISE EXCEPTION 'order_payments column reorder changed row count: % <> %',
                replacement_row_count,
                original_row_count;
        END IF;

        DROP TABLE order_payments;
        ALTER TABLE order_payments_reordered RENAME TO order_payments;
        ALTER TABLE order_payments
            RENAME CONSTRAINT order_payments_reordered_order_id_not_null
                TO order_payments_order_id_not_null;
        ALTER TABLE order_payments
            RENAME CONSTRAINT order_payments_reordered_payment_sequential_not_null
                TO order_payments_payment_sequential_not_null;
        ALTER TABLE order_payments
            RENAME CONSTRAINT order_payments_reordered_payment_type_not_null
                TO order_payments_payment_type_not_null;
        ALTER TABLE order_payments
            RENAME CONSTRAINT order_payments_reordered_payment_value_not_null
                TO order_payments_payment_value_not_null;
        ALTER TABLE order_payments
            RENAME CONSTRAINT order_payments_reordered_payment_status_not_null
                TO order_payments_payment_status_not_null;
        ALTER TABLE order_payments
            RENAME CONSTRAINT order_payments_reordered_created_at_not_null
                TO order_payments_created_at_not_null;
        ALTER TABLE order_payments
            RENAME CONSTRAINT order_payments_reordered_updated_at_not_null
                TO order_payments_updated_at_not_null;
        ALTER TABLE order_payments
            ADD CONSTRAINT order_payments_pkey PRIMARY KEY (order_id, payment_sequential),
            ADD CONSTRAINT order_payments_order_id_fkey
                FOREIGN KEY (order_id) REFERENCES orders (order_id),
            ADD CONSTRAINT order_payments_sequential_check CHECK (payment_sequential > 0),
            ADD CONSTRAINT order_payments_installments_check
                CHECK (payment_installments IS NULL OR payment_installments >= 0),
            ADD CONSTRAINT order_payments_value_check CHECK (payment_value >= 0),
            ADD CONSTRAINT order_payments_status_check
                CHECK (payment_status IN ('pending', 'completed', 'failed', 'refunded')),
            ADD CONSTRAINT order_payments_updated_at_check CHECK (updated_at >= created_at);
        CREATE INDEX order_payments_updated_at_order_id_payment_sequential_idx
            ON order_payments (updated_at, order_id COLLATE "C", payment_sequential);
    END IF;
END $$;
