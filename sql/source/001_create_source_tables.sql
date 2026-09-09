CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    customer_unique_id VARCHAR(64) COLLATE "C" NOT NULL,
    customer_city VARCHAR(128),
    customer_state CHAR(2),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_memberships (
    customer_unique_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    membership_level VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT customer_memberships_level_check
        CHECK (membership_level IN ('bronze', 'silver', 'gold')),
    CONSTRAINT customer_memberships_updated_at_check CHECK (updated_at >= created_at)
);

-- 이전 계정 단위 멤버십 스키마를 안전하게 사람 단위 테이블로 올린다.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'customers' AND column_name = 'membership_level'
    ) THEN
        INSERT INTO customer_memberships (
            customer_unique_id, membership_level, created_at, updated_at
        )
        SELECT
            customer_unique_id,
            (array_agg(membership_level ORDER BY updated_at DESC, customer_id DESC))[1],
            min(created_at),
            max(updated_at)
        FROM customers
        GROUP BY customer_unique_id
        ON CONFLICT (customer_unique_id) DO UPDATE
        SET membership_level = EXCLUDED.membership_level,
            created_at = LEAST(customer_memberships.created_at, EXCLUDED.created_at),
            updated_at = GREATEST(customer_memberships.updated_at, EXCLUDED.updated_at);

        ALTER TABLE customers DROP CONSTRAINT IF EXISTS customers_membership_level_check;
        ALTER TABLE customers DROP CONSTRAINT IF EXISTS customers_updated_at_check;
        ALTER TABLE customers DROP COLUMN membership_level;
        ALTER TABLE customers DROP COLUMN updated_at;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    product_category_name VARCHAR(256),
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT products_weight_check CHECK (product_weight_g IS NULL OR product_weight_g >= 0),
    CONSTRAINT products_length_check CHECK (product_length_cm IS NULL OR product_length_cm >= 0),
    CONSTRAINT products_height_check CHECK (product_height_cm IS NULL OR product_height_cm >= 0),
    CONSTRAINT products_width_check CHECK (product_width_cm IS NULL OR product_width_cm >= 0),
    CONSTRAINT products_updated_at_check CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    seller_city VARCHAR(128),
    seller_state CHAR(2),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT sellers_updated_at_check CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    customer_id VARCHAR(64) COLLATE "C" NOT NULL REFERENCES customers (customer_id),
    order_status VARCHAR(16) NOT NULL,
    order_purchase_timestamp TIMESTAMPTZ NOT NULL,
    order_approved_at TIMESTAMPTZ,
    order_delivered_carrier_date TIMESTAMPTZ,
    order_delivered_customer_date TIMESTAMPTZ,
    order_estimated_delivery_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT orders_status_check CHECK (
        order_status IN (
            'created', 'approved', 'processing', 'invoiced', 'shipped', 'delivered', 'canceled',
            'unavailable'
        )
    ),
    CONSTRAINT orders_updated_at_check CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(64) COLLATE "C" NOT NULL REFERENCES orders (order_id),
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(64) COLLATE "C" NOT NULL REFERENCES products (product_id),
    seller_id VARCHAR(64) COLLATE "C" NOT NULL REFERENCES sellers (seller_id),
    price NUMERIC(14, 2) NOT NULL,
    freight_value NUMERIC(14, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT order_items_id_check CHECK (order_item_id > 0),
    CONSTRAINT order_items_price_check CHECK (price >= 0),
    CONSTRAINT order_items_freight_value_check CHECK (freight_value >= 0)
);

CREATE TABLE IF NOT EXISTS order_payments (
    order_id VARCHAR(64) COLLATE "C" NOT NULL REFERENCES orders (order_id),
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR(32) NOT NULL,
    payment_installments INTEGER,
    payment_value NUMERIC(14, 2) NOT NULL,
    payment_status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    CONSTRAINT order_payments_sequential_check CHECK (payment_sequential > 0),
    CONSTRAINT order_payments_installments_check
        CHECK (payment_installments IS NULL OR payment_installments >= 0),
    CONSTRAINT order_payments_value_check CHECK (payment_value >= 0),
    CONSTRAINT order_payments_status_check
        CHECK (payment_status IN ('pending', 'completed', 'failed', 'refunded')),
    CONSTRAINT order_payments_updated_at_check CHECK (updated_at >= created_at)
);

DROP INDEX IF EXISTS customers_updated_at_customer_id_idx;
CREATE INDEX IF NOT EXISTS customers_created_at_customer_id_idx
    ON customers (created_at, customer_id COLLATE "C");
CREATE INDEX IF NOT EXISTS customer_memberships_updated_at_customer_unique_id_idx
    ON customer_memberships (updated_at, customer_unique_id COLLATE "C");
CREATE INDEX IF NOT EXISTS products_updated_at_product_id_idx
    ON products (updated_at, product_id COLLATE "C");
CREATE INDEX IF NOT EXISTS sellers_updated_at_seller_id_idx
    ON sellers (updated_at, seller_id COLLATE "C");
CREATE INDEX IF NOT EXISTS orders_updated_at_order_id_idx
    ON orders (updated_at, order_id COLLATE "C");
CREATE INDEX IF NOT EXISTS order_items_created_at_order_id_order_item_id_idx
    ON order_items (created_at, order_id COLLATE "C", order_item_id);
CREATE INDEX IF NOT EXISTS order_payments_updated_at_order_id_payment_sequential_idx
    ON order_payments (updated_at, order_id COLLATE "C", payment_sequential);
