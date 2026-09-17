CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    customer_unique_id VARCHAR(64) COLLATE "C" NOT NULL,
    customer_city VARCHAR(128),
    customer_state CHAR(2),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_subscriptions (
    subscription_id UUID PRIMARY KEY,
    customer_unique_id VARCHAR(64) COLLATE "C" NOT NULL,
    subscription_status VARCHAR(32) NOT NULL,
    auto_renew_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    subscription_started_at TIMESTAMPTZ NOT NULL,
    current_period_started_at TIMESTAMPTZ,
    current_period_ends_at TIMESTAMPTZ,
    billing_due_at TIMESTAMPTZ,
    next_payment_attempt_at TIMESTAMPTZ,
    payment_failed_at TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    status_changed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT customer_subscriptions_status_check CHECK (
        subscription_status IN (
            'ACTIVE', 'PAYMENT_FAILED', 'CANCEL_REQUESTED', 'CHURNED'
        )
    ),
    CONSTRAINT customer_subscriptions_period_check CHECK (
        current_period_started_at IS NULL
        OR current_period_ends_at IS NULL
        OR current_period_ends_at > current_period_started_at
    ),
    CONSTRAINT customer_subscriptions_active_check CHECK (
        subscription_status <> 'ACTIVE'
        OR (
            current_period_started_at IS NOT NULL
            AND current_period_ends_at IS NOT NULL
            AND billing_due_at IS NOT NULL
            AND next_payment_attempt_at IS NOT NULL
        )
    ),
    CONSTRAINT customer_subscriptions_payment_failed_check CHECK (
        subscription_status <> 'PAYMENT_FAILED'
        OR payment_failed_at IS NOT NULL
    ),
    CONSTRAINT customer_subscriptions_cancel_requested_check CHECK (
        subscription_status <> 'CANCEL_REQUESTED'
        OR (
            NOT auto_renew_enabled
            AND
            cancel_requested_at IS NOT NULL
            AND next_payment_attempt_at IS NULL
        )
    ),
    CONSTRAINT customer_subscriptions_churned_check CHECK (
        subscription_status <> 'CHURNED'
        OR (
            NOT auto_renew_enabled
            AND ended_at IS NOT NULL
            AND billing_due_at IS NULL
            AND next_payment_attempt_at IS NULL
        )
    ),
    CONSTRAINT customer_subscriptions_updated_at_check CHECK (updated_at >= created_at),
    CONSTRAINT customer_subscriptions_status_changed_at_check
        CHECK (status_changed_at >= subscription_started_at AND status_changed_at <= updated_at)
);

CREATE TABLE IF NOT EXISTS customer_membership_tiers (
    customer_unique_id VARCHAR(64) COLLATE "C" PRIMARY KEY,
    membership_tier VARCHAR(16) NOT NULL DEFAULT 'BRONZE',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT customer_membership_tiers_tier_check
        CHECK (membership_tier IN ('BRONZE', 'SILVER', 'GOLD')),
    CONSTRAINT customer_membership_tiers_updated_at_check CHECK (updated_at >= created_at)
);

CREATE TABLE IF NOT EXISTS subscription_payments (
    payment_id UUID PRIMARY KEY,
    subscription_id UUID NOT NULL REFERENCES customer_subscriptions (subscription_id),
    billing_cycle_sequence INTEGER NOT NULL,
    attempt_sequence INTEGER NOT NULL,
    payment_status VARCHAR(16) NOT NULL,
    payment_at TIMESTAMPTZ NOT NULL,
    payment_value NUMERIC(14, 2) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    billing_period_start_at TIMESTAMPTZ NOT NULL,
    billing_period_end_at TIMESTAMPTZ NOT NULL,
    payment_method_type VARCHAR(32),
    payment_provider VARCHAR(32),
    provider_payment_id VARCHAR(128),
    failure_code VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT subscription_payments_cycle_attempt_unique
        UNIQUE (subscription_id, billing_cycle_sequence, attempt_sequence),
    CONSTRAINT subscription_payments_sequence_check
        CHECK (billing_cycle_sequence > 0 AND attempt_sequence > 0),
    CONSTRAINT subscription_payments_status_check
        CHECK (payment_status IN ('completed', 'failed')),
    CONSTRAINT subscription_payments_value_check CHECK (payment_value >= 0),
    CONSTRAINT subscription_payments_period_check
        CHECK (billing_period_end_at > billing_period_start_at),
    CONSTRAINT subscription_payments_failure_code_check
        CHECK (payment_status <> 'completed' OR failure_code IS NULL),
    CONSTRAINT subscription_payments_updated_at_check CHECK (updated_at >= created_at)
);

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
    shipping_limit_date TIMESTAMPTZ NOT NULL,
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
    payment_initiated_at TIMESTAMPTZ,
    payment_completed_at TIMESTAMPTZ,
    payment_failed_at TIMESTAMPTZ,
    payment_refunded_at TIMESTAMPTZ,
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

ALTER TABLE order_payments
    ADD COLUMN IF NOT EXISTS payment_initiated_at TIMESTAMPTZ;
ALTER TABLE order_payments
    ADD COLUMN IF NOT EXISTS payment_completed_at TIMESTAMPTZ;
ALTER TABLE order_payments
    ADD COLUMN IF NOT EXISTS payment_failed_at TIMESTAMPTZ;
ALTER TABLE order_payments
    ADD COLUMN IF NOT EXISTS payment_refunded_at TIMESTAMPTZ;

DROP INDEX IF EXISTS customers_updated_at_customer_id_idx;
CREATE INDEX IF NOT EXISTS customers_created_at_customer_id_idx
    ON customers (created_at, customer_id COLLATE "C");
CREATE INDEX IF NOT EXISTS customer_subscriptions_updated_at_subscription_id_idx
    ON customer_subscriptions (updated_at, subscription_id);
CREATE UNIQUE INDEX IF NOT EXISTS customer_subscriptions_one_open_contract_idx
    ON customer_subscriptions (customer_unique_id COLLATE "C")
    WHERE subscription_status IN ('ACTIVE', 'PAYMENT_FAILED', 'CANCEL_REQUESTED');
CREATE INDEX IF NOT EXISTS customer_membership_tiers_updated_at_customer_unique_id_idx
    ON customer_membership_tiers (updated_at, customer_unique_id COLLATE "C");
CREATE INDEX IF NOT EXISTS subscription_payments_updated_at_payment_id_idx
    ON subscription_payments (updated_at, payment_id);
CREATE UNIQUE INDEX IF NOT EXISTS subscription_payments_provider_payment_unique_idx
    ON subscription_payments (payment_provider, provider_payment_id)
    WHERE payment_provider IS NOT NULL AND provider_payment_id IS NOT NULL;

COMMENT ON TABLE customer_subscriptions IS
    '고객별 구독 계약과 현재 자동갱신 운영 상태를 보관한다.';
COMMENT ON COLUMN customer_subscriptions.subscription_id IS '구독 계약의 불변 식별자다.';
COMMENT ON COLUMN customer_subscriptions.customer_unique_id IS '구독 계약을 보유한 고객의 사람 단위 식별자다.';
COMMENT ON COLUMN customer_subscriptions.subscription_status IS '구독 계약의 현재 상태다.';
COMMENT ON COLUMN customer_subscriptions.auto_renew_enabled IS '정기 결제 자동갱신 허용 여부다.';
COMMENT ON COLUMN customer_subscriptions.subscription_started_at IS '구독 계약이 시작된 업무 시각이다.';
COMMENT ON COLUMN customer_subscriptions.current_period_started_at IS '현재 혜택 제공 기간의 시작 시각이다.';
COMMENT ON COLUMN customer_subscriptions.current_period_ends_at IS '현재 혜택 제공 기간의 종료 예정 시각이다.';
COMMENT ON COLUMN customer_subscriptions.billing_due_at IS '현재 청구 회차의 원래 정기 청구 기한이다.';
COMMENT ON COLUMN customer_subscriptions.next_payment_attempt_at IS '자동갱신 또는 실패 재시도로 다음 결제를 시도할 예정 시각이다.';
COMMENT ON COLUMN customer_subscriptions.payment_failed_at IS '최근 결제 실패가 확정된 업무 시각이다.';
COMMENT ON COLUMN customer_subscriptions.cancel_requested_at IS '자동갱신 중지 또는 해지를 요청한 업무 시각이다.';
COMMENT ON COLUMN customer_subscriptions.ended_at IS '구독 혜택이 실제로 종료된 업무 시각이다.';
COMMENT ON COLUMN customer_subscriptions.status_changed_at IS '현재 구독 상태로 전이한 업무 시각이다.';
COMMENT ON COLUMN customer_subscriptions.created_at IS '원천 구독 계약 레코드 생성 시각이다.';
COMMENT ON COLUMN customer_subscriptions.updated_at IS '원천 구독 계약 레코드의 마지막 변경 및 증분 Cursor 시각이다.';
COMMENT ON TABLE subscription_payments IS
    '구독 계약별 청구 회차와 재시도 순번의 결제 시도 이력을 추가 방식으로 보관한다.';
COMMENT ON COLUMN subscription_payments.payment_id IS '결제 시도의 불변 식별자다.';
COMMENT ON COLUMN subscription_payments.subscription_id IS '결제 시도의 대상 구독 계약 식별자다.';
COMMENT ON COLUMN subscription_payments.billing_cycle_sequence IS '구독 계약 안에서 첫 청구부터 증가하는 청구 회차다.';
COMMENT ON COLUMN subscription_payments.attempt_sequence IS '같은 청구 회차 안에서 첫 시도부터 증가하는 재시도 순번이다.';
COMMENT ON COLUMN subscription_payments.payment_status IS '결제 시도의 최종 결과 상태다.';
COMMENT ON COLUMN subscription_payments.payment_at IS '결제를 시도하고 결과가 확정된 업무 시각이다.';
COMMENT ON COLUMN subscription_payments.payment_value IS '결제 시도에서 청구한 금액이다.';
COMMENT ON COLUMN subscription_payments.currency_code IS '결제 금액의 ISO 4217 통화 코드다.';
COMMENT ON COLUMN subscription_payments.billing_period_start_at IS '결제로 적용하려는 혜택 기간의 시작 시각이다.';
COMMENT ON COLUMN subscription_payments.billing_period_end_at IS '결제로 적용하려는 혜택 기간의 종료 시각이다.';
COMMENT ON COLUMN subscription_payments.payment_method_type IS '결제수단의 분류값이다.';
COMMENT ON COLUMN subscription_payments.payment_provider IS '결제대행사 식별값이다.';
COMMENT ON COLUMN subscription_payments.provider_payment_id IS '결제대행사가 부여한 거래 식별값이다.';
COMMENT ON COLUMN subscription_payments.failure_code IS '실패 결제의 원인 코드다.';
COMMENT ON COLUMN subscription_payments.created_at IS '원천 결제 레코드를 생성한 시각이다.';
COMMENT ON COLUMN subscription_payments.updated_at IS '원천 결제 레코드의 마지막 변경 및 증분 Cursor 시각이다.';
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
