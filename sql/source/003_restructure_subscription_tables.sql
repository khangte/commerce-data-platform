-- 구독 계약·결제 원천 구조를 자동갱신과 재시도 분석이 가능한 형태로 전환한다.
-- 기존 NON_MEMBER 행은 구독 계약이 없다는 뜻이므로 이전 대상에서 제외한다.

DO $$
BEGIN
    IF to_regclass('public.customer_subscriptions') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema = 'public'
             AND table_name = 'customer_subscriptions'
             AND column_name = 'subscription_id'
       ) THEN
        ALTER TABLE subscription_payments RENAME TO subscription_payments_legacy;
        ALTER TABLE customer_subscriptions RENAME TO customer_subscriptions_legacy;
    END IF;
END $$;

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
        subscription_status IN ('ACTIVE', 'PAYMENT_FAILED', 'CANCEL_REQUESTED', 'CHURNED')
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
        subscription_status <> 'PAYMENT_FAILED' OR payment_failed_at IS NOT NULL
    ),
    CONSTRAINT customer_subscriptions_cancel_requested_check CHECK (
        subscription_status <> 'CANCEL_REQUESTED'
        OR (NOT auto_renew_enabled AND cancel_requested_at IS NOT NULL AND next_payment_attempt_at IS NULL)
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
    CONSTRAINT subscription_payments_status_check CHECK (payment_status IN ('completed', 'failed')),
    CONSTRAINT subscription_payments_value_check CHECK (payment_value >= 0),
    CONSTRAINT subscription_payments_period_check
        CHECK (billing_period_end_at > billing_period_start_at),
    CONSTRAINT subscription_payments_failure_code_check
        CHECK (payment_status <> 'completed' OR failure_code IS NULL),
    CONSTRAINT subscription_payments_updated_at_check CHECK (updated_at >= created_at)
);

DO $$
BEGIN
    IF to_regclass('public.customer_subscriptions_legacy') IS NOT NULL THEN
        WITH migrated_contracts AS (
            SELECT
                (
                    substr(md5('subscription:' || customer_unique_id), 1, 8) || '-'
                    || substr(md5('subscription:' || customer_unique_id), 9, 4) || '-'
                    || substr(md5('subscription:' || customer_unique_id), 13, 4) || '-'
                    || substr(md5('subscription:' || customer_unique_id), 17, 4) || '-'
                    || substr(md5('subscription:' || customer_unique_id), 21, 12)
                )::uuid AS subscription_id,
                customer_unique_id,
                CASE WHEN subscription_status = 'TRIAL' THEN 'ACTIVE' ELSE subscription_status END
                    AS subscription_status,
                subscription_status NOT IN ('CANCEL_REQUESTED', 'CHURNED') AS auto_renew_enabled,
                created_at AS subscription_started_at,
                coalesce(benefit_ends_at - interval '30 days', created_at) AS current_period_started_at,
                benefit_ends_at AS current_period_ends_at,
                CASE
                    WHEN subscription_status IN ('ACTIVE', 'TRIAL') THEN next_billing_at
                    ELSE NULL
                END AS billing_due_at,
                CASE
                    WHEN subscription_status IN ('ACTIVE', 'TRIAL') THEN next_billing_at
                    ELSE NULL
                END AS next_payment_attempt_at,
                payment_failed_at,
                cancel_requested_at,
                CASE WHEN subscription_status = 'CHURNED' THEN updated_at END AS ended_at,
                updated_at AS status_changed_at,
                created_at,
                updated_at
            FROM customer_subscriptions_legacy
            WHERE subscription_status <> 'NON_MEMBER'
        )
        INSERT INTO customer_subscriptions (
            subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
            subscription_started_at, current_period_started_at, current_period_ends_at,
            billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
            ended_at, status_changed_at, created_at, updated_at
        )
        SELECT
            subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
            subscription_started_at, current_period_started_at, current_period_ends_at,
            billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
            ended_at, status_changed_at, created_at, updated_at
        FROM migrated_contracts;

        INSERT INTO subscription_payments (
            payment_id, subscription_id, billing_cycle_sequence, attempt_sequence, payment_status,
            payment_at, payment_value, currency_code, billing_period_start_at, billing_period_end_at,
            payment_method_type, payment_provider, provider_payment_id, failure_code, created_at, updated_at
        )
        SELECT
            (
                substr(md5('payment:' || legacy.customer_unique_id || ':' || legacy.billing_sequence), 1, 8) || '-'
                || substr(md5('payment:' || legacy.customer_unique_id || ':' || legacy.billing_sequence), 9, 4) || '-'
                || substr(md5('payment:' || legacy.customer_unique_id || ':' || legacy.billing_sequence), 13, 4) || '-'
                || substr(md5('payment:' || legacy.customer_unique_id || ':' || legacy.billing_sequence), 17, 4) || '-'
                || substr(md5('payment:' || legacy.customer_unique_id || ':' || legacy.billing_sequence), 21, 12)
            )::uuid,
            contract.subscription_id,
            legacy.billing_sequence,
            1,
            legacy.payment_status,
            legacy.created_at,
            legacy.payment_value,
            'BRL',
            legacy.billing_period_start,
            legacy.billing_period_end,
            NULL,
            NULL,
            NULL,
            CASE WHEN legacy.payment_status = 'failed' THEN 'LEGACY_FAILURE' END,
            legacy.created_at,
            legacy.updated_at
        FROM subscription_payments_legacy AS legacy
        JOIN customer_subscriptions AS contract
            ON contract.customer_unique_id = legacy.customer_unique_id;

        DROP TABLE subscription_payments_legacy;
        DROP TABLE customer_subscriptions_legacy;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS customer_subscriptions_updated_at_subscription_id_idx
    ON customer_subscriptions (updated_at, subscription_id);
CREATE UNIQUE INDEX IF NOT EXISTS customer_subscriptions_one_open_contract_idx
    ON customer_subscriptions (customer_unique_id COLLATE "C")
    WHERE subscription_status IN ('ACTIVE', 'PAYMENT_FAILED', 'CANCEL_REQUESTED');
CREATE INDEX IF NOT EXISTS subscription_payments_updated_at_payment_id_idx
    ON subscription_payments (updated_at, payment_id);
CREATE UNIQUE INDEX IF NOT EXISTS subscription_payments_provider_payment_unique_idx
    ON subscription_payments (payment_provider, provider_payment_id)
    WHERE payment_provider IS NOT NULL AND provider_payment_id IS NOT NULL;
