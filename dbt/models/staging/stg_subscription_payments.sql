-- 최신 구독 결제 시도와 대상 구독 계약의 고객 키를 함께 노출한다.
with payments as (
    select *
    from {{ current_bronze_records('subscription_payments', ['payment_id']) }}
),
subscriptions as (
    select *
    from {{ current_bronze_records('customer_subscriptions', ['subscription_id']) }}
)
select
    payments.payment_id,
    payments.subscription_id,
    subscriptions.customer_unique_id as customer_id,
    payments.billing_cycle_sequence,
    payments.attempt_sequence,
    payments.payment_status,
    payments.payment_at,
    payments.payment_value,
    payments.currency_code,
    payments.billing_period_start_at,
    payments.billing_period_end_at,
    payments.payment_method_type,
    payments.payment_provider,
    payments.provider_payment_id,
    payments.failure_code,
    payments.created_at,
    payments.updated_at,
    payments._batch_id,
    payments._run_id,
    payments._ingested_at,
    payments._source_table,
    payments._schema_version
from payments
join subscriptions
    on payments.subscription_id = subscriptions.subscription_id
