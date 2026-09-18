{{ config(unique_key='payment_id', incremental_strategy='delete+insert') }}

-- dbt unique_key는 교체 단위인 결제 시도다. Grain 유일성은 schema.yml의 unique Test가 강제한다.
select
    payment_id,
    subscription_key,
    customer_key,
    payment_date_key,
    subscription_id,
    billing_cycle_sequence,
    attempt_sequence,
    provider_payment_id,
    payment_status,
    payment_method_type,
    payment_provider,
    failure_code,
    currency_code,
    payment_at,
    billing_period_start_at,
    billing_period_end_at,
    payment_value,
    completed_payment_value,
    attempt_count
from {{ ref('int_subscription_payments_enriched') }}
{% if is_incremental() %}
where payment_id in (select payment_id from {{ ref('int_affected_subscription_payment_keys') }})
{% endif %}
