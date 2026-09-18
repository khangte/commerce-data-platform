-- 결제 시점에 유효한 계약·고객 Version과 날짜 키를 결합해 Fact 입력을 준비한다.
select
    payments.payment_id,
    subscription.subscription_key,
    customer.customer_key,
    cast(strftime(date_trunc('day', payments.payment_at), '%Y%m%d') as integer) as payment_date_key,
    payments.subscription_id,
    payments.billing_cycle_sequence,
    payments.attempt_sequence,
    payments.provider_payment_id,
    payments.payment_status,
    payments.payment_method_type,
    payments.payment_provider,
    payments.failure_code,
    payments.currency_code,
    payments.payment_at,
    payments.billing_period_start_at,
    payments.billing_period_end_at,
    payments.payment_value,
    case when payments.payment_status = 'completed' then payments.payment_value end as completed_payment_value,
    cast(1 as smallint) as attempt_count
from {{ ref('stg_subscription_payments') }} as payments
inner join {{ ref('dim_subscription') }} as subscription
    on payments.subscription_id = subscription.subscription_id
    and payments.payment_at >= subscription.effective_from
    and payments.payment_at < coalesce(subscription.valid_to, timestamptz 'infinity')
inner join {{ ref('dim_customer') }} as customer
    on payments.customer_id = customer.customer_id
    and payments.payment_at >= customer.effective_from
    and payments.payment_at < coalesce(customer.valid_to, timestamptz 'infinity')
