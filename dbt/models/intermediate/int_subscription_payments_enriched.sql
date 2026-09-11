-- 구독 결제를 결제 시점의 고객 SCD2 Version과 결합해 Fact 입력을 준비한다.
select
    dim_customer.customer_key,
    stg_subscription_payments.customer_id as customer_unique_id,
    stg_subscription_payments.billing_sequence,
    stg_subscription_payments.payment_status,
    stg_subscription_payments.payment_value,
    stg_subscription_payments.billing_period_start,
    stg_subscription_payments.billing_period_end
from {{ ref('stg_subscription_payments') }} as stg_subscription_payments
left join {{ ref('dim_customer') }} as dim_customer
    on stg_subscription_payments.customer_id = dim_customer.customer_id
    and stg_subscription_payments.billing_period_start >= dim_customer.valid_from
    and stg_subscription_payments.billing_period_start
        < coalesce(dim_customer.valid_to, timestamptz 'infinity')
