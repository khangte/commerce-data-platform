-- 구독 결제 1건이 1행이다. 주문 결제인 fact_payments와 별개 Fact다.
-- 결제 시각으로 dim_customer와 Temporal Join하므로 결제 당시의 구독 상태와 등급을 함께 분석한다.

{{
    config(
        unique_key=['customer_unique_id', 'billing_sequence'],
        incremental_strategy='delete+insert'
    )
}}

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
