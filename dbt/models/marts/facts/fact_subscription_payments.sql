-- 구독 결제 1건이 1행이다. 주문 결제인 fact_payments와 별개 Fact다.
-- 결제 시각으로 dim_customer와 Temporal Join하므로 결제 당시의 구독 상태와 등급을 함께 분석한다.

{{
    config(
        unique_key=['customer_unique_id', 'billing_sequence'],
        incremental_strategy='delete+insert'
    )
}}

select
    customer_key,
    customer_unique_id,
    billing_sequence,
    payment_status,
    payment_value,
    billing_period_start,
    billing_period_end
from {{ ref('int_subscription_payments_enriched') }}
