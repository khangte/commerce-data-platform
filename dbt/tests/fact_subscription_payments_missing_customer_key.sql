-- 정상 구독 결제는 결제 시점의 고객 SCD2 Version과 반드시 결합되어야 한다.
select
    customer_unique_id,
    billing_sequence,
    billing_period_start
from {{ ref('fact_subscription_payments') }}
where customer_key is null
