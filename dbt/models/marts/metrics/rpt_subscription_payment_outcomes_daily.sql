-- 청구 기간 시작일 기준 일별 구독 결제 성공·실패 건수와 금액이다.
select
    cast(strftime(date_trunc('day', billing_period_start), '%Y%m%d') as integer) as billing_date_key,
    payment_status,
    count(*) as payment_count,
    count(distinct customer_unique_id) as paying_customer_count,
    sum(payment_value) as payment_value_total
from {{ ref('fact_subscription_payments') }}
group by billing_date_key, payment_status
