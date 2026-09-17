-- 청구 기간 시작일 기준 일별 구독 결제 성공·실패 건수와 금액이다.
select
    payment_date_key,
    payment_status,
    count(*) as payment_count,
    count(distinct customer_key) as paying_customer_count,
    sum(completed_payment_value) as completed_payment_value_total
from {{ ref('fact_subscription_payments') }}
group by payment_date_key, payment_status
