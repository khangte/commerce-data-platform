-- 결제일·상태별 구독 결제 건수·고객 수·완료 금액을 결제 Fact와 대조한다.
with expected as (
    select
        payment_date_key,
        payment_status,
        count(*) as payment_count,
        count(distinct customer_key) as paying_customer_count,
        sum(completed_payment_value) as completed_payment_value_total
    from {{ ref('fct_subscription_payment') }}
    group by payment_date_key, payment_status
)
select payment_date_key, payment_status
from expected
full outer join {{ ref('rpt_subscription_payment_outcomes_daily') }} as actual
    using (payment_date_key, payment_status)
where expected.payment_count is null or actual.payment_count is null
    or expected.paying_customer_count is null or actual.paying_customer_count is null
    or expected.payment_count != actual.payment_count
    or expected.paying_customer_count != actual.paying_customer_count
    or (expected.completed_payment_value_total is null)
        <> (actual.completed_payment_value_total is null)
    or abs(expected.completed_payment_value_total - actual.completed_payment_value_total) > 0.01
