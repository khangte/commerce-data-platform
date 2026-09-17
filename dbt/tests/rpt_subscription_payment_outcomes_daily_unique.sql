-- 구독 결제 결과 지표는 결제일·결제 상태마다 한 행이다.
select
    payment_date_key,
    payment_status,
    count(*) as row_count
from {{ ref('rpt_subscription_payment_outcomes_daily') }}
group by payment_date_key, payment_status
having count(*) > 1
