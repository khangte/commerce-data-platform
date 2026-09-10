-- 구독 결제 결과 지표는 청구 시작일·결제 상태마다 한 행이다.
select
    billing_date_key,
    payment_status,
    count(*) as row_count
from {{ ref('rpt_subscription_payment_outcomes_daily') }}
group by billing_date_key, payment_status
having count(*) > 1
