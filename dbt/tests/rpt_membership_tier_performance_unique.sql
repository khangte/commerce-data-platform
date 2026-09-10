-- 등급 성과 지표는 주문 시점 등급·구독 상태마다 한 행이다.
select
    membership_tier,
    subscription_status,
    count(*) as row_count
from {{ ref('rpt_membership_tier_performance') }}
group by membership_tier, subscription_status
having count(*) > 1
