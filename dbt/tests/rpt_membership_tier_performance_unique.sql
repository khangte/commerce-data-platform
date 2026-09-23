-- 등급 성과 지표는 주문 시점 거래 실적 등급마다 한 행이다.
select
    membership_tier,
    count(*) as row_count
from {{ ref('rpt_membership_tier_performance') }}
group by membership_tier
having count(*) > 1
