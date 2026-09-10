-- 축별 Bronze 관측에서 같은 사람·같은 원천 변경 시각에 서로 다른 속성 Hash가 생기면 안 된다.
-- int_customer_history는 시간축을 한 행으로 병합하므로, 이 단계에서 검사해야 동률 시각의
-- 비결정적인 as-of 선택을 막을 수 있다.
with conflicting_subscription_observations as (
    select
        customer_id,
        updated_at as observed_at,
        count(distinct attribute_hash) as distinct_hash_count
    from {{ ref('stg_customer_subscription_observations') }}
    group by customer_id, updated_at
    having count(distinct attribute_hash) > 1
),
conflicting_tier_observations as (
    select
        customer_id,
        updated_at as observed_at,
        count(distinct attribute_hash) as distinct_hash_count
    from {{ ref('stg_customer_tier_observations') }}
    group by customer_id, updated_at
    having count(distinct attribute_hash) > 1
)
select 'subscription' as observation_axis, *
from conflicting_subscription_observations
union all
select 'tier' as observation_axis, *
from conflicting_tier_observations
