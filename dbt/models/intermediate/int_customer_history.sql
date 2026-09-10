-- 두 Source 축(구독 상태, 거래 실적 등급)을 하나의 시간축으로 병합해 SCD2 Version을 만든다.
-- 어느 한 축만 새 관측이 있으면 다른 축은 직전 값을 이어받는다. 이 단계가 두 Source의
-- 독립 Watermark로 생기는 Late Arrival 부분 결측을 흡수한다.

with subscription_obs as (
    select
        customer_id,
        subscription_status,
        trial_ends_at,
        benefit_ends_at,
        payment_failed_at,
        cancel_requested_at,
        created_at,
        updated_at
    from {{ ref('stg_customer_subscription_observations') }}
),
tier_obs as (
    select
        customer_id,
        membership_tier,
        created_at,
        updated_at
    from {{ ref('stg_customer_tier_observations') }}
),

-- 사람별 최초 등장 시각. 최초 Version의 valid_from Baseline이다.
customer_created as (
    select customer_id, min(created_at) as created_at
    from (
        select customer_id, created_at from subscription_obs
        union all
        select customer_id, created_at from tier_obs
    )
    group by customer_id
),

-- 두 축의 모든 관측 시각을 한 시간축으로 모은다.
timeline as (
    select customer_id, updated_at as observed_at from subscription_obs
    union
    select customer_id, updated_at as observed_at from tier_obs
),

-- 각 시점에서 두 축의 그 시점 유효 값을 as-of로 채운다.
merged as (
    select
        timeline.customer_id,
        timeline.observed_at,
        (
            select s.subscription_status
            from subscription_obs as s
            where s.customer_id = timeline.customer_id and s.updated_at <= timeline.observed_at
            order by s.updated_at desc
            limit 1
        ) as subscription_status,
        (
            select s.trial_ends_at
            from subscription_obs as s
            where s.customer_id = timeline.customer_id and s.updated_at <= timeline.observed_at
            order by s.updated_at desc
            limit 1
        ) as trial_ends_at,
        (
            select s.benefit_ends_at
            from subscription_obs as s
            where s.customer_id = timeline.customer_id and s.updated_at <= timeline.observed_at
            order by s.updated_at desc
            limit 1
        ) as benefit_ends_at,
        (
            select s.payment_failed_at
            from subscription_obs as s
            where s.customer_id = timeline.customer_id and s.updated_at <= timeline.observed_at
            order by s.updated_at desc
            limit 1
        ) as payment_failed_at,
        (
            select s.cancel_requested_at
            from subscription_obs as s
            where s.customer_id = timeline.customer_id and s.updated_at <= timeline.observed_at
            order by s.updated_at desc
            limit 1
        ) as cancel_requested_at,
        (
            select t.membership_tier
            from tier_obs as t
            where t.customer_id = timeline.customer_id and t.updated_at <= timeline.observed_at
            order by t.updated_at desc
            limit 1
        ) as membership_tier
    from timeline
),

-- next_billing_at은 SCD2 Hash에서 제외한다. 1개월 고정 주기라 상태 변화 없이 매달
-- 갱신되므로 Version 폭증을 만든다.
with_attribute_hash as (
    select
        *,
        md5(
            coalesce('status:' || subscription_status, 'status:')
            || '|' || coalesce('tier:' || membership_tier, 'tier:')
            || '|' || coalesce('trial:' || cast(trial_ends_at as varchar), 'trial:')
            || '|' || coalesce('benefit:' || cast(benefit_ends_at as varchar), 'benefit:')
            || '|' || coalesce('failed:' || cast(payment_failed_at as varchar), 'failed:')
            || '|' || coalesce('cancel:' || cast(cancel_requested_at as varchar), 'cancel:')
        ) as attribute_hash
    from merged
),
ordered_observations as (
    select
        *,
        lag(attribute_hash) over (
            partition by customer_id order by observed_at
        ) as _previous_attribute_hash
    from with_attribute_hash
),
changed_observations as (
    select
        *,
        row_number() over (partition by customer_id order by observed_at) as _version_rank
    from ordered_observations
    where _previous_attribute_hash is null or _previous_attribute_hash != attribute_hash
),
versioned as (
    select
        changed_observations.customer_id,
        changed_observations.subscription_status,
        changed_observations.membership_tier,
        changed_observations.trial_ends_at,
        changed_observations.benefit_ends_at,
        changed_observations.payment_failed_at,
        changed_observations.cancel_requested_at,
        changed_observations.attribute_hash,
        case
            when changed_observations._version_rank = 1 then customer_created.created_at
            else changed_observations.observed_at
        end as valid_from,
        changed_observations._version_rank,
        -- 재가입: 직전 Version이 CHURNED이고 현재가 TRIAL 또는 ACTIVE인 지점.
        case
            when lag(changed_observations.subscription_status) over (
                partition by changed_observations.customer_id
                order by changed_observations._version_rank
            ) = 'CHURNED'
            and changed_observations.subscription_status in ('TRIAL', 'ACTIVE')
            then 1
            else 0
        end as _is_rejoin_event
    from changed_observations
    join customer_created using (customer_id)
),
with_rejoin as (
    select
        *,
        sum(_is_rejoin_event) over (
            partition by customer_id order by _version_rank
        ) as rejoin_count,
        max(case when _is_rejoin_event = 1 then valid_from end) over (
            partition by customer_id order by _version_rank
            rows between unbounded preceding and current row
        ) as rejoined_at
    from versioned
)
select
    customer_id,
    subscription_status,
    membership_tier,
    trial_ends_at,
    benefit_ends_at,
    payment_failed_at,
    cancel_requested_at,
    attribute_hash,
    valid_from,
    lead(valid_from) over (partition by customer_id order by _version_rank) as valid_to,
    lead(_version_rank) over (partition by customer_id order by _version_rank) is null as is_current,
    rejoin_count,
    rejoined_at
from with_rejoin
