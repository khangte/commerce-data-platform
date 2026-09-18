-- 계약별 관측을 상태 변화 Version으로 접어 dim_subscription 입력을 준비한다.
with ordered_observations as (
    select
        *,
        lag(attribute_hash) over (
            partition by subscription_id order by updated_at, _ingested_at, _batch_id
        ) as previous_attribute_hash
    from {{ ref('stg_customer_subscription_observations') }}
),
changed_observations as (
    select
        *,
        row_number() over (
            partition by subscription_id order by updated_at, _ingested_at, _batch_id
        ) as version_rank
    from ordered_observations
    where previous_attribute_hash is null or previous_attribute_hash != attribute_hash
),
versioned as (
    select
        *,
        case when version_rank = 1 then subscription_started_at else updated_at end as valid_from
    from changed_observations
)
select
    subscription_id,
    customer_id,
    subscription_status,
    auto_renew_enabled,
    subscription_started_at,
    current_period_started_at,
    current_period_ends_at,
    payment_failed_at,
    cancel_requested_at,
    ended_at,
    status_changed_at,
    attribute_hash,
    valid_from,
    case when version_rank = 1 then timestamptz '-infinity' else valid_from end as effective_from,
    lead(valid_from) over (partition by subscription_id order by version_rank) as valid_to,
    lead(version_rank) over (partition by subscription_id order by version_rank) is null as is_current
from versioned
