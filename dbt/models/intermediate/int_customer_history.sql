-- 거래 실적 등급만으로 고객 SCD2 Version을 만든다. 구독 계약 이력은 int_subscription_history가 맡는다.
with ordered_observations as (
    select
        customer_id,
        membership_tier,
        attribute_hash,
        created_at,
        updated_at,
        _ingested_at,
        _batch_id,
        lag(attribute_hash) over (
            partition by customer_id order by updated_at, _ingested_at, _batch_id
        ) as previous_attribute_hash
    from {{ ref('stg_customer_tier_observations') }}
),
changed_observations as (
    select
        *,
        row_number() over (
            partition by customer_id order by updated_at, _ingested_at, _batch_id
        ) as version_rank
    from ordered_observations
    where previous_attribute_hash is null or previous_attribute_hash != attribute_hash
)
select
    customer_id,
    membership_tier,
    attribute_hash,
    case when version_rank = 1 then created_at else updated_at end as valid_from,
    case when version_rank = 1 then timestamptz '-infinity' else updated_at end as effective_from,
    lead(case when version_rank = 1 then created_at else updated_at end) over (
        partition by customer_id order by version_rank
    ) as valid_to,
    lead(version_rank) over (partition by customer_id order by version_rank) is null as is_current
from changed_observations
