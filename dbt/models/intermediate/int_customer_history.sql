with observations as (
    select
        customer_id,
        membership_level,
        city,
        state,
        attribute_hash,
        created_at,
        updated_at,
        row_number() over (
            partition by customer_id
            order by updated_at, _ingested_at, _batch_id
        ) as _version_rank
    from {{ ref('stg_customer_observations') }}
),
versioned as (
    select
        customer_id,
        membership_level,
        city,
        state,
        attribute_hash,
        case when _version_rank = 1 then created_at else updated_at end as valid_from,
        _version_rank
    from observations
)
select
    customer_id,
    membership_level,
    city,
    state,
    attribute_hash,
    valid_from,
    lead(valid_from) over (partition by customer_id order by _version_rank) as valid_to,
    lead(_version_rank) over (partition by customer_id order by _version_rank) is null as is_current
from versioned
