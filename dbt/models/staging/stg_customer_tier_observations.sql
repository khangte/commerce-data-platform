with tier_observations as (
    select
        customer_unique_id as customer_id,
        {{ standardized_membership_tier('membership_tier') }} as membership_tier,
        created_at,
        updated_at,
        _batch_id,
        _run_id,
        _ingested_at,
        _source_table,
        _schema_version
    from {{ bronze_source('customer_membership_tiers') }}
),
with_attribute_hash as (
    select
        *,
        md5(coalesce('membership_tier:' || length(membership_tier) || ':' || membership_tier, 'membership_tier:-1:'))
            as attribute_hash
    from tier_observations
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by customer_id, updated_at, attribute_hash
            order by _ingested_at desc, _batch_id desc
        ) as _observation_rank
    from with_attribute_hash
)
select
    customer_id,
    membership_tier,
    attribute_hash,
    created_at,
    updated_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from deduplicated
where _observation_rank = 1
