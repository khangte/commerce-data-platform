with membership_observations as (
    select
        customer_unique_id as customer_id,
        {{ standardized_membership_level('membership_level') }} as membership_level,
        created_at,
        updated_at,
        _batch_id,
        _run_id,
        _ingested_at,
        _source_table,
        _schema_version
    from {{ bronze_source('customer_memberships') }}
),
with_attribute_hash as (
    select
        *,
        md5(coalesce('membership_level:' || length(membership_level) || ':' || membership_level, 'membership_level:-1:'))
            as attribute_hash
    from membership_observations
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
    membership_level,
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
