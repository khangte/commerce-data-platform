with customer_versions as (
    select
        customer_id as source_customer_id,
        customer_unique_id as customer_id,
        customer_city as city,
        customer_state as state,
        {{ standardized_membership_level('membership_level') }} as membership_level,
        created_at,
        updated_at,
        _batch_id,
        _run_id,
        _ingested_at,
        _source_table,
        _schema_version
    from {{ current_bronze_records('customers', ['customer_id']) }}
),
with_attribute_hash as (
    select
        *,
        md5(
            concat(
                coalesce('membership_level:' || length(membership_level) || ':' || membership_level, 'membership_level:-1:'),
                '|',
                coalesce('city:' || length(city) || ':' || city, 'city:-1:'),
                '|',
                coalesce('state:' || length(state) || ':' || state, 'state:-1:')
            )
        ) as attribute_hash
    from customer_versions
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by customer_id, updated_at, attribute_hash
            order by _ingested_at desc, _batch_id desc, source_customer_id desc
        ) as _observation_rank
    from with_attribute_hash
)
select
    source_customer_id,
    customer_id,
    membership_level,
    city,
    state,
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
