with ordered_versions as (
    select
        subscription_id,
        subscription_key,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by subscription_id order by valid_from, subscription_key
        ) as next_valid_from
    from {{ ref('dim_subscription') }}
)
select *
from ordered_versions
where valid_to > next_valid_from
