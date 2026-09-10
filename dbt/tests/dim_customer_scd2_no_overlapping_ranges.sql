-- 같은 고객의 SCD2 유효 구간은 서로 겹치면 안 된다.
with ordered_versions as (
    select
        customer_id,
        customer_key,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id
            order by valid_from, customer_key
        ) as next_valid_from
    from {{ ref('dim_customer') }}
)
select
    customer_id,
    customer_key,
    valid_from,
    valid_to,
    next_valid_from
from ordered_versions
where valid_to > next_valid_from
