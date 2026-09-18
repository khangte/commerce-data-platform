-- 고객마다 최초 SCD2 Version(가장 이른 valid_from)의 effective_from은 -infinity여야 한다.
with first_versions as (
    select
        customer_id,
        effective_from,
        row_number() over (partition by customer_id order by valid_from) as version_rank
    from {{ ref('dim_customer') }}
)
select customer_id, effective_from
from first_versions
where version_rank = 1
and effective_from != timestamptz '-infinity'
