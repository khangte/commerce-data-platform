-- 고객마다 현재 SCD2 Version은 정확히 한 행이어야 한다.
select
    customer_id,
    sum(case when is_current then 1 else 0 end) as current_version_count
from {{ ref('dim_customer') }}
group by customer_id
having sum(case when is_current then 1 else 0 end) != 1
