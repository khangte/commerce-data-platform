select customer_id, updated_at, count(distinct attribute_hash) as distinct_hash_count
from {{ ref('stg_customer_observations') }}
group by customer_id, updated_at
having count(distinct attribute_hash) > 1
