select
    customer_id,
    updated_at,
    attribute_hash,
    count(*) as row_count
from {{ ref('stg_customer_observations') }}
group by customer_id, updated_at, attribute_hash
having count(*) > 1
