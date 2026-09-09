select
    seller_id,
    city,
    state
from {{ ref('stg_sellers') }}
