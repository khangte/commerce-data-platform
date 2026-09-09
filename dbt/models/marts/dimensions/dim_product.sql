select
    product_id,
    category_name,
    weight_g,
    length_cm,
    height_cm,
    width_cm
from {{ ref('stg_products') }}
