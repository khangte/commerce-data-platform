select
    stg_order_items.order_id,
    stg_order_items.order_item_id,
    stg_order_items.product_id,
    stg_order_items.seller_id,
    stg_order_items.price,
    stg_order_items.freight_value,
    stg_order_items.price + stg_order_items.freight_value as line_gross_value,
    stg_order_items.created_at
from {{ ref('stg_order_items') }}
left join {{ ref('dim_product') }} using (product_id)
left join {{ ref('dim_seller') }} using (seller_id)
