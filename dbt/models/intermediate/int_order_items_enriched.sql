select
    stg_order_items.order_id,
    stg_order_items.order_item_id,
    stg_order_items.product_id,
    stg_order_items.seller_id,
    cast(strftime(date_trunc('day', stg_orders.purchase_at), '%Y%m%d') as integer) as purchase_date_key,
    stg_order_items.price,
    stg_order_items.freight_value,
    stg_order_items.price + stg_order_items.freight_value as line_gross_value,
    stg_order_items.created_at
from {{ ref('stg_order_items') }}
left join {{ ref('stg_orders') }} using (order_id)
left join {{ ref('dim_product') }} using (product_id)
left join {{ ref('dim_seller') }} using (seller_id)
