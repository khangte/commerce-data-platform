{{
    config(
        unique_key=['order_id', 'order_item_id']
    )
}}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price as item_price,
    freight_value,
    line_gross_value
from {{ ref('int_order_items_enriched') }}
