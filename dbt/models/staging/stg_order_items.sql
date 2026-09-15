select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date as shipping_limit_at,
    price,
    freight_value,
    created_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from {{ bronze_source('order_items') }}
