select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    created_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from {{ bronze_source('order_items') }}
