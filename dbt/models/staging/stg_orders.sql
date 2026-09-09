with orders_current as (
    select
        order_id,
        customer_id as source_customer_id,
        {{ standardized_order_status('order_status') }} as order_status,
        order_purchase_timestamp as purchase_at,
        order_approved_at as approved_at,
        order_delivered_carrier_date as carrier_at,
        order_delivered_customer_date as delivered_at,
        order_estimated_delivery_date as estimated_delivery_at,
        created_at,
        updated_at,
        _batch_id,
        _run_id,
        _ingested_at,
        _source_table,
        _schema_version
    from {{ current_bronze_records('orders', ['order_id']) }}
)
select
    orders_current.order_id,
    orders_current.source_customer_id,
    stg_customers_current.customer_id,
    orders_current.order_status,
    orders_current.purchase_at,
    orders_current.approved_at,
    orders_current.carrier_at,
    orders_current.delivered_at,
    orders_current.estimated_delivery_at,
    orders_current.created_at,
    orders_current.updated_at,
    orders_current._batch_id,
    orders_current._run_id,
    orders_current._ingested_at,
    orders_current._source_table,
    orders_current._schema_version
from orders_current
left join {{ ref('stg_customers_current') }} using (source_customer_id)
