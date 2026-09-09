select
    stg_orders.order_id,
    stg_orders.source_customer_id,
    stg_orders.customer_id,
    dim_customer.customer_key,
    stg_orders.order_status,
    stg_orders.purchase_at,
    stg_orders.approved_at,
    stg_orders.carrier_at,
    stg_orders.delivered_at,
    stg_orders.estimated_delivery_at,
    stg_orders.created_at,
    stg_orders.updated_at,
    stg_orders._batch_id,
    stg_orders._ingested_at
from {{ ref('stg_orders') }}
left join {{ ref('dim_customer') }}
    on stg_orders.customer_id = dim_customer.customer_id
    and stg_orders.purchase_at >= dim_customer.valid_from
    and stg_orders.purchase_at < coalesce(dim_customer.valid_to, timestamptz 'infinity')
