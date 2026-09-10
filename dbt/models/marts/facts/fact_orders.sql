{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with item_totals as (
    select
        order_id,
        sum(item_price) as item_subtotal,
        sum(freight_value) as freight_total
    from {{ ref('fact_order_items') }}
    group by order_id
)

select
    int_orders_enriched.order_id,
    int_orders_enriched.customer_key,
    cast(strftime(date_trunc('day', int_orders_enriched.purchase_at), '%Y%m%d') as integer)
        as purchase_date_key,
    int_orders_enriched.order_status,
    int_orders_enriched.customer_city,
    int_orders_enriched.customer_state,
    coalesce(item_totals.item_subtotal, 0) + coalesce(item_totals.freight_total, 0)
        as gross_order_value,
    coalesce(int_payment_summary.payment_total, 0) as payment_total,
    1 as order_count,
    date_diff('day', int_orders_enriched.purchase_at, int_orders_enriched.carrier_at)
        as carrier_handoff_days,
    date_diff('day', int_orders_enriched.purchase_at, int_orders_enriched.delivered_at)
        as delivery_days,
    date_diff('day', int_orders_enriched.estimated_delivery_at, int_orders_enriched.delivered_at)
        as delivery_delay_days,
    int_orders_enriched.delivered_at > int_orders_enriched.estimated_delivery_at as is_late
from {{ ref('int_orders_enriched') }}
left join item_totals on int_orders_enriched.order_id = item_totals.order_id
left join {{ ref('int_payment_summary') }} on int_orders_enriched.order_id = int_payment_summary.order_id
