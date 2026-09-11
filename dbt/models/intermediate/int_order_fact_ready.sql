-- 주문 Fact가 투영할 주문 1건 Grain의 금액·날짜·배송 측정값을 준비한다.
select
    int_orders_enriched.order_id,
    int_orders_enriched.customer_key,
    cast(strftime(date_trunc('day', int_orders_enriched.purchase_at), '%Y%m%d') as integer)
        as purchase_date_key,
    int_orders_enriched.order_status,
    int_orders_enriched.customer_city,
    int_orders_enriched.customer_state,
    coalesce(int_order_item_totals.item_subtotal, 0)
        + coalesce(int_order_item_totals.freight_total, 0) as gross_order_value,
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
left join {{ ref('int_order_item_totals') }} using (order_id)
left join {{ ref('int_payment_summary') }} using (order_id)
