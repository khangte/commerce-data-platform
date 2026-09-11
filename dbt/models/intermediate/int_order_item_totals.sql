-- 주문 Line을 주문 1건 Grain으로 사전 집계한다.
select
    order_id,
    sum(price) as item_subtotal,
    sum(freight_value) as freight_total
from {{ ref('int_order_items_enriched') }}
group by order_id
