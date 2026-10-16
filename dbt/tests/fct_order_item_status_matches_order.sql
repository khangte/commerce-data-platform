-- 주문 항목의 상태는 같은 주문 Fact의 상태와 같아야 한다.
select
    fct_order_item.order_id,
    fct_order_item.order_status as item_order_status,
    fct_order.order_status
from {{ ref('fct_order_item') }} as fct_order_item
inner join {{ ref('fct_order') }} as fct_order using (order_id)
where fct_order_item.order_status is distinct from fct_order.order_status
