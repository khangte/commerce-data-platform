-- 주문 항목의 구매일은 같은 주문 Fact의 구매일과 같아야 한다.
select
    fct_order_item.order_id,
    fct_order_item.purchase_date_key as item_purchase_date_key,
    fct_order.purchase_date_key as order_purchase_date_key
from {{ ref('fct_order_item') }} as fct_order_item
inner join {{ ref('fct_order') }} as fct_order using (order_id)
where fct_order_item.purchase_date_key != fct_order.purchase_date_key
