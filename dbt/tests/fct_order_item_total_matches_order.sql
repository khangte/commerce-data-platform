-- 주문 Fact의 gross_order_value는 해당 주문 Line Fact의 line_gross_value 합계와 같아야 한다.
with item_totals as (
    select order_id, sum(line_gross_value) as line_total
    from {{ ref('fct_order_item') }}
    group by order_id
)
select fct_order.order_id, fct_order.gross_order_value, coalesce(item_totals.line_total, 0) as line_total
from {{ ref('fct_order') }} as fct_order
left join item_totals using (order_id)
where abs(fct_order.gross_order_value - coalesce(item_totals.line_total, 0)) > 0.01
