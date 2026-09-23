-- 주문일·신규 또는 재구매 고객 구분별 고객·주문·배송 완료 GMV 집계다.
with orders_with_customer as (
    select
        fct_order.purchase_date_key,
        fct_order.order_status,
        fct_order.order_count,
        fct_order.gross_order_value,
        dim_customer.customer_id,
        min(fct_order.purchase_date_key) over (
            partition by dim_customer.customer_id
        ) as first_purchase_date_key
    from {{ ref('fct_order') }} as fct_order
    inner join {{ ref('dim_customer') }} as dim_customer
        on fct_order.customer_key = dim_customer.customer_key
)
select
    purchase_date_key,
    case
        when purchase_date_key = first_purchase_date_key then 'new'
        else 'repeat'
    end as customer_kind,
    count(distinct customer_id) as customer_count,
    sum(order_count) as order_count,
    sum(
        case when order_status = 'DELIVERED' then gross_order_value else 0 end
    ) as delivered_gmv
from orders_with_customer
group by purchase_date_key, customer_kind
