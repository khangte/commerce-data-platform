-- 주문 시점 구독 상태·거래 실적 등급별 주문 성과다. 현재 고객 분포와 혼용하지 않는다.
select
    dim_customer.membership_tier,
    count(distinct dim_customer.customer_id) as customer_count,
    count(*) as order_count,
    sum(fct_order.gross_order_value) as gross_order_value,
    sum(case when fct_order.order_status = 'DELIVERED' then 1 else 0 end) as delivered_order_count,
    sum(
        case when fct_order.order_status = 'DELIVERED' then fct_order.gross_order_value else 0 end
    ) as delivered_gmv,
    sum(
        case when fct_order.order_status = 'DELIVERED' then fct_order.gross_order_value else 0 end
    ) / nullif(
        sum(case when fct_order.order_status = 'DELIVERED' then 1 else 0 end),
        0
    ) as delivered_aov
from {{ ref('fct_order') }} as fct_order
inner join {{ ref('dim_customer') }} as dim_customer
    on fct_order.customer_key = dim_customer.customer_key
group by dim_customer.membership_tier
