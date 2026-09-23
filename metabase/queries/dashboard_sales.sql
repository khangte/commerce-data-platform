-- Sales Dashboard 기준 Query: 배송 완료 GMV·전체 Orders·배송 완료 AOV.
select
    purchase_date_key,
    sum(case when order_status = 'DELIVERED' then gross_order_value else 0 end) as daily_gmv,
    sum(order_count) as daily_orders,
    sum(case when order_status = 'DELIVERED' then gross_order_value else 0 end)
        / nullif(sum(case when order_status = 'DELIVERED' then order_count else 0 end), 0) as daily_aov
from facts.fct_order
group by purchase_date_key;
