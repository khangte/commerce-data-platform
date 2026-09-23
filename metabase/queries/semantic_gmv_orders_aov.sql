-- Metabase 지표 정의 검증용: 2017-01 UTC 고정 기간의 GMV, Orders, AOV.
select
    sum(case when order_status = 'DELIVERED' then gross_order_value end) as gmv,
    sum(order_count) as orders,
    sum(case when order_status = 'DELIVERED' then gross_order_value end)
        / nullif(sum(case when order_status = 'DELIVERED' then order_count end), 0) as aov
from facts.fct_order
where purchase_date_key between 20170101 and 20170131;
