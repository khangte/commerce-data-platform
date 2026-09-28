-- 주문 시점 등급별 고객 수 추이: rpt_membership_tier_performance와 같은 Fact·SCD2 결합을 날짜 축으로 펼친다.
select
    dim_date.calendar_date,
    dim_customer.membership_tier,
    count(distinct dim_customer.customer_id) as customer_count
from facts.fct_order
inner join dimensions.dim_customer
    on fct_order.customer_key = dim_customer.customer_key
inner join dimensions.dim_date
    on fct_order.purchase_date_key = dim_date.date_key
group by dim_date.calendar_date, dim_customer.membership_tier
order by dim_date.calendar_date, dim_customer.membership_tier;
