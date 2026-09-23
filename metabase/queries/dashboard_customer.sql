-- Customer Dashboard: 사건 발생 시점과 현재 시점 Query를 분리한다.
select purchase_date_key, customer_kind, customer_count
from metrics.rpt_customer_order_activity_daily;

select membership_tier, customer_count, order_count, delivered_gmv
from metrics.rpt_membership_tier_performance;

select customer_state, sum(order_count) as order_count
from facts.fct_order
group by customer_state;

select membership_tier, count(distinct customer_id) as customer_count
from dimensions.dim_customer
where is_current = true
group by membership_tier;
