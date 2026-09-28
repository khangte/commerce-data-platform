-- Customer Dashboard: 사건 발생 시점과 현재 시점 Query를 분리한다.
select purchase_date_key, customer_kind, customer_count
from metrics.rpt_customer_order_activity_daily;

select membership_tier, customer_count, order_count, delivered_gmv
from metrics.rpt_membership_tier_performance;

-- 주문 시점 거래 실적 등급별 고객 수 추이. 상세 SQL은 dashboard_membership_tier_trend.sql을 따른다.
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

select customer_state, sum(order_count) as order_count
from facts.fct_order
group by customer_state;

select membership_tier, count(distinct customer_id) as customer_count
from dimensions.dim_customer
where is_current = true
group by membership_tier;

-- 현재 시점 구독 상태별 계약 수 분포. 상세 SQL은 dashboard_subscription_status_distribution.sql을 따른다.
select
    subscription_status,
    count(distinct subscription_id) as contract_count
from dimensions.dim_subscription
where is_current = true
group by subscription_status
order by contract_count desc, subscription_status;
