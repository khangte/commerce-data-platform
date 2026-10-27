-- 날짜별 고객·주문·배송 완료 금액과 신규 고객의 단일 집계를 주문 Fact와 대조한다.
with orders_with_customer as (
    select fct_order.*, dim_customer.customer_id
    from {{ ref('fct_order') }} as fct_order
    inner join {{ ref('dim_customer') }} as dim_customer
        on fct_order.customer_key = dim_customer.customer_key
), expected as (
    select
        purchase_date_key,
        count(distinct customer_id) as customer_count,
        sum(order_count) as order_count,
        sum(case when order_status = 'DELIVERED' then gross_order_value else 0 end)
            as delivered_gmv
    from orders_with_customer
    group by purchase_date_key
), actual as (
    select
        purchase_date_key,
        sum(customer_count) as customer_count,
        sum(order_count) as order_count,
        sum(delivered_gmv) as delivered_gmv
    from {{ ref('rpt_customer_order_activity_daily') }}
    group by purchase_date_key
), new_customers as (
    select coalesce(sum(customer_count), 0) as customer_count
    from {{ ref('rpt_customer_order_activity_daily') }}
    where customer_kind = 'new'
)
select purchase_date_key
from expected
full outer join actual using (purchase_date_key)
where expected.customer_count is null or actual.customer_count is null
    or expected.order_count is null or actual.order_count is null
    or expected.delivered_gmv is null or actual.delivered_gmv is null
    or expected.customer_count != actual.customer_count
    or expected.order_count != actual.order_count
    or abs(expected.delivered_gmv - actual.delivered_gmv) > 0.01
union all
select null as purchase_date_key
from new_customers
where customer_count != (select count(distinct customer_id) from orders_with_customer)
