-- 한 행은 주문 축 Fact가 다시 계산해야 하는 (주문, 구매일) 1건이다.
{% set lower_bound = processed_batch_lower_bound() %}

with tier_axis_changes as (
    -- 이번 경계에서 등급 관측이 바뀐 고객과 그 최소 변경 시각.
    select
        customer_id,
        min(updated_at) as changed_from
    from {{ ref('stg_customer_tier_observations') }}
    where _batch_id > {{ lower_bound }}
    group by customer_id
),
affected_orders as (
    select order_id, purchase_at
    from {{ ref('stg_orders') }}
    where _batch_id > {{ lower_bound }}
),
affected_from_items as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_order_items') }} as order_items
    inner join {{ ref('stg_orders') }} as orders
        on order_items.order_id = orders.order_id
    where order_items._batch_id > {{ lower_bound }}
),
affected_from_payments as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_payments') }} as payments
    inner join {{ ref('stg_orders') }} as orders
        on payments.order_id = orders.order_id
    where payments._batch_id > {{ lower_bound }}
),
affected_from_customers as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_orders') }} as orders
    inner join tier_axis_changes
        on orders.customer_id = tier_axis_changes.customer_id
        and orders.purchase_at >= tier_axis_changes.changed_from
),
affected_from_products as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_products') }} as products
    inner join {{ ref('stg_order_items') }} as order_items
        on products.product_id = order_items.product_id
    inner join {{ ref('stg_orders') }} as orders
        on order_items.order_id = orders.order_id
    where products._batch_id > {{ lower_bound }}
),
affected_from_sellers as (
    select orders.order_id, orders.purchase_at
    from {{ ref('stg_sellers') }} as sellers
    inner join {{ ref('stg_order_items') }} as order_items
        on sellers.seller_id = order_items.seller_id
    inner join {{ ref('stg_orders') }} as orders
        on order_items.order_id = orders.order_id
    where sellers._batch_id > {{ lower_bound }}
),
unioned as (
    select * from affected_orders
    union
    select * from affected_from_items
    union
    select * from affected_from_payments
    union
    select * from affected_from_customers
    union
    select * from affected_from_products
    union
    select * from affected_from_sellers
)
select
    order_id,
    cast(strftime(date_trunc('day', purchase_at), '%Y%m%d') as integer) as business_date_key
from unioned
