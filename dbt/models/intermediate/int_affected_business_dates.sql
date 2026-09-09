with latest_batch as (
    select max(_batch_id) as _batch_id
    from {{ ref('stg_orders') }}
),
affected_orders as (
    select order_id, purchase_at
    from {{ ref('stg_orders') }}
    where _batch_id = (select _batch_id from latest_batch)
),
affected_from_items as (
    select stg_orders.order_id, stg_orders.purchase_at
    from {{ ref('stg_order_items') }}
    inner join {{ ref('stg_orders') }} using (order_id)
    where stg_order_items._batch_id = (select _batch_id from latest_batch)
),
affected_from_payments as (
    select stg_orders.order_id, stg_orders.purchase_at
    from {{ ref('stg_payments') }}
    inner join {{ ref('stg_orders') }} using (order_id)
    where stg_payments._batch_id = (select _batch_id from latest_batch)
),
affected_from_customers as (
    select stg_orders.order_id, stg_orders.purchase_at
    from {{ ref('int_customer_history') }}
    inner join {{ ref('stg_orders') }}
        on int_customer_history.customer_id = stg_orders.customer_id
        and stg_orders.purchase_at >= int_customer_history.valid_from
        and stg_orders.purchase_at < coalesce(int_customer_history.valid_to, timestamptz 'infinity')
    where int_customer_history.valid_from >= (
        select min(created_at) from {{ ref('stg_customer_observations') }}
        where _batch_id = (select _batch_id from latest_batch)
    )
),
affected_from_products as (
    select stg_orders.order_id, stg_orders.purchase_at
    from {{ ref('stg_products') }}
    inner join {{ ref('stg_order_items') }} using (product_id)
    inner join {{ ref('stg_orders') }} using (order_id)
    where stg_products._batch_id = (select _batch_id from latest_batch)
),
affected_from_sellers as (
    select stg_orders.order_id, stg_orders.purchase_at
    from {{ ref('stg_sellers') }}
    inner join {{ ref('stg_order_items') }} using (seller_id)
    inner join {{ ref('stg_orders') }} using (order_id)
    where stg_sellers._batch_id = (select _batch_id from latest_batch)
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
