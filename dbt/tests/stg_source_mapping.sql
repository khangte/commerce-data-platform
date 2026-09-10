with products_source as (
    select *
    from {{ current_bronze_records('products', ['product_id']) }}
),
sellers_source as (
    select *
    from {{ current_bronze_records('sellers', ['seller_id']) }}
),
payments_source as (
    select *
    from {{ current_bronze_records('order_payments', ['order_id', 'payment_sequential']) }}
),
orders_source as (
    select *
    from {{ current_bronze_records('orders', ['order_id']) }}
),
customers_source as (
    select *
    from {{ current_bronze_records('customers', ['customer_id'], 'created_at') }}
),
subscriptions_source as (
    select *
    from {{ current_bronze_records('customer_subscriptions', ['customer_unique_id']) }}
),
tiers_source as (
    select *
    from {{ current_bronze_records('customer_membership_tiers', ['customer_unique_id']) }}
),
mapping_failures as (
    select 'products' as source_table, products_source.product_id as business_key
    from products_source
    left join {{ ref('stg_products') }} using (product_id)
    where
        stg_products.category_name is distinct from products_source.product_category_name
        or stg_products.weight_g is distinct from products_source.product_weight_g
        or stg_products.length_cm is distinct from products_source.product_length_cm
        or stg_products.height_cm is distinct from products_source.product_height_cm
        or stg_products.width_cm is distinct from products_source.product_width_cm

    union all

    select 'sellers' as source_table, sellers_source.seller_id as business_key
    from sellers_source
    left join {{ ref('stg_sellers') }} using (seller_id)
    where
        stg_sellers.city is distinct from sellers_source.seller_city
        or stg_sellers.state is distinct from sellers_source.seller_state

    union all

    select
        'order_payments' as source_table,
        payments_source.order_id || ':' || payments_source.payment_sequential as business_key
    from payments_source
    left join {{ ref('stg_payments') }}
        on payments_source.order_id = stg_payments.order_id
        and payments_source.payment_sequential = stg_payments.payment_sequence
    where
        stg_payments.payment_sequence is null
        or stg_payments.installments is distinct from payments_source.payment_installments
        or stg_payments.payment_status is distinct from {{ standardized_payment_status('payments_source.payment_status') }}

    union all

    select 'order_items' as source_table, order_items.order_id || ':' || order_items.order_item_id as business_key
    from {{ bronze_source('order_items') }} as order_items
    left join {{ ref('stg_order_items') }}
        on order_items.order_id = stg_order_items.order_id
        and order_items.order_item_id = stg_order_items.order_item_id
    where
        stg_order_items.order_item_id is null
        or stg_order_items.product_id is distinct from order_items.product_id
        or stg_order_items.seller_id is distinct from order_items.seller_id
        or stg_order_items.price is distinct from order_items.price
        or stg_order_items.freight_value is distinct from order_items.freight_value

    union all

    select 'customers' as source_table, customers_source.customer_id as business_key
    from customers_source
    left join {{ ref('stg_customers_current') }}
        on customers_source.customer_id = stg_customers_current.source_customer_id
    where
        stg_customers_current.source_customer_id is null
        or stg_customers_current.customer_id is distinct from customers_source.customer_unique_id
        or stg_customers_current.city is distinct from customers_source.customer_city
        or stg_customers_current.state is distinct from customers_source.customer_state
        or stg_customers_current.created_at is distinct from customers_source.created_at

    union all

    select
        'customer_subscriptions' as source_table,
        subscriptions_source.customer_unique_id as business_key
    from subscriptions_source
    left join {{ ref('stg_customer_subscription_observations') }} as stg_subscription_obs
        on subscriptions_source.customer_unique_id = stg_subscription_obs.customer_id
        and subscriptions_source.updated_at = stg_subscription_obs.updated_at
    where
        stg_subscription_obs.customer_id is null
        or stg_subscription_obs.subscription_status is distinct from
            {{ standardized_subscription_status('subscriptions_source.subscription_status') }}
        or stg_subscription_obs.trial_ends_at is distinct from subscriptions_source.trial_ends_at
        or stg_subscription_obs.benefit_ends_at is distinct from subscriptions_source.benefit_ends_at
        or stg_subscription_obs.payment_failed_at
            is distinct from subscriptions_source.payment_failed_at
        or stg_subscription_obs.cancel_requested_at
            is distinct from subscriptions_source.cancel_requested_at
        or stg_subscription_obs.created_at is distinct from subscriptions_source.created_at

    union all

    select
        'customer_membership_tiers' as source_table,
        tiers_source.customer_unique_id as business_key
    from tiers_source
    left join {{ ref('stg_customer_tier_observations') }} as stg_tier_obs
        on tiers_source.customer_unique_id = stg_tier_obs.customer_id
        and tiers_source.updated_at = stg_tier_obs.updated_at
    where
        stg_tier_obs.customer_id is null
        or stg_tier_obs.membership_tier is distinct from
            {{ standardized_membership_tier('tiers_source.membership_tier') }}
        or stg_tier_obs.created_at is distinct from tiers_source.created_at

    union all

    select 'orders' as source_table, orders_source.order_id as business_key
    from orders_source
    left join {{ ref('stg_orders') }} using (order_id)
    left join {{ ref('stg_customers_current') }}
        on orders_source.customer_id = stg_customers_current.source_customer_id
    where
        stg_orders.source_customer_id is distinct from orders_source.customer_id
        or stg_orders.customer_id is distinct from stg_customers_current.customer_id
        or stg_orders.order_status is distinct from {{ standardized_order_status('orders_source.order_status') }}
        or stg_orders.purchase_at is distinct from orders_source.order_purchase_timestamp
        or stg_orders.approved_at is distinct from orders_source.order_approved_at
        or stg_orders.carrier_at is distinct from orders_source.order_delivered_carrier_date
        or stg_orders.delivered_at is distinct from orders_source.order_delivered_customer_date
        or stg_orders.estimated_delivery_at is distinct from orders_source.order_estimated_delivery_date
)
select *
from mapping_failures
