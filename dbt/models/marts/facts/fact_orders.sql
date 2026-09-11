{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

select
    order_id,
    customer_key,
    purchase_date_key,
    order_status,
    customer_city,
    customer_state,
    gross_order_value,
    payment_total,
    order_count,
    carrier_handoff_days,
    delivery_days,
    delivery_delay_days,
    is_late
from {{ ref('int_order_fact_ready') }}
