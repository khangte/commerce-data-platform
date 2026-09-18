{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

-- dbt unique_key는 교체 단위다. Grain 유일성은 schema.yml의 unique Test가 강제한다.
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
{% if is_incremental() %}
where order_id in (select order_id from {{ ref('int_affected_order_keys') }})
{% endif %}
