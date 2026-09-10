{{
    config(
        unique_key=['order_id', 'payment_sequence'],
        incremental_strategy='delete+insert'
    )
}}

select
    order_id,
    payment_sequence,
    payment_status,
    payment_value
from {{ ref('stg_payments') }}
