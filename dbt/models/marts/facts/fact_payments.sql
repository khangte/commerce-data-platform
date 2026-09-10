{{
    config(
        unique_key=['order_id', 'payment_sequence']
    )
}}

select
    order_id,
    payment_sequence,
    payment_status,
    payment_value
from {{ ref('stg_payments') }}
