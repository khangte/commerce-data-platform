select
    order_id,
    sum(payment_value) as payment_total
from {{ ref('stg_payments') }}
group by order_id
