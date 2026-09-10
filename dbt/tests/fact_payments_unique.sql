select
    order_id,
    payment_sequence,
    count(*) as row_count
from {{ ref('fact_payments') }}
group by order_id, payment_sequence
having count(*) > 1
