select
    order_id,
    payment_sequence,
    count(*) as row_count
from {{ ref('fct_order_payment') }}
group by order_id, payment_sequence
having count(*) > 1
