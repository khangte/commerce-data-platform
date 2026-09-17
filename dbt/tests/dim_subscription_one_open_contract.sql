select
    customer_id,
    count(*) as open_contract_count
from {{ ref('dim_subscription') }}
where is_current
    and subscription_status in ('ACTIVE', 'PAYMENT_FAILED', 'CANCEL_REQUESTED')
group by customer_id
having count(*) > 1
