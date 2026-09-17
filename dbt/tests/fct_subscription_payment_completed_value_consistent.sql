select *
from {{ ref('fact_subscription_payments') }}
where (payment_status = 'completed' and completed_payment_value is distinct from payment_value)
    or (payment_status = 'failed' and completed_payment_value is not null)
