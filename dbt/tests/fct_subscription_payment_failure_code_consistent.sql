select *
from {{ ref('fact_subscription_payments') }}
where payment_status = 'completed' and failure_code is not null
