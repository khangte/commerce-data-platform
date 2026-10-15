select *
from {{ ref('fct_subscription_payment') }}
where (payment_status = 'COMPLETED' and completed_payment_value is distinct from payment_value)
    or (payment_status = 'FAILED' and completed_payment_value is not null)
