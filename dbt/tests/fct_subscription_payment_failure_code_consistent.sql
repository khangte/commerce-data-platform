select *
from {{ ref('fct_subscription_payment') }}
where payment_status = 'completed' and failure_code is not null
