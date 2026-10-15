select *
from {{ ref('fct_subscription_payment') }}
where payment_status = 'COMPLETED' and failure_code is not null
