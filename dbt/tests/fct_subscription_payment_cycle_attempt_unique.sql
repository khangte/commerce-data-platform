select
    subscription_id,
    billing_cycle_sequence,
    attempt_sequence,
    count(*) as row_count
from {{ ref('fct_subscription_payment') }}
group by subscription_id, billing_cycle_sequence, attempt_sequence
having count(*) > 1
