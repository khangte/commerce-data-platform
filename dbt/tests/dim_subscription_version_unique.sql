select
    subscription_id,
    valid_from,
    count(*) as row_count
from {{ ref('dim_subscription') }}
group by subscription_id, valid_from
having count(*) > 1
