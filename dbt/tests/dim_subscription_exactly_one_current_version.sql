select
    subscription_id,
    sum(case when is_current then 1 else 0 end) as current_version_count
from {{ ref('dim_subscription') }}
group by subscription_id
having sum(case when is_current then 1 else 0 end) != 1
