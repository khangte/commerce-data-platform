-- 한 행은 구독 계약 상태 버전 1건이다. Materialization은 dbt_project.yml의 table을 따른다.
select
    md5(concat(subscription_id, '|', cast(valid_from as varchar), '|', attribute_hash)) as subscription_key,
    subscription_id,
    customer_id,
    subscription_status,
    auto_renew_enabled,
    subscription_started_at,
    current_period_started_at,
    current_period_ends_at,
    payment_failed_at,
    cancel_requested_at,
    ended_at,
    status_changed_at,
    attribute_hash,
    valid_from,
    valid_to,
    is_current
from {{ ref('int_subscription_history') }}
