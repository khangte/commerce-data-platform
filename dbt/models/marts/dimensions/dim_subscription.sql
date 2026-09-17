{{ config(unique_key='subscription_key', incremental_strategy='delete+insert') }}

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
