with subscription_observations as (
    select
        subscription_id,
        customer_unique_id as customer_id,
        {{ standardized_subscription_status('subscription_status') }} as subscription_status,
        auto_renew_enabled,
        subscription_started_at,
        current_period_started_at,
        current_period_ends_at,
        billing_due_at,
        next_payment_attempt_at,
        payment_failed_at,
        cancel_requested_at,
        ended_at,
        status_changed_at,
        created_at,
        updated_at,
        _batch_id,
        _run_id,
        _ingested_at,
        _source_table,
        _schema_version
    from {{ bronze_source('customer_subscriptions') }}
),
with_attribute_hash as (
    select
        *,
        md5(
            coalesce('status:' || subscription_status, 'status:')
            || '|' || coalesce('renew:' || cast(auto_renew_enabled as varchar), 'renew:')
            || '|' || coalesce('period-start:' || cast(current_period_started_at as varchar), 'period-start:')
            || '|' || coalesce('period-end:' || cast(current_period_ends_at as varchar), 'period-end:')
            || '|' || coalesce('failed:' || cast(payment_failed_at as varchar), 'failed:')
            || '|' || coalesce('cancel:' || cast(cancel_requested_at as varchar), 'cancel:')
            || '|' || coalesce('ended:' || cast(ended_at as varchar), 'ended:')
        ) as attribute_hash
    from subscription_observations
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by subscription_id, updated_at, attribute_hash
            order by _ingested_at desc, _batch_id desc
        ) as _observation_rank
    from with_attribute_hash
)
select
    subscription_id,
    customer_id,
    subscription_status,
    auto_renew_enabled,
    subscription_started_at,
    current_period_started_at,
    current_period_ends_at,
    billing_due_at,
    next_payment_attempt_at,
    payment_failed_at,
    cancel_requested_at,
    ended_at,
    status_changed_at,
    attribute_hash,
    created_at,
    updated_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from deduplicated
where _observation_rank = 1
