with subscription_observations as (
    select
        customer_unique_id as customer_id,
        {{ standardized_subscription_status('subscription_status') }} as subscription_status,
        trial_ends_at,
        benefit_ends_at,
        payment_failed_at,
        cancel_requested_at,
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
            || '|' || coalesce('trial:' || cast(trial_ends_at as varchar), 'trial:')
            || '|' || coalesce('benefit:' || cast(benefit_ends_at as varchar), 'benefit:')
            || '|' || coalesce('failed:' || cast(payment_failed_at as varchar), 'failed:')
            || '|' || coalesce('cancel:' || cast(cancel_requested_at as varchar), 'cancel:')
        ) as attribute_hash
    from subscription_observations
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by customer_id, updated_at, attribute_hash
            order by _ingested_at desc, _batch_id desc
        ) as _observation_rank
    from with_attribute_hash
)
select
    customer_id,
    subscription_status,
    trial_ends_at,
    benefit_ends_at,
    payment_failed_at,
    cancel_requested_at,
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
