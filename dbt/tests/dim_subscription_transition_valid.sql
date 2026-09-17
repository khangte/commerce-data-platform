with ordered_versions as (
    select
        subscription_id,
        valid_from,
        subscription_status,
        lag(subscription_status) over (
            partition by subscription_id order by valid_from, subscription_key
        ) as previous_subscription_status
    from {{ ref('dim_subscription') }}
)
select *
from ordered_versions
where previous_subscription_status is not null
    and previous_subscription_status != subscription_status
    and not (
        (previous_subscription_status = 'ACTIVE' and subscription_status in ('PAYMENT_FAILED', 'CANCEL_REQUESTED'))
        or (previous_subscription_status = 'PAYMENT_FAILED' and subscription_status in ('ACTIVE', 'CANCEL_REQUESTED', 'CHURNED'))
        or (previous_subscription_status = 'CANCEL_REQUESTED' and subscription_status in ('ACTIVE', 'CHURNED'))
    )
