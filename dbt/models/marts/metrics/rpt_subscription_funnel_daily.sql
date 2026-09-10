-- 일별 구독 전이 건수다. 현재 상태 분포가 아니라 상태에 진입한 이벤트를 센다.
with lifecycle_versions as (
    select
        customer_id,
        valid_from,
        subscription_status,
        lag(subscription_status) over (
            partition by customer_id
            order by valid_from, customer_key
        ) as previous_subscription_status
    from {{ ref('dim_customer') }}
),
subscription_events as (
    select
        cast(strftime(date_trunc('day', valid_from), '%Y%m%d') as integer) as event_date_key,
        subscription_status,
        previous_subscription_status
    from lifecycle_versions
    where previous_subscription_status is null
        or previous_subscription_status != subscription_status
)
select
    event_date_key,
    sum(
        case
            when subscription_status = 'TRIAL'
                and coalesce(previous_subscription_status, 'NON_MEMBER') in ('NON_MEMBER', 'CHURNED')
            then 1 else 0
        end
    ) as trial_started_count,
    sum(case when subscription_status = 'ACTIVE' then 1 else 0 end) as activated_count,
    sum(case when subscription_status = 'PAYMENT_FAILED' then 1 else 0 end) as payment_failed_count,
    sum(case when subscription_status = 'CANCEL_REQUESTED' then 1 else 0 end) as cancel_requested_count,
    sum(case when subscription_status = 'CHURNED' then 1 else 0 end) as churned_count,
    sum(
        case
            when previous_subscription_status = 'CHURNED'
                and subscription_status in ('TRIAL', 'ACTIVE')
            then 1 else 0
        end
    ) as rejoined_count
from subscription_events
group by event_date_key
