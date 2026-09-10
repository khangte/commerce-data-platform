-- 등급만 변경된 Version은 같은 구독 상태를 유지할 수 있다.
-- 상태가 바뀐 경우에는 Generator가 허용한 구독 전이만 허용한다.
with ordered_versions as (
    select
        customer_id,
        valid_from,
        subscription_status,
        lag(subscription_status) over (
            partition by customer_id
            order by valid_from, customer_key
        ) as previous_subscription_status
    from {{ ref('dim_customer') }}
)
select
    customer_id,
    valid_from,
    previous_subscription_status,
    subscription_status
from ordered_versions
where previous_subscription_status is not null
    and previous_subscription_status != subscription_status
    and not (
        (previous_subscription_status = 'NON_MEMBER' and subscription_status in ('TRIAL', 'ACTIVE'))
        or (previous_subscription_status = 'TRIAL' and subscription_status in ('ACTIVE', 'PAYMENT_FAILED', 'CANCEL_REQUESTED'))
        or (previous_subscription_status = 'ACTIVE' and subscription_status in ('PAYMENT_FAILED', 'CANCEL_REQUESTED'))
        or (previous_subscription_status = 'PAYMENT_FAILED' and subscription_status in ('ACTIVE', 'CANCEL_REQUESTED', 'CHURNED'))
        or (previous_subscription_status = 'CANCEL_REQUESTED' and subscription_status in ('ACTIVE', 'CHURNED'))
        or (previous_subscription_status = 'CHURNED' and subscription_status in ('TRIAL', 'ACTIVE'))
    )
