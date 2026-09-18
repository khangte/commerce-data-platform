-- 한 행은 구독 결제 Fact가 다시 계산해야 하는 (결제 시도, 결제일) 1건이다.
{% set lower_bound = processed_batch_lower_bound() %}

with subscription_axis_changes as (
    -- 이번 경계에서 계약 관측이 바뀐 계약과 그 최소 변경 시각.
    select
        subscription_id,
        min(updated_at) as changed_from
    from {{ ref('stg_customer_subscription_observations') }}
    where _batch_id > {{ lower_bound }}
    group by subscription_id
),
tier_axis_changes as (
    -- 이번 경계에서 등급 관측이 바뀐 고객과 그 최소 변경 시각.
    select
        customer_id,
        min(updated_at) as changed_from
    from {{ ref('stg_customer_tier_observations') }}
    where _batch_id > {{ lower_bound }}
    group by customer_id
),
affected_payments as (
    select payment_id, payment_at
    from {{ ref('stg_subscription_payments') }}
    where _batch_id > {{ lower_bound }}
),
affected_from_subscription_versions as (
    select payments.payment_id, payments.payment_at
    from {{ ref('stg_subscription_payments') }} as payments
    inner join subscription_axis_changes
        on payments.subscription_id = subscription_axis_changes.subscription_id
        and payments.payment_at >= subscription_axis_changes.changed_from
),
affected_from_customer_versions as (
    select payments.payment_id, payments.payment_at
    from {{ ref('stg_subscription_payments') }} as payments
    inner join tier_axis_changes
        on payments.customer_id = tier_axis_changes.customer_id
        and payments.payment_at >= tier_axis_changes.changed_from
),
unioned as (
    select * from affected_payments
    union
    select * from affected_from_subscription_versions
    union
    select * from affected_from_customer_versions
)
select
    payment_id,
    cast(strftime(date_trunc('day', payment_at), '%Y%m%d') as integer) as business_date_key
from unioned
