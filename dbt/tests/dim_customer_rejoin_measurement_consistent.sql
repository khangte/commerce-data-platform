-- 재가입은 CHURNED에서 TRIAL 또는 ACTIVE로 전이한 Version마다 정확히 한 번 누적한다.
with ordered_versions as (
    select
        customer_id,
        customer_key,
        valid_from,
        subscription_status,
        rejoin_count,
        rejoined_at,
        lag(subscription_status) over (
            partition by customer_id
            order by valid_from, customer_key
        ) as previous_subscription_status
    from {{ ref('dim_customer') }}
),
expected_measurements as (
    select
        *,
        sum(
            case
                when previous_subscription_status = 'CHURNED'
                    and subscription_status in ('TRIAL', 'ACTIVE')
                then 1 else 0
            end
        ) over (
            partition by customer_id
            order by valid_from, customer_key
            rows between unbounded preceding and current row
        ) as expected_rejoin_count,
        max(
            case
                when previous_subscription_status = 'CHURNED'
                    and subscription_status in ('TRIAL', 'ACTIVE')
                then valid_from
            end
        ) over (
            partition by customer_id
            order by valid_from, customer_key
            rows between unbounded preceding and current row
        ) as expected_rejoined_at
    from ordered_versions
)
select
    customer_id,
    valid_from,
    rejoin_count,
    expected_rejoin_count,
    rejoined_at,
    expected_rejoined_at
from expected_measurements
where rejoin_count is distinct from expected_rejoin_count
    or rejoined_at is distinct from expected_rejoined_at
