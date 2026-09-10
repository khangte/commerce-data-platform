-- 거래 실적 등급은 주문 실적 누적에 따라 유지하거나 상승할 수 있지만 하락하면 안 된다.
with ordered_versions as (
    select
        customer_id,
        valid_from,
        membership_tier,
        lag(
            case membership_tier
                when 'BRONZE' then 1
                when 'SILVER' then 2
                when 'GOLD' then 3
            end
        ) over (
            partition by customer_id
            order by valid_from, customer_key
        ) as previous_tier_rank,
        case membership_tier
            when 'BRONZE' then 1
            when 'SILVER' then 2
            when 'GOLD' then 3
        end as tier_rank
    from {{ ref('dim_customer') }}
)
select
    customer_id,
    valid_from,
    membership_tier,
    previous_tier_rank,
    tier_rank
from ordered_versions
where tier_rank < previous_tier_rank
