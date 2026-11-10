-- 등급별 주문·금액 합계와 배송 완료 평균 주문 금액을 주문 Fact와 대조한다.
{{ config(severity=var('metrics_test_severity', 'warn')) }}
with expected as (
    select
        count(*) as order_count,
        coalesce(sum(gross_order_value), 0) as gross_order_value,
        coalesce(sum(case when order_status = 'DELIVERED' then 1 else 0 end), 0)
            as delivered_order_count,
        coalesce(sum(case when order_status = 'DELIVERED' then gross_order_value else 0 end), 0)
            as delivered_gmv
    from {{ ref('fct_order') }}
), actual as (
    select
        coalesce(sum(order_count), 0) as order_count,
        coalesce(sum(gross_order_value), 0) as gross_order_value,
        coalesce(sum(delivered_order_count), 0) as delivered_order_count,
        coalesce(sum(delivered_gmv), 0) as delivered_gmv
    from {{ ref('rpt_membership_tier_performance') }}
)
select 'totals' as mismatch
from expected
cross join actual
where expected.order_count != actual.order_count
    or abs(expected.gross_order_value - actual.gross_order_value) > 0.01
    or expected.delivered_order_count != actual.delivered_order_count
    or abs(expected.delivered_gmv - actual.delivered_gmv) > 0.01
union all
select membership_tier as mismatch
from {{ ref('rpt_membership_tier_performance') }}
where (delivered_order_count = 0 and delivered_aov is not null)
    or (delivered_order_count != 0 and (
        delivered_aov is null
        or abs(delivered_aov - delivered_gmv / delivered_order_count) > 0.01
    ))
