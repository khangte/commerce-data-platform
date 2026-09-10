-- 구독 결제 Fact의 Grain은 고객별 청구 순번 1건이다.
select
    customer_unique_id,
    billing_sequence,
    count(*) as row_count
from {{ ref('fact_subscription_payments') }}
group by customer_unique_id, billing_sequence
having count(*) > 1
