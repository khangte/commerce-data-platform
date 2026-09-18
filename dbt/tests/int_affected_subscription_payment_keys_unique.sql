-- 구독 결제 축 영향 Key는 (payment_id, business_date_key)로 유일하다.
select
    payment_id,
    business_date_key,
    count(*) as row_count
from {{ ref('int_affected_subscription_payment_keys') }}
group by payment_id, business_date_key
having count(*) > 1
