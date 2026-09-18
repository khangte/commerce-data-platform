-- 주문 축 영향 Key는 (order_id, business_date_key)로 유일하다.
select
    order_id,
    business_date_key,
    count(*) as row_count
from {{ ref('int_affected_order_keys') }}
group by order_id, business_date_key
having count(*) > 1
