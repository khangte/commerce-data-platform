-- 승인·배송완료 시각은 구매 시각보다 빠를 수 없다. NULL 시각은 아직 발생하지 않은 이벤트로 통과한다.
select order_id, purchase_at, approved_at, delivered_at
from {{ ref('stg_orders') }}
where approved_at < purchase_at or delivered_at < purchase_at
