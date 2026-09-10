-- 병합된 시간축에서 같은 사람·같은 시점에 서로 다른 속성 Hash가 생기면 안 된다.
select customer_id, valid_from, count(distinct attribute_hash) as distinct_hash_count
from {{ ref('int_customer_history') }}
group by customer_id, valid_from
having count(distinct attribute_hash) > 1
