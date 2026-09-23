-- 신규·재구매 고객 일별 Report는 날짜와 고객 구분마다 한 행이어야 한다.
select
    purchase_date_key,
    customer_kind
from {{ ref('rpt_customer_order_activity_daily') }}
group by purchase_date_key, customer_kind
having count(*) > 1
