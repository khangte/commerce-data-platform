-- 결제 행을 보존하고 주문의 UTC 구매일을 날짜 Dimension 키로 준비한다.
select
    stg_payments.order_id,
    stg_payments.payment_sequence,
    cast(strftime(date_trunc('day', stg_orders.purchase_at), '%Y%m%d') as integer) as purchase_date_key,
    stg_payments.payment_status,
    stg_payments.payment_value
from {{ ref('stg_payments') }}
left join {{ ref('stg_orders') }} using (order_id)
