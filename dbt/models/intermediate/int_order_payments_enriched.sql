-- 결제 행을 보존하고 주문의 고객 버전과 UTC 구매일을 Dimension 키로 준비한다.
select
    stg_payments.order_id,
    stg_payments.payment_sequence,
    int_orders_enriched.customer_key,
    cast(strftime(date_trunc('day', int_orders_enriched.purchase_at), '%Y%m%d') as integer) as purchase_date_key,
    stg_payments.payment_status,
    stg_payments.payment_value
from {{ ref('stg_payments') }}
left join {{ ref('int_orders_enriched') }} using (order_id)
