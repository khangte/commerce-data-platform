-- 결제의 고객 버전은 같은 주문 Fact의 고객 버전과 같아야 한다.
select
    fct_order_payment.order_id,
    fct_order_payment.payment_sequence,
    fct_order_payment.customer_key as payment_customer_key,
    fct_order.customer_key as order_customer_key
from {{ ref('fct_order_payment') }} as fct_order_payment
inner join {{ ref('fct_order') }} as fct_order using (order_id)
where fct_order_payment.customer_key != fct_order.customer_key
