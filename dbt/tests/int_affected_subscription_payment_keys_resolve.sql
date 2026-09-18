-- 영향 Key는 Staging에 실재하는 결제 시도만 가리킨다.
select affected.payment_id
from {{ ref('int_affected_subscription_payment_keys') }} as affected
left join {{ ref('stg_subscription_payments') }} as payments
    on affected.payment_id = payments.payment_id
where payments.payment_id is null
