select
    md5(concat(customer_id, '|', cast(valid_from as varchar), '|', attribute_hash)) as customer_key,
    customer_id,
    customer_id as source_customer_unique_id,
    subscription_status,
    membership_tier,
    trial_ends_at,
    benefit_ends_at,
    payment_failed_at,
    cancel_requested_at,
    attribute_hash,
    valid_from,
    valid_to,
    is_current,
    rejoin_count,
    rejoined_at
from {{ ref('int_customer_history') }}
