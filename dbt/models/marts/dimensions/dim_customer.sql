select
    md5(concat(customer_id, '|', cast(valid_from as varchar), '|', attribute_hash)) as customer_key,
    customer_id,
    customer_id as source_customer_unique_id,
    membership_level,
    city,
    state,
    attribute_hash,
    valid_from,
    valid_to,
    is_current
from {{ ref('int_customer_history') }}
