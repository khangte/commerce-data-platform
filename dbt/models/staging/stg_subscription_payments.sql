select
    customer_unique_id as customer_id,
    billing_sequence,
    payment_status,
    payment_value,
    billing_period_start,
    billing_period_end,
    created_at,
    updated_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from {{ current_bronze_records('subscription_payments', ['customer_unique_id', 'billing_sequence']) }}
