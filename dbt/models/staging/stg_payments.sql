select
    order_id,
    payment_sequential as payment_sequence,
    payment_type,
    payment_installments as installments,
    payment_value,
    {{ standardized_payment_status('payment_status') }} as payment_status,
    payment_initiated_at,
    payment_completed_at,
    payment_failed_at,
    payment_refunded_at,
    created_at,
    updated_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from {{ current_bronze_records('order_payments', ['order_id', 'payment_sequential']) }}
