select
    customer_id as source_customer_id,
    customer_unique_id as customer_id,
    customer_city as city,
    customer_state as state,
    created_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from {{ current_bronze_records('customers', ['customer_id'], 'created_at') }}
