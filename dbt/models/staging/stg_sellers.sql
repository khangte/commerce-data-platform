select
    seller_id,
    seller_city as city,
    seller_state as state,
    created_at,
    updated_at,
    _batch_id,
    _run_id,
    _ingested_at,
    _source_table,
    _schema_version
from {{ current_bronze_records('sellers', ['seller_id']) }}
