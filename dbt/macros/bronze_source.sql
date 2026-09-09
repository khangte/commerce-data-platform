{% macro bronze_source(source_table) -%}
    {%- set supported_tables = [
        'customers', 'customer_memberships', 'products', 'sellers', 'orders', 'order_items', 'order_payments'
    ] -%}
    {%- if source_table not in supported_tables -%}
        {{ exceptions.raise_compiler_error('SOURCE_CONTRACT_ERROR: unsupported source_table=' ~ source_table) }}
    {%- endif -%}

    {%- if execute -%}
        {%- set catalog_query -%}
            select object_key, schema_version
            from control.bronze_files
            where source_table = '{{ source_table }}'
            order by committed_at, object_key
        {%- endset -%}
        {%- set catalog_rows = run_query(catalog_query) -%}
        {%- set object_keys = [] -%}
        {%- for row in catalog_rows.rows -%}
            {%- if row[1] != 1 -%}
                {{ exceptions.raise_compiler_error(
                    'SOURCE_CONTRACT_ERROR: unsupported schema_version=' ~ row[1]
                    ~ ' for source_table=' ~ source_table
                ) }}
            {%- endif -%}
            {%- do object_keys.append(row[0]) -%}
        {%- endfor -%}
        {%- if object_keys -%}
            read_parquet(
                [
                {%- for object_key in object_keys -%}
                    's3://{{ env_var("SEAWEEDFS_BUCKET") }}/{{ object_key | replace("'", "''") }}'
                    {%- if not loop.last %}, {% endif -%}
                {%- endfor -%}
                ],
                union_by_name = true
            )
        {%- else -%}
            ({{ bronze_empty_relation(source_table) }})
        {%- endif -%}
    {%- else -%}
        ({{ bronze_empty_relation(source_table) }})
    {%- endif -%}
{%- endmacro %}

{% macro bronze_empty_relation(source_table) -%}
    {%- set schemas = {
        'customers': [
            ('customer_id', 'varchar'), ('customer_unique_id', 'varchar'),
            ('customer_city', 'varchar'), ('customer_state', 'varchar'),
            ('created_at', 'timestamptz')
        ],
        'customer_memberships': [
            ('customer_unique_id', 'varchar'), ('membership_level', 'varchar'), ('created_at', 'timestamptz'),
            ('updated_at', 'timestamptz')
        ],
        'products': [
            ('product_id', 'varchar'), ('product_category_name', 'varchar'),
            ('product_weight_g', 'integer'), ('product_length_cm', 'integer'),
            ('product_height_cm', 'integer'), ('product_width_cm', 'integer'),
            ('created_at', 'timestamptz'), ('updated_at', 'timestamptz')
        ],
        'sellers': [
            ('seller_id', 'varchar'), ('seller_city', 'varchar'), ('seller_state', 'varchar'),
            ('created_at', 'timestamptz'), ('updated_at', 'timestamptz')
        ],
        'orders': [
            ('order_id', 'varchar'), ('customer_id', 'varchar'), ('order_status', 'varchar'),
            ('order_purchase_timestamp', 'timestamptz'), ('order_approved_at', 'timestamptz'),
            ('order_delivered_carrier_date', 'timestamptz'),
            ('order_delivered_customer_date', 'timestamptz'),
            ('order_estimated_delivery_date', 'timestamptz'), ('created_at', 'timestamptz'),
            ('updated_at', 'timestamptz')
        ],
        'order_items': [
            ('order_id', 'varchar'), ('order_item_id', 'integer'), ('product_id', 'varchar'),
            ('seller_id', 'varchar'), ('price', 'decimal(14, 2)'),
            ('freight_value', 'decimal(14, 2)'), ('created_at', 'timestamptz')
        ],
        'order_payments': [
            ('order_id', 'varchar'), ('payment_sequential', 'integer'), ('payment_type', 'varchar'),
            ('payment_installments', 'integer'), ('payment_value', 'decimal(14, 2)'),
            ('payment_status', 'varchar'), ('created_at', 'timestamptz'), ('updated_at', 'timestamptz')
        ]
    } -%}
    {%- set technical_columns = [
        ('_batch_id', 'varchar'), ('_run_id', 'varchar'), ('_ingested_at', 'timestamptz'),
        ('_source_table', 'varchar'), ('_schema_version', 'integer')
    ] -%}
    select
        {%- for column_name, column_type in schemas[source_table] + technical_columns %}
        cast(null as {{ column_type }}) as {{ column_name }}{{ ', ' if not loop.last }}
        {%- endfor %}
    where false
{%- endmacro %}

{% macro validate_bronze_catalog(verify_read=false) -%}
    {%- if execute -%}
        {%- set invalid_versions_query -%}
            select distinct schema_version
            from control.bronze_files
            where schema_version != 1
            order by schema_version
        {%- endset -%}
        {%- set invalid_versions = run_query(invalid_versions_query) -%}
        {%- if invalid_versions.rows -%}
            {{ exceptions.raise_compiler_error(
                'SOURCE_CONTRACT_ERROR: unsupported schema_version(s)='
                ~ invalid_versions.columns[0].values() | join(', ')
            ) }}
        {%- endif -%}
        {%- for source_table in [
            'customers', 'customer_memberships', 'products', 'sellers', 'orders', 'order_items', 'order_payments'
        ] -%}
            {%- set relation = bronze_source(source_table) -%}
            {%- do log('Validated Bronze catalog relation for ' ~ source_table ~ ': ' ~ relation, info=True) -%}
            {%- if verify_read -%}
                {%- set row_count = run_query('select count(*) as row_count from ' ~ relation) -%}
                {%- do log('Read committed Bronze rows for ' ~ source_table ~ ': ' ~ row_count.rows[0][0], info=True) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro %}
