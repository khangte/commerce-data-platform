{% macro record_affected_keys() -%}
    {%- if execute -%}
        {%- do run_query("CREATE SCHEMA IF NOT EXISTS control") -%}
        {%- do run_query("
            CREATE TABLE IF NOT EXISTS control.affected_keys (
                invocation_id VARCHAR NOT NULL,
                order_id VARCHAR NOT NULL,
                business_date_key INTEGER NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        ") -%}
        {%- set affected_relation = ref('int_affected_business_dates') -%}
        {%- set insert_sql -%}
            insert into control.affected_keys (invocation_id, order_id, business_date_key)
            select '{{ invocation_id }}', order_id, business_date_key
            from {{ affected_relation }}
        {%- endset -%}
        {%- do run_query(insert_sql) -%}
    {%- endif -%}
{%- endmacro %}
