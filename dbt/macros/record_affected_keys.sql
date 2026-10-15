{% macro record_affected_keys() -%}
    {#- 이번 Invocation이 재계산한 영향 Key를 도메인과 함께 감사 기록에 남긴다. -#}
    {%- if execute -%}
        {%- do run_query("CREATE SCHEMA IF NOT EXISTS control") -%}
        {%- do run_query("
            CREATE TABLE IF NOT EXISTS control.affected_keys (
                invocation_id VARCHAR NOT NULL,
                affected_domain VARCHAR NOT NULL,
                entity_key VARCHAR NOT NULL,
                business_date_key INTEGER NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        ") -%}
        {%- set insert_sql -%}
            insert into control.affected_keys (invocation_id, affected_domain, entity_key, business_date_key)
            select '{{ invocation_id }}', 'order', order_id, business_date_key
            from {{ ref('int_affected_order_keys') }}
            union all
            select '{{ invocation_id }}', 'subscription_payment', payment_id, business_date_key
            from {{ ref('int_affected_subscription_payment_keys') }}
        {%- endset -%}
        {%- do run_query(insert_sql) -%}
    {%- endif -%}
{%- endmacro %}
