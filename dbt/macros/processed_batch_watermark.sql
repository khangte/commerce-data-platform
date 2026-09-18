{% macro ensure_processed_batch_watermark() -%}
    {#- 재계산 경계 Table을 Build 시작 시점에 보장한다. -#}
    {%- if execute -%}
        {%- do run_query("CREATE SCHEMA IF NOT EXISTS control") -%}
        {%- do run_query("
            CREATE TABLE IF NOT EXISTS control.dbt_processed_batch (
                processed_batch_id VARCHAR NOT NULL,
                invocation_id VARCHAR NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        ") -%}
    {%- endif -%}
{%- endmacro %}


{% macro processed_batch_lower_bound() -%}
    {#- 이번 Build가 재계산해야 하는 _batch_id의 하한을 Scalar Subquery로 돌려준다. -#}
    (select coalesce(max(processed_batch_id), '') from control.dbt_processed_batch)
{%- endmacro %}


{% macro batch_id_source_models() -%}
    {#- _batch_id를 노출하는 Staging Model 목록을 돌려준다. -#}
    {{ return([
        'stg_orders',
        'stg_order_items',
        'stg_payments',
        'stg_products',
        'stg_sellers',
        'stg_customer_tier_observations',
        'stg_customer_subscription_observations',
        'stg_subscription_payments',
    ]) }}
{%- endmacro %}


{% macro advance_processed_batch_watermark() -%}
    {#- 모든 Fact를 함께 Build한 경우에만 재계산 경계를 전진시킨다. -#}
    {%- if execute and replay_boundary() is none -%}
        {%- set required_facts = [
            'model.commerce_data_platform.fact_orders',
            'model.commerce_data_platform.fact_order_items',
            'model.commerce_data_platform.fact_payments',
            'model.commerce_data_platform.fact_subscription_payments',
        ] -%}
        {%- set selected = selected_resources | list -%}
        {%- set missing = required_facts | reject('in', selected) | list -%}
        {%- if missing | length > 0 -%}
            {%- do log("Skipping watermark advance: facts not in this selection " ~ missing, info=true) -%}
        {%- else -%}
            {%- set blocking_statuses = ['error', 'fail', 'runtime error', 'skipped'] -%}
            {%- set blocking_nodes = [] -%}
            {%- for result in results -%}
                {%- if result.status in blocking_statuses -%}
                    {%- do blocking_nodes.append(result.node.unique_id ~ '=' ~ result.status) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if blocking_nodes | length > 0 -%}
                {%- do log("Skipping watermark advance: build had failures " ~ blocking_nodes, info=true) -%}
            {%- else -%}
                {%- set selects = [] -%}
                {%- for model_name in batch_id_source_models() -%}
                    {%- do selects.append("select max(_batch_id) as batch_id from " ~ ref(model_name)) -%}
                {%- endfor -%}
                {%- set advance_sql -%}
                    insert into control.dbt_processed_batch (processed_batch_id, invocation_id)
                    select max(batch_id), '{{ invocation_id }}'
                    from ({{ selects | join(' union all ') }})
                    where batch_id is not null
                    having max(batch_id) is not null
                {%- endset -%}
                {%- do run_query(advance_sql) -%}
            {%- endif -%}
        {%- endif -%}
    {%- elif execute -%}
        {%- do log("Skipping watermark advance: bronze_as_of replay boundary build", info=true) -%}
    {%- endif -%}
{%- endmacro %}
