{% macro bronze_source(source_table) -%}
    {#- Python 계약을 확인한 뒤 확정된 Bronze 파일 또는 빈 관계를 노출한다. -#}
    {%- if execute -%}
        {%- set schemas = var('bronze_schemas', none) -%}
        {%- set versions = var('bronze_schema_versions', none) -%}
        {%- if schemas is none or versions is none -%}
            {{ exceptions.raise_compiler_error(
                'SOURCE_CONTRACT_ERROR: missing bronze_schemas or bronze_schema_versions; '
                ~ 'pass --vars from python -m src.warehouse.dbt_vars'
            ) }}
        {%- endif -%}
        {%- if source_table not in schemas -%}
            {{ exceptions.raise_compiler_error('SOURCE_CONTRACT_ERROR: unsupported source_table=' ~ source_table) }}
        {%- endif -%}
        {%- set catalog_query -%}
            select object_key, schema_version
            from control.bronze_files
            where source_table = '{{ source_table }}'
            {{ replay_boundary_predicate() }}
            order by committed_at, object_key
        {%- endset -%}
        {%- set catalog_rows = run_query(catalog_query) -%}
        {%- set object_keys = [] -%}
        {%- for row in catalog_rows.rows -%}
            {%- if row[1] not in versions -%}
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
        (select 1 where false)
    {%- endif -%}
{%- endmacro %}

{% macro bronze_empty_relation(source_table) -%}
    {#- 원천·기술 컬럼을 Python 계약에서 받은 순서와 타입 그대로 노출한다. -#}
    {%- if execute -%}
        {%- set schemas = var('bronze_schemas', none) -%}
        {%- if schemas is none -%}
            {{ exceptions.raise_compiler_error(
                'SOURCE_CONTRACT_ERROR: missing bronze_schemas; '
                ~ 'pass --vars from python -m src.warehouse.dbt_vars'
            ) }}
        {%- endif -%}
        select
            {%- for column_name, column_type in schemas[source_table] %}
            cast(null as {{ column_type }}) as {{ column_name }}{{ ', ' if not loop.last }}
            {%- endfor %}
        where false
    {%- else -%}
        select 1 where false
    {%- endif -%}
{%- endmacro %}

{% macro validate_bronze_catalog(verify_read=false) -%}
    {#- 지원 테이블·버전으로 카탈로그를 검증하고 필요하면 실제 읽기를 확인한다. -#}
    {%- if execute -%}
        {%- set schemas = var('bronze_schemas', none) -%}
        {%- set versions = var('bronze_schema_versions', none) -%}
        {%- if schemas is none or versions is none -%}
            {{ exceptions.raise_compiler_error(
                'SOURCE_CONTRACT_ERROR: missing bronze_schemas or bronze_schema_versions; '
                ~ 'pass --vars from python -m src.warehouse.dbt_vars'
            ) }}
        {%- endif -%}
        {%- set invalid_versions_query -%}
            select distinct schema_version
            from control.bronze_files
            where schema_version not in ({{ versions | join(', ') }})
            {{ replay_boundary_predicate() }}
            order by schema_version
        {%- endset -%}
        {%- set invalid_versions = run_query(invalid_versions_query) -%}
        {%- if invalid_versions.rows -%}
            {{ exceptions.raise_compiler_error(
                'SOURCE_CONTRACT_ERROR: unsupported schema_version(s)='
                ~ invalid_versions.columns[0].values() | join(', ')
            ) }}
        {%- endif -%}
        {%- for source_table in schemas -%}
            {%- set relation = bronze_source(source_table) -%}
            {%- do log('Validated Bronze catalog relation for ' ~ source_table ~ ': ' ~ relation, info=True) -%}
            {%- if verify_read -%}
                {%- set row_count = run_query('select count(*) as row_count from ' ~ relation) -%}
                {%- do log('Read committed Bronze rows for ' ~ source_table ~ ': ' ~ row_count.rows[0][0], info=True) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro %}
