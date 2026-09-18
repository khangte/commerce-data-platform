{% macro replay_boundary() -%}
    {#- bronze_as_of var를 검증해 DuckDB Literal용 UTC 문자열 또는 none을 돌려준다. -#}
    {%- set raw = var('bronze_as_of', none) -%}
    {%- if raw is none -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set value = raw | string | trim -%}
    {%- if value == '' -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set pattern = '^\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,6})?(Z|\\+00:00)$' -%}
    {%- if modules.re.match(pattern, value) is none -%}
        {{ exceptions.raise_compiler_error(
            'REPLAY_BOUNDARY_ERROR: bronze_as_of must be an ISO-8601 UTC timestamp, got ' ~ value
        ) }}
    {%- endif -%}
    {{ return(value | replace('Z', '+00:00') | replace('T', ' ')) }}
{%- endmacro %}


{% macro replay_boundary_predicate(column_name='committed_at') -%}
    {#- 경계가 있으면 Catalog 조회에 붙일 AND 절을, 없으면 빈 문자열을 돌려준다. -#}
    {%- set boundary = replay_boundary() -%}
    {%- if boundary is none -%}
        {{ return('') }}
    {%- endif -%}
    {{ return(' and ' ~ column_name ~ " <= timestamptz '" ~ boundary ~ "'") }}
{%- endmacro %}


{% macro assert_replay_boundary_mode() -%}
    {#- 경계 Build는 Full Refresh만 허용한다. Incremental 혼합 결과를 막는다. -#}
    {%- if execute -%}
        {%- if replay_boundary() is not none and not flags.FULL_REFRESH -%}
            {{ exceptions.raise_compiler_error(
                'REPLAY_BOUNDARY_ERROR: bronze_as_of requires --full-refresh'
            ) }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}


{% macro record_replay_boundary() -%}
    {#- 이번 Build가 어떤 경계로 몇 개 Object를 읽었는지 증거로 남긴다. -#}
    {%- if execute -%}
        {%- do run_query("CREATE SCHEMA IF NOT EXISTS control") -%}
        {%- do run_query("
            CREATE TABLE IF NOT EXISTS control.dbt_replay_boundary (
                invocation_id VARCHAR NOT NULL,
                bronze_as_of TIMESTAMPTZ,
                object_count BIGINT NOT NULL,
                max_committed_at TIMESTAMPTZ,
                full_refresh BOOLEAN NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
            )
        ") -%}
        {%- set catalog_exists = run_query("
            select count(*) as table_count
            from information_schema.tables
            where table_schema = 'control' and table_name = 'bronze_files'
        ") -%}
        {%- if catalog_exists.rows[0][0] == 0 -%}
            {%- do log('Skipping replay boundary evidence: control.bronze_files is absent', info=true) -%}
        {%- else -%}
            {%- set boundary = replay_boundary() -%}
            {%- if boundary is none -%}
                {%- set boundary_literal = 'cast(null as timestamptz)' -%}
            {%- else -%}
                {%- set boundary_literal = "timestamptz '" ~ boundary ~ "'" -%}
            {%- endif -%}
            {%- set record_sql -%}
                insert into control.dbt_replay_boundary
                    (invocation_id, bronze_as_of, object_count, max_committed_at, full_refresh)
                select
                    '{{ invocation_id }}',
                    {{ boundary_literal }},
                    count(*),
                    max(committed_at),
                    {{ 'true' if flags.FULL_REFRESH else 'false' }}
                from control.bronze_files
                where true {{ replay_boundary_predicate() }}
            {%- endset -%}
            {%- do run_query(record_sql) -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
