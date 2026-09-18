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
