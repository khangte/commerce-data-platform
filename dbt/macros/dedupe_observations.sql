{% macro dedupe_observations(relation, key_column) -%}
    {#- 같은 키·수정 시각·속성의 관측은 최신 수집 시각과 배치 순서로 하나만 남긴다. -#}
    select *
    from {{ relation }}
    qualify row_number() over (
        partition by {{ key_column }}, updated_at, attribute_hash
        order by _ingested_at desc, _batch_id desc
    ) = 1
{%- endmacro %}
