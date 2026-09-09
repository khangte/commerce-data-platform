{% macro current_bronze_records(source_table, partition_columns, updated_at_column='updated_at') -%}
    (
        select * exclude (_current_record_rank)
        from (
            select
                *,
                row_number() over (
                    partition by {{ partition_columns | join(', ') }}
                    order by
                        {{ updated_at_column }} desc,
                        _ingested_at desc,
                        _batch_id desc
                ) as _current_record_rank
            from {{ bronze_source(source_table) }}
        )
        where _current_record_rank = 1
    )
{%- endmacro %}
