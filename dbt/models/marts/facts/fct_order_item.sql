-- depends_on: {{ ref('int_affected_order_keys') }}
{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

-- dbt unique_key는 교체 단위인 주문이다. Grain 유일성은 tests/fct_order_item_unique.sql이 강제한다.
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price as item_price,
    freight_value,
    line_gross_value
from {{ ref('int_order_items_enriched') }}
{% if is_incremental() %}
where order_id in (select order_id from {{ ref('int_affected_order_keys') }})
{% endif %}
