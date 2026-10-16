-- depends_on: {{ ref('int_affected_order_keys') }}
{{
    config(
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

-- Olist 원본 order_items에는 상품(항목)별 상태 데이터가 없고 취소는 orders.order_status로만 표현된다. 원본 구조를 유지하므로 항목별 상태는 관리하지 않고 소속 주문 상태를 상속한다.
-- dbt unique_key는 교체 단위인 주문이다. Grain 유일성은 tests/fct_order_item_unique.sql이 강제한다.
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    purchase_date_key,
    order_status,
    price as item_price,
    freight_value,
    line_gross_value
from {{ ref('int_order_items_enriched') }}
{% if is_incremental() %}
where order_id in (select order_id from {{ ref('int_affected_order_keys') }})
{% endif %}
