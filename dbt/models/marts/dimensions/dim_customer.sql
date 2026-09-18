-- 한 행은 고객 거래 실적 등급 버전 1건이다. Materialization은 dbt_project.yml의 table을 따른다.
select
    md5(concat(customer_id, '|', cast(valid_from as varchar), '|', attribute_hash)) as customer_key,
    customer_id,
    membership_tier,
    attribute_hash,
    valid_from,
    effective_from,
    valid_to,
    is_current
from {{ ref('int_customer_history') }}
