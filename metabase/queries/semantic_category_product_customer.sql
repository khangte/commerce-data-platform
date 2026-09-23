-- 카테고리 GMV: 상품 항목 Grain을 유지하고 주문 Fact와 결합하지 않는다.
select
    dim_product.category_name,
    sum(fct_order_item.line_gross_value) as category_gmv
from facts.fct_order_item
inner join dimensions.dim_product using (product_id)
group by dim_product.category_name;

-- 상품 판매 수량: 한 행이 주문 상품 항목 하나이므로 행 수를 센다.
select
    product_id,
    count(*) as sales_volume
from facts.fct_order_item
group by product_id;

-- 고객 수: 이미 distinct로 접힌 비가산 값을 다시 합산하지 않는다.
select
    membership_tier,
    customer_count
from metrics.rpt_membership_tier_performance;
