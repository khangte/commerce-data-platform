-- Product Dashboard는 주문 상품 항목 Grain만 사용한다.
select
    dim_product.category_name,
    sum(fct_order_item.line_gross_value) as category_gmv
from facts.fct_order_item
inner join dimensions.dim_product using (product_id)
inner join dimensions.dim_date on fct_order_item.purchase_date_key = dim_date.date_key
group by dim_product.category_name;

select product_id, sum(line_gross_value) as product_gmv
from facts.fct_order_item
group by product_id
order by product_gmv desc
limit 10;

select product_id, count(*) as sales_volume
from facts.fct_order_item
group by product_id
order by sales_volume desc
limit 10;
