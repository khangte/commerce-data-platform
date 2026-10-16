"""상품 지표의 배송 완료 범위와 카테고리·주문 GMV 일치를 검증한다."""

import json
import re
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("source", ["dashboard_product", "semantic_category_product_customer", 49, 50, 51])
def test_product_metrics_count_only_delivered_items(source: str | int) -> None:
    """취소·진행 주문 항목을 제외하고 여러 항목의 배송 완료 주문 합계를 보존한다."""
    if isinstance(source, int):
        card = json.loads((ROOT / f"metabase/export/card-{source}.json").read_text())
        sql = re.sub(r"\[\[.*?\]\]", "", card["dataset_query"]["stages"][0]["native"])
    else:
        sql = (ROOT / f"metabase/queries/{source}.sql").read_text()
    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA facts; CREATE SCHEMA dimensions; CREATE SCHEMA metrics")
        connection.execute("""
            CREATE TABLE facts.fct_order_item AS
            SELECT * FROM (VALUES
                ('p1', 20261001, 'DELIVERED', 10.00),
                ('p1', 20261001, 'DELIVERED', 20.00),
                ('p1', 20261001, 'CANCELED', 100.00),
                ('p2', 20261001, 'PROCESSING', 200.00)
            ) AS t(product_id, purchase_date_key, order_status, line_gross_value);
            CREATE TABLE dimensions.dim_product AS
                SELECT * FROM (VALUES ('p1', 'category1'), ('p2', 'category2'))
                AS t(product_id, category_name);
            CREATE TABLE dimensions.dim_date AS SELECT 20261001 AS date_key;
            CREATE TABLE metrics.rpt_membership_tier_performance
                (membership_tier VARCHAR, customer_count INTEGER);
            CREATE TABLE facts.fct_order AS
                SELECT 'DELIVERED' AS order_status, 30.00 AS gross_order_value
                UNION ALL SELECT 'CANCELED', 100.00
                UNION ALL SELECT 'PROCESSING', 200.00;
        """)
        queries = [part for part in sql.split(";") if part.strip()]
        if isinstance(source, int):
            expected = 2 if source == 51 else Decimal("30.00")
            assert connection.execute(queries[0]).fetchall() == [("p1" if source != 49 else "category1", expected)]
        else:
            category_rows = connection.execute(queries[0]).fetchall()
            sales_gmv = connection.execute(
                "SELECT sum(gross_order_value) FROM facts.fct_order WHERE order_status = 'DELIVERED'"
            ).fetchone()[0]
            assert category_rows == [("category1", sales_gmv)]
            if source == "dashboard_product":
                assert connection.execute(queries[1]).fetchall() == [("p1", Decimal("30.00"))]
                assert connection.execute(queries[2]).fetchall() == [("p1", 2)]
            else:
                assert connection.execute(queries[1]).fetchall() == [("p1", 2)]
