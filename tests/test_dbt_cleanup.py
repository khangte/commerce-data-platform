"""dbt 정리 후 주문 항목 보존과 구독 재가입 계산을 검증한다."""

from pathlib import Path

import duckdb
import yaml
from jinja2 import Environment, StrictUndefined

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


def _render(relative_path: str) -> str:
    """격리된 DuckDB 테이블을 참조하는 SQL을 만든다."""
    return Environment(undefined=StrictUndefined).from_string(
        (DBT_DIR / relative_path).read_text(encoding="utf-8")
    ).render(ref=lambda name: name)


def test_order_items_preserve_lines_without_dimensions() -> None:
    """차원 테이블 없이 주문 항목의 금액·구매일과 참조 주문 누락 행을 보존한다."""
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE stg_orders (order_id VARCHAR, purchase_at TIMESTAMP, order_status VARCHAR)")
        connection.execute("INSERT INTO stg_orders VALUES ('o1', '2026-10-01 12:00:00', 'DELIVERED')")
        connection.execute("""
            CREATE TABLE stg_order_items (
                order_id VARCHAR, order_item_id INTEGER, product_id VARCHAR,
                seller_id VARCHAR, price INTEGER, freight_value INTEGER, created_at TIMESTAMP
            )
        """)
        connection.execute("""
            INSERT INTO stg_order_items VALUES
                ('o1', 1, 'p1', 's1', 20, 3, '2026-10-01'),
                ('o1', 2, 'p2', 's2', 10, 2, '2026-10-01'),
                ('missing', 1, 'p3', 's3', 5, 1, '2026-10-01')
        """)
        connection.execute(
            "CREATE TABLE enriched AS " + _render("models/intermediate/int_order_items_enriched.sql")
        )
        assert connection.execute("""
            SELECT order_id, order_item_id, purchase_date_key, line_gross_value, order_status
            FROM enriched ORDER BY order_id, order_item_id
        """).fetchall() == [
            ("missing", 1, None, 6, None),
            ("o1", 1, 20261001, 23, "DELIVERED"),
            ("o1", 2, 20261001, 12, "DELIVERED"),
        ]


def test_rejoined_count_requires_active_after_churn() -> None:
    """TRIAL 제외 계약을 확인하고 이탈 후 활성 전이만 재가입으로 계산한다."""
    schema = yaml.safe_load((DBT_DIR / "models/staging/schema.yml").read_text(encoding="utf-8"))
    model = next(
        item for item in schema["models"]
        if item["name"] == "stg_customer_subscription_observations"
    )
    status_tests = next(
        column for column in model["columns"] if column["name"] == "subscription_status"
    )["data_tests"]
    assert "not_null" in status_tests
    allowed = next(
        test["accepted_values"]["arguments"]["values"] for test in status_tests
        if isinstance(test, dict) and "accepted_values" in test
    )
    assert "TRIAL" not in allowed
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE dim_subscription (
                customer_id VARCHAR, subscription_key VARCHAR,
                valid_from TIMESTAMP, subscription_status VARCHAR
            )
        """)
        connection.execute("""
            INSERT INTO dim_subscription VALUES
                ('c1', 'v1', '2026-10-01', 'ACTIVE'),
                ('c1', 'v2', '2026-10-02', 'CHURNED'),
                ('c1', 'v3', '2026-10-03', 'ACTIVE'),
                ('c1', 'v4', '2026-10-04', 'ACTIVE'),
                ('c2', 'v5', '2026-10-01', 'CHURNED'),
                ('c2', 'v6', '2026-10-03', 'PAYMENT_FAILED')
        """)
        connection.execute(
            "CREATE TABLE funnel AS " + _render("models/marts/metrics/rpt_subscription_funnel_daily.sql")
        )
        assert connection.execute(
            "SELECT event_date_key, rejoined_count FROM funnel ORDER BY event_date_key"
        ).fetchall() == [(20261001, 0), (20261002, 0), (20261003, 1)]


def test_order_items_staging_keeps_one_row_per_line() -> None:
    """재추출된 주문 항목은 최신 수집 관측 하나만 남기고 컬럼 계약을 유지한다."""
    environment = Environment(undefined=StrictUndefined)
    environment.globals["bronze_source"] = lambda table: table
    macros = environment.from_string(
        (DBT_DIR / "macros/current_bronze_records.sql").read_text(encoding="utf-8")
    ).module
    sql = environment.from_string(
        (DBT_DIR / "models/staging/stg_order_items.sql").read_text(encoding="utf-8")
    ).render(current_bronze_records=macros.current_bronze_records)
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE order_items (
                order_id VARCHAR, order_item_id INTEGER, product_id VARCHAR,
                seller_id VARCHAR, shipping_limit_date TIMESTAMP, price DECIMAL(14,2),
                freight_value DECIMAL(14,2), created_at TIMESTAMP,
                _batch_id VARCHAR, _run_id VARCHAR, _ingested_at TIMESTAMP,
                _source_table VARCHAR, _schema_version INTEGER
            )
        """)
        connection.execute("""
            INSERT INTO order_items VALUES
                ('o1', 1, 'p1', 's1', '2026-10-10', 20, 3, '2026-10-01',
                 'b1', 'r1', '2026-10-02', 'order_items', 1),
                ('o1', 1, 'p1', 's1', '2026-10-10', 20, 3, '2026-10-01',
                 'b2', 'r2', '2026-10-05', 'order_items', 1),
                ('o1', 2, 'p2', 's2', '2026-10-10', 10, 2, '2026-10-01',
                 'b1', 'r1', '2026-10-02', 'order_items', 1),
                ('o2', 1, 'p3', 's3', '2026-10-10', 5, 1, '2026-10-03',
                 'b1', 'r1', '2026-10-04', 'order_items', 1)
        """)
        cursor = connection.execute(sql)
        column_names = [column[0] for column in cursor.description]
        connection.execute("CREATE TABLE staged AS " + sql)
        rows = connection.execute("""
            SELECT order_id, order_item_id, _batch_id
            FROM staged ORDER BY order_id, order_item_id
        """).fetchall()
    assert rows == [("o1", 1, "b2"), ("o1", 2, "b1"), ("o2", 1, "b1")]
    assert column_names == [
        "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_at",
        "price", "freight_value", "created_at", "_batch_id", "_run_id", "_ingested_at",
        "_source_table", "_schema_version",
    ]
