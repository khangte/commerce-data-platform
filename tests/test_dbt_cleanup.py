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
        connection.execute("CREATE TABLE stg_orders (order_id VARCHAR, purchase_at TIMESTAMP)")
        connection.execute("INSERT INTO stg_orders VALUES ('o1', '2026-10-01 12:00:00')")
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
            SELECT order_id, order_item_id, purchase_date_key, line_gross_value
            FROM enriched ORDER BY order_id, order_item_id
        """).fetchall() == [("missing", 1, None, 6), ("o1", 1, 20261001, 23), ("o1", 2, 20261001, 12)]


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
