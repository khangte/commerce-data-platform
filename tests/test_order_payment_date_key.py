"""주문 결제의 구매일 키와 결제 행·증분 범위 보존을 격리된 DuckDB에서 검증한다."""

from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from jinja2 import Environment, StrictUndefined

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


def _render_model(relative_path: str, *, incremental: bool = False) -> str:
    """격리된 테이블을 참조하도록 dbt SQL의 참조와 증분 조건을 렌더링한다."""
    return (
        Environment(undefined=StrictUndefined)
        .from_string((DBT_DIR / relative_path).read_text(encoding="utf-8"))
        .render(
            ref=lambda name: name,
            config=lambda **kwargs: "",
            is_incremental=lambda: incremental,
        )
    )


@pytest.fixture
def payment_warehouse() -> Iterator[duckdb.DuckDBPyConnection]:
    """운영 파일을 열지 않고 다중 결제와 UTC 날짜 경계의 주문을 준비한다."""
    with duckdb.connect() as connection:
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("CREATE TABLE stg_orders (order_id VARCHAR, purchase_at TIMESTAMPTZ)")
        connection.execute(
            """
            INSERT INTO stg_orders VALUES
                ('order-a', '2026-09-30 00:30:00+09'),
                ('order-b', '2026-09-30 23:30:00-03')
            """
        )
        connection.execute(
            """
            CREATE TABLE stg_payments (
                order_id VARCHAR, payment_sequence INTEGER, payment_status VARCHAR,
                payment_value DECIMAL(14,2), payment_completed_at TIMESTAMPTZ
            )
            """
        )
        connection.execute(
            """
            INSERT INTO stg_payments VALUES
                ('order-a', 1, 'COMPLETED', 40, NULL),
                ('order-a', 2, 'REFUNDED', 10, '2026-10-12 00:00:00+00'),
                ('order-b', 1, 'FAILED', 20, NULL)
            """
        )
        connection.execute("CREATE TABLE int_affected_order_keys (order_id VARCHAR)")
        connection.execute("INSERT INTO int_affected_order_keys VALUES ('order-a')")
        yield connection


def _prepare_enriched_payments(connection: duckdb.DuckDBPyConnection) -> None:
    """실제 Intermediate SQL로 결제 Fact 입력을 만든다."""
    connection.execute(
        "CREATE TABLE int_order_payments_enriched AS "
        + _render_model("models/intermediate/int_order_payments_enriched.sql")
    )


def test_payment_purchase_date_uses_order_day_and_preserves_lines(
    payment_warehouse: duckdb.DuckDBPyConnection,
) -> None:
    """결제 사건 시각 유무와 무관하게 UTC 주문일과 기존 결제 Grain·금액을 보존한다."""
    _prepare_enriched_payments(payment_warehouse)
    result = payment_warehouse.execute(
        _render_model("models/marts/facts/fct_order_payment.sql")
        + " ORDER BY order_id, payment_sequence"
    )
    assert [column[0] for column in result.description] == [
        "order_id",
        "payment_sequence",
        "purchase_date_key",
        "payment_status",
        "payment_value",
    ]
    assert result.fetchall() == [
        ("order-a", 1, 20260929, "COMPLETED", 40),
        ("order-a", 2, 20260929, "REFUNDED", 10),
        ("order-b", 1, 20261001, "FAILED", 20),
    ]


def test_payment_without_order_remains_visible_to_null_key_gate(
    payment_warehouse: duckdb.DuckDBPyConnection,
) -> None:
    """참조 주문이 없는 결제를 조인으로 숨기지 않고 NULL 구매일로 남긴다."""
    payment_warehouse.execute(
        "INSERT INTO stg_payments VALUES ('missing-order', 1, 'PENDING', 5, NULL)"
    )
    _prepare_enriched_payments(payment_warehouse)
    assert payment_warehouse.execute(
        "SELECT purchase_date_key FROM int_order_payments_enriched WHERE order_id = 'missing-order'"
    ).fetchall() == [(None,)]


def test_incremental_payment_projection_keeps_all_affected_order_lines(
    payment_warehouse: duckdb.DuckDBPyConnection,
) -> None:
    """증분 결제 Fact는 영향 주문의 모든 결제 행을 투영하고 다른 주문은 제외한다."""
    _prepare_enriched_payments(payment_warehouse)
    assert payment_warehouse.execute(
        _render_model("models/marts/facts/fct_order_payment.sql", incremental=True)
        + " ORDER BY order_id, payment_sequence"
    ).fetchall() == [
        ("order-a", 1, 20260929, "COMPLETED", 40),
        ("order-a", 2, 20260929, "REFUNDED", 10),
    ]


def test_payment_date_singular_gate_detects_order_date_mismatch(
    payment_warehouse: duckdb.DuckDBPyConnection,
) -> None:
    """구매일 일치 Test가 정상 결제를 통과시키고 어긋난 결제 순번을 찾아낸다."""
    _prepare_enriched_payments(payment_warehouse)
    payment_warehouse.execute(
        "CREATE TABLE fct_order_payment AS "
        + _render_model("models/marts/facts/fct_order_payment.sql")
    )
    payment_warehouse.execute(
        "CREATE TABLE fct_order (order_id VARCHAR, purchase_date_key INTEGER)"
    )
    payment_warehouse.execute(
        "INSERT INTO fct_order VALUES ('order-a', 20260929), ('order-b', 20261001)"
    )
    gate_sql = _render_model("tests/fct_order_payment_date_matches_order.sql")
    assert payment_warehouse.execute(gate_sql).fetchall() == []
    payment_warehouse.execute(
        "UPDATE fct_order_payment SET purchase_date_key = 20260930 "
        "WHERE order_id = 'order-a' AND payment_sequence = 2"
    )
    assert payment_warehouse.execute(gate_sql).fetchall() == [("order-a", 2, 20260930, 20260929)]
