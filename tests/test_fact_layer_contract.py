"""Fact 계층이 집계·파생 책임을 Intermediate에 위임하는지 검증한다."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _read_model(relative_path: str) -> str:
    """프로젝트 루트 기준 dbt Model SQL을 반환한다."""
    return (PROJECT_ROOT / relative_path).read_text()


def test_fact_orders_projects_the_fact_ready_intermediate() -> None:
    """주문 Fact가 집계·날짜·배송 파생을 Intermediate 결과로만 받는지 검증한다."""
    sql = _read_model("dbt/models/marts/facts/fact_orders.sql").lower()

    assert "ref('int_order_fact_ready')" in sql
    assert "group by" not in sql
    assert "sum(" not in sql
    assert "date_diff(" not in sql
    assert "date_trunc(" not in sql
    assert "coalesce(" not in sql


def test_subscription_payment_fact_projects_the_enriched_intermediate() -> None:
    """구독 결제 Fact가 고객 SCD2 시점 결합을 Intermediate에 위임하는지 검증한다."""
    sql = _read_model("dbt/models/marts/facts/fact_subscription_payments.sql").lower()

    assert "ref('int_subscription_payments_enriched')" in sql
    assert "left join" not in sql
    assert "coalesce(" not in sql
