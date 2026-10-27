"""Fact 계층이 집계·파생 책임을 Intermediate에 위임하는지 검증한다."""

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _read_model(relative_path: str) -> str:
    """프로젝트 루트 기준 dbt Model SQL을 반환한다."""
    return (PROJECT_ROOT / relative_path).read_text()


def test_fct_order_projects_the_fact_ready_intermediate() -> None:
    """주문 Fact가 집계·날짜·배송 파생을 Intermediate 결과로만 받는지 검증한다."""
    sql = _read_model("dbt/models/marts/facts/fct_order.sql").lower()

    assert "ref('int_order_fact_ready')" in sql
    assert "group by" not in sql
    assert "sum(" not in sql
    assert "date_diff(" not in sql
    assert "date_trunc(" not in sql
    assert "coalesce(" not in sql


def test_subscription_payment_fact_projects_the_enriched_intermediate() -> None:
    """구독 결제 Fact가 고객 SCD2 시점 결합을 Intermediate에 위임하는지 검증한다."""
    sql = _read_model("dbt/models/marts/facts/fct_subscription_payment.sql").lower()

    assert "ref('int_subscription_payments_enriched')" in sql
    assert "left join" not in sql
    assert "coalesce(" not in sql


def test_fct_order_exposes_purchase_timestamp() -> None:
    """주문 Fact가 구매 시각을 그대로 투영하고 필수값으로 검증한다."""
    fact = _read_model("dbt/models/marts/facts/fct_order.sql").lower()
    ready = _read_model("dbt/models/intermediate/int_order_fact_ready.sql").lower()
    schema = yaml.safe_load(_read_model("dbt/models/marts/facts/schema.yml"))
    columns = next(m for m in schema["models"] if m["name"] == "fct_order")["columns"]
    assert "purchase_at," in fact
    assert "int_orders_enriched.purchase_at," in ready
    assert "not_null" in next(c for c in columns if c["name"] == "purchase_at")["data_tests"]
