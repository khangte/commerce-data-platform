"""Phase 7 품질 Gate가 요구하는 dbt Test 선언을 검증한다."""

from __future__ import annotations

from pathlib import Path

import yaml

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
FACT_SCHEMA = DBT_DIR / "models/marts/facts/schema.yml"
DBT_PROFILE = DBT_DIR / "profiles.yml"


def _tests(model: str, column: str) -> list:
    """지정한 모델 Column의 data_tests를 반환한다."""
    schema = yaml.safe_load(FACT_SCHEMA.read_text(encoding="utf-8"))
    entry = next(item for item in schema["models"] if item["name"] == model)
    return next(item for item in entry["columns"] if item["name"] == column).get("data_tests", [])


def _named(tests: list, name: str) -> dict:
    """이름이 일치하는 설정형 Test를 반환한다."""
    return next(test[name] for test in tests if isinstance(test, dict) and name in test)


def test_status_domains_and_foreign_keys_are_declared() -> None:
    """주문·결제 상태 도메인 및 Fact FK 선언을 확인한다."""
    assert "not_null" in _tests("fct_order", "order_status")
    assert _named(_tests("fct_order", "order_status"), "accepted_values")["arguments"]["values"] == [
        "CREATED", "APPROVED", "PROCESSING", "INVOICED", "SHIPPED", "DELIVERED", "CANCELED", "UNAVAILABLE"
    ]
    assert _named(_tests("fct_order_item", "order_id"), "relationships")["arguments"]["to"] == "ref('fct_order')"
    assert _named(_tests("fct_order_item", "purchase_date_key"), "relationships")[
        "arguments"
    ] == {"to": "ref('dim_date')", "field": "date_key"}
    assert _named(_tests("fct_subscription_payment", "payment_status"), "accepted_values")[
        "arguments"
    ]["values"] == ["completed", "failed"]


def test_non_negative_and_singular_gate_tests_exist() -> None:
    """금액 범위와 Publish Gate에 필요한 모든 SQL Test 파일을 확인한다."""
    for model, column in (("fct_order_item", "item_price"), ("fct_order_item", "freight_value"), ("fct_order_payment", "payment_value"), ("fct_subscription_payment", "payment_value")):
        assert "non_negative" in _tests(model, column)
    for path in (
        "generic/non_negative.sql",
        "stg_orders_timestamp_order.sql",
        "fct_order_item_total_matches_order.sql",
        "fct_order_item_date_matches_order.sql",
        "publish_gate_canary.sql",
    ):
        assert (DBT_DIR / "tests" / path).is_file()
    assert "var('publish_gate_canary', false)" in (DBT_DIR / "tests/publish_gate_canary.sql").read_text(encoding="utf-8")


def test_mart_build_session_timezone_is_utc() -> None:
    """Mart의 날짜·대체 키는 실행 환경과 무관하게 UTC 기준으로 계산한다."""
    profile = yaml.safe_load(DBT_PROFILE.read_text(encoding="utf-8"))
    settings = profile["commerce_data_platform"]["outputs"]["dev"]["settings"]
    assert settings["TimeZone"] == "UTC"
