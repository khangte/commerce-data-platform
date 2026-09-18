"""Fact가 영향 Key로만 재계산하고 교체 단위를 올바르게 선언하는지 검증한다."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

ORDER_AXIS_FACTS = (
    "dbt/models/marts/facts/fact_orders.sql",
    "dbt/models/marts/facts/fact_order_items.sql",
    "dbt/models/marts/facts/fact_payments.sql",
)


def test_order_axis_facts_filter_by_affected_order_keys() -> None:
    """주문 축 Fact는 Incremental 실행에서 영향 주문만 다시 계산한다."""
    for relative_path in ORDER_AXIS_FACTS:
        sql = (PROJECT_ROOT / relative_path).read_text()

        assert "is_incremental()" in sql, relative_path
        assert "ref('int_affected_order_keys')" in sql, relative_path


def test_order_axis_facts_replace_whole_orders() -> None:
    """자식 Fact가 주문 단위로 교체되어야 사라진 행이 남지 않는다."""
    for relative_path in ORDER_AXIS_FACTS:
        sql = (PROJECT_ROOT / relative_path).read_text()

        assert "unique_key='order_id'" in sql, relative_path
        assert "order_item_id'," not in sql, relative_path


def test_subscription_payment_fact_filters_by_affected_payment_keys() -> None:
    """구독 결제 Fact는 영향 결제 시도만 다시 계산한다."""
    sql = (PROJECT_ROOT / "dbt/models/marts/facts/fact_subscription_payments.sql").read_text()

    assert "is_incremental()" in sql
    assert "ref('int_affected_subscription_payment_keys')" in sql
    assert "unique_key='payment_id'" in sql
