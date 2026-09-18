"""영향 Key 감사 Table의 스키마 전환을 검증한다."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_record_macro_declares_the_domain_schema() -> None:
    """감사 Table이 도메인과 Entity Key 컬럼을 갖는다."""
    macro = (PROJECT_ROOT / "dbt/macros/record_affected_keys.sql").read_text()

    assert "affected_domain VARCHAR NOT NULL" in macro
    assert "entity_key VARCHAR NOT NULL" in macro
    assert "order_id VARCHAR NOT NULL" not in macro


def test_record_macro_drops_the_legacy_table() -> None:
    """구 스키마 Table이 남아 있으면 감지해서 버린다."""
    macro = (PROJECT_ROOT / "dbt/macros/record_affected_keys.sql").read_text()

    assert "column_name = 'order_id'" in macro
    assert "DROP TABLE control.affected_keys" in macro


def test_record_macro_writes_both_domains() -> None:
    """주문 축과 구독 결제 축을 모두 기록한다."""
    macro = (PROJECT_ROOT / "dbt/macros/record_affected_keys.sql").read_text()

    assert "'order', order_id" in macro
    assert "'subscription_payment', payment_id" in macro
