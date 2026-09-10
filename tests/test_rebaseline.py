"""구독·등급 전환 재기준화의 Source 범위를 검증한다."""

from __future__ import annotations

from datetime import datetime, timezone

from src.ingestion.service import TableIngestionResult
from src.rebaseline import LEGACY_SOURCE_TABLES, SOURCE_TABLES, _ingest_baseline


def test_rebaseline_replaces_the_legacy_membership_table() -> None:
    """재기준화가 구 Membership Table을 명시적으로 제거하도록 계약을 고정한다."""
    assert LEGACY_SOURCE_TABLES == ("customer_memberships",)
    assert "customer_memberships" not in SOURCE_TABLES
    assert set(SOURCE_TABLES) == {
        "customers",
        "customer_subscriptions",
        "customer_membership_tiers",
        "subscription_payments",
        "products",
        "sellers",
        "orders",
        "order_items",
        "order_payments",
    }


def test_rebaseline_allows_an_empty_subscription_payment_baseline(monkeypatch) -> None:
    """결제 이력이 없는 초기 기준은 Bronze Object 없이도 성공으로 처리한다."""
    recorded_row_counts: list[tuple[str, int]] = []

    def fake_ingest_table(*args, **kwargs) -> TableIngestionResult:
        """결제 이력만 비어 있는 초기 적재 결과를 반환한다."""
        source_table = args[2].source_table
        status = "SUCCESS_NO_DATA" if source_table == "subscription_payments" else "SUCCESS"
        row_count = 0 if status == "SUCCESS_NO_DATA" else 1
        recorded_row_counts.append((source_table, row_count))
        return TableIngestionResult(run=None, status=status, row_count=row_count)  # type: ignore[arg-type]

    monkeypatch.setattr("src.rebaseline.assert_source_mutation_lease", lambda *args: None)
    monkeypatch.setattr("src.rebaseline.ingest_table", fake_ingest_table)

    result = _ingest_baseline(
        postgres=None,  # type: ignore[arg-type]
        storage=None,  # type: ignore[arg-type]
        logical_date=datetime(2026, 9, 3, tzinfo=timezone.utc),
        lease=None,  # type: ignore[arg-type]
    )

    assert result["subscription_payments"] == 0
    assert dict(recorded_row_counts) == result
