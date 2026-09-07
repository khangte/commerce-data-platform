"""결정적 Business ID와 논리 Hash 유틸리티를 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from src.generator.ids import deterministic_uuid, logical_hash


def test_deterministic_uuid_is_stable_and_changes_with_business_input() -> None:
    """동일 입력은 같은 UUIDv5를 만들고 다른 입력은 다른 ID를 만든다."""
    first = deterministic_uuid("order", "seed:abc123", 42, 1)
    second = deterministic_uuid("order", "seed:abc123", 42, 1)
    changed = deterministic_uuid("order", "seed:abc123", 42, 2)

    assert first == second
    assert first != changed


def test_logical_hash_normalizes_mapping_order_and_timestamp_offset() -> None:
    """같은 논리 값의 Mapping 순서와 Offset 표현은 Hash에 영향을 주지 않는다."""
    first = {"created_at": datetime(2026, 9, 4, tzinfo=UTC), "order_id": "order-1"}
    second = {
        "order_id": "order-1",
        "created_at": datetime(2026, 9, 4, 9, tzinfo=timezone(timedelta(hours=9))),
    }

    assert logical_hash(first) == logical_hash(second)


def test_deterministic_uuid_rejects_naive_datetime() -> None:
    """Timezone 없는 시각은 결정적 ID 입력으로 허용하지 않는다."""
    with pytest.raises(ValueError, match="UTC offset"):
        deterministic_uuid("order", datetime.fromisoformat("2026-09-04T00:00:00"))
