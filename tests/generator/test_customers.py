"""Phase 2 Customer 시나리오의 결정성 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    CustomerAddress,
    CustomerRecord,
    address_change_customer_record,
    membership_change_records,
    membership_level,
    new_customer_record,
    repurchase_customer_record,
)


def _config() -> GeneratorConfig:
    """Customer 시나리오에 사용할 고정 Generator Config를 반환한다."""
    return GeneratorConfig(
        source_snapshot_id="seed:abc123",
        random_seed=42,
        logical_date=datetime(2026, 9, 4, tzinfo=UTC),
        order_count=10,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


def test_new_customer_record_uses_stable_person_and_order_record_ids() -> None:
    """신규 고객은 반복 실행에도 같은 ID와 주문 시점 Record를 만든다."""
    first = new_customer_record(_config(), 1)
    second = new_customer_record(_config(), 1)
    another = new_customer_record(_config(), 2)

    assert first == second
    assert len(first.customer_unique_id) == 32
    assert len(first.customer_id) == 32
    assert first.customer_unique_id != another.customer_unique_id
    assert first.customer_id != another.customer_id
    assert first.membership_level == "bronze"
    assert first.created_at == first.updated_at == _config().logical_date


def test_repurchase_uses_existing_person_with_a_new_customer_record() -> None:
    """재구매는 정렬되지 않은 입력에서도 같은 인물과 새 Record를 선택한다."""
    first = new_customer_record(_config(), 1)
    second = new_customer_record(_config(), 2)

    purchase_from_original = repurchase_customer_record(_config(), (first, second), 3)
    purchase_from_reversed = repurchase_customer_record(_config(), (second, first), 3)

    assert purchase_from_original == purchase_from_reversed
    assert purchase_from_original.customer_unique_id in {
        first.customer_unique_id,
        second.customer_unique_id,
    }
    assert purchase_from_original.customer_id not in {first.customer_id, second.customer_id}
    selected = (
        first
        if purchase_from_original.customer_unique_id == first.customer_unique_id
        else second
    )
    assert purchase_from_original.address == selected.address


def test_address_change_creates_a_new_record_without_mutating_history() -> None:
    """주소 변경은 동일 인물의 새 Record에만 반영한다."""
    existing = new_customer_record(_config(), 1)

    changed = address_change_customer_record(_config(), existing, 2)

    assert changed.customer_unique_id == existing.customer_unique_id
    assert changed.customer_id != existing.customer_id
    assert changed.address != existing.address
    assert existing == new_customer_record(_config(), 1)


@pytest.mark.parametrize(
    ("delivered_order_count", "expected"),
    ((0, "bronze"), (4, "bronze"), (5, "silver"), (14, "silver"), (15, "gold")),
)
def test_membership_level_matches_the_seed_membership_thresholds(
    delivered_order_count: int, expected: str
) -> None:
    """Synthetic Membership은 Seed와 같은 완료 주문 기준을 사용한다."""
    assert membership_level(delivered_order_count) == expected


def test_membership_change_updates_every_record_for_one_person() -> None:
    """Membership 변경은 동일 인물의 과거·신규 Record를 모두 같은 Transaction 후보로 만든다."""
    old_time = _config().logical_date - timedelta(days=1)
    newer_time = old_time + timedelta(hours=1)
    original = CustomerRecord(
        customer_id="customer-1",
        customer_unique_id="person-1",
        address=CustomerAddress("sao paulo", "SP"),
        membership_level="bronze",
        created_at=old_time,
        updated_at=old_time,
    )
    newer = CustomerRecord(
        customer_id="customer-2",
        customer_unique_id="person-1",
        address=CustomerAddress("curitiba", "PR"),
        membership_level="bronze",
        created_at=newer_time,
        updated_at=newer_time,
    )

    changed = membership_change_records(_config(), (newer, original), delivered_order_count=5)

    assert [record.customer_id for record in changed] == ["customer-1", "customer-2"]
    assert {record.membership_level for record in changed} == {"silver"}
    assert changed[0].updated_at == _config().logical_date
    assert changed[1].updated_at == _config().logical_date
    assert changed[0].created_at == original.created_at


def test_membership_change_rejects_a_non_increasing_mutation_time() -> None:
    """기존 Membership을 바꾸는 시각은 이전 updated_at보다 반드시 커야 한다."""
    current = new_customer_record(_config(), 1)

    with pytest.raises(ValueError, match="logical_date"):
        membership_change_records(_config(), (current,), delivered_order_count=5)
