"""구독 계약과 Customer 생성의 결정성 계약을 검증한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    MembershipTierRecord,
    address_change_customer_record,
    membership_tier,
    membership_tier_change_records,
    new_customer_record,
    new_subscription_record_for_customer,
    repurchase_customer_record,
    subscription_transition_records,
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
    """신규 고객은 반복 실행에도 같은 사람·주문 계정 식별자를 가진다."""
    first = new_customer_record(_config(), 1)
    second = new_customer_record(_config(), 1)
    another = new_customer_record(_config(), 2)

    assert first == second
    assert first.customer_unique_id != another.customer_unique_id
    assert first.customer_id != another.customer_id


def test_repurchase_and_address_change_preserve_the_person_identity() -> None:
    """재구매·주소 변경은 사람 키를 보존하고 주문 계정만 새로 만든다."""
    first = new_customer_record(_config(), 1)
    second = new_customer_record(_config(), 2)
    repurchase = repurchase_customer_record(_config(), (first, second), 3)
    changed = address_change_customer_record(_config(), first, 2)

    assert repurchase.customer_unique_id in {first.customer_unique_id, second.customer_unique_id}
    assert changed.customer_unique_id == first.customer_unique_id
    assert changed.customer_id != first.customer_id


@pytest.mark.parametrize(
    ("delivered_order_count", "expected"),
    ((0, "BRONZE"), (4, "BRONZE"), (5, "SILVER"), (14, "SILVER"), (15, "GOLD")),
)
def test_membership_tier_matches_the_seed_membership_thresholds(
    delivered_order_count: int, expected: str
) -> None:
    """거래 실적 등급은 Seed와 같은 완료 주문 경계를 사용한다."""
    assert membership_tier(delivered_order_count) == expected


def test_membership_change_updates_one_person_grain_record() -> None:
    """등급 변경은 사람 단위 Record 하나만 갱신 후보로 만든다."""
    old_time = _config().logical_date - timedelta(days=1)
    original = MembershipTierRecord("person-1", "BRONZE", old_time, old_time)
    changed = membership_tier_change_records(_config(), (original,), delivered_order_count=5)

    assert changed[0].membership_tier == "SILVER"
    assert changed[0].updated_at == _config().logical_date


def test_subscription_contract_transitions_preserve_schedule_semantics() -> None:
    """활성 계약은 실패 재시도와 자동갱신 중지 상태로 전이한다."""
    started_at = _config().logical_date - timedelta(days=1)
    active = new_subscription_record_for_customer("person-1", started_at)
    failed_at = _config().logical_date
    failed = subscription_transition_records(
        replace(_config(), logical_date=failed_at), (active,), "PAYMENT_FAILED"
    )[0]
    canceled = subscription_transition_records(
        replace(_config(), logical_date=failed_at + timedelta(days=1)),
        (failed,),
        "CANCEL_REQUESTED",
    )[0]

    assert failed.payment_failed_at == failed_at
    assert failed.next_payment_attempt_at == failed_at + timedelta(days=2)
    assert canceled.auto_renew_enabled is False
    assert canceled.next_payment_attempt_at is None
