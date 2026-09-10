"""Phase 2 Customer 시나리오의 결정성 계약을 검증한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    MembershipTierRecord,
    SubscriptionRecord,
    address_change_customer_record,
    membership_tier,
    membership_tier_change_records,
    new_customer_record,
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


def _tier_record(updated_at: datetime, tier: str = "BRONZE") -> MembershipTierRecord:
    """등급 축 테스트용 사람 단위 Record를 만든다."""
    return MembershipTierRecord(
        customer_unique_id="person-1",
        membership_tier=tier,
        created_at=updated_at,
        updated_at=updated_at,
    )


def _subscription_record(
    updated_at: datetime,
    *,
    status: str = "NON_MEMBER",
    trial_ends_at: datetime | None = None,
    benefit_ends_at: datetime | None = None,
    next_billing_at: datetime | None = None,
    payment_failed_at: datetime | None = None,
    cancel_requested_at: datetime | None = None,
    created_at: datetime | None = None,
) -> SubscriptionRecord:
    """구독 축 테스트용 사람 단위 Record를 만든다."""
    return SubscriptionRecord(
        customer_unique_id="person-1",
        subscription_status=status,
        trial_ends_at=trial_ends_at,
        benefit_ends_at=benefit_ends_at,
        next_billing_at=next_billing_at,
        payment_failed_at=payment_failed_at,
        cancel_requested_at=cancel_requested_at,
        created_at=created_at or updated_at,
        updated_at=updated_at,
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
    assert first.created_at == _config().logical_date


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
        first if purchase_from_original.customer_unique_id == first.customer_unique_id else second
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
    ((0, "BRONZE"), (4, "BRONZE"), (5, "SILVER"), (14, "SILVER"), (15, "GOLD")),
)
def test_membership_tier_matches_the_seed_membership_thresholds(
    delivered_order_count: int, expected: str
) -> None:
    """Synthetic Membership은 Seed와 같은 완료 주문 기준을 사용한다."""
    assert membership_tier(delivered_order_count) == expected


def test_membership_change_updates_one_person_grain_record() -> None:
    """Membership 등급 변경은 사람 단위 Record 하나만 갱신 후보로 만든다."""
    old_time = _config().logical_date - timedelta(days=1)
    original = _tier_record(old_time)

    changed = membership_tier_change_records(_config(), (original,), delivered_order_count=5)

    assert [record.customer_unique_id for record in changed] == ["person-1"]
    assert {record.membership_tier for record in changed} == {"SILVER"}
    assert changed[0].updated_at == _config().logical_date
    assert changed[0].created_at == original.created_at


def test_membership_change_rejects_a_non_increasing_mutation_time() -> None:
    """기존 등급을 바꾸는 시각은 이전 updated_at보다 반드시 커야 한다."""
    current = _tier_record(_config().logical_date)

    with pytest.raises(ValueError, match="logical_date"):
        membership_tier_change_records(_config(), (current,), delivered_order_count=5)


def test_subscription_transitions_enforce_lifecycle_and_set_operational_times() -> None:
    """구독 전이는 허용된 순서를 따르고 상태별 시각을 결정적으로 만든다."""
    old_time = _config().logical_date - timedelta(days=1)
    initial = _subscription_record(old_time)

    trial = subscription_transition_records(_config(), (initial,), "TRIAL")[0]
    active = subscription_transition_records(
        replace(_config(), logical_date=_config().logical_date + timedelta(days=1)),
        (trial,),
        "ACTIVE",
    )[0]
    failed_at = _config().logical_date + timedelta(days=2)
    failed = subscription_transition_records(
        replace(_config(), logical_date=failed_at), (active,), "PAYMENT_FAILED"
    )[0]

    assert trial.trial_ends_at == _config().logical_date + timedelta(days=30)
    assert active.next_billing_at == _config().logical_date + timedelta(days=31)
    assert failed.payment_failed_at == failed_at
    assert failed.benefit_ends_at == failed_at + timedelta(days=7)
    with pytest.raises(ValueError, match="Unsupported subscription transition"):
        subscription_transition_records(_config(), (initial,), "CHURNED")


def test_churned_customer_can_rejoin_but_cannot_return_to_non_member() -> None:
    """해지 고객의 재가입은 ACTIVE 전이로 남고 NON_MEMBER 복귀는 거부한다."""
    churned_at = _config().logical_date - timedelta(days=1)
    churned = _subscription_record(
        churned_at,
        status="CHURNED",
        benefit_ends_at=churned_at,
        cancel_requested_at=churned_at - timedelta(days=7),
        created_at=churned_at - timedelta(days=30),
    )

    rejoined = subscription_transition_records(_config(), (churned,), "ACTIVE")[0]

    assert rejoined.subscription_status == "ACTIVE"
    with pytest.raises(ValueError, match="Unsupported subscription transition"):
        subscription_transition_records(_config(), (churned,), "NON_MEMBER")
