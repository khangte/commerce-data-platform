"""구독 자동결제 계획의 결정성과 도메인 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.subscription_payments import (
    MONTHLY_SUBSCRIPTION_FEE,
    SubscriptionPaymentRecord,
    plan_subscription_payment,
)


def _config(seed: int = 42) -> GeneratorConfig:
    """구독 결제 계획에 사용할 고정 Generator Config를 반환한다."""
    return GeneratorConfig(
        source_snapshot_id="seed:abc123",
        random_seed=seed,
        logical_date=datetime(2026, 9, 10, tzinfo=UTC),
        order_count=10,
        anomaly_profile="subscription-active",
        generator_version=GENERATOR_VERSION,
    )


def test_plan_is_deterministic_for_the_same_inputs() -> None:
    """같은 결정성 입력은 같은 결제 상태와 금액을 만든다."""
    start = datetime(2026, 9, 1, tzinfo=UTC)
    first = plan_subscription_payment(_config(), "person-1", 1, start)
    second = plan_subscription_payment(_config(), "person-1", 1, start)

    assert first == second
    assert first.payment_value == MONTHLY_SUBSCRIPTION_FEE
    assert first.billing_period_end == start + timedelta(days=30)
    assert first.payment_status in {"completed", "failed"}


def test_plan_varies_by_billing_sequence() -> None:
    """결제 순번이 다르면 성공·실패 판정 입력이 달라진다."""
    start = datetime(2026, 9, 1, tzinfo=UTC)
    statuses = {
        plan_subscription_payment(_config(), "person-1", sequence, start).payment_status
        for sequence in range(1, 40)
    }

    assert statuses == {"completed", "failed"}


def test_record_rejects_a_non_positive_billing_sequence() -> None:
    """billing_sequence는 1부터 시작해야 한다."""
    start = datetime(2026, 9, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="billing_sequence must start at one"):
        SubscriptionPaymentRecord(
            customer_unique_id="person-1",
            billing_sequence=0,
            payment_status="completed",
            payment_value=Decimal("29.90"),
            billing_period_start=start,
            billing_period_end=start + timedelta(days=30),
            created_at=start,
            updated_at=start,
        )


def test_record_rejects_an_unknown_payment_status() -> None:
    """구독 결제 상태 도메인은 completed와 failed 둘로만 좁힌다."""
    start = datetime(2026, 9, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="Unsupported payment_status"):
        SubscriptionPaymentRecord(
            customer_unique_id="person-1",
            billing_sequence=1,
            payment_status="refunded",
            payment_value=Decimal("29.90"),
            billing_period_start=start,
            billing_period_end=start + timedelta(days=30),
            created_at=start,
            updated_at=start,
        )
