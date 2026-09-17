"""구독 자동결제 계획의 결정성과 도메인 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

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
    """같은 계약·청구 회차·시도 입력은 같은 결제 계획을 만든다."""
    subscription_id = UUID("00000000-0000-0000-0000-000000000001")
    start = datetime(2026, 9, 1, tzinfo=UTC)
    first = plan_subscription_payment(_config(), subscription_id, 1, 1, start)
    second = plan_subscription_payment(_config(), subscription_id, 1, 1, start)

    assert first == second
    assert first.payment_value == MONTHLY_SUBSCRIPTION_FEE
    assert first.billing_period_end_at == start + timedelta(days=30)
    assert first.payment_at == _config().logical_date
    assert first.payment_status in {"completed", "failed"}


def test_plan_varies_by_attempt_sequence() -> None:
    """재시도 순번은 성공·실패 판정의 결정성 입력에 포함된다."""
    subscription_id = UUID("00000000-0000-0000-0000-000000000001")
    start = datetime(2026, 9, 1, tzinfo=UTC)
    statuses = {
        plan_subscription_payment(_config(), subscription_id, 1, attempt, start).payment_status
        for attempt in range(1, 40)
    }

    assert statuses == {"completed", "failed"}


def test_record_rejects_a_non_positive_attempt_sequence() -> None:
    """청구 회차와 결제 시도 순번은 모두 1부터 시작한다."""
    start = datetime(2026, 9, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="sequences must start at one"):
        SubscriptionPaymentRecord(
            payment_id=UUID("00000000-0000-0000-0000-000000000001"),
            subscription_id=UUID("00000000-0000-0000-0000-000000000002"),
            billing_cycle_sequence=1,
            attempt_sequence=0,
            payment_status="completed",
            payment_at=start,
            payment_value=Decimal("29.90"),
            currency_code="BRL",
            billing_period_start_at=start,
            billing_period_end_at=start + timedelta(days=30),
            payment_method_type=None,
            payment_provider=None,
            provider_payment_id=None,
            failure_code=None,
            created_at=start,
            updated_at=start,
        )
