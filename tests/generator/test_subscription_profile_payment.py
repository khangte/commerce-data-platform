"""실패 결제 Profile과 후속 재시도의 청구 회차를 검증한다."""

from __future__ import annotations

from dataclasses import astuple
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import AxisMutationResult, new_subscription_record_for_customer


def _config(logical_date: datetime) -> GeneratorConfig:
    """실패 결제 Profile 검증용 실행 입력을 만든다."""
    return GeneratorConfig(
        source_snapshot_id="seed:test",
        random_seed=45,
        logical_date=logical_date,
        order_count=1,
        anomaly_profile="subscription-payment-failed",
        generator_version=GENERATOR_VERSION,
    )


@pytest.fixture
def failed_profile(monkeypatch):
    """실패 결제 전이와 결제 이력을 메모리에서 함께 관찰한다."""
    from src.generator import service, subscription_payments

    started_at = datetime(2026, 9, 4, tzinfo=UTC)
    failed_at = datetime(2026, 9, 7, tzinfo=UTC)
    current = new_subscription_record_for_customer("customer-test", started_at)
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = [astuple(current)]
    payments = []
    changed = []
    counts = {"subscription_payments_inserted": 0, "subscriptions_updated": 0}
    logical_rows = []

    def save_subscriptions(_, records):
        """변경된 구독 상태를 기록한다."""
        changed.extend(records)
        return AxisMutationResult(inserted=0, updated=1, skipped=0)

    def save_payments(_, records):
        """결제 행을 메모리에 기록한다."""
        payments.extend(records)
        return len(records)

    def next_cycle(_, subscription_id):
        """현재 결제 이력에서 다음 회차를 계산한다."""
        return max((p.billing_cycle_sequence for p in payments if p.subscription_id == subscription_id), default=0) + 1

    def next_attempt(_, subscription_id, cycle):
        """현재 결제 이력에서 같은 회차의 다음 시도를 계산한다."""
        return max((p.attempt_sequence for p in payments if p.subscription_id == subscription_id and p.billing_cycle_sequence == cycle), default=0) + 1

    monkeypatch.setattr(service, "persist_subscription_records", save_subscriptions)
    monkeypatch.setattr(service, "persist_subscription_payments", save_payments)
    monkeypatch.setattr(service, "next_billing_cycle_sequence", next_cycle)
    monkeypatch.setattr(service, "next_attempt_sequence", next_attempt)
    monkeypatch.setattr(subscription_payments, "_billing_outcome", lambda *_: "completed")
    monkeypatch.setattr(service, "_persist_scan_transition", lambda *_: None)

    service._apply_subscription_transition(connection, _config(failed_at), counts, logical_rows)
    return connection, payments, changed, counts, logical_rows, failed_at


def test_failed_profile_persists_first_failed_payment(failed_profile) -> None:
    """결제 실패 전이는 회차 1·시도 1의 실패 결제 행을 함께 남긴다."""
    _, payments, changed, counts, logical_rows, failed_at = failed_profile

    assert len(payments) == 1
    assert (payments[0].billing_cycle_sequence, payments[0].attempt_sequence) == (1, 1)
    assert (payments[0].payment_status, payments[0].failure_code) == ("failed", "DECLINED")
    assert payments[0].billing_period_start_at == datetime(2026, 10, 4, tzinfo=UTC)
    assert payments[0].payment_at == payments[0].created_at == payments[0].updated_at == failed_at
    assert changed[0].subscription_status == "PAYMENT_FAILED"
    assert counts["subscription_payments_inserted"] == 1
    assert logical_rows[-1]["attempt_sequence"] == 1


def test_failed_profile_retry_uses_same_cycle_and_second_attempt(failed_profile) -> None:
    """이틀 뒤 재시도는 기존 실패 회차에서 시도 2를 기록한다."""
    from src.generator import service

    connection, payments, changed, counts, logical_rows, failed_at = failed_profile
    service._bill_and_transition(
        connection,
        _config(failed_at + timedelta(days=2)),
        changed[0],
        counts,
        logical_rows,
    )

    assert [(p.billing_cycle_sequence, p.attempt_sequence) for p in payments] == [(1, 1), (1, 2)]
    assert counts["subscription_payments_inserted"] == 2
