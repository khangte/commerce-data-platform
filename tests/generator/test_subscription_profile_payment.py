"""구독 청구 기간, 실패 Profile, 재시도와 재정렬을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.generator import service, subscription_payments
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import AxisMutationResult, new_subscription_record_for_customer

DUE = datetime(2026, 10, 4, tzinfo=UTC)


def _config(logical_date: datetime, profile: str = "default") -> GeneratorConfig:
    """시나리오별 실행 입력을 만든다."""
    return GeneratorConfig("seed:test", 45, logical_date, 0, profile, GENERATOR_VERSION)


@pytest.fixture
def billing_memory(monkeypatch):
    """구독·결제 저장을 메모리에서 관찰하도록 서비스 의존성을 바꾼다."""
    subscriptions = [
        new_subscription_record_for_customer("customer-test", DUE - timedelta(days=30))
    ]
    payments = []
    counts = {
        "subscription_payments_inserted": 0,
        "subscriptions_updated": 0,
        "subscriptions_skipped": 0,
    }
    rows = []

    def save_subscriptions(_, records):
        """마지막 구독 상태를 저장한다."""
        subscriptions[:] = records
        return AxisMutationResult(inserted=0, updated=1, skipped=0)

    def save_payments(_, records):
        """결제 이력을 저장한다."""
        payments.extend(records)
        return len(records)

    def next_cycle(_, subscription_id):
        """메모리 이력에서 다음 회차를 계산한다."""
        return (
            max(
                (
                    p.billing_cycle_sequence
                    for p in payments
                    if p.subscription_id == subscription_id
                ),
                default=0,
            )
            + 1
        )

    def next_attempt(_, subscription_id, cycle):
        """메모리 이력에서 다음 시도를 계산한다."""
        return (
            max(
                (
                    p.attempt_sequence
                    for p in payments
                    if p.subscription_id == subscription_id and p.billing_cycle_sequence == cycle
                ),
                default=0,
            )
            + 1
        )

    def latest(_, subscription_id):
        """최근 완료 결제 기간을 반환한다."""
        completed = [
            p
            for p in payments
            if p.subscription_id == subscription_id and p.payment_status == "completed"
        ]
        if not completed:
            return None
        payment = max(completed, key=lambda p: (p.billing_cycle_sequence, p.attempt_sequence))
        return payment.billing_period_start_at, payment.billing_period_end_at

    monkeypatch.setattr(service, "_fetch_subscriptions_ordered", lambda _: tuple(subscriptions))
    monkeypatch.setattr(service, "persist_subscription_records", save_subscriptions)
    monkeypatch.setattr(service, "persist_subscription_payments", save_payments)
    monkeypatch.setattr(service, "next_billing_cycle_sequence", next_cycle)
    monkeypatch.setattr(service, "next_attempt_sequence", next_attempt)
    monkeypatch.setattr(service, "latest_completed_billing_period", latest)
    return subscriptions, payments, counts, rows


def test_failed_profile_requires_due_subscription_and_reuses_scan(billing_memory):
    """실패 Profile은 예정일에만 한 회차의 첫 실패 결제를 기록한다."""
    subscriptions, payments, counts, rows = billing_memory
    with pytest.raises(ValueError, match="requires an ACTIVE subscription due for billing"):
        service._select_due_subscription_for_failed_profile(
            None, _config(DUE - timedelta(days=1), "subscription-payment-failed")
        )
    assert payments == []
    config = _config(DUE, "subscription-payment-failed")
    selected = service._select_due_subscription_for_failed_profile(None, config)
    assert selected == service._select_due_subscription_for_failed_profile(None, config)
    service._run_subscription_expiry_scan(None, config, counts, rows, selected)
    assert len(payments) == 1
    assert (payments[0].billing_cycle_sequence, payments[0].attempt_sequence) == (1, 1)
    assert payments[0].payment_status == "failed"
    assert payments[0].payment_at >= payments[0].billing_period_start_at
    assert subscriptions[0].subscription_status == "PAYMENT_FAILED"
    assert counts["subscription_payments_inserted"] == 1


def test_retry_success_uses_original_billing_period(billing_memory, monkeypatch):
    """실패 후 D+2 성공은 원래 회차 기간으로 ACTIVE를 복귀시킨다."""
    subscriptions, payments, counts, rows = billing_memory
    service._run_subscription_expiry_scan(
        None, _config(DUE), counts, rows, subscriptions[0].subscription_id
    )
    monkeypatch.setattr(subscription_payments, "_billing_outcome", lambda *_: "completed")
    service._run_subscription_expiry_scan(None, _config(DUE + timedelta(days=2)), counts, rows)
    record = subscriptions[0]
    assert [(p.billing_cycle_sequence, p.attempt_sequence) for p in payments] == [(1, 1), (1, 2)]
    assert record.subscription_status == "ACTIVE"
    assert (record.current_period_started_at, record.current_period_ends_at) == (
        DUE,
        DUE + timedelta(days=30),
    )
    assert record.billing_due_at == record.next_payment_attempt_at == DUE + timedelta(days=30)
    assert record.payment_failed_at is None
    assert record.status_changed_at == DUE + timedelta(days=2)


def test_retry_failure_reschedules_without_extending_grace(billing_memory, monkeypatch):
    """재시도 실패는 이틀 뒤로 미루고 한 시간 뒤 스캔은 청구하지 않는다."""
    subscriptions, payments, counts, rows = billing_memory
    service._run_subscription_expiry_scan(
        None, _config(DUE), counts, rows, subscriptions[0].subscription_id
    )
    grace_end = subscriptions[0].current_period_ends_at
    monkeypatch.setattr(subscription_payments, "_billing_outcome", lambda *_: "failed")
    retry_at = DUE + timedelta(days=2)
    service._run_subscription_expiry_scan(None, _config(retry_at), counts, rows)
    assert len(payments) == 2
    assert subscriptions[0].next_payment_attempt_at == retry_at + timedelta(days=2)
    assert subscriptions[0].payment_failed_at == subscriptions[0].updated_at == retry_at
    assert subscriptions[0].current_period_ends_at == grace_end
    assert subscriptions[0].status_changed_at == DUE
    assert subscriptions[0].billing_due_at == DUE
    service._run_subscription_expiry_scan(
        None, _config(retry_at + timedelta(hours=1)), counts, rows
    )
    assert len(payments) == 2


def test_regular_success_advances_period(billing_memory, monkeypatch):
    """정기 청구 성공은 결제 기간과 다음 청구 시각을 맞춘다."""
    subscriptions, payments, counts, rows = billing_memory
    previous_status_at = subscriptions[0].status_changed_at
    monkeypatch.setattr(subscription_payments, "_billing_outcome", lambda *_: "completed")
    service._run_subscription_expiry_scan(None, _config(DUE), counts, rows)
    assert subscriptions[0].status_changed_at == previous_status_at
    assert subscriptions[0].current_period_started_at == payments[0].billing_period_start_at
    assert subscriptions[0].current_period_ends_at == payments[0].billing_period_end_at


def test_paid_future_period_is_realigned_without_payment(billing_memory, monkeypatch):
    """이미 완료된 청구 기간이 남으면 구독만 맞추고 결제하지 않는다."""
    subscriptions, payments, counts, rows = billing_memory
    monkeypatch.setattr(subscription_payments, "_billing_outcome", lambda *_: "completed")
    payment = subscription_payments.plan_subscription_payment(
        _config(DUE), subscriptions[0].subscription_id, 1, 1, DUE - timedelta(days=3)
    )
    payments.append(payment)
    service._run_subscription_expiry_scan(None, _config(DUE + timedelta(days=1)), counts, rows)
    assert len(payments) == 1
    assert subscriptions[0].current_period_started_at == payment.billing_period_start_at
    assert subscriptions[0].current_period_ends_at == payment.billing_period_end_at
    assert (
        subscriptions[0].billing_due_at
        == subscriptions[0].next_payment_attempt_at
        == payment.billing_period_end_at
    )
    assert subscriptions[0].updated_at == DUE + timedelta(days=1)
    assert rows[-1]["scan_subscription_status"] == "ACTIVE_REALIGNED"


def test_new_contract_starts_without_payment(billing_memory):
    """신규 계약의 첫 30일에는 결제 행이 없다."""
    subscriptions, payments, _, _ = billing_memory
    assert payments == []
    assert subscriptions[0].current_period_started_at == DUE - timedelta(days=30)
    assert subscriptions[0].current_period_ends_at == DUE
