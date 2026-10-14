"""logical_date 기준 구독 만료 스캔과 정기 결제·재결제 전이를 수행한다."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import datetime, timedelta

from src.generator.config import GeneratorConfig
from src.generator.customers import (
    SubscriptionRecord,
    persist_subscription_records,
    subscription_transition_records,
)
from src.generator.ids import logical_hash
from src.generator.subscription_payments import (
    latest_completed_billing_period,
    next_attempt_sequence,
    next_billing_cycle_sequence,
    persist_subscription_payments,
    plan_subscription_payment,
)


def _fetch_subscriptions_ordered(connection) -> tuple[SubscriptionRecord, ...]:
    """구독 계약 상태 Record 전체를 사람 키와 계약 키 순서로 읽는다."""
    rows = connection.execute(
        """
        SELECT subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
               subscription_started_at, current_period_started_at, current_period_ends_at,
               billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
               ended_at, status_changed_at, created_at, updated_at
        FROM customer_subscriptions
        ORDER BY customer_unique_id COLLATE "C", subscription_id
        """
    ).fetchall()
    return tuple(SubscriptionRecord(*row) for row in rows)


def _fetch_due_subscriptions(connection, now: datetime) -> tuple[SubscriptionRecord, ...]:
    """스캔이 처리할 수 있는 구독만 SQL로 좁혀 사람 키와 계약 키 순서로 읽는다."""
    rows = connection.execute(
        """
        SELECT subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
               subscription_started_at, current_period_started_at, current_period_ends_at,
               billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
               ended_at, status_changed_at, created_at, updated_at
        FROM customer_subscriptions
        WHERE updated_at < %(now)s
          AND (
            (subscription_status IN ('CANCEL_REQUESTED', 'PAYMENT_FAILED')
                AND current_period_ends_at <= %(now)s)
            OR (subscription_status = 'ACTIVE' AND auto_renew_enabled
                AND next_payment_attempt_at <= %(now)s)
            OR (subscription_status = 'PAYMENT_FAILED' AND next_payment_attempt_at <= %(now)s)
          )
        ORDER BY customer_unique_id COLLATE "C", subscription_id
        """,
        {"now": now},
    ).fetchall()
    return tuple(SubscriptionRecord(*row) for row in rows)


def select_due_subscription_for_failed_profile(connection, config: GeneratorConfig) -> uuid.UUID:
    """예정일이 지난 미결제 정기 청구 후보를 결정적으로 고른다."""
    candidates = tuple(
        record
        for record in _fetch_subscriptions_ordered(connection)
        if record.subscription_status == "ACTIVE"
        and record.auto_renew_enabled
        and record.updated_at < config.logical_date
        and record.next_payment_attempt_at is not None
        and record.next_payment_attempt_at <= config.logical_date
        and not _has_unexpired_completed_period(connection, record, config.logical_date)
    )
    if not candidates:
        raise ValueError(
            "subscription-payment-failed requires an ACTIVE subscription due for billing"
        )
    selector = int(
        logical_hash(
            {
                "generator_inputs": config.deterministic_inputs(),
                "entity": "subscription-transition-selection",
            }
        ),
        16,
    ) % len(candidates)
    return candidates[selector].subscription_id


def _has_unexpired_completed_period(connection, record: SubscriptionRecord, now: datetime) -> bool:
    """최근 완료 결제 기간이 실행 시각 이후까지 남았는지 확인한다."""
    period = latest_completed_billing_period(connection, record.subscription_id)
    return period is not None and period[1] > now


def run_subscription_expiry_scan(
    connection,
    config: GeneratorConfig,
    result_counts: dict[str, int],
    logical_rows: list,
    forced_failure_id: uuid.UUID | None = None,
) -> None:
    """logical_date 기준 시각 스캔으로 만료 종료·정기 결제·재결제를 처리한다.

    요구사항 5.1절 순서를 그대로 따른다. 무작위가 아니라 결정적 스캔이므로 같은 logical_date로
    재실행하면 같은 결과가 나온다.
    """
    now = config.logical_date
    for record in _fetch_due_subscriptions(connection, now):
        if record.updated_at >= now:
            continue
        status = record.subscription_status
        # 1. 만료 종료
        if (
            status in {"CANCEL_REQUESTED", "PAYMENT_FAILED"}
            and record.current_period_ends_at is not None
            and record.current_period_ends_at <= now
        ):
            _persist_scan_transition(connection, config, record, "CHURNED", result_counts)
            logical_rows.append(_subscription_scan_row(record.customer_unique_id, "CHURNED"))
            continue
        # 2. 정기 결제
        if (
            status == "ACTIVE"
            and record.auto_renew_enabled
            and record.next_payment_attempt_at is not None
            and record.next_payment_attempt_at <= now
        ):
            period = latest_completed_billing_period(connection, record.subscription_id)
            if period is not None and period[1] > now:
                realigned = replace(
                    record,
                    current_period_started_at=period[0],
                    current_period_ends_at=period[1],
                    billing_due_at=period[1],
                    next_payment_attempt_at=period[1],
                    updated_at=now,
                )
                outcome = persist_subscription_records(connection, (realigned,))
                result_counts["subscriptions_updated"] += outcome.updated
                result_counts["subscriptions_skipped"] += outcome.skipped
                logical_rows.append(
                    _subscription_scan_row(record.customer_unique_id, "ACTIVE_REALIGNED")
                )
                continue
            _bill_and_transition(
                connection,
                config,
                record,
                result_counts,
                logical_rows,
                forced_status="failed" if record.subscription_id == forced_failure_id else None,
            )
            continue
        # 3. 재결제
        if (
            status == "PAYMENT_FAILED"
            and record.next_payment_attempt_at is not None
            and record.next_payment_attempt_at <= now
        ):
            _bill_and_transition(connection, config, record, result_counts, logical_rows)


def _bill_and_transition(
    connection,
    config: GeneratorConfig,
    record: SubscriptionRecord,
    result_counts: dict[str, int],
    logical_rows: list,
    forced_status: str | None = None,
) -> None:
    """결제를 시도해 subscription_payments 행을 남기고 성공·실패에 따라 상태를 바꾼다."""
    if record.subscription_status == "PAYMENT_FAILED":
        billing_cycle_sequence = next_billing_cycle_sequence(connection, record.subscription_id) - 1
        attempt_sequence = next_attempt_sequence(
            connection, record.subscription_id, billing_cycle_sequence
        )
    else:
        billing_cycle_sequence = next_billing_cycle_sequence(connection, record.subscription_id)
        attempt_sequence = 1
    period_start = record.billing_due_at or config.logical_date
    payment = plan_subscription_payment(
        config,
        record.subscription_id,
        billing_cycle_sequence,
        attempt_sequence,
        period_start,
        forced_status=forced_status,
    )
    result_counts["subscription_payments_inserted"] += persist_subscription_payments(
        connection, (payment,)
    )
    logical_rows.append(
        {
            "customer_unique_id": record.customer_unique_id,
            "subscription_id": str(record.subscription_id),
            "billing_cycle_sequence": billing_cycle_sequence,
            "attempt_sequence": attempt_sequence,
            "payment_status": payment.payment_status,
        }
    )
    if payment.payment_status == "completed":
        _advance_active_billing(connection, config, record, payment, result_counts)
        if record.subscription_status != "ACTIVE":
            logical_rows.append(_subscription_scan_row(record.customer_unique_id, "ACTIVE"))
    elif record.subscription_status == "PAYMENT_FAILED":
        retry = replace(
            record,
            next_payment_attempt_at=config.logical_date + timedelta(days=2),
            payment_failed_at=config.logical_date,
            updated_at=config.logical_date,
        )
        outcome = persist_subscription_records(connection, (retry,))
        result_counts["subscriptions_updated"] += outcome.updated
        result_counts["subscriptions_skipped"] += outcome.skipped
    else:
        _persist_scan_transition(connection, config, record, "PAYMENT_FAILED", result_counts)
        logical_rows.append(_subscription_scan_row(record.customer_unique_id, "PAYMENT_FAILED"))


def _advance_active_billing(
    connection, config: GeneratorConfig, record: SubscriptionRecord, payment, result_counts
) -> None:
    """결제 성공 시 이전 상태와 관계없이 청구 기간으로 구독을 갱신한다."""
    advanced = SubscriptionRecord(
        subscription_id=record.subscription_id,
        customer_unique_id=record.customer_unique_id,
        subscription_status="ACTIVE",
        auto_renew_enabled=True,
        subscription_started_at=record.subscription_started_at,
        current_period_started_at=payment.billing_period_start_at,
        current_period_ends_at=payment.billing_period_end_at,
        billing_due_at=payment.billing_period_end_at,
        next_payment_attempt_at=payment.billing_period_end_at,
        payment_failed_at=None,
        cancel_requested_at=None,
        ended_at=None,
        status_changed_at=(
            record.status_changed_at
            if record.subscription_status == "ACTIVE"
            else config.logical_date
        ),
        created_at=record.created_at,
        updated_at=config.logical_date,
    )
    outcome = persist_subscription_records(connection, (advanced,))
    result_counts["subscriptions_updated"] += outcome.updated
    result_counts["subscriptions_skipped"] += outcome.skipped


def _persist_scan_transition(
    connection,
    config: GeneratorConfig,
    record: SubscriptionRecord,
    next_status: str,
    result_counts: dict[str, int],
) -> None:
    """스캔이 만든 구독 상태 전이를 저장하고 Count에 누적한다."""
    changed = subscription_transition_records(config, (record,), next_status)
    outcome = persist_subscription_records(connection, changed)
    result_counts["subscriptions_updated"] += outcome.updated
    result_counts["subscriptions_skipped"] += outcome.skipped


def _subscription_scan_row(customer_unique_id: str, next_status: str) -> dict[str, str]:
    """만료 스캔이 만든 구독 전이의 결정성 Hash 입력 Row를 반환한다."""
    return {
        "customer_unique_id": customer_unique_id,
        "scan_subscription_status": next_status,
    }


def forced_failure_row(connection, subscription_id: uuid.UUID) -> dict[str, str]:
    """실패 Profile이 고른 구독의 스캔 후 상태를 결정성 Hash 입력 Row로 만든다."""
    selected = next(
        record
        for record in _fetch_subscriptions_ordered(connection)
        if record.subscription_id == subscription_id
    )
    return {
        "customer_unique_id": selected.customer_unique_id,
        "subscription_status": selected.subscription_status,
        "subscription_updated_at": selected.updated_at.isoformat(),
    }
