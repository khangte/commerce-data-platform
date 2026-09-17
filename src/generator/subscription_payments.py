"""구독 자동결제 시도를 결정적으로 계획하고 원천 이력에 저장한다."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import UUID

import psycopg

from src.generator.config import GeneratorConfig
from src.generator.customers import SUBSCRIPTION_PERIOD
from src.generator.ids import deterministic_uuid, logical_hash

MONTHLY_SUBSCRIPTION_FEE = Decimal("29.90")
PAYMENT_FAILURE_RATE_BASIS_POINTS = 1500
PAYMENT_STATUSES = frozenset({"completed", "failed"})


@dataclass(frozen=True)
class SubscriptionPaymentRecord:
    """청구 회차와 재시도 순번이 정해진 구독 결제 시도 원천 Record다."""

    payment_id: UUID
    subscription_id: UUID
    billing_cycle_sequence: int
    attempt_sequence: int
    payment_status: str
    payment_at: datetime
    payment_value: Decimal
    currency_code: str
    billing_period_start_at: datetime
    billing_period_end_at: datetime
    payment_method_type: str | None
    payment_provider: str | None
    provider_payment_id: str | None
    failure_code: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """결제 식별자·상태·금액·업무 시각의 원천 계약을 확인한다."""
        if self.billing_cycle_sequence < 1 or self.attempt_sequence < 1:
            raise ValueError("Billing cycle and attempt sequences must start at one")
        if self.payment_status not in PAYMENT_STATUSES:
            raise ValueError(f"Unsupported payment_status: {self.payment_status}")
        if self.payment_value < 0:
            raise ValueError("payment_value must be zero or greater")
        if len(self.currency_code) != 3 or self.currency_code != self.currency_code.upper():
            raise ValueError("currency_code must be a three-character uppercase ISO code")
        if self.payment_status == "completed" and self.failure_code is not None:
            raise ValueError("Completed payments must not include failure_code")
        for name, value in (
            ("payment_at", self.payment_at),
            ("billing_period_start_at", self.billing_period_start_at),
            ("billing_period_end_at", self.billing_period_end_at),
            ("created_at", self.created_at),
            ("updated_at", self.updated_at),
        ):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ValueError(f"{name} must be normalized to UTC")
        if self.billing_period_end_at <= self.billing_period_start_at:
            raise ValueError("billing period must end after it starts")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must be greater than or equal to created_at")


def plan_subscription_payment(
    config: GeneratorConfig,
    subscription_id: UUID,
    billing_cycle_sequence: int,
    attempt_sequence: int,
    billing_period_start_at: datetime,
) -> SubscriptionPaymentRecord:
    """한 청구 회차의 결제 성공·실패를 결정적으로 판정한 Record를 만든다."""
    status = _billing_outcome(config, subscription_id, billing_cycle_sequence, attempt_sequence)
    return SubscriptionPaymentRecord(
        payment_id=deterministic_uuid(
            "subscription-payment", subscription_id, billing_cycle_sequence, attempt_sequence
        ),
        subscription_id=subscription_id,
        billing_cycle_sequence=billing_cycle_sequence,
        attempt_sequence=attempt_sequence,
        payment_status=status,
        payment_at=config.logical_date,
        payment_value=MONTHLY_SUBSCRIPTION_FEE,
        currency_code="BRL",
        billing_period_start_at=billing_period_start_at,
        billing_period_end_at=billing_period_start_at + SUBSCRIPTION_PERIOD,
        payment_method_type="credit_card",
        payment_provider="simulator",
        provider_payment_id=str(
            deterministic_uuid(
                "subscription-provider-payment", subscription_id, billing_cycle_sequence, attempt_sequence
            )
        ),
        failure_code="DECLINED" if status == "failed" else None,
        created_at=config.logical_date,
        updated_at=config.logical_date,
    )


def next_billing_cycle_sequence(connection: psycopg.Connection, subscription_id: UUID) -> int:
    """해당 계약의 다음 정규 청구 회차를 현재 최대값 다음으로 계산한다."""
    row = connection.execute(
        """
        SELECT coalesce(max(billing_cycle_sequence), 0)
        FROM subscription_payments
        WHERE subscription_id = %s
        """,
        (subscription_id,),
    ).fetchone()
    return int(row[0]) + 1


def next_attempt_sequence(
    connection: psycopg.Connection, subscription_id: UUID, billing_cycle_sequence: int
) -> int:
    """같은 청구 회차에서 다음 재시도 순번을 계산한다."""
    row = connection.execute(
        """
        SELECT coalesce(max(attempt_sequence), 0)
        FROM subscription_payments
        WHERE subscription_id = %s AND billing_cycle_sequence = %s
        """,
        (subscription_id, billing_cycle_sequence),
    ).fetchone()
    return int(row[0]) + 1


def persist_subscription_payments(
    connection: psycopg.Connection, records: Iterable[SubscriptionPaymentRecord]
) -> int:
    """구독 결제 시도 Record를 멱등적으로 INSERT하고 저장 건수를 반환한다."""
    inserted = 0
    for record in records:
        existing = connection.execute(
            "SELECT 1 FROM subscription_payments WHERE payment_id = %s FOR UPDATE",
            (record.payment_id,),
        ).fetchone()
        if existing is not None:
            continue
        connection.execute(
            """
            INSERT INTO subscription_payments (
                payment_id, subscription_id, billing_cycle_sequence, attempt_sequence,
                payment_status, payment_at, payment_value, currency_code,
                billing_period_start_at, billing_period_end_at, payment_method_type,
                payment_provider, provider_payment_id, failure_code, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                record.payment_id,
                record.subscription_id,
                record.billing_cycle_sequence,
                record.attempt_sequence,
                record.payment_status,
                record.payment_at,
                record.payment_value,
                record.currency_code,
                record.billing_period_start_at,
                record.billing_period_end_at,
                record.payment_method_type,
                record.payment_provider,
                record.provider_payment_id,
                record.failure_code,
                record.created_at,
                record.updated_at,
            ),
        )
        inserted += 1
    return inserted


def _billing_outcome(
    config: GeneratorConfig,
    subscription_id: UUID,
    billing_cycle_sequence: int,
    attempt_sequence: int,
) -> str:
    """결정적 Hash 하위 구간으로 결제 성공·실패를 판정한다."""
    digest = logical_hash(
        {
            "generator_inputs": config.deterministic_inputs(),
            "entity": "subscription-billing",
            "subscription_id": subscription_id,
            "billing_cycle_sequence": billing_cycle_sequence,
            "attempt_sequence": attempt_sequence,
        }
    )
    if int(digest[:4], 16) % 10000 < PAYMENT_FAILURE_RATE_BASIS_POINTS:
        return "failed"
    return "completed"
