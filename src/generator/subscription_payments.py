"""구독 자동결제 이벤트를 결정적으로 계획하고 subscription_payments에 저장한다."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg

from src.generator.config import GeneratorConfig
from src.generator.customers import SUBSCRIPTION_PERIOD
from src.generator.ids import logical_hash

MONTHLY_SUBSCRIPTION_FEE = Decimal("29.90")
PAYMENT_FAILURE_RATE_BASIS_POINTS = 1500
PAYMENT_STATUSES = frozenset({"completed", "failed"})


@dataclass(frozen=True)
class SubscriptionPaymentRecord:
    """구독 결제 1건의 Source Record 계획이다. subscription_payments 축이다."""

    customer_unique_id: str
    billing_sequence: int
    payment_status: str
    payment_value: Decimal
    billing_period_start: datetime
    billing_period_end: datetime
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """사람 키·결제 순번·상태·기간·시각의 Source 계약을 확인한다."""
        if not self.customer_unique_id or len(self.customer_unique_id) > 64:
            raise ValueError("customer_unique_id must contain 1 to 64 characters")
        if isinstance(self.billing_sequence, bool) or not isinstance(self.billing_sequence, int):
            raise TypeError("billing_sequence must be an integer")
        if self.billing_sequence < 1:
            raise ValueError("billing_sequence must start at one")
        if self.payment_status not in PAYMENT_STATUSES:
            raise ValueError(f"Unsupported payment_status: {self.payment_status}")
        if self.payment_value < 0:
            raise ValueError("payment_value must be zero or greater")
        for name, value in (
            ("billing_period_start", self.billing_period_start),
            ("billing_period_end", self.billing_period_end),
            ("created_at", self.created_at),
            ("updated_at", self.updated_at),
        ):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ValueError(f"{name} must be normalized to UTC")
        if self.billing_period_end <= self.billing_period_start:
            raise ValueError("billing_period_end must be after billing_period_start")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must be greater than or equal to created_at")


def plan_subscription_payment(
    config: GeneratorConfig,
    customer_unique_id: str,
    billing_sequence: int,
    billing_period_start: datetime,
) -> SubscriptionPaymentRecord:
    """한 청구 주기의 결제 성공·실패를 결정적으로 판정한 Record를 만든다."""
    status = _billing_outcome(config, customer_unique_id, billing_sequence)
    return SubscriptionPaymentRecord(
        customer_unique_id=customer_unique_id,
        billing_sequence=billing_sequence,
        payment_status=status,
        payment_value=MONTHLY_SUBSCRIPTION_FEE,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_start + SUBSCRIPTION_PERIOD,
        created_at=config.logical_date,
        updated_at=config.logical_date,
    )


def next_billing_sequence(connection: psycopg.Connection, customer_unique_id: str) -> int:
    """해당 사람의 다음 결제 순번을 현재 최대값 다음으로 계산한다."""
    row = connection.execute(
        """
        SELECT coalesce(max(billing_sequence), 0)
        FROM subscription_payments
        WHERE customer_unique_id = %s
        """,
        (customer_unique_id,),
    ).fetchone()
    return int(row[0]) + 1


def persist_subscription_payments(
    connection: psycopg.Connection, records: Iterable[SubscriptionPaymentRecord]
) -> int:
    """구독 결제 Record를 멱등적으로 INSERT하고 저장 건수를 반환한다."""
    inserted = 0
    for record in records:
        existing = connection.execute(
            """
            SELECT 1 FROM subscription_payments
            WHERE customer_unique_id = %s AND billing_sequence = %s
            FOR UPDATE
            """,
            (record.customer_unique_id, record.billing_sequence),
        ).fetchone()
        if existing is not None:
            continue
        connection.execute(
            """
            INSERT INTO subscription_payments (
                customer_unique_id, billing_sequence, payment_status, payment_value,
                billing_period_start, billing_period_end, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                record.customer_unique_id,
                record.billing_sequence,
                record.payment_status,
                record.payment_value,
                record.billing_period_start,
                record.billing_period_end,
                record.created_at,
                record.updated_at,
            ),
        )
        inserted += 1
    return inserted


def _billing_outcome(
    config: GeneratorConfig, customer_unique_id: str, billing_sequence: int
) -> str:
    """결정적 Hash 하위 구간으로 결제 성공·실패를 판정한다."""
    digest = logical_hash(
        {
            "generator_inputs": config.deterministic_inputs(),
            "entity": "subscription-billing",
            "customer_unique_id": customer_unique_id,
            "billing_sequence": billing_sequence,
        }
    )
    if int(digest[:4], 16) % 10000 < PAYMENT_FAILURE_RATE_BASIS_POINTS:
        return "failed"
    return "completed"
