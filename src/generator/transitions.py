"""Raw 호환 Order·Payment 상태 전이와 Mutation Time 안전성을 제공한다."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta

import psycopg

from src.common.database import PostgresSettings

ORDER_TRANSITIONS = {
    "created": frozenset({"approved", "canceled"}),
    "approved": frozenset({"shipped", "canceled"}),
    "shipped": frozenset({"delivered"}),
    "delivered": frozenset(),
    "canceled": frozenset(),
}
PAYMENT_TRANSITIONS = {
    "pending": frozenset({"completed", "failed"}),
    "completed": frozenset({"refunded"}),
    "failed": frozenset(),
    "refunded": frozenset(),
}


@dataclass(frozen=True)
class OrderState:
    """Source `orders` Table의 상태 전이에 필요한 현재 Row 값이다."""

    order_id: str
    order_status: str
    order_approved_at: datetime | None
    order_delivered_carrier_date: datetime | None
    order_delivered_customer_date: datetime | None
    updated_at: datetime


@dataclass(frozen=True)
class PaymentState:
    """Source `order_payments` Table의 상태 전이에 필요한 현재 Row 값이다."""

    order_id: str
    payment_sequential: int
    payment_status: str
    updated_at: datetime


@dataclass(frozen=True)
class OrderTransition:
    """기대 Version과 다음 상태를 포함한 Order Mutation 계획이다."""

    order_id: str
    expected_status: str
    expected_updated_at: datetime
    next_status: str
    mutation_time: datetime


@dataclass(frozen=True)
class PaymentTransition:
    """기대 Version과 다음 상태를 포함한 Payment Mutation 계획이다."""

    order_id: str
    payment_sequential: int
    expected_status: str
    expected_updated_at: datetime
    next_status: str
    mutation_time: datetime


@dataclass(frozen=True)
class TransitionResult:
    """하나의 상태 전이에 대한 Update 또는 Idempotent Skip 결과다."""

    updated: int
    skipped: int


def fetch_order_state(connection: psycopg.Connection, order_id: str) -> OrderState:
    """Source에서 Order 상태와 Mutation Version을 읽는다."""
    row = connection.execute(
        """
        SELECT order_id, order_status, order_approved_at, order_delivered_carrier_date,
               order_delivered_customer_date, updated_at
        FROM orders
        WHERE order_id = %s
        """,
        (order_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Order not found: {order_id}")
    return OrderState(
        order_id=row[0],
        order_status=row[1],
        order_approved_at=row[2],
        order_delivered_carrier_date=row[3],
        order_delivered_customer_date=row[4],
        updated_at=row[5],
    )


def fetch_payment_state(
    connection: psycopg.Connection, order_id: str, payment_sequential: int
) -> PaymentState:
    """Source에서 Payment 상태와 Mutation Version을 읽는다."""
    row = connection.execute(
        """
        SELECT order_id, payment_sequential, payment_status, updated_at
        FROM order_payments
        WHERE order_id = %s AND payment_sequential = %s
        """,
        (order_id, payment_sequential),
    ).fetchone()
    if row is None:
        raise ValueError(f"Payment not found: {order_id}/{payment_sequential}")
    return PaymentState(
        order_id=row[0],
        payment_sequential=row[1],
        payment_status=row[2],
        updated_at=row[3],
    )


def plan_order_transition(
    current: OrderState, next_status: str, mutation_time: datetime
) -> OrderTransition:
    """현재 Order 상태와 결정적 Mutation Time으로 허용 전이를 계획한다."""
    _assert_allowed_transition(ORDER_TRANSITIONS, current.order_status, next_status, "Order")
    _assert_increasing_mutation_time(current.updated_at, mutation_time)
    return OrderTransition(
        order_id=current.order_id,
        expected_status=current.order_status,
        expected_updated_at=current.updated_at,
        next_status=next_status,
        mutation_time=mutation_time,
    )


def plan_payment_transition(
    current: PaymentState, next_status: str, mutation_time: datetime
) -> PaymentTransition:
    """현재 Payment 상태와 결정적 Mutation Time으로 허용 전이를 계획한다."""
    _assert_allowed_transition(PAYMENT_TRANSITIONS, current.payment_status, next_status, "Payment")
    _assert_increasing_mutation_time(current.updated_at, mutation_time)
    return PaymentTransition(
        order_id=current.order_id,
        payment_sequential=current.payment_sequential,
        expected_status=current.payment_status,
        expected_updated_at=current.updated_at,
        next_status=next_status,
        mutation_time=mutation_time,
    )


def apply_order_transition(settings: PostgresSettings, transition: OrderTransition) -> TransitionResult:
    """Order 상태 전이를 하나의 Source Transaction으로 적용한다."""
    with settings.source_connection() as connection, connection.transaction():
        return persist_order_transition(connection, transition)


def apply_payment_transition(settings: PostgresSettings, transition: PaymentTransition) -> TransitionResult:
    """Payment 상태 전이를 하나의 Source Transaction으로 적용한다."""
    with settings.source_connection() as connection, connection.transaction():
        return persist_payment_transition(connection, transition)


def persist_order_transition(
    connection: psycopg.Connection, transition: OrderTransition
) -> TransitionResult:
    """외부 Transaction 안에서 Order 상태 전이를 낙관적 Version 검사와 함께 저장한다."""
    current = _locked_order_state(connection, transition.order_id)
    if _is_idempotent_order_transition(current, transition):
        return TransitionResult(updated=0, skipped=1)
    _assert_expected_order_version(current, transition)
    desired = _order_state_after(current, transition.next_status, transition.mutation_time)
    connection.execute(
        """
        UPDATE orders
        SET order_status = %s,
            order_approved_at = %s,
            order_delivered_carrier_date = %s,
            order_delivered_customer_date = %s,
            updated_at = %s
        WHERE order_id = %s
        """,
        (
            desired.order_status,
            desired.order_approved_at,
            desired.order_delivered_carrier_date,
            desired.order_delivered_customer_date,
            desired.updated_at,
            desired.order_id,
        ),
    )
    return TransitionResult(updated=1, skipped=0)


def persist_payment_transition(
    connection: psycopg.Connection, transition: PaymentTransition
) -> TransitionResult:
    """외부 Transaction 안에서 Payment 상태 전이를 낙관적 Version 검사와 함께 저장한다."""
    current = _locked_payment_state(connection, transition.order_id, transition.payment_sequential)
    if current.payment_status == transition.next_status and current.updated_at == transition.mutation_time:
        return TransitionResult(updated=0, skipped=1)
    _assert_expected_payment_version(current, transition)
    _assert_allowed_transition(
        PAYMENT_TRANSITIONS, current.payment_status, transition.next_status, "Payment"
    )
    _assert_increasing_mutation_time(current.updated_at, transition.mutation_time)
    desired = replace(current, payment_status=transition.next_status, updated_at=transition.mutation_time)
    connection.execute(
        """
        UPDATE order_payments
        SET payment_status = %s, updated_at = %s
        WHERE order_id = %s AND payment_sequential = %s
        """,
        (
            desired.payment_status,
            desired.updated_at,
            desired.order_id,
            desired.payment_sequential,
        ),
    )
    return TransitionResult(updated=1, skipped=0)


def _locked_order_state(connection: psycopg.Connection, order_id: str) -> OrderState:
    """저장 시점의 현재 Order 상태를 Row Lock과 함께 읽는다."""
    row = connection.execute(
        """
        SELECT order_id, order_status, order_approved_at, order_delivered_carrier_date,
               order_delivered_customer_date, updated_at
        FROM orders
        WHERE order_id = %s
        FOR UPDATE
        """,
        (order_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Order not found: {order_id}")
    return OrderState(
        order_id=row[0],
        order_status=row[1],
        order_approved_at=row[2],
        order_delivered_carrier_date=row[3],
        order_delivered_customer_date=row[4],
        updated_at=row[5],
    )


def _locked_payment_state(
    connection: psycopg.Connection, order_id: str, payment_sequential: int
) -> PaymentState:
    """저장 시점의 현재 Payment 상태를 Row Lock과 함께 읽는다."""
    row = connection.execute(
        """
        SELECT order_id, payment_sequential, payment_status, updated_at
        FROM order_payments
        WHERE order_id = %s AND payment_sequential = %s
        FOR UPDATE
        """,
        (order_id, payment_sequential),
    ).fetchone()
    if row is None:
        raise ValueError(f"Payment not found: {order_id}/{payment_sequential}")
    return PaymentState(
        order_id=row[0],
        payment_sequential=row[1],
        payment_status=row[2],
        updated_at=row[3],
    )


def _order_state_after(
    current: OrderState, next_status: str, mutation_time: datetime
) -> OrderState:
    """허용된 다음 Order 상태와 해당 Business Timestamp를 구성한다."""
    _assert_allowed_transition(ORDER_TRANSITIONS, current.order_status, next_status, "Order")
    _assert_increasing_mutation_time(current.updated_at, mutation_time)
    if next_status == "approved":
        return replace(current, order_status=next_status, order_approved_at=mutation_time, updated_at=mutation_time)
    if next_status == "shipped":
        return replace(
            current,
            order_status=next_status,
            order_delivered_carrier_date=mutation_time,
            updated_at=mutation_time,
        )
    if next_status == "delivered":
        return replace(
            current,
            order_status=next_status,
            order_delivered_customer_date=mutation_time,
            updated_at=mutation_time,
        )
    return replace(current, order_status=next_status, updated_at=mutation_time)


def _is_idempotent_order_transition(current: OrderState, transition: OrderTransition) -> bool:
    """동일 Mutation Time의 재실행이 정확히 같은 Order 상태인지 확인한다."""
    if current.order_status != transition.next_status or current.updated_at != transition.mutation_time:
        return False
    expected_timestamp = {
        "approved": current.order_approved_at,
        "shipped": current.order_delivered_carrier_date,
        "delivered": current.order_delivered_customer_date,
    }.get(transition.next_status, transition.mutation_time)
    if expected_timestamp != transition.mutation_time:
        raise ValueError("Order has different values at the same updated_at cursor")
    return True


def _assert_expected_order_version(current: OrderState, transition: OrderTransition) -> None:
    """현재 Order 상태가 계획 시점의 상태와 Mutation Version인지 확인한다."""
    if (
        current.order_status != transition.expected_status
        or current.updated_at != transition.expected_updated_at
    ):
        raise ValueError("Order changed after transition planning")


def _assert_expected_payment_version(current: PaymentState, transition: PaymentTransition) -> None:
    """현재 Payment 상태가 계획 시점의 상태와 Mutation Version인지 확인한다."""
    if (
        current.payment_status != transition.expected_status
        or current.updated_at != transition.expected_updated_at
    ):
        raise ValueError("Payment changed after transition planning")


def _assert_allowed_transition(
    transitions: dict[str, frozenset[str]], current_status: str, next_status: str, entity_name: str
) -> None:
    """현재 상태에서 다음 상태로의 Generator 전이가 허용되는지 확인한다."""
    allowed = transitions.get(current_status)
    if allowed is None or next_status not in allowed:
        raise ValueError(f"Unsupported {entity_name} transition: {current_status} -> {next_status}")


def _assert_increasing_mutation_time(current_updated_at: datetime, mutation_time: datetime) -> None:
    """새 Source Mutation Time이 현재 Version보다 큰 UTC 시각인지 확인한다."""
    if mutation_time.tzinfo is None or mutation_time.utcoffset() != timedelta(0):
        raise ValueError("mutation_time must be normalized to UTC")
    if mutation_time <= current_updated_at:
        raise ValueError("mutation_time must be greater than the current updated_at")
