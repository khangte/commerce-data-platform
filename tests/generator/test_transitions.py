"""Order·Payment 상태 전이와 Mutation Time 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.generator.transitions import (
    OrderState,
    PaymentState,
    plan_order_transition,
    plan_payment_transition,
)


def _order_state() -> OrderState:
    """상태 전이 테스트에 사용할 생성 직후 Order 상태를 반환한다."""
    return OrderState(
        order_id="order-1",
        order_status="created",
        order_approved_at=None,
        order_delivered_carrier_date=None,
        order_delivered_customer_date=None,
        updated_at=datetime(2026, 9, 4, tzinfo=UTC),
    )


def _payment_state() -> PaymentState:
    """상태 전이 테스트에 사용할 생성 직후 Payment 상태를 반환한다."""
    return PaymentState(
        order_id="order-1",
        payment_sequential=1,
        payment_status="pending",
        updated_at=datetime(2026, 9, 4, tzinfo=UTC),
    )


def test_order_transition_captures_expected_version_and_next_status() -> None:
    """Order 전이 계획은 현재 상태·Version·다음 상태·Mutation Time을 보관한다."""
    mutation_time = _order_state().updated_at + timedelta(days=1)

    transition = plan_order_transition(_order_state(), "approved", mutation_time)

    assert transition.expected_status == "created"
    assert transition.expected_updated_at == _order_state().updated_at
    assert transition.next_status == "approved"
    assert transition.mutation_time == mutation_time


@pytest.mark.parametrize(
    ("current_status", "next_status"),
    (("created", "shipped"), ("delivered", "canceled"), ("canceled", "approved")),
)
def test_order_transition_rejects_disallowed_source_status_changes(
    current_status: str, next_status: str
) -> None:
    """Generator는 Raw Domain 전체가 아니라 정의된 Order 전이만 허용한다."""
    with pytest.raises(ValueError, match="Unsupported Order transition"):
        plan_order_transition(
            OrderState(
                order_id="order-1",
                order_status=current_status,
                order_approved_at=None,
                order_delivered_carrier_date=None,
                order_delivered_customer_date=None,
                updated_at=datetime(2026, 9, 4, tzinfo=UTC),
            ),
            next_status,
            datetime(2026, 9, 5, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("next_status", "expected_status"),
    (("completed", "pending"), ("refunded", "completed"), ("failed", "pending")),
)
def test_payment_transition_allows_only_the_declared_source_transitions(
    next_status: str, expected_status: str
) -> None:
    """Payment 상태 전이는 Pending과 Completed의 허용 분기만 사용한다."""
    state = _payment_state()
    if expected_status == "completed":
        state = PaymentState(
            order_id=state.order_id,
            payment_sequential=state.payment_sequential,
            payment_status="completed",
            updated_at=state.updated_at,
        )

    transition = plan_payment_transition(state, next_status, state.updated_at + timedelta(days=1))

    assert transition.next_status == next_status


def test_transition_rejects_same_or_earlier_mutation_time() -> None:
    """상태 변경은 현재 `updated_at`보다 엄격히 큰 Mutation Time을 요구한다."""
    with pytest.raises(ValueError, match="mutation_time"):
        plan_order_transition(_order_state(), "approved", _order_state().updated_at)
    with pytest.raises(ValueError, match="mutation_time"):
        plan_payment_transition(
            _payment_state(), "completed", _payment_state().updated_at - timedelta(seconds=1)
        )
