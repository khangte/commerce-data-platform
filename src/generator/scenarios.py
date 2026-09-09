"""Late Arrival과 Membership Change를 위한 Service-level Generator Scenario를 제공한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

from src.generator.config import GeneratorConfig
from src.generator.customers import CustomerRecord, MembershipRecord, membership_change_records
from src.generator.orders import OrderBundle, OrderCatalog, new_order_bundle
from src.generator.transitions import (
    OrderState,
    OrderTransition,
    PaymentState,
    PaymentTransition,
    plan_order_transition,
    plan_payment_transition,
)


def late_order_bundle(
    config: GeneratorConfig,
    customer: CustomerRecord,
    catalog: OrderCatalog,
    order_ordinal: int,
    business_event_time: datetime,
) -> OrderBundle:
    """과거 주문 시각과 현재 Source Mutation Time을 분리한 Late Order Bundle을 만든다."""
    _assert_past_business_event_time(business_event_time, config.logical_date)
    bundle = new_order_bundle(config, customer, catalog, order_ordinal)
    return replace(
        bundle,
        order=replace(
            bundle.order,
            order_purchase_timestamp=business_event_time,
            order_estimated_delivery_date=business_event_time + timedelta(days=7),
        ),
    )


def delayed_payment_transition(
    current: PaymentState, payment_created_at: datetime, mutation_time: datetime
) -> PaymentTransition:
    """기존 Pending Payment를 나중에 Completed로 전이하는 지연 결제를 계획한다."""
    _assert_past_business_event_time(payment_created_at, mutation_time)
    return plan_payment_transition(current, "completed", mutation_time)


def late_order_update_transition(
    current: OrderState,
    next_status: str,
    business_event_time: datetime,
    mutation_time: datetime,
) -> OrderTransition:
    """과거 비즈니스 이벤트를 현재 Source Mutation Time으로 반영하는 전이를 계획한다."""
    _assert_past_business_event_time(business_event_time, mutation_time)
    return plan_order_transition(
        current,
        next_status,
        mutation_time,
        business_event_time=business_event_time,
    )


def membership_change_scenario(
    config: GeneratorConfig,
    records: tuple[MembershipRecord, ...],
    delivered_order_count: int,
) -> tuple[MembershipRecord, ...]:
    """한 사람 Membership을 결정적 변경 후보로 만든다."""
    return membership_change_records(config, records, delivered_order_count)


def _assert_past_business_event_time(
    business_event_time: datetime, mutation_time: datetime
) -> None:
    """Late 시나리오의 Business Event Time이 현재 Mutation Time보다 과거인지 확인한다."""
    if business_event_time.tzinfo is None or business_event_time.utcoffset() != timedelta(0):
        raise ValueError("business_event_time must be normalized to UTC")
    if business_event_time >= mutation_time:
        raise ValueError(
            "business_event_time must be earlier than mutation_time for a late scenario"
        )
