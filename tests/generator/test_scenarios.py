"""Late Arrival과 Membership Change Service-level Scenario를 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    MembershipTierRecord,
    SubscriptionRecord,
    new_customer_record,
)
from src.generator.orders import OrderCatalog, ProductReference, SellerReference
from src.generator.scenarios import (
    delayed_payment_transition,
    late_order_bundle,
    late_order_update_transition,
    membership_change_scenario,
    subscription_transition_scenario,
)
from src.generator.transitions import OrderState, PaymentState


def _config() -> GeneratorConfig:
    """Service-level Scenario 생성에 사용할 고정 Config를 반환한다."""
    return GeneratorConfig(
        source_snapshot_id="seed:abc123",
        random_seed=42,
        logical_date=datetime(2026, 9, 10, tzinfo=UTC),
        order_count=10,
        anomaly_profile="late-arrival",
        generator_version=GENERATOR_VERSION,
    )


def _catalog() -> OrderCatalog:
    """Late Order 생성에 사용할 최소 Seed Catalog를 반환한다."""
    return OrderCatalog(
        products=(ProductReference("product-1"),),
        sellers=(SellerReference("seller-1"),),
    )


def test_late_order_keeps_past_business_time_and_current_source_mutation_time() -> None:
    """Late Order의 구매 시각은 과거이고 생성·변경 시각은 현재 Logical Date다."""
    business_event_time = _config().logical_date - timedelta(days=3)

    bundle = late_order_bundle(
        _config(), new_customer_record(_config(), 1), _catalog(), 1, business_event_time
    )

    assert bundle.order.order_purchase_timestamp == business_event_time
    assert bundle.order.created_at == bundle.order.updated_at == _config().logical_date
    assert {item.created_at for item in bundle.items} == {_config().logical_date}
    assert {payment.created_at for payment in bundle.payments} == {_config().logical_date}


def test_delayed_payment_and_late_update_plan_current_mutation_time() -> None:
    """지연 결제와 Late Update는 과거 Event와 현재 Mutation Time을 분리한다."""
    payment = PaymentState(
        order_id="order-1",
        payment_sequential=1,
        payment_status="pending",
        updated_at=_config().logical_date,
    )
    order = OrderState(
        order_id="order-1",
        order_status="created",
        order_approved_at=None,
        order_delivered_carrier_date=None,
        order_delivered_customer_date=None,
        updated_at=_config().logical_date,
    )
    payment_mutation_time = _config().logical_date + timedelta(days=2)
    order_mutation_time = _config().logical_date + timedelta(days=1)
    order_business_event_time = _config().logical_date - timedelta(days=2)

    delayed_payment = delayed_payment_transition(
        payment, _config().logical_date - timedelta(days=1), payment_mutation_time
    )
    late_update = late_order_update_transition(
        order, "approved", order_business_event_time, order_mutation_time
    )

    assert delayed_payment.next_status == "completed"
    assert delayed_payment.mutation_time == payment_mutation_time
    assert late_update.business_event_time == order_business_event_time
    assert late_update.mutation_time == order_mutation_time


def test_membership_change_scenario_updates_one_person_grain_record() -> None:
    """Membership Change Scenario는 사람 단위 Membership 갱신 후보를 반환한다."""
    old_time = _config().logical_date - timedelta(days=1)
    record = MembershipTierRecord(
        customer_unique_id="person-1",
        membership_tier="BRONZE",
        created_at=old_time,
        updated_at=old_time,
    )

    changed = membership_change_scenario(_config(), (record,), delivered_order_count=5)

    assert changed[0].membership_tier == "SILVER"
    assert changed[0].updated_at == _config().logical_date


def test_subscription_transition_scenario_returns_a_person_grain_state_change() -> None:
    """구독 상태 Scenario는 사람 단위 TRIAL 변경 후보를 반환한다."""
    old_time = _config().logical_date - timedelta(days=1)
    record = SubscriptionRecord(
        customer_unique_id="person-1",
        subscription_status="NON_MEMBER",
        trial_ends_at=None,
        benefit_ends_at=None,
        next_billing_at=None,
        payment_failed_at=None,
        cancel_requested_at=None,
        created_at=old_time,
        updated_at=old_time,
    )

    changed = subscription_transition_scenario(_config(), (record,), "TRIAL")

    assert changed[0].subscription_status == "TRIAL"
    assert changed[0].trial_ends_at == _config().logical_date + timedelta(days=30)


@pytest.mark.parametrize(
    ("business_event_time", "message"),
    ((_config().logical_date, "earlier"), (_config().logical_date + timedelta(days=1), "earlier")),
)
def test_late_scenarios_reject_current_or_future_business_event_time(
    business_event_time: datetime, message: str
) -> None:
    """Late Scenario는 현재 또는 미래 Business Event Time을 허용하지 않는다."""
    with pytest.raises(ValueError, match=message):
        late_order_bundle(
            _config(), new_customer_record(_config(), 1), _catalog(), 1, business_event_time
        )
