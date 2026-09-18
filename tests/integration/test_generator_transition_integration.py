"""실제 PostgreSQL의 Order·Payment 상태 전이 계약을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import new_customer_record
from src.generator.orders import (
    OrderBundle,
    apply_order_bundle,
    fetch_order_catalog,
    new_order_bundle,
)
from src.generator.transitions import (
    OrderTransition,
    apply_order_transition,
    apply_payment_transition,
    fetch_order_state,
    fetch_payment_state,
    plan_order_transition,
    plan_payment_transition,
)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_order_and_payment_transitions_are_idempotent_and_version_safe() -> None:
    """허용 전이는 재실행 가능하며 오래된 기대 Version의 변경은 거부한다."""
    settings = PostgresSettings.from_environment()
    config = GeneratorConfig(
        source_snapshot_id=f"test:{uuid.uuid4()}",
        random_seed=42,
        logical_date=datetime(2026, 9, 4, tzinfo=UTC),
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    customer = new_customer_record(config, 1)
    failed_customer = new_customer_record(config, 2)
    with settings.source_connection() as connection:
        catalog = fetch_order_catalog(connection)
        bundle = new_order_bundle(config, customer, catalog, 1)
        failed_bundle = new_order_bundle(config, failed_customer, catalog, 2)

    try:
        apply_order_bundle(settings, bundle)
        with settings.source_connection() as connection:
            created_order = fetch_order_state(connection, bundle.order.order_id)
            pending_payment = fetch_payment_state(connection, bundle.order.order_id, 1)

        approve = plan_order_transition(
            created_order, "approved", config.logical_date + timedelta(days=1)
        )
        assert apply_order_transition(settings, approve).updated == 1
        assert apply_order_transition(settings, approve).skipped == 1

        with settings.source_connection() as connection:
            approved_order = fetch_order_state(connection, bundle.order.order_id)
        assert approved_order.order_approved_at == approve.mutation_time

        stale_cancel = OrderTransition(
            order_id=created_order.order_id,
            expected_status=created_order.order_status,
            expected_updated_at=created_order.updated_at,
            next_status="canceled",
            mutation_time=approve.mutation_time + timedelta(days=1),
        )
        with pytest.raises(ValueError, match="changed after"):
            apply_order_transition(settings, stale_cancel)

        complete = plan_payment_transition(
            pending_payment, "completed", config.logical_date + timedelta(days=1)
        )
        assert apply_payment_transition(settings, complete).updated == 1
        assert apply_payment_transition(settings, complete).skipped == 1
        with settings.source_connection() as connection:
            completed_payment = fetch_payment_state(connection, bundle.order.order_id, 1)
        assert completed_payment.payment_status == "completed"
        assert completed_payment.payment_completed_at == complete.mutation_time

        refund = plan_payment_transition(
            completed_payment, "refunded", config.logical_date + timedelta(days=2)
        )
        assert apply_payment_transition(settings, refund).updated == 1
        with settings.source_connection() as connection:
            refunded_payment = fetch_payment_state(connection, bundle.order.order_id, 1)
        assert refunded_payment.payment_status == "refunded"
        assert refunded_payment.payment_completed_at == complete.mutation_time
        assert refunded_payment.payment_refunded_at == refund.mutation_time

        apply_order_bundle(settings, failed_bundle)
        with settings.source_connection() as connection:
            pending_failed_payment = fetch_payment_state(
                connection, failed_bundle.order.order_id, 1
            )
        fail = plan_payment_transition(
            pending_failed_payment, "failed", config.logical_date + timedelta(days=1)
        )
        assert apply_payment_transition(settings, fail).updated == 1
        with settings.source_connection() as connection:
            failed_payment = fetch_payment_state(connection, failed_bundle.order.order_id, 1)
        assert failed_payment.payment_status == "failed"
        assert failed_payment.payment_failed_at == fail.mutation_time
    finally:
        _delete_bundle(settings, bundle)
        _delete_bundle(settings, failed_bundle)


def _delete_bundle(settings: PostgresSettings, bundle: OrderBundle) -> None:
    """통합 테스트가 생성한 정확한 Source Row만 FK 역순으로 제거한다."""
    with settings.source_connection() as connection:
        connection.execute(
            "DELETE FROM order_payments WHERE order_id = %s", (bundle.order.order_id,)
        )
        connection.execute("DELETE FROM order_items WHERE order_id = %s", (bundle.order.order_id,))
        connection.execute(
            """
            DELETE FROM subscription_payments
            WHERE subscription_id IN (
                SELECT subscription_id FROM customer_subscriptions WHERE customer_unique_id = %s
            )
            """,
            (bundle.customer.customer_unique_id,),
        )
        connection.execute(
            "DELETE FROM customer_subscriptions WHERE customer_unique_id = %s",
            (bundle.customer.customer_unique_id,),
        )

        connection.execute(
            "DELETE FROM customer_membership_tiers WHERE customer_unique_id = %s",
            (bundle.customer.customer_unique_id,),
        )
        connection.execute("DELETE FROM orders WHERE order_id = %s", (bundle.order.order_id,))
        connection.execute(
            "DELETE FROM customers WHERE customer_id = %s", (bundle.customer.customer_id,)
        )
        connection.commit()
