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
    with settings.source_connection() as connection:
        bundle = new_order_bundle(config, customer, fetch_order_catalog(connection), 1)

    try:
        apply_order_bundle(settings, bundle)
        with settings.source_connection() as connection:
            created_order = fetch_order_state(connection, bundle.order.order_id)
            pending_payment = fetch_payment_state(connection, bundle.order.order_id, 1)

        approve = plan_order_transition(created_order, "approved", config.logical_date + timedelta(days=1))
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
            assert fetch_payment_state(connection, bundle.order.order_id, 1).payment_status == "completed"
    finally:
        _delete_bundle(settings, bundle)


def _delete_bundle(settings: PostgresSettings, bundle: OrderBundle) -> None:
    """통합 테스트가 생성한 정확한 Source Row만 FK 역순으로 제거한다."""
    with settings.source_connection() as connection:
        connection.execute("DELETE FROM order_payments WHERE order_id = %s", (bundle.order.order_id,))
        connection.execute("DELETE FROM order_items WHERE order_id = %s", (bundle.order.order_id,))
        connection.execute("DELETE FROM orders WHERE order_id = %s", (bundle.order.order_id,))
        connection.execute("DELETE FROM customers WHERE customer_id = %s", (bundle.customer.customer_id,))
        connection.commit()
