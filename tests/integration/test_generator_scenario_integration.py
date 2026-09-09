"""실제 PostgreSQL Service-level Generator Scenario를 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    new_customer_record,
    new_membership_record,
    persist_membership_records,
)
from src.generator.orders import OrderBundle, apply_order_bundle, fetch_order_catalog
from src.generator.scenarios import (
    delayed_payment_transition,
    late_order_bundle,
    late_order_update_transition,
    membership_change_scenario,
)
from src.generator.transitions import (
    apply_order_transition,
    apply_payment_transition,
    fetch_order_state,
    fetch_payment_state,
)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_service_level_scenarios_keep_business_and_mutation_times_separate() -> None:
    """Late Order·Delayed Payment·Late Update·Membership Change를 Source에 안전하게 반영한다."""
    settings = PostgresSettings.from_environment()
    config = GeneratorConfig(
        source_snapshot_id=f"test:{uuid.uuid4()}",
        random_seed=42,
        logical_date=datetime(2026, 9, 10, tzinfo=UTC),
        order_count=1,
        anomaly_profile="late-arrival",
        generator_version=GENERATOR_VERSION,
    )
    customer = new_customer_record(config, 1)
    business_purchase_time = config.logical_date - timedelta(days=3)
    with settings.source_connection() as connection:
        bundle = late_order_bundle(
            config,
            customer,
            fetch_order_catalog(connection),
            1,
            business_purchase_time,
        )

    try:
        apply_order_bundle(settings, bundle)
        with settings.source_connection() as connection:
            created_order = fetch_order_state(connection, bundle.order.order_id)
            pending_payment = fetch_payment_state(connection, bundle.order.order_id, 1)
            source_order = connection.execute(
                "SELECT order_purchase_timestamp, created_at, updated_at FROM orders WHERE order_id = %s",
                (bundle.order.order_id,),
            ).fetchone()

        assert source_order == (business_purchase_time, config.logical_date, config.logical_date)

        late_approval_time = config.logical_date - timedelta(days=2)
        approval_mutation_time = config.logical_date + timedelta(days=1)
        approval = late_order_update_transition(
            created_order,
            "approved",
            late_approval_time,
            approval_mutation_time,
        )
        assert apply_order_transition(settings, approval).updated == 1

        delayed_payment = delayed_payment_transition(
            pending_payment,
            bundle.payments[0].created_at,
            config.logical_date + timedelta(days=2),
        )
        assert apply_payment_transition(settings, delayed_payment).updated == 1

        membership_config = GeneratorConfig(
            source_snapshot_id=config.source_snapshot_id,
            random_seed=config.random_seed,
            logical_date=config.logical_date + timedelta(days=3),
            order_count=config.order_count,
            anomaly_profile="membership-change",
            generator_version=config.generator_version,
        )
        changed_customer = membership_change_scenario(
            membership_config, (new_membership_record(bundle.customer),), delivered_order_count=5
        )
        with settings.source_connection() as connection, connection.transaction():
            assert persist_membership_records(connection, changed_customer).updated == 1
            source_customer = connection.execute(
                "SELECT membership_level, updated_at FROM customer_memberships WHERE customer_unique_id = %s",
                (bundle.customer.customer_unique_id,),
            ).fetchone()

        assert source_customer == ("silver", membership_config.logical_date)
        with settings.source_connection() as connection:
            approved_order = fetch_order_state(connection, bundle.order.order_id)
            completed_payment = fetch_payment_state(connection, bundle.order.order_id, 1)
        assert approved_order.order_approved_at == late_approval_time
        assert approved_order.updated_at == approval_mutation_time
        assert completed_payment.payment_status == "completed"
        assert completed_payment.updated_at == delayed_payment.mutation_time
    finally:
        _delete_bundle(settings, bundle)


def _delete_bundle(settings: PostgresSettings, bundle: OrderBundle) -> None:
    """통합 테스트가 생성한 정확한 Source Row만 FK 역순으로 제거한다."""
    with settings.source_connection() as connection:
        connection.execute(
            "DELETE FROM order_payments WHERE order_id = %s", (bundle.order.order_id,)
        )
        connection.execute("DELETE FROM order_items WHERE order_id = %s", (bundle.order.order_id,))
        connection.execute("DELETE FROM orders WHERE order_id = %s", (bundle.order.order_id,))
        connection.execute(
            "DELETE FROM customers WHERE customer_id = %s", (bundle.customer.customer_id,)
        )
        connection.execute(
            "DELETE FROM customer_memberships WHERE customer_unique_id = %s",
            (bundle.customer.customer_unique_id,),
        )
        connection.commit()
