"""실제 PostgreSQL Order Bundle 저장의 원자성과 멱등성을 검증한다."""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime

import psycopg
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

pytestmark = pytest.mark.integration


def _config() -> GeneratorConfig:
    """실제 Source 검증에 사용할 고유한 Generator Config를 반환한다."""
    return GeneratorConfig(
        source_snapshot_id=f"test:{uuid.uuid4()}",
        random_seed=42,
        logical_date=datetime(2026, 9, 4, tzinfo=UTC),
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_order_bundle_is_atomic_and_idempotent() -> None:
    """Customer·Order·Item·Payment Bundle은 저장과 재실행에서 원자적으로 동작한다."""
    settings = PostgresSettings.from_environment()
    config = _config()
    customer = new_customer_record(config, 1)
    with settings.source_connection() as connection:
        bundle = new_order_bundle(config, customer, fetch_order_catalog(connection), 1)

    try:
        first = apply_order_bundle(settings, bundle)
        second = apply_order_bundle(settings, bundle)

        assert first.customer.inserted == 1
        assert first.orders_inserted == 1
        assert first.items_inserted == len(bundle.items)
        assert first.payments_inserted == len(bundle.payments)
        assert second.customer.skipped == 1
        assert second.orders_skipped == 1
        assert second.items_skipped == len(bundle.items)
        assert second.payments_skipped == len(bundle.payments)
    finally:
        _delete_bundle(settings, bundle)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_invalid_item_rolls_back_customer_and_order() -> None:
    """Product FK 오류가 발생하면 Customer와 Order를 포함한 전체 Bundle을 Rollback한다."""
    settings = PostgresSettings.from_environment()
    config = _config()
    customer = new_customer_record(config, 1)
    with settings.source_connection() as connection:
        original = new_order_bundle(config, customer, fetch_order_catalog(connection), 1)
    invalid_item = replace(original.items[0], product_id="missing-product")
    bundle = OrderBundle(
        customer=original.customer,
        order=original.order,
        items=(invalid_item, *original.items[1:]),
        payments=original.payments,
    )

    with pytest.raises(psycopg.Error):
        apply_order_bundle(settings, bundle)

    with settings.source_connection() as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM customers WHERE customer_id = %s", (customer.customer_id,)
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT count(*) FROM orders WHERE order_id = %s", (original.order.order_id,)
            ).fetchone()[0]
            == 0
        )


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
            "DELETE FROM subscription_payments WHERE customer_unique_id = %s",
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
        connection.commit()
