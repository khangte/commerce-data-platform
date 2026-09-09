"""실제 Generator 실행 서비스의 적재·재실행·Lease 보호를 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import new_customer_record
from src.generator.lease import (
    WAREHOUSE_OWNER_TYPE,
    LeaseUnavailableError,
    acquire_source_mutation_lease,
    release_source_mutation_lease,
)
from src.generator.orders import fetch_order_catalog, new_order_bundle
from src.generator.service import resolve_source_snapshot_id, run_generator

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_generator_creates_bundles_and_reuses_a_successful_deterministic_run() -> None:
    """Generator는 Source Bundle과 성공 Metadata를 만들고 동일 성공 입력을 재사용한다."""
    settings = PostgresSettings.from_environment()
    config = GeneratorConfig(
        source_snapshot_id=resolve_source_snapshot_id(settings),
        random_seed=9_001,
        logical_date=datetime(2026, 9, 20, tzinfo=UTC),
        order_count=2,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    bundles = _expected_bundles(settings, config)
    result = None
    try:
        result = run_generator(config, settings)
        repeated = run_generator(config, settings)

        assert result.reused_successful_run is False
        assert result.result_counts["orders_inserted"] == 2
        assert repeated.reused_successful_run is True
        assert repeated.generator_run_id == result.generator_run_id
        assert repeated.logical_content_hash == result.logical_content_hash
        with settings.pipeline_connection() as connection:
            status = connection.execute(
                "SELECT status FROM generator_runs WHERE generator_run_id = %s",
                (result.generator_run_id,),
            ).fetchone()[0]
        assert status == "SUCCESS"
    finally:
        _delete_bundles(settings, bundles)
        if result is not None:
            with settings.pipeline_connection() as connection:
                connection.execute(
                    "DELETE FROM generator_runs WHERE generator_run_id = %s",
                    (result.generator_run_id,),
                )
                connection.commit()


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_generator_stops_before_source_mutation_when_warehouse_lease_is_active() -> None:
    """Warehouse Lease가 활성 상태면 실행 서비스는 Source 변경 전에 실패한다."""
    settings = PostgresSettings.from_environment()
    now = datetime.now(tz=UTC)
    warehouse_lease = acquire_source_mutation_lease(
        settings,
        owner_type=WAREHOUSE_OWNER_TYPE,
        owner_id=uuid.uuid4(),
        now=now,
    )
    config = GeneratorConfig(
        source_snapshot_id=resolve_source_snapshot_id(settings),
        random_seed=9_002,
        logical_date=datetime(2026, 9, 21, tzinfo=UTC),
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    try:
        with settings.source_connection() as connection:
            before_count = connection.execute("SELECT count(*) FROM orders").fetchone()[0]
        with pytest.raises(LeaseUnavailableError):
            run_generator(config, settings)
        with settings.source_connection() as connection:
            after_count = connection.execute("SELECT count(*) FROM orders").fetchone()[0]
        assert after_count == before_count
    finally:
        release_source_mutation_lease(settings, warehouse_lease)


def _expected_bundles(settings: PostgresSettings, config: GeneratorConfig):
    """통합 테스트가 생성하고 정리할 결정적 Source Bundle 목록을 반환한다."""
    with settings.source_connection() as connection:
        catalog = fetch_order_catalog(connection)
    return tuple(
        new_order_bundle(config, new_customer_record(config, ordinal), catalog, ordinal)
        for ordinal in range(1, config.order_count + 1)
    )


def _delete_bundles(settings: PostgresSettings, bundles) -> None:
    """통합 테스트가 생성한 정확한 Source Bundle을 FK 역순으로 제거한다."""
    with settings.source_connection() as connection:
        for bundle in bundles:
            connection.execute(
                "DELETE FROM order_payments WHERE order_id = %s", (bundle.order.order_id,)
            )
            connection.execute(
                "DELETE FROM order_items WHERE order_id = %s", (bundle.order.order_id,)
            )
            connection.execute("DELETE FROM orders WHERE order_id = %s", (bundle.order.order_id,))
            connection.execute(
                "DELETE FROM customers WHERE customer_id = %s", (bundle.customer.customer_id,)
            )
            connection.execute(
                "DELETE FROM customer_memberships WHERE customer_unique_id = %s",
                (bundle.customer.customer_unique_id,),
            )
        connection.commit()
