"""실제 Generator 실행 서비스의 적재·재실행·Lease 보호를 검증한다."""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.generator import service
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import new_customer_record
from src.generator.lease import (
    WAREHOUSE_OWNER_TYPE,
    LeaseOwnershipLostError,
    LeaseUnavailableError,
    acquire_source_mutation_lease,
    release_source_mutation_lease,
)
from src.generator.orders import fetch_order_catalog, new_order_bundle
from src.generator.service import MUTABLE_SOURCE_TABLES, resolve_source_snapshot_id, run_generator
from src.ingestion.tables import TABLE_CONFIGS

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
        random_seed=uuid.uuid4().int % (2**63),
        logical_date=_forward_logical_date(settings),
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
        with settings.source_connection() as connection:
            connection.execute("DELETE FROM generator_commits WHERE random_seed = %s", (config.random_seed,))
            connection.commit()
        with settings.pipeline_connection() as connection:
            connection.execute(
                """
                DELETE FROM generator_runs
                WHERE source_snapshot_id = %s AND random_seed = %s AND logical_date = %s
                  AND order_count = %s AND anomaly_profile = %s AND generator_version = %s
                """,
                (
                    config.source_snapshot_id,
                    config.random_seed,
                    config.logical_date,
                    config.order_count,
                    config.anomaly_profile,
                    config.generator_version,
                ),
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


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_generator_recovers_source_commit_after_success_metadata_write_fails(monkeypatch) -> None:
    """Source 커밋 뒤 성공 기록이 실패해도 동일 입력의 재실행이 원래 결과를 복구한다."""
    settings = PostgresSettings.from_environment()
    config = GeneratorConfig(
        source_snapshot_id=resolve_source_snapshot_id(settings),
        random_seed=uuid.uuid4().int % (2**63),
        logical_date=_forward_logical_date(settings),
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    bundles = _expected_bundles(settings, config)
    original = service.record_finished_run
    failed = False

    def fail_once(*args, **kwargs) -> None:
        """첫 성공 메타데이터 쓰기만 일시 오류로 중단한다."""
        nonlocal failed
        if kwargs["status"] == "SUCCESS" and not failed:
            failed = True
            raise OSError("temporary metadata error")
        original(*args, **kwargs)

    monkeypatch.setattr(service, "record_finished_run", fail_once)
    try:
        with pytest.raises(OSError, match="temporary metadata error"):
            run_generator(config, settings)
        with settings.pipeline_connection() as connection:
            run_id, status = connection.execute(
                "SELECT generator_run_id, status FROM generator_runs WHERE random_seed = %s",
                (config.random_seed,),
            ).fetchone()
        assert status == "FAILED"
        recovered = run_generator(config, settings)
        assert recovered.reused_successful_run is True
        assert recovered.generator_run_id == run_id
        assert recovered.result_counts["orders_inserted"] == 1
        with settings.pipeline_connection() as connection:
            assert connection.execute(
                "SELECT status FROM generator_runs WHERE generator_run_id = %s", (run_id,)
            ).fetchone()[0] == "SUCCESS"
    finally:
        _delete_bundles(settings, bundles)
        with settings.source_connection() as connection:
            connection.execute("DELETE FROM generator_commits WHERE random_seed = %s", (config.random_seed,))
            connection.commit()
        with settings.pipeline_connection() as connection:
            connection.execute("DELETE FROM generator_runs WHERE random_seed = %s", (config.random_seed,))
            connection.commit()


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
@pytest.mark.parametrize("order_count", [0, 1])
def test_generator_rolls_back_scan_mutation_when_lease_is_lost(monkeypatch, order_count) -> None:
    """스캔 뒤 Lease 소유권이 사라지면 주문 0건 경로까지 Source 변경을 롤백한다."""
    settings = PostgresSettings.from_environment()
    config = GeneratorConfig(
        source_snapshot_id=resolve_source_snapshot_id(settings),
        random_seed=uuid.uuid4().int % (2**63),
        logical_date=_forward_logical_date(settings),
        order_count=order_count,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    bundles = _expected_bundles(settings, config)
    marker_id = f"lease-loss-{uuid.uuid4()}"
    original_scan = service._run_subscription_expiry_scan
    original_fence = service.fenced_source_commit
    scan_finished = False

    def scan_and_write(connection, *args) -> None:
        """스캔 시점에 롤백 확인용 Source 행을 같은 거래에 기록한다."""
        nonlocal scan_finished
        original_scan(connection, *args)
        connection.execute(
            "INSERT INTO customers (customer_id, customer_unique_id, created_at) VALUES (%s, %s, %s)",
            (marker_id, marker_id, config.logical_date),
        )
        scan_finished = True

    @contextmanager
    def lose_after_scan(*args):
        """스캔 이후 커밋 경계에서 소유권 상실을 모의한다."""
        if scan_finished:
            raise LeaseOwnershipLostError("lease owner changed during scan")
        with original_fence(*args):
            yield

    monkeypatch.setattr(service, "_run_subscription_expiry_scan", scan_and_write)
    monkeypatch.setattr(service, "fenced_source_commit", lose_after_scan)
    try:
        with pytest.raises(LeaseOwnershipLostError, match="during scan"):
            run_generator(config, settings)
        with settings.source_connection() as connection:
            assert connection.execute(
                "SELECT count(*) FROM customers WHERE customer_id = %s", (marker_id,)
            ).fetchone()[0] == 0
            assert connection.execute(
                "SELECT count(*) FROM generator_commits WHERE random_seed = %s", (config.random_seed,)
            ).fetchone()[0] == 0
    finally:
        _delete_bundles(settings, bundles)
        with settings.pipeline_connection() as connection:
            connection.execute("DELETE FROM generator_runs WHERE random_seed = %s", (config.random_seed,))
            connection.commit()


def _forward_logical_date(settings: PostgresSettings) -> datetime:
    """Generator 대상 테이블의 최신 수집 커서보다 1초 뒤 실행 시각을 고른다."""
    maxima = [datetime.now(tz=UTC)]
    with settings.source_connection() as connection:
        for table_name in MUTABLE_SOURCE_TABLES:
            column = TABLE_CONFIGS[table_name].cursor_timestamp_column
            maximum = connection.execute(
                f'SELECT max("{column}") FROM "{table_name}"'
            ).fetchone()[0]
            if maximum is not None:
                maxima.append(maximum)
    return max(maxima) + timedelta(seconds=1)


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
