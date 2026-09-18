"""Incremental Build 결과가 같은 입력의 Full Refresh와 논리적으로 같은지 검증한다."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.common.database import PostgresSettings
from src.generator.customers import (
    ensure_membership_tier_records,
    ensure_subscription_records,
    new_customer_record,
    new_membership_tier_record,
    new_subscription_record,
    persist_customer_records,
)
from src.generator.subscription_payments import (
    persist_subscription_payments,
    plan_subscription_payment,
)
from src.ingestion.metadata import CursorPosition
from src.ingestion.service import TableIngestionResult
from src.ingestion.storage import SeaweedFSSettings
from src.warehouse.mart_hash import describe_mart_difference, mart_logical_hashes, target_for
from tests.integration.test_subscription_payment_temporal_join_integration import (
    FIXTURE_START,
    _append_fixture_catalog,
    _cleanup,
    _combined_output,
    _create_fixture_catalog,
    _generator_config,
    _ingest,
    _run_dbt_build,
    _set_watermark,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_incremental_marts_match_a_full_refresh_of_the_same_input(tmp_path: Path) -> None:
    """지연 도착 Batch를 Incremental로 반영한 Mart가 같은 입력의 Full Refresh와 Hash까지 같다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_full_refresh_hash_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    first_payment = plan_subscription_payment(
        _generator_config(FIXTURE_START + timedelta(days=1), anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    late_payment_at = FIXTURE_START + timedelta(days=2)
    late_arrival_at = FIXTURE_START + timedelta(days=5)
    late_payment = replace(
        plan_subscription_payment(
            _generator_config(late_payment_at, anomaly_profile="default"),
            subscription.subscription_id,
            billing_cycle_sequence=2,
            attempt_sequence=1,
            billing_period_start_at=late_payment_at,
        ),
        updated_at=late_arrival_at,
    )
    results: list[TableIngestionResult] = []
    incremental_path = tmp_path / "warehouse.duckdb"
    full_refresh_path = tmp_path / "warehouse-full-refresh.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (first_payment,)) == 1
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
            ("subscription_payments", first_payment.updated_at, str(first_payment.payment_id)),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, incremental_path, results)
        first_build = _run_dbt_build(incremental_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)

        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (late_payment,)) == 1
            connection.commit()

        late_results = [
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "subscription_payments",
                late_arrival_at,
                1,
                tmp_path,
                ingested_at,
            )
        ]
        results.extend(late_results)
        _append_fixture_catalog(postgres, incremental_path, late_results)
        second_build = _run_dbt_build(incremental_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)

        shutil.copy2(incremental_path, full_refresh_path)
        full_refresh_build = _run_full_refresh_build(full_refresh_path, storage, tmp_path)
        assert full_refresh_build.returncode == 0, _combined_output(full_refresh_build)

        incremental_hashes = mart_logical_hashes(incremental_path)
        full_refresh_hashes = mart_logical_hashes(full_refresh_path)
        mismatched = [
            relation
            for relation in incremental_hashes
            if incremental_hashes[relation] != full_refresh_hashes[relation]
        ]
        report = "\n".join(
            describe_mart_difference(incremental_path, full_refresh_path, target_for(relation))
            for relation in mismatched
        )

        assert mismatched == [], report
        assert len(incremental_hashes) == 9
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


def _run_full_refresh_build(
    warehouse_path: Path, storage: SeaweedFSSettings, tmp_path: Path
) -> subprocess.CompletedProcess[str]:
    """복사한 Warehouse에만 Full Refresh Build를 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_HOST": storage.host,
        "SEAWEEDFS_S3_PORT": str(storage.port),
        "SEAWEEDFS_BUCKET": storage.bucket,
        "SEAWEEDFS_ACCESS_KEY": storage.access_key,
        "SEAWEEDFS_SECRET_KEY": storage.secret_key,
    }
    return subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "build",
            "--full-refresh",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target-path",
            str(tmp_path / "dbt-target-full-refresh"),
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
