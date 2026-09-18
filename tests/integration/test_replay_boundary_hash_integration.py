"""as-of 경계 Replay가 그 시점 Build를 Hash까지 재현하는지 검증한다."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
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
    _set_watermark,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_as_of_replay_reproduces_the_build_of_that_moment(tmp_path: Path) -> None:
    """둘째 Batch 수집 뒤에도 첫 Batch 시점 경계 Replay는 첫 Build와 Hash가 같다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_replay_boundary_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    results: list[TableIngestionResult] = []
    customer_unique_id = ""
    try:
        first_results, customer_unique_id = _ingest_first_batch(
            postgres, storage, pipeline_name, tmp_path, ingested_at
        )
        results.extend(first_results)

        first_warehouse = tmp_path / "first.duckdb"
        _create_fixture_catalog(postgres, first_warehouse, results)
        first_build = _run_build(first_warehouse, storage, tmp_path, "first")
        assert first_build.returncode == 0, _combined_output(first_build)
        first_hashes = mart_logical_hashes(first_warehouse)
        boundary = _max_committed_at(first_warehouse)

        second_results = _ingest_second_batch(
            postgres, storage, pipeline_name, customer_unique_id, tmp_path, ingested_at
        )
        results.extend(second_results)

        replay_warehouse = tmp_path / "replay.duckdb"
        shutil.copyfile(first_warehouse, replay_warehouse)
        _append_fixture_catalog(postgres, replay_warehouse, second_results)
        replay_build = _run_build(
            replay_warehouse,
            storage,
            tmp_path,
            "replay",
            bronze_as_of=boundary,
        )
        assert replay_build.returncode == 0, _combined_output(replay_build)
        replay_hashes = mart_logical_hashes(replay_warehouse)

        mismatched = [name for name, value in first_hashes.items() if replay_hashes[name] != value]
        report = "\n".join(
            describe_mart_difference(first_warehouse, replay_warehouse, target_for(name))
            for name in mismatched
        )
        assert mismatched == [], report

        with duckdb.connect(str(replay_warehouse)) as connection:
            boundary_rows = connection.execute(
                "SELECT bronze_as_of, object_count FROM control.dbt_replay_boundary "
                "WHERE bronze_as_of IS NOT NULL"
            ).fetchall()
            watermark_rows = connection.execute(
                "SELECT count(*) FROM control.dbt_processed_batch"
            ).fetchone()

        assert len(boundary_rows) == 1
        assert boundary_rows[0][1] == len(first_results)
        assert watermark_rows[0] == 1
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer_unique_id)


def test_boundary_build_without_full_refresh_is_rejected(tmp_path: Path) -> None:
    """경계를 준 Incremental Build는 Model 실행 전에 막힌다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA control")

    result = _run_build(
        warehouse_path,
        None,
        tmp_path,
        "guard",
        bronze_as_of="2026-09-10 00:00:00+00:00",
        full_refresh=False,
    )

    assert result.returncode != 0
    assert "REPLAY_BOUNDARY_ERROR" in _combined_output(result)


def _ingest_first_batch(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    tmp_path: Path,
    ingested_at: datetime,
) -> tuple[list[TableIngestionResult], str]:
    """기준 Batch를 Source에 넣고 Bronze까지 수집한다."""
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
    with postgres.source_connection() as connection:
        assert persist_customer_records(connection, (customer,)).inserted == 1
        assert ensure_subscription_records(connection, (subscription,)).inserted == 1
        assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
        assert persist_subscription_payments(connection, (first_payment,)) == 1
        connection.commit()

    results: list[TableIngestionResult] = []
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
    return results, customer.customer_unique_id


def _ingest_second_batch(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    customer_unique_id: str,
    tmp_path: Path,
    ingested_at: datetime,
) -> list[TableIngestionResult]:
    """첫 Batch 이후에 도착한 지연 Batch를 수집한다."""
    with postgres.source_connection() as connection:
        subscription_id = connection.execute(
            """
            SELECT subscription_id
            FROM customer_subscriptions
            WHERE customer_unique_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (customer_unique_id,),
        ).fetchone()
        if subscription_id is None:
            raise RuntimeError("Fixture subscription is missing")
        late_payment_at = FIXTURE_START + timedelta(days=2)
        late_arrival_at = FIXTURE_START + timedelta(days=5)
        late_payment = replace(
            plan_subscription_payment(
                _generator_config(late_payment_at, anomaly_profile="default"),
                subscription_id[0],
                billing_cycle_sequence=2,
                attempt_sequence=1,
                billing_period_start_at=late_payment_at,
            ),
            updated_at=late_arrival_at,
        )
        assert persist_subscription_payments(connection, (late_payment,)) == 1
        connection.commit()

    return [
        _ingest(
            postgres,
            storage,
            pipeline_name,
            "subscription_payments",
            late_arrival_at,
            1,
            tmp_path,
            ingested_at + timedelta(seconds=1),
        )
    ]


def _max_committed_at(warehouse_path: Path) -> str:
    """Catalog에 실린 Object의 최대 Commit 시각을 경계 값으로 돌려준다."""
    with duckdb.connect(str(warehouse_path)) as connection:
        row = connection.execute("SELECT max(committed_at) FROM control.bronze_files").fetchone()
    return row[0].astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")


def _run_build(
    warehouse_path: Path,
    storage: SeaweedFSSettings | None,
    tmp_path: Path,
    label: str,
    *,
    bronze_as_of: str | None = None,
    full_refresh: bool = True,
) -> subprocess.CompletedProcess[str]:
    """지정한 Warehouse에만 경계 Build를 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_BUCKET": storage.bucket if storage else "test-bucket",
        "SEAWEEDFS_ACCESS_KEY": storage.access_key if storage else "test-access-key",
        "SEAWEEDFS_SECRET_KEY": storage.secret_key if storage else "test-secret-key",
    }
    if storage is not None:
        environment["SEAWEEDFS_HOST"] = storage.host
        environment["SEAWEEDFS_S3_PORT"] = str(storage.port)
    command = [
        str(Path(sys.executable).with_name("dbt")),
        "build",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
        "--target-path",
        str(tmp_path / f"dbt-target-{label}"),
    ]
    if full_refresh:
        command.insert(2, "--full-refresh")
    if bronze_as_of is not None:
        command += ["--vars", json.dumps({"bronze_as_of": bronze_as_of})]
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
