"""결정적 구독 결제가 Bronze와 dbt Temporal Join까지 이어지는지 검증한다."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    ensure_membership_tier_records,
    ensure_subscription_records,
    new_customer_record,
    new_membership_tier_record,
    new_subscription_record,
    persist_customer_records,
    persist_subscription_records,
    subscription_transition_records,
)
from src.generator.subscription_payments import (
    persist_subscription_payments,
    plan_subscription_payment,
)
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.service import TableIngestionRequest, TableIngestionResult, ingest_table
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_SNAPSHOT_ID = "fixture:subscription-payment-temporal-join"
FIXTURE_RANDOM_SEED = 20260911
FIXTURE_START = datetime(2100, 1, 1, tzinfo=UTC)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_subscription_payment_uses_the_active_customer_version_at_billing_time(tmp_path) -> None:
    """결정적 구독 전이·결제가 Bronze 이력과 dbt 시점 결합에서 한 행으로 이어진다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_subscription_temporal_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    active_at = FIXTURE_START + timedelta(days=1)
    active_config = _generator_config(active_at, anomaly_profile="subscription-active")
    payment_at = active_at + timedelta(days=1)
    payment_config = _generator_config(payment_at, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    initial_subscription = new_subscription_record(customer)
    initial_tier = new_membership_tier_record(customer)
    active_subscription = subscription_transition_records(
        active_config, (initial_subscription,), "ACTIVE"
    )[0]
    payment = plan_subscription_payment(
        payment_config,
        customer.customer_unique_id,
        billing_sequence=1,
        billing_period_start=active_at,
    )
    results: list[TableIngestionResult] = []

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (initial_subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (initial_tier,)).inserted == 1
            connection.commit()

        _set_watermark(
            postgres,
            pipeline_name,
            "customer_subscriptions",
            CursorPosition(
                initial_subscription.updated_at - timedelta(microseconds=1),
                (customer.customer_unique_id,),
            ),
            now=ingested_at,
        )
        _set_watermark(
            postgres,
            pipeline_name,
            "customer_membership_tiers",
            CursorPosition(
                initial_tier.updated_at - timedelta(microseconds=1),
                (customer.customer_unique_id,),
            ),
            now=ingested_at,
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_subscriptions",
                initial_subscription.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_membership_tiers",
                initial_tier.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )

        with postgres.source_connection() as connection:
            assert persist_subscription_records(connection, (active_subscription,)).updated == 1
            assert persist_subscription_payments(connection, (payment,)) == 1
            connection.commit()

        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_subscriptions",
                active_subscription.updated_at,
                2,
                tmp_path,
                ingested_at,
            )
        )
        _set_watermark(
            postgres,
            pipeline_name,
            "subscription_payments",
            CursorPosition(
                payment.updated_at - timedelta(microseconds=1),
                (customer.customer_unique_id, payment.billing_sequence),
            ),
            now=ingested_at,
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "subscription_payments",
                payment.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )

        assert [result.row_count for result in results] == [1, 1, 1, 1]
        warehouse_path = tmp_path / "warehouse.duckdb"
        _create_fixture_catalog(postgres, warehouse_path, results)
        dbt_result = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert dbt_result.returncode == 0, _combined_output(dbt_result)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            fact_rows = connection.execute(
                """
                SELECT
                    fact.customer_key,
                    fact.payment_status,
                    fact.payment_value,
                    dimension.subscription_status,
                    dimension.membership_tier,
                    dimension.valid_from,
                    dimension.valid_to
                FROM facts.fact_subscription_payments AS fact
                LEFT JOIN dimensions.dim_customer AS dimension USING (customer_key)
                WHERE fact.customer_unique_id = ? AND fact.billing_sequence = ?
                """,
                [customer.customer_unique_id, payment.billing_sequence],
            ).fetchall()

        assert len(fact_rows) == 1
        fact_row = fact_rows[0]
        assert fact_row[0] is not None
        assert fact_row == (
            fact_row[0],
            payment.payment_status,
            payment.payment_value,
            "ACTIVE",
            "BRONZE",
            active_at,
            None,
        )
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


def _generator_config(logical_date: datetime, *, anomaly_profile: str) -> GeneratorConfig:
    """고정된 Snapshot·Seed로 Fixture의 각 업무 시각 Generator 입력을 만든다."""
    return GeneratorConfig(
        source_snapshot_id=FIXTURE_SNAPSHOT_ID,
        random_seed=FIXTURE_RANDOM_SEED,
        logical_date=logical_date,
        order_count=1,
        anomaly_profile=anomaly_profile,
        generator_version=GENERATOR_VERSION,
    )


def _set_watermark(
    postgres: PostgresSettings,
    pipeline_name: str,
    source_table: str,
    cursor: CursorPosition,
    *,
    now: datetime,
) -> None:
    """Fixture 행 직전 Cursor를 넣어 기존 Source 기준선이 Bronze에 섞이지 않게 한다."""
    get_or_create_watermark(postgres, pipeline_name, source_table, now=now)
    with postgres.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks
            SET watermark_timestamp = %s, watermark_keys = %s, updated_at = %s
            WHERE pipeline_name = %s AND source_table = %s
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), now, pipeline_name, source_table),
        )
        connection.commit()


def _ingest(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    source_table: str,
    logical_date: datetime,
    sequence: int,
    tmp_path: Path,
    ingested_at: datetime,
) -> TableIngestionResult:
    """Fixture 전용 Batch로 Source 한 Table을 Bronze까지 수집한다."""
    request = TableIngestionRequest.for_dag_run(
        source_table=source_table,
        dag_id=f"subscription_temporal_fixture_{sequence}",
        logical_date=logical_date,
        pipeline_name=pipeline_name,
    )
    result = ingest_table(
        postgres,
        storage,
        request,
        local_directory=tmp_path / "bronze",
        now=ingested_at,
    )
    assert result.status == "SUCCESS"
    return result


def _create_fixture_catalog(
    postgres: PostgresSettings,
    warehouse_path: Path,
    results: list[TableIngestionResult],
) -> None:
    """Fixture가 Commit한 Object만 임시 Warehouse의 Bronze Catalog에 등록한다."""
    table_batch_ids = [f"{result.run.batch_id}__{result.run.source_table}" for result in results]
    rows: list[tuple[object, ...]] = []
    with postgres.pipeline_connection() as connection:
        for table_batch_id in table_batch_ids:
            row = connection.execute(
                """
                SELECT source_table, object_key, schema_version, batch_id,
                       committed_at, row_count, logical_hash
                FROM bronze_objects
                WHERE table_batch_id = %s AND status = 'COMMITTED'
                """,
                (table_batch_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Fixture Bronze object is missing: {table_batch_id}")
            rows.append(tuple(row))

    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            """
            CREATE TABLE control.bronze_files (
                source_table VARCHAR NOT NULL,
                object_key VARCHAR PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                batch_id VARCHAR NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                row_count BIGINT NOT NULL,
                logical_hash VARCHAR NOT NULL
            )
            """
        )
        connection.executemany(
            "INSERT INTO control.bronze_files VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )


def _run_dbt_build(
    warehouse_path: Path, storage: SeaweedFSSettings, tmp_path: Path
) -> subprocess.CompletedProcess[str]:
    """격리된 Warehouse와 Fixture Bronze 목록만 사용해 전체 dbt Build를 실행한다."""
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
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target-path",
            str(tmp_path / "dbt-target"),
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _cleanup(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    results: list[TableIngestionResult],
    customer_unique_id: str,
) -> None:
    """Fixture가 만든 Source 행·Bronze Object·Pipeline Metadata만 역순으로 제거한다."""
    client = seaweedfs_s3_client(storage)
    for result in results:
        for object_key in (result.manifest_key, result.object_key):
            if object_key is not None:
                client.delete_object(Bucket=storage.bucket, Key=object_key)

    with postgres.pipeline_connection() as connection:
        for result in results:
            connection.execute(
                "DELETE FROM bronze_objects WHERE table_batch_id = %s",
                (f"{result.run.batch_id}__{result.run.source_table}",),
            )
        connection.execute("DELETE FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,))
        connection.execute("DELETE FROM watermarks WHERE pipeline_name = %s", (pipeline_name,))
        connection.commit()

    with postgres.source_connection() as connection:
        connection.execute(
            "DELETE FROM subscription_payments WHERE customer_unique_id = %s",
            (customer_unique_id,),
        )
        connection.execute(
            "DELETE FROM customer_subscriptions WHERE customer_unique_id = %s",
            (customer_unique_id,),
        )
        connection.execute(
            "DELETE FROM customer_membership_tiers WHERE customer_unique_id = %s",
            (customer_unique_id,),
        )
        connection.execute(
            "DELETE FROM customers WHERE customer_unique_id = %s",
            (customer_unique_id,),
        )
        connection.commit()


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    """dbt 실패 시 버전별 출력 위치 차이와 무관하게 전체 진단을 반환한다."""
    return f"{result.stdout}\n{result.stderr}"
