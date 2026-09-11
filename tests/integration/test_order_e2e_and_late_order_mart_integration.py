"""AC-01(E2E Count 추적)과 AC-11(Late Order 과거 Mart 갱신)을 dbt Fact까지 검증한다."""

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
from src.generator.customers import new_customer_record
from src.generator.orders import (
    OrderBundle,
    OrderCatalog,
    fetch_order_catalog,
    new_order_bundle,
    persist_order_bundle,
)
from src.generator.scenarios import late_order_bundle
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.service import TableIngestionRequest, TableIngestionResult, ingest_table
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_SNAPSHOT_ID = "fixture:order-e2e-late-order-mart"
FIXTURE_RANDOM_SEED = 20260911
FIXTURE_START = datetime(2100, 3, 1, tzinfo=UTC)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_fixed_order_is_traceable_from_source_to_fact(tmp_path) -> None:
    """AC-01: 고정 주문 하나가 Source→Bronze Catalog→Fact까지 같은 Count로 이어진다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    ingested_at = datetime.now(UTC)
    config = _generator_config(FIXTURE_START)
    catalog = _fetch_catalog(postgres)
    customer = new_customer_record(config, 1)
    bundle = new_order_bundle(config, customer, catalog, order_ordinal=1)
    results: list[TableIngestionResult] = []
    pipeline_name = f"test_order_e2e_{uuid.uuid4().hex}"

    try:
        with postgres.source_connection() as connection, connection.transaction():
            mutation = persist_order_bundle(connection, bundle)
        assert mutation.orders_inserted == 1
        assert mutation.items_inserted == len(bundle.items)
        assert mutation.payments_inserted == len(bundle.payments)

        _seed_watermarks(postgres, pipeline_name, bundle, ingested_at)
        for source_table in ("customers", "customer_subscriptions", "customer_membership_tiers", "orders", "order_items", "order_payments"):
            results.append(
                _ingest(postgres, storage, pipeline_name, source_table, FIXTURE_START, tmp_path, ingested_at)
            )
        assert all(result.row_count == 1 for result in results)

        warehouse_path = tmp_path / "warehouse.duckdb"
        _create_fixture_catalog(postgres, warehouse_path, results)
        dbt_result = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert dbt_result.returncode == 0, _combined_output(dbt_result)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            fact_row = connection.execute(
                "SELECT order_id, order_count FROM facts.fact_orders WHERE order_id = ?",
                [bundle.order.order_id],
            ).fetchone()
        assert fact_row == (bundle.order.order_id, 1)
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_late_order_updates_the_past_business_date_mart(tmp_path) -> None:
    """AC-11: 3일 전 Business Time의 Late Order가 새 updated_at으로 수집되어 과거 날짜 Mart를 갱신한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    ingested_at = datetime.now(UTC)
    mutation_time = FIXTURE_START
    business_event_time = mutation_time - timedelta(days=3)
    config = _generator_config(mutation_time)
    catalog = _fetch_catalog(postgres)
    customer = new_customer_record(config, 1)
    bundle = late_order_bundle(config, customer, catalog, order_ordinal=1, business_event_time=business_event_time)
    results: list[TableIngestionResult] = []
    pipeline_name = f"test_late_order_{uuid.uuid4().hex}"

    try:
        with postgres.source_connection() as connection, connection.transaction():
            mutation = persist_order_bundle(connection, bundle)
        assert mutation.orders_inserted == 1

        _seed_watermarks(postgres, pipeline_name, bundle, ingested_at)
        for source_table in ("customers", "customer_subscriptions", "customer_membership_tiers", "orders", "order_items", "order_payments"):
            results.append(
                _ingest(postgres, storage, pipeline_name, source_table, mutation_time, tmp_path, ingested_at)
            )
        assert all(result.row_count == 1 for result in results)

        warehouse_path = tmp_path / "warehouse.duckdb"
        _create_fixture_catalog(postgres, warehouse_path, results)
        dbt_result = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert dbt_result.returncode == 0, _combined_output(dbt_result)

        expected_date_key = int(business_event_time.strftime("%Y%m%d"))
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            fact_row = connection.execute(
                "SELECT purchase_date_key FROM facts.fact_orders WHERE order_id = ?",
                [bundle.order.order_id],
            ).fetchone()
        assert fact_row == (expected_date_key,)
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


def _generator_config(logical_date: datetime) -> GeneratorConfig:
    """고정 Snapshot·Seed로 Fixture의 Mutation 시각 Generator 입력을 만든다."""
    return GeneratorConfig(
        source_snapshot_id=FIXTURE_SNAPSHOT_ID,
        random_seed=FIXTURE_RANDOM_SEED,
        logical_date=logical_date,
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


def _fetch_catalog(postgres: PostgresSettings) -> OrderCatalog:
    """기존 Seed의 Product·Seller 한 개씩만 골라 결정적 Catalog를 만든다."""
    with postgres.source_connection() as connection:
        full_catalog = fetch_order_catalog(connection)
    return OrderCatalog(
        products=(full_catalog.products[0],),
        sellers=(full_catalog.sellers[0],),
    )


def _seed_watermarks(
    postgres: PostgresSettings,
    pipeline_name: str,
    bundle: OrderBundle,
    now: datetime,
) -> None:
    """Bundle 직전 시각으로 6개 Table Watermark를 세팅해 기존 Seed 데이터가 수집되지 않게 한다."""
    _set_watermark(
        postgres, pipeline_name, "customers",
        CursorPosition(bundle.customer.created_at - timedelta(microseconds=1), (bundle.customer.customer_id,)),
        now,
    )
    _set_watermark(
        postgres, pipeline_name, "customer_subscriptions",
        CursorPosition(bundle.customer.created_at - timedelta(microseconds=1), (bundle.customer.customer_unique_id,)),
        now,
    )
    _set_watermark(
        postgres, pipeline_name, "customer_membership_tiers",
        CursorPosition(bundle.customer.created_at - timedelta(microseconds=1), (bundle.customer.customer_unique_id,)),
        now,
    )
    _set_watermark(
        postgres, pipeline_name, "orders",
        CursorPosition(bundle.order.updated_at - timedelta(microseconds=1), (bundle.order.order_id,)),
        now,
    )
    first_item = bundle.items[0]
    _set_watermark(
        postgres, pipeline_name, "order_items",
        CursorPosition(
            first_item.created_at - timedelta(microseconds=1),
            (first_item.order_id, first_item.order_item_id),
        ),
        now,
    )
    first_payment = bundle.payments[0]
    _set_watermark(
        postgres, pipeline_name, "order_payments",
        CursorPosition(
            first_payment.updated_at - timedelta(microseconds=1),
            (first_payment.order_id, first_payment.payment_sequential),
        ),
        now,
    )


def _set_watermark(
    postgres: PostgresSettings,
    pipeline_name: str,
    source_table: str,
    cursor: CursorPosition,
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
    tmp_path: Path,
    ingested_at: datetime,
) -> TableIngestionResult:
    """Fixture 전용 Batch로 Source 한 Table을 Bronze까지 수집한다."""
    request = TableIngestionRequest.for_dag_run(
        source_table=source_table,
        dag_id="order_e2e_late_order_fixture",
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
        connection.execute("DELETE FROM order_payments WHERE order_id IN (SELECT order_id FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE customer_unique_id = %s))", (customer_unique_id,))
        connection.execute("DELETE FROM order_items WHERE order_id IN (SELECT order_id FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE customer_unique_id = %s))", (customer_unique_id,))
        connection.execute("DELETE FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE customer_unique_id = %s)", (customer_unique_id,))
        connection.execute("DELETE FROM customer_subscriptions WHERE customer_unique_id = %s", (customer_unique_id,))
        connection.execute("DELETE FROM customer_membership_tiers WHERE customer_unique_id = %s", (customer_unique_id,))
        connection.execute("DELETE FROM customers WHERE customer_unique_id = %s", (customer_unique_id,))
        connection.commit()


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    """dbt 실패 시 버전별 출력 위치 차이와 무관하게 전체 진단을 반환한다."""
    return f"{result.stdout}\n{result.stderr}"
