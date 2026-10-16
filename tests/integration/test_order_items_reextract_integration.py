"""주문 항목 재추출이 Fact 중복이나 주문 금액 변화를 만들지 않는지 검증한다."""

from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.generator.customers.customers import (
    ensure_subscription_records,
    new_customer_record,
    new_subscription_record,
)
from src.generator.orders.orders import new_order_bundle
from src.ingestion.bronze.storage import SeaweedFSSettings
from src.ingestion.control.metadata import CursorPosition
from src.warehouse.mart_hash import mart_logical_hashes, mismatched_relations
from tests.generator_support import apply_order_bundle
from tests.integration.test_incremental_full_refresh_hash_integration import _run_full_refresh_build
from tests.integration.test_order_e2e_and_late_order_mart_integration import (
    FIXTURE_START,
    _cleanup,
    _fetch_catalog,
    _generator_config,
    _seed_watermarks,
)
from tests.integration.test_subscription_payment_temporal_join_integration import (
    _append_fixture_catalog,
    _combined_output,
    _create_fixture_catalog,
    _ingest,
    _run_dbt_build,
    _set_watermark,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("status_only", [False, True])
@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_reextracted_order_items_do_not_duplicate_lines(tmp_path: Path, status_only: bool) -> None:
    """항목 재수집·주문 상태 변경은 항목을 중복 없이 교체하고 전체 재계산과 해시가 같다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_items_reextract_{uuid.uuid4().hex}"
    config = replace(_generator_config(FIXTURE_START), source_snapshot_id=pipeline_name)
    customer = new_customer_record(config, 1)
    bundle = new_order_bundle(config, customer, _fetch_catalog(postgres), order_ordinal=1)
    first_item = bundle.items[0]
    items = (first_item, replace(first_item, order_item_id=2))
    bundle = replace(
        bundle,
        items=items,
        payments=(replace(bundle.payments[0], payment_value=sum(
            item.price + item.freight_value for item in items
        )),),
    )
    order_id = bundle.order.order_id
    ingested_at = datetime.now(UTC)
    results = []
    warehouse_path = tmp_path / "incremental.duckdb"
    full_refresh_path = tmp_path / "full-refresh.duckdb"
    try:
        assert apply_order_bundle(postgres, bundle).items_inserted == 2
        with postgres.source_connection() as connection, connection.transaction():
            ensure_subscription_records(connection, (new_subscription_record(customer),))
        _seed_watermarks(postgres, pipeline_name, bundle, ingested_at)
        for source_table in (
            "customers", "customer_subscriptions", "customer_membership_tiers",
            "products", "sellers", "orders", "order_items", "order_payments",
        ):
            results.append(_ingest(
                postgres, storage, pipeline_name, source_table, FIXTURE_START,
                1, tmp_path, ingested_at,
            ))
        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            gross_before = connection.execute(
                "SELECT gross_order_value FROM facts.fct_order WHERE order_id = ?", [order_id]
            ).fetchone()[0]
        source_table = "orders" if status_only else "order_items"
        if status_only:
            with postgres.source_connection() as connection, connection.transaction():
                connection.execute(
                    "UPDATE orders SET order_status = 'canceled', updated_at = %s "
                    "WHERE order_id = %s",
                    (FIXTURE_START + timedelta(seconds=1), order_id),
                )
        else:
            _set_watermark(
                postgres, pipeline_name, "order_items",
                CursorPosition(first_item.created_at - timedelta(microseconds=1), (order_id, 1)),
                now=ingested_at,
            )
        repeated = _ingest(
            postgres, storage, pipeline_name, source_table,
            FIXTURE_START + timedelta(seconds=1),
            2, tmp_path, ingested_at + timedelta(seconds=1),
        )
        results.append(repeated)
        assert repeated.row_count == (1 if status_only else 2)
        _append_fixture_catalog(postgres, warehouse_path, [repeated])
        second_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            bronze_rows = connection.execute(
                "SELECT sum(row_count) FROM control.bronze_files WHERE source_table = 'order_items'"
            ).fetchone()[0]
        assert bronze_rows == (2 if status_only else 4)
        assert second_build.returncode == 0, _combined_output(second_build)
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            assert connection.execute(
                "SELECT order_id, order_item_id, order_status FROM facts.fct_order_item "
                "WHERE order_id = ? ORDER BY order_item_id", [order_id]
            ).fetchall() == [
                (order_id, item_id, "CANCELED" if status_only else "CREATED")
                for item_id in (1, 2)
            ]
            assert connection.execute(
                "SELECT gross_order_value FROM facts.fct_order WHERE order_id = ?", [order_id]
            ).fetchone()[0] == gross_before
        shutil.copy2(warehouse_path, full_refresh_path)
        full_build = _run_full_refresh_build(full_refresh_path, storage, tmp_path)
        assert full_build.returncode == 0, _combined_output(full_build)
        assert mismatched_relations(
            mart_logical_hashes(warehouse_path), mart_logical_hashes(full_refresh_path)
        ) == ()
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)
