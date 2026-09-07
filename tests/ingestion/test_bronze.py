"""`orders` Local Bronze Parquet Writer 계약을 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.ingestion.bronze import (
    BRONZE_SCHEMA_VERSION,
    ORDERS_BRONZE_SCHEMA,
    BronzeWriteContext,
    OrdersBronzeWriter,
)
from src.ingestion.metadata import CursorPosition
from src.ingestion.orders import OrdersPage, SourceOrderRecord


def test_orders_writer_writes_explicit_schema_technical_columns_and_row_groups(tmp_path) -> None:
    """Page 단위 Write는 UTC Parquet Schema·기술 컬럼·제한 Row Group을 보존한다."""
    base_time = datetime(2026, 9, 7, 1, 2, 3, 456789, tzinfo=UTC)
    output_path = tmp_path / "orders.parquet"
    context = BronzeWriteContext(
        batch_id="warehouse__20260907T000000Z",
        run_id=uuid.UUID("5f809cf8-92e8-4c29-a843-b771a288e5bb"),
        ingested_at=base_time,
    )
    writer = OrdersBronzeWriter(output_path, context, row_group_target_rows=2)

    writer.write_page(_page(base_time, 1, 2))
    writer.write_page(_page(base_time, 3))
    artifact = writer.close()

    table = pq.read_table(output_path)
    parquet_file = pq.ParquetFile(output_path)
    assert artifact.row_count == 3
    assert table.schema == ORDERS_BRONZE_SCHEMA
    assert table.column("_batch_id").to_pylist() == [context.batch_id] * 3
    assert table.column("_run_id").to_pylist() == [str(context.run_id)] * 3
    assert table.column("_ingested_at").to_pylist() == [base_time] * 3
    assert table.column("_source_table").to_pylist() == ["orders"] * 3
    assert table.column("_schema_version").to_pylist() == [BRONZE_SCHEMA_VERSION] * 3
    assert pa.types.is_timestamp(table.schema.field("updated_at").type)
    assert table.schema.field("updated_at").type.unit == "us"
    assert table.schema.field("updated_at").type.tz == "UTC"
    assert parquet_file.metadata.num_row_groups == 2
    assert parquet_file.metadata.row_group(0).num_rows == 2
    assert parquet_file.metadata.row_group(1).num_rows == 1
    assert parquet_file.metadata.row_group(0).column(0).compression == "ZSTD"


def test_orders_writer_rejects_existing_file_and_writes_after_close(tmp_path) -> None:
    """불변 Local Artifact 경로의 덮어쓰기와 닫힌 Writer 재사용을 막는다."""
    output_path = tmp_path / "orders.parquet"
    output_path.touch()
    context = BronzeWriteContext(
        batch_id="warehouse__20260907T000000Z",
        run_id=uuid.uuid4(),
        ingested_at=datetime(2026, 9, 7, tzinfo=UTC),
    )
    with pytest.raises(FileExistsError):
        OrdersBronzeWriter(output_path, context)

    writable_path = tmp_path / "new-orders.parquet"
    writer = OrdersBronzeWriter(writable_path, context)
    writer.write_page(_page(context.ingested_at, 1))
    writer.close()
    with pytest.raises(RuntimeError, match="closed"):
        writer.write_page(_page(context.ingested_at, 2))


def _page(base_time: datetime, *ordinals: int) -> OrdersPage:
    """지정 Ordinal의 Raw-compatible Order Row를 가진 Keyset Page를 반환한다."""
    records = tuple(_record(base_time, ordinal) for ordinal in ordinals)
    return OrdersPage(
        records=records,
        lower_bound=CursorPosition(base_time - timedelta(seconds=1), ("order-0000",)),
        extract_upper_bound=records[-1].cursor,
    )


def _record(base_time: datetime, ordinal: int) -> SourceOrderRecord:
    """명시적 Timestamp 정밀도 검증에 사용할 최소 Source Order Row를 반환한다."""
    timestamp = base_time + timedelta(seconds=ordinal)
    return SourceOrderRecord(
        order_id=f"order-{ordinal:04d}",
        customer_id=f"customer-{ordinal:04d}",
        order_status="created",
        order_purchase_timestamp=timestamp,
        order_approved_at=None,
        order_delivered_carrier_date=None,
        order_delivered_customer_date=None,
        order_estimated_delivery_date=timestamp + timedelta(days=7),
        created_at=timestamp,
        updated_at=timestamp,
    )
