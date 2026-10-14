"""6개 Table 공통 Bronze Writer의 Schema·Decimal·Logical Hash를 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.ingestion.bronze import (
    BronzeWriteContext,
    TableBronzeWriter,
    table_logical_hash,
    table_logical_hash_bytes,
)
from src.ingestion.extract import SourceRecord
from src.ingestion.tables import ORDER_PAYMENTS_TABLE


def test_table_bronze_writer_supports_non_orders_decimal_table_and_stable_logical_hash(
    tmp_path,
) -> None:
    """Payment Writer는 Decimal Schema·기술 Column과 실행 독립 Hash를 보존한다."""
    timestamp = datetime(2026, 9, 7, tzinfo=UTC)
    record = _payment(timestamp)
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    first_writer = TableBronzeWriter(
        first,
        ORDER_PAYMENTS_TABLE,
        BronzeWriteContext("batch-one", uuid.uuid4(), timestamp, "order_payments"),
    )
    second_writer = TableBronzeWriter(
        second,
        ORDER_PAYMENTS_TABLE,
        BronzeWriteContext("batch-two", uuid.uuid4(), timestamp, "order_payments"),
    )

    first_writer.write_records((record,))
    second_writer.write_records((record,))
    artifact = first_writer.close()
    second_writer.close()
    table = pq.read_table(first)

    assert artifact.row_count == 1
    assert table.schema == ORDER_PAYMENTS_TABLE.bronze_schema
    assert table.schema.field("payment_value").type == pa.decimal128(14, 2)
    assert table_logical_hash(first, ORDER_PAYMENTS_TABLE) == table_logical_hash(
        second, ORDER_PAYMENTS_TABLE
    )
    assert table_logical_hash_bytes(first.read_bytes(), ORDER_PAYMENTS_TABLE) == table_logical_hash(
        first, ORDER_PAYMENTS_TABLE
    )


def test_table_bronze_writer_keeps_technical_columns_and_limits_row_groups(tmp_path) -> None:
    """Writer는 UTC Timestamp·기술 Column·ZSTD를 보존하고 Row Group을 제한 크기로 나눈다."""
    base_time = datetime(2026, 9, 7, 1, 2, 3, 456789, tzinfo=UTC)
    output_path = tmp_path / "payments.parquet"
    context = BronzeWriteContext(
        "warehouse__20260907T000000Z",
        uuid.UUID("5f809cf8-92e8-4c29-a843-b771a288e5bb"),
        base_time,
        "order_payments",
    )
    writer = TableBronzeWriter(output_path, ORDER_PAYMENTS_TABLE, context, row_group_target_rows=2)

    writer.write_records((_payment(base_time), _payment(base_time + timedelta(seconds=1))))
    writer.write_records((_payment(base_time + timedelta(seconds=2)),))
    artifact = writer.close()

    table = pq.read_table(output_path)
    metadata = pq.ParquetFile(output_path).metadata
    assert artifact.row_count == 3
    assert table.column("_batch_id").to_pylist() == [context.batch_id] * 3
    assert table.column("_run_id").to_pylist() == [str(context.run_id)] * 3
    assert table.column("_ingested_at").to_pylist() == [base_time] * 3
    assert table.column("_source_table").to_pylist() == ["order_payments"] * 3
    assert table.schema.field("updated_at").type.unit == "us"
    assert table.schema.field("updated_at").type.tz == "UTC"
    assert metadata.num_row_groups == 2
    assert [metadata.row_group(index).num_rows for index in range(2)] == [2, 1]
    assert metadata.row_group(0).column(0).compression == "ZSTD"


def test_table_bronze_writer_rejects_existing_file_and_writes_after_close(tmp_path) -> None:
    """불변 Local Artifact 경로의 덮어쓰기와 닫힌 Writer 재사용을 막는다."""
    timestamp = datetime(2026, 9, 7, tzinfo=UTC)
    context = BronzeWriteContext("batch", uuid.uuid4(), timestamp, "order_payments")
    existing = tmp_path / "existing.parquet"
    existing.touch()
    with pytest.raises(FileExistsError):
        TableBronzeWriter(existing, ORDER_PAYMENTS_TABLE, context)

    writer = TableBronzeWriter(tmp_path / "new.parquet", ORDER_PAYMENTS_TABLE, context)
    writer.write_records((_payment(timestamp),))
    writer.close()
    with pytest.raises(RuntimeError, match="closed"):
        writer.write_records((_payment(timestamp),))


def _payment(timestamp: datetime) -> SourceRecord:
    """금액 Decimal을 포함한 Raw-compatible Payment Source Record를 만든다."""
    values = {
        "order_id": "order-0001",
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 1,
        "payment_value": Decimal("12.34"),
        "payment_status": "completed",
        "payment_initiated_at": timestamp,
        "payment_completed_at": timestamp,
        "payment_failed_at": None,
        "payment_refunded_at": None,
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    return SourceRecord(ORDER_PAYMENTS_TABLE, MappingProxyType(values))
