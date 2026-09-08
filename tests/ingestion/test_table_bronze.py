"""6개 Table 공통 Bronze Writer의 Schema·Decimal·Logical Hash를 검증한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq

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


def _payment(timestamp: datetime) -> SourceRecord:
    """금액 Decimal을 포함한 Raw-compatible Payment Source Record를 만든다."""
    values = {
        "order_id": "order-0001",
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 1,
        "payment_value": Decimal("12.34"),
        "payment_status": "completed",
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    return SourceRecord(ORDER_PAYMENTS_TABLE, MappingProxyType(values))
