"""`orders` Page를 명시적 Arrow Schema의 Local Bronze Parquet으로 기록한다."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.extract import SourcePage, SourceRecord
from src.ingestion.orders import OrdersPage, SourceOrderRecord
from src.ingestion.tables import BRONZE_SCHEMA_VERSION, ORDERS_TABLE, TableConfig

ROW_GROUP_TARGET_ROWS = 128_000
ORDERS_BUSINESS_COLUMNS = (
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "created_at",
    "updated_at",
)
ORDERS_BRONZE_SCHEMA = ORDERS_TABLE.bronze_schema


@dataclass(frozen=True)
class BronzeWriteContext:
    """- 한 Table Batch의 Bronze 기술 Column에 반복 기록할 고정 값이다."""

    batch_id: str
    run_id: uuid.UUID
    ingested_at: datetime
    source_table: str = "orders"
    schema_version: int = BRONZE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """- Batch 식별자, UTC 수집 시각, Source Table과 Schema Version을 검증한다."""
        if not self.batch_id or not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.ingested_at.tzinfo is None or self.ingested_at.utcoffset() != UTC.utcoffset(None):
            raise ValueError("ingested_at must be normalized to UTC")
        if not self.source_table.strip():
            raise ValueError("source_table must not be empty")
        if self.schema_version != BRONZE_SCHEMA_VERSION:
            raise ValueError(f"Unsupported orders Bronze schema_version: {self.schema_version}")


@dataclass(frozen=True)
class LocalParquetArtifact:
    """- 닫힌 Local Bronze Parquet의 경로와 최종 Row Count다."""

    path: Path
    row_count: int


class TableBronzeWriter:
    """- 등록된 Source Table의 Valid Record를 Local Bronze Parquet으로 기록한다."""

    def __init__(
        self,
        output_path: Path,
        config: TableConfig,
        context: BronzeWriteContext,
        *,
        row_group_target_rows: int = ROW_GROUP_TARGET_ROWS,
    ) -> None:
        """- 새 Local 파일 Writer를 열고 Table Schema·Context 일치를 검증한다."""
        if context.source_table != config.source_table:
            raise ValueError("Bronze write context source_table differs from the table config")
        if row_group_target_rows <= 0:
            raise ValueError("row_group_target_rows must be greater than zero")
        if output_path.exists():
            raise FileExistsError(f"Local Bronze Parquet already exists: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._path = output_path
        self._config = config
        self._context = context
        self._row_group_target_rows = row_group_target_rows
        self._writer = pq.ParquetWriter(
            output_path,
            config.bronze_schema,
            compression="zstd",
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
        )
        self._row_count = 0
        self._closed = False

    def write_page(self, page: SourcePage) -> None:
        """- Valid Source Page의 모든 Record를 명시적 Arrow Schema로 기록한다."""
        self.write_records(page.records)

    def write_records(self, records: tuple[SourceRecord, ...]) -> None:
        """- 비어 있지 않은 Valid Record 묶음을 Row Group 크기로 나눠 기록한다."""
        if self._closed:
            raise RuntimeError("Table Bronze Writer is already closed")
        if not records:
            return
        if any(record.config != self._config for record in records):
            raise ValueError("Bronze records must use the writer table config")
        table = pa.Table.from_pylist(
            [_source_record_row(record, self._context) for record in records],
            schema=self._config.bronze_schema,
        )
        self._writer.write_table(table, row_group_size=self._row_group_target_rows)
        self._row_count += table.num_rows

    def close(self) -> LocalParquetArtifact:
        """- Writer를 닫고 Local Artifact 경로와 Row Count를 반환한다."""
        if self._closed:
            raise RuntimeError("Table Bronze Writer is already closed")
        self._writer.close()
        self._closed = True
        return LocalParquetArtifact(path=self._path, row_count=self._row_count)

class OrdersBronzeWriter:
    """- `orders` Page를 제한된 Row Group Buffer로 Local Parquet에 순차 기록한다."""

    def __init__(
        self,
        output_path: Path,
        context: BronzeWriteContext,
        *,
        row_group_target_rows: int = ROW_GROUP_TARGET_ROWS,
    ) -> None:
        """- 새 Local 파일 Writer를 열고 최대 128K Row의 메모리 Buffer를 준비한다."""
        if row_group_target_rows <= 0:
            raise ValueError("row_group_target_rows must be greater than zero")
        if output_path.exists():
            raise FileExistsError(f"Local Bronze Parquet already exists: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._path = output_path
        self._context = context
        self._row_group_target_rows = row_group_target_rows
        self._writer = pq.ParquetWriter(
            output_path,
            ORDERS_BRONZE_SCHEMA,
            compression="zstd",
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
        )
        self._buffer: list[pa.Table] = []
        self._buffer_row_count = 0
        self._row_count = 0
        self._closed = False

    def write_page(self, page: OrdersPage) -> None:
        """- 한 Keyset Page를 Arrow Table로 변환해 제한된 Row Group Buffer에 추가한다."""
        self._assert_open()
        table = _orders_page_to_arrow_table(page, self._context)
        self._row_count += table.num_rows
        offset = 0
        while offset < table.num_rows:
            remaining_capacity = self._row_group_target_rows - self._buffer_row_count
            length = min(remaining_capacity, table.num_rows - offset)
            self._buffer.append(table.slice(offset, length))
            self._buffer_row_count += length
            offset += length
            if self._buffer_row_count == self._row_group_target_rows:
                self._flush_row_group()

    def close(self) -> LocalParquetArtifact:
        """- 남은 Row Group을 쓰고 파일을 닫은 뒤 Local Artifact 증적을 반환한다."""
        self._assert_open()
        try:
            self._flush_row_group()
            self._writer.close()
        finally:
            self._closed = True
        return LocalParquetArtifact(path=self._path, row_count=self._row_count)

    def _flush_row_group(self) -> None:
        """- 현재 Buffer를 한 Row Group으로 기록하고 메모리를 비운다."""
        if not self._buffer:
            return
        table = pa.concat_tables(self._buffer)
        self._writer.write_table(table, row_group_size=self._row_group_target_rows)
        self._buffer.clear()
        self._buffer_row_count = 0

    def _assert_open(self) -> None:
        """- 닫힌 Writer에 대한 추가 쓰기 또는 중복 Close를 막는다."""
        if self._closed:
            raise RuntimeError("Orders Bronze Writer is already closed")


def _orders_page_to_arrow_table(page: OrdersPage, context: BronzeWriteContext) -> pa.Table:
    """- Raw-compatible Source Order Page와 기술 Column을 Arrow Schema로 변환한다."""
    rows = [_order_row(record, context) for record in page.records]
    return pa.Table.from_pylist(rows, schema=ORDERS_BRONZE_SCHEMA)


def _order_row(record: SourceOrderRecord, context: BronzeWriteContext) -> dict[str, object]:
    """- Source Order와 고정 기술 Column을 Bronze Arrow Row 표현으로 변환한다."""
    return {
        "order_id": record.order_id,
        "customer_id": record.customer_id,
        "order_status": record.order_status,
        "order_purchase_timestamp": record.order_purchase_timestamp,
        "order_approved_at": record.order_approved_at,
        "order_delivered_carrier_date": record.order_delivered_carrier_date,
        "order_delivered_customer_date": record.order_delivered_customer_date,
        "order_estimated_delivery_date": record.order_estimated_delivery_date,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "_batch_id": context.batch_id,
        "_run_id": str(context.run_id),
        "_ingested_at": context.ingested_at,
        "_source_table": context.source_table,
        "_schema_version": context.schema_version,
    }


def orders_logical_hash(path: Path) -> str:
    """- Local Bronze를 PK 순서 Business Column Canonical JSON으로 Hash한다."""
    digest = hashlib.sha256()
    column_list = ", ".join(ORDERS_BUSINESS_COLUMNS)
    connection = duckdb.connect()
    try:
        batches = connection.execute(
            f"SELECT {column_list} FROM read_parquet(?) ORDER BY order_id", [str(path)]
        ).to_arrow_reader(batch_size=50_000)
        for batch in batches:
            for row in batch.to_pylist():
                digest.update(_canonical_business_json(row).encode("utf-8"))
                digest.update(b"\n")
    finally:
        connection.close()
    return digest.hexdigest()


def _canonical_business_json(row: dict[str, object]) -> str:
    """- Timestamp를 UTC ISO 문자열로 통일한 Business Row JSON을 반환한다."""
    return json.dumps(
        row,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_default(value: object) -> str:
    """- Business Hash JSON의 UTC Timestamp 표현을 결정적으로 변환한다."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Business timestamp must include a UTC offset")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"Unsupported business value: {type(value).__name__}")


def table_logical_hash(path: Path, config: TableConfig) -> str:
    """- 기술 Column을 제외한 Source Column을 PK 순서로 Canonical Hash한다."""
    digest = hashlib.sha256()
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    connection = duckdb.connect()
    try:
        batches = connection.execute(
            f"SELECT {column_list} FROM read_parquet(?) ORDER BY {order_by}", [str(path)]
        ).to_arrow_reader(batch_size=50_000)
        for batch in batches:
            for row in batch.to_pylist():
                digest.update(_canonical_business_json(row).encode("utf-8"))
                digest.update(b"\n")
    finally:
        connection.close()
    return digest.hexdigest()


def _source_record_row(record: SourceRecord, context: BronzeWriteContext) -> dict[str, object]:
    """- 일반 Source Record와 공통 기술 Column을 Bronze Arrow Row로 바꾼다."""
    return {
        **record.values,
        "_batch_id": context.batch_id,
        "_run_id": str(context.run_id),
        "_ingested_at": context.ingested_at,
        "_source_table": context.source_table,
        "_schema_version": context.schema_version,
    }
