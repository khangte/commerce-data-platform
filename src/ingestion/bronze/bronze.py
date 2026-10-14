"""Source Table Record를 명시적 Arrow Schema의 Local Bronze Parquet으로 기록한다."""

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

from src.ingestion.contract.tables import BRONZE_SCHEMA_VERSION, TableConfig
from src.ingestion.extraction.extract import SourcePage, SourceRecord

ROW_GROUP_TARGET_ROWS = 128_000


@dataclass(frozen=True)
class BronzeWriteContext:
    """한 Table Batch의 Bronze 기술 Column에 반복 기록할 고정 값이다."""

    batch_id: str
    run_id: uuid.UUID
    ingested_at: datetime
    source_table: str = "orders"
    schema_version: int = BRONZE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Batch 식별자, UTC 수집 시각, Source Table과 Schema Version을 검증한다."""
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
    """닫힌 Local Bronze Parquet의 경로와 최종 Row Count다."""

    path: Path
    row_count: int


class TableBronzeWriter:
    """등록된 Source Table의 Valid Record를 Local Bronze Parquet으로 기록한다."""

    def __init__(
        self,
        output_path: Path,
        config: TableConfig,
        context: BronzeWriteContext,
        *,
        row_group_target_rows: int = ROW_GROUP_TARGET_ROWS,
    ) -> None:
        """새 Local 파일 Writer를 열고 Table Schema·Context 일치를 검증한다."""
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
        """Valid Source Page의 모든 Record를 명시적 Arrow Schema로 기록한다."""
        self.write_records(page.records)

    def write_records(self, records: tuple[SourceRecord, ...]) -> None:
        """비어 있지 않은 Valid Record 묶음을 Row Group 크기로 나눠 기록한다."""
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
        """Writer를 닫고 Local Artifact 경로와 Row Count를 반환한다."""
        if self._closed:
            raise RuntimeError("Table Bronze Writer is already closed")
        self._writer.close()
        self._closed = True
        return LocalParquetArtifact(path=self._path, row_count=self._row_count)


def _canonical_business_json(row: dict[str, object]) -> str:
    """Timestamp를 UTC ISO 문자열로 통일한 Business Row JSON을 반환한다."""
    return json.dumps(
        row,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_default(value: object) -> str:
    """Business Hash JSON의 UTC Timestamp 표현을 결정적으로 변환한다."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Business timestamp must include a UTC offset")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"Unsupported business value: {type(value).__name__}")


def table_logical_hash(path: Path, config: TableConfig) -> str:
    """기술 Column을 제외한 Source Column을 PK 순서로 Canonical Hash한다."""
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    connection = duckdb.connect()
    try:
        return _hash_ordered_business_rows(
            connection,
            f"SELECT {column_list} FROM read_parquet(?) ORDER BY {order_by}",
            [str(path)],
        )
    finally:
        connection.close()


def table_logical_hash_bytes(payload: bytes, config: TableConfig) -> str:
    """재수신한 Bronze Parquet Byte의 PK 정렬 Business Hash를 다시 계산한다."""
    column_list = ", ".join(f'"{column}"' for column in config.source_column_names)
    order_by = ", ".join(f'"{column}"' for column in config.primary_key_columns)
    connection = duckdb.connect()
    try:
        connection.register("bronze_rows", pq.read_table(pa.BufferReader(payload)))
        return _hash_ordered_business_rows(
            connection, f"SELECT {column_list} FROM bronze_rows ORDER BY {order_by}"
        )
    finally:
        connection.close()


def _hash_ordered_business_rows(
    connection: duckdb.DuckDBPyConnection, query: str, parameters: list[str] | None = None
) -> str:
    """정렬 Query 결과의 Business Row를 Canonical JSON SHA-256으로 축적한다."""
    digest = hashlib.sha256()
    batches = connection.execute(query, parameters or []).to_arrow_reader(batch_size=50_000)
    for batch in batches:
        for row in batch.to_pylist():
            digest.update(_canonical_business_json(row).encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


def _source_record_row(record: SourceRecord, context: BronzeWriteContext) -> dict[str, object]:
    """일반 Source Record와 공통 기술 Column을 Bronze Arrow Row로 바꾼다."""
    return {
        **record.arrow_compatible_values(),
        "_batch_id": context.batch_id,
        "_run_id": str(context.run_id),
        "_ingested_at": context.ingested_at,
        "_source_table": context.source_table,
        "_schema_version": context.schema_version,
    }
