"""Reject Record의 결정적 식별자·Local Quarantine Parquet·Threshold 정책을 제공한다."""

from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.validation import RejectedRecord

MAX_REJECT_RATE = 0.05
QUARANTINE_RECORD_NAMESPACE = uuid.UUID("a5c4e58c-fac7-5fc5-93fa-35f3f74d6518")
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("_record_id", pa.string(), nullable=False),
        pa.field("_source_table", pa.string(), nullable=False),
        pa.field("_batch_id", pa.string(), nullable=False),
        pa.field("_run_id", pa.string(), nullable=False),
        pa.field("_detected_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("_error_codes", pa.list_(pa.string()), nullable=False),
        pa.field("_raw_payload", pa.string(), nullable=False),
    ]
)


class RejectRateExceededError(RuntimeError):
    """- Reject 비율이 허용 Threshold를 넘을 때 발생한다."""


@dataclass(frozen=True)
class QuarantineWriteContext:
    """- 한 Table Batch Quarantine 기술 Column의 고정 값이다."""

    batch_id: str
    run_id: uuid.UUID
    detected_at: datetime

    @property
    def table_batch_id(self) -> str:
        """- Metadata와 결정적 Record ID에 쓸 Table Batch ID를 반환한다."""
        return f"{self.batch_id}__{self.source_table}"

    source_table: str = ""

    def __post_init__(self) -> None:
        """- Table 이름과 UTC 탐지 시각을 검증한다."""
        if not self.batch_id.strip() or not self.source_table.strip():
            raise ValueError("batch_id and source_table must not be empty")
        if self.detected_at.tzinfo is None or self.detected_at.utcoffset() != UTC.utcoffset(None):
            raise ValueError("detected_at must be normalized to UTC")


@dataclass(frozen=True)
class LocalQuarantineArtifact:
    """- 닫힌 Local Quarantine Parquet의 경로·Row Count·오류 집계다."""

    path: Path
    row_count: int
    error_counts: dict[str, int]


def quarantine_record_id(table_batch_id: str, ordinal: int) -> str:
    """- Table Batch ID와 추출 순번으로 PRD의 결정적 UUIDv5를 만든다."""
    if not table_batch_id.strip() or ordinal < 0:
        raise ValueError("table_batch_id must not be empty and ordinal must be non-negative")
    return str(uuid.uuid5(QUARANTINE_RECORD_NAMESPACE, f"{table_batch_id}:{ordinal}"))


class QuarantineWriter:
    """- Reject Record를 Raw Payload와 Error Code가 있는 Zstd Parquet으로 기록한다."""

    def __init__(self, output_path: Path, context: QuarantineWriteContext) -> None:
        """- 기존 Local Artifact 덮어쓰기를 막고 새 Quarantine Writer를 연다."""
        if output_path.exists():
            raise FileExistsError(f"Local Quarantine Parquet already exists: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._path = output_path
        self._context = context
        self._writer = pq.ParquetWriter(output_path, QUARANTINE_SCHEMA, compression="zstd")
        self._row_count = 0
        self._error_counts: Counter[str] = Counter()
        self._closed = False

    def write_rejected_records(self, rejected_records: tuple[RejectedRecord, ...]) -> None:
        """- 한 검증 Page의 Reject를 명시적 Schema Quarantine Row로 기록한다."""
        if self._closed:
            raise RuntimeError("Quarantine Writer is already closed")
        if not rejected_records:
            return
        rows = [_quarantine_row(rejected, self._context) for rejected in rejected_records]
        self._writer.write_table(pa.Table.from_pylist(rows, schema=QUARANTINE_SCHEMA))
        self._row_count += len(rows)
        self._error_counts.update(
            code for rejected in rejected_records for code in rejected.error_codes
        )

    def close(self) -> LocalQuarantineArtifact:
        """- Writer를 닫고 Local Artifact와 결정적 오류 Code Count를 반환한다."""
        if self._closed:
            raise RuntimeError("Quarantine Writer is already closed")
        self._writer.close()
        self._closed = True
        return LocalQuarantineArtifact(
            self._path, self._row_count, dict(sorted(self._error_counts.items()))
        )


def assert_reject_rate(*, total_rows: int, rejected_rows: int) -> None:
    """- Reject 비율이 5% 이하면 통과시키고 초과하면 Batch 실패로 전환한다."""
    if total_rows < 0 or rejected_rows < 0 or rejected_rows > total_rows:
        raise ValueError("Reject counts must be within the total row count")
    if total_rows and rejected_rows / total_rows > MAX_REJECT_RATE:
        raise RejectRateExceededError(f"Reject rate exceeds {MAX_REJECT_RATE:.0%}")


def _quarantine_row(rejected: RejectedRecord, context: QuarantineWriteContext) -> dict[str, object]:
    """- Type 오류도 보존할 수 있는 JSON Raw Payload Quarantine Row를 만든다."""
    return {
        "_record_id": quarantine_record_id(context.table_batch_id, rejected.ordinal),
        "_source_table": rejected.record.config.source_table,
        "_batch_id": context.batch_id,
        "_run_id": str(context.run_id),
        "_detected_at": context.detected_at,
        "_error_codes": list(rejected.error_codes),
        "_raw_payload": _canonical_json(dict(rejected.record.values)),
    }


def _canonical_json(value: object) -> str:
    """- Datetime·Decimal을 안정적인 문자열로 바꾼 Raw Payload JSON을 반환한다."""
    return json.dumps(
        value, default=_json_default, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )


def _json_default(value: object) -> str:
    """- Quarantine Raw Payload의 JSON 비기본 Type을 결정적으로 직렬화한다."""
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"Unsupported quarantine value: {type(value).__name__}")
