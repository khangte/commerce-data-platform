"""6개 Table 공통 Batch Identity와 Commit된 Batch 재사용 판정을 제공한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from src.common.database import PostgresSettings
from src.ingestion.metadata import CursorPosition, Watermark
from src.ingestion.tables import TableConfig, table_config


class BatchIdentityConflictError(RuntimeError):
    """같은 Table Batch Identity가 다른 범위 또는 Schema를 가리킬 때 발생한다."""


@dataclass(frozen=True)
class BatchIdentity:
    """DAG와 UTC Logical Date에서 결정적으로 만드는 모든 Table 공통 Batch ID다."""

    dag_id: str
    logical_date: datetime

    def __post_init__(self) -> None:
        """Metadata Batch ID 길이 안의 DAG 식별자와 UTC Logical Date를 검증한다."""
        if not self.dag_id.strip() or len(self.dag_id) > 174:
            raise ValueError("dag_id must contain between 1 and 174 characters")
        if self.logical_date.tzinfo is None or self.logical_date.utcoffset() != timedelta(0):
            raise ValueError("logical_date must be normalized to UTC")

    @property
    def batch_id(self) -> str:
        """`{dag_id}__{UTC logical date}` 형식의 결정적 Batch ID를 반환한다."""
        return f"{self.dag_id}__{self.logical_date.astimezone(UTC):%Y%m%dT%H%M%SZ}"

    def table_batch(self, source_table: str) -> TableBatchIdentity:
        """지원 Source Table 하나에 대한 Table Batch Identity를 반환한다."""
        return TableBatchIdentity(batch=self, config=table_config(source_table))


@dataclass(frozen=True)
class TableBatchIdentity:
    """공통 Batch ID와 Source Table을 결합한 불변 Bronze Commit 식별자다."""

    batch: BatchIdentity
    config: TableConfig

    @property
    def batch_id(self) -> str:
        """모든 Table이 공유하는 Batch ID를 반환한다."""
        return self.batch.batch_id

    @property
    def table_batch_id(self) -> str:
        """Metadata·Object Catalog의 Table별 Batch 고유 식별자를 반환한다."""
        return f"{self.batch_id}__{self.config.source_table}"


@dataclass(frozen=True)
class CommittedTableBatch:
    """Metadata Catalog에 이미 Commit된 Table Batch의 재사용 판정 입력이다."""

    identity: TableBatchIdentity
    object_key: str
    manifest_key: str
    schema_version: int
    row_count: int
    watermark_before: CursorPosition
    watermark_after: CursorPosition


def get_committed_table_batch(
    settings: PostgresSettings, identity: TableBatchIdentity
) -> CommittedTableBatch | None:
    """COMMITTED Catalog Row가 있으면 재실행 판정에 필요한 증적을 반환한다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT object_key, manifest_key, schema_version, row_count, watermark_before, watermark_after
            FROM bronze_objects
            WHERE table_batch_id = %s AND status = 'COMMITTED'
            """,
            (identity.table_batch_id,),
        ).fetchone()
    if row is None:
        return None
    return CommittedTableBatch(
        identity=identity,
        object_key=row[0],
        manifest_key=row[1],
        schema_version=row[2],
        row_count=row[3],
        watermark_before=_cursor_from_metadata(row[4]),
        watermark_after=_cursor_from_metadata(row[5]),
    )


def assert_reusable_table_batch(
    existing: CommittedTableBatch,
    *,
    current_watermark: Watermark,
    schema_version: int,
) -> None:
    """현재 Cursor와 Schema가 Commit 증적과 같을 때만 Object 재사용을 허용한다."""
    if existing.schema_version != schema_version:
        raise BatchIdentityConflictError(
            "Committed batch schema_version differs from the requested schema"
        )
    if current_watermark.cursor != existing.watermark_after:
        raise BatchIdentityConflictError("Committed batch range differs from the current watermark")


def _cursor_from_metadata(value: dict[str, object]) -> CursorPosition:
    """Metadata JSONB의 Cursor Object를 타입 검증한 CursorPosition으로 복원한다."""
    timestamp_value = value.get("timestamp")
    keys_value = value.get("keys")
    if timestamp_value is None:
        return CursorPosition(None)
    if not isinstance(timestamp_value, str) or not isinstance(keys_value, list):
        raise TypeError("Invalid committed batch cursor metadata")
    timestamp = datetime.fromisoformat(timestamp_value).astimezone(UTC)
    return CursorPosition(timestamp, tuple(keys_value))
