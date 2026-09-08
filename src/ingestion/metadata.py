"""증분 수집 Metadata의 Watermark, 실행 상태, 원자적 Commit을 제공한다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file

RUNNING = "RUNNING"
SUCCESS = "SUCCESS"
SUCCESS_NO_DATA = "SUCCESS_NO_DATA"
SKIPPED_ALREADY_COMMITTED = "SKIPPED_ALREADY_COMMITTED"
FAILED = "FAILED"


class WatermarkConflictError(RuntimeError):
    """기대한 Watermark Version 또는 Cursor가 달라 Commit할 수 없을 때 발생한다."""


class PipelineRunStateError(RuntimeError):
    """허용되지 않은 Pipeline Run 상태 전이를 시도했을 때 발생한다."""


@dataclass(frozen=True)
class CursorPosition:
    """Timestamp와 전체 PK Tie-breaker로 구성된 저장용 Composite Cursor다."""

    timestamp: datetime | None
    keys: tuple[str | int, ...] = ()

    def __post_init__(self) -> None:
        """초기 Cursor와 UTC Timestamp·PK Key 표현을 검증한다."""
        if self.timestamp is None:
            if self.keys:
                raise ValueError("Initial cursor cannot contain keys")
            return
        _assert_utc(self.timestamp, "cursor timestamp")
        if not self.keys:
            raise ValueError("Non-initial cursor requires complete primary-key tie-breakers")
        if any(isinstance(value, bool) or not isinstance(value, (str, int)) for value in self.keys):
            raise TypeError("Cursor keys must be strings or integers")

    def as_json(self) -> list[str | int]:
        """Metadata JSONB에 저장할 PK Tie-breaker 배열을 반환한다."""
        return list(self.keys)

    def as_metadata_json(self) -> dict[str, object]:
        """실행과 Object Metadata에 남길 전체 Cursor 표현을 반환한다."""
        return {
            "timestamp": self.timestamp.isoformat() if self.timestamp is not None else None,
            "keys": self.as_json(),
        }


@dataclass(frozen=True)
class Watermark:
    """Table별 현재 Cursor와 CAS·Lease에 사용할 Version Snapshot이다."""

    pipeline_name: str
    source_table: str
    cursor: CursorPosition
    version: int


@dataclass(frozen=True)
class PipelineRun:
    """한 Source Table 수집 시도의 식별자와 시작 상태 입력이다."""

    run_id: uuid.UUID
    pipeline_name: str
    source_table: str
    batch_id: str
    logical_date: datetime
    attempt_number: int
    watermark_before: CursorPosition
    extract_upper_bound: CursorPosition | None
    dag_id: str | None = None

    def __post_init__(self) -> None:
        """수집 실행 식별자와 논리 시각·Attempt 범위를 검증한다."""
        _assert_nonempty(self.pipeline_name, "pipeline_name", 128)
        _assert_nonempty(self.source_table, "source_table", 64)
        _assert_nonempty(self.batch_id, "batch_id", 192)
        if self.attempt_number <= 0:
            raise ValueError("attempt_number must be greater than zero")
        _assert_utc(self.logical_date, "logical_date")
        if self.dag_id is not None:
            _assert_nonempty(self.dag_id, "dag_id", 250)


@dataclass(frozen=True)
class VerifiedBronzeObject:
    """검증을 마쳐 Metadata Commit만 남은 불변 Bronze Object의 증적이다."""

    table_batch_id: str
    object_key: str
    manifest_key: str
    schema_version: int
    row_count: int
    content_sha256: str
    logical_hash: str
    watermark_after: CursorPosition

    def __post_init__(self) -> None:
        """Commit 가능한 Object Key, Hash, Schema, Row Count를 검증한다."""
        _assert_nonempty(self.table_batch_id, "table_batch_id", 320)
        _assert_nonempty(self.object_key, "object_key")
        _assert_nonempty(self.manifest_key, "manifest_key")
        if self.schema_version <= 0:
            raise ValueError("schema_version must be greater than zero")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        _assert_sha256(self.content_sha256, "content_sha256")
        _assert_sha256(self.logical_hash, "logical_hash")


@dataclass(frozen=True)
class QuarantineBatch:
    """Metadata Commit에 함께 기록할 검증된 Quarantine Object 증적이다."""

    table_batch_id: str
    object_key: str
    row_count: int
    error_counts: dict[str, int]

    def __post_init__(self) -> None:
        """Table Batch·Object Key·Row Count·오류 집계의 유효성을 검증한다."""
        _assert_nonempty(self.table_batch_id, "table_batch_id", 320)
        _assert_nonempty(self.object_key, "object_key")
        if self.row_count < 0 or any(not code or count < 0 for code, count in self.error_counts.items()):
            raise ValueError("Quarantine counts must be non-negative and named")


@dataclass(frozen=True)
class TableCommit:
    """검증된 Object, 실행 성공, Watermark CAS를 함께 Commit할 입력이다."""

    run: PipelineRun
    object: VerifiedBronzeObject
    expected_watermark: Watermark
    rows_extracted: int
    rows_valid: int
    rows_rejected: int
    rows_loaded: int
    quarantine: QuarantineBatch | None = None

    def __post_init__(self) -> None:
        """Commit Count와 Watermark가 실행의 시작 Cursor에 대응하는지 검증한다."""
        if self.expected_watermark.pipeline_name != self.run.pipeline_name:
            raise ValueError("Expected watermark pipeline_name differs from the pipeline run")
        if self.expected_watermark.source_table != self.run.source_table:
            raise ValueError("Expected watermark source_table differs from the pipeline run")
        if self.expected_watermark.cursor != self.run.watermark_before:
            raise ValueError("Expected watermark cursor differs from the pipeline run")
        counts = (self.rows_extracted, self.rows_valid, self.rows_rejected, self.rows_loaded)
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts
        ):
            raise ValueError("Commit row counts must be non-negative integers")
        if self.quarantine is not None:
            if self.quarantine.table_batch_id != self.object.table_batch_id:
                raise ValueError("Quarantine table_batch_id differs from the Bronze object")
            if self.quarantine.row_count != self.rows_rejected:
                raise ValueError("Quarantine row_count must equal rows_rejected")


def ensure_ingestion_metadata(settings: PostgresSettings) -> None:
    """증분 수집의 Metadata Table·제약조건·Index를 멱등적으로 준비한다."""
    with settings.pipeline_connection() as connection:
        apply_sql_file(connection, "sql/metadata/004_create_ingestion_metadata.sql")


def get_or_create_watermark(
    settings: PostgresSettings,
    pipeline_name: str,
    source_table: str,
    *,
    now: datetime | None = None,
) -> Watermark:
    """초기 Watermark를 필요할 때 만들고 현재 CAS Snapshot을 반환한다."""
    _assert_nonempty(pipeline_name, "pipeline_name", 128)
    _assert_nonempty(source_table, "source_table", 64)
    current_time = _utc_now(now)
    ensure_ingestion_metadata(settings)
    with settings.pipeline_connection() as connection, connection.transaction():
        return _get_or_create_watermark(connection, pipeline_name, source_table, current_time)


def record_started_run(
    settings: PostgresSettings, run: PipelineRun, *, now: datetime | None = None
) -> None:
    """수집 시작 전 RUNNING 상태와 고정 범위 Metadata를 기록한다."""
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, source_table, pipeline_name, batch_id, dag_id, logical_date,
                attempt_number, started_at, watermark_before, extract_upper_bound, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run.run_id,
                run.source_table,
                run.pipeline_name,
                run.batch_id,
                run.dag_id,
                run.logical_date,
                run.attempt_number,
                current_time,
                Jsonb(run.watermark_before.as_metadata_json()),
                _cursor_json(run.extract_upper_bound),
                RUNNING,
            ),
        )


def commit_table_run(
    settings: PostgresSettings, commit: TableCommit, *, now: datetime | None = None
) -> None:
    """Object·성공 Run·Watermark CAS를 하나의 Metadata Transaction으로 Commit한다."""
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        if commit.quarantine is not None:
            connection.execute(
                """
                INSERT INTO quarantine_batches (table_batch_id, object_key, row_count, error_counts, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    commit.quarantine.table_batch_id,
                    commit.quarantine.object_key,
                    commit.quarantine.row_count,
                    Jsonb(commit.quarantine.error_counts),
                    current_time,
                ),
            )
        connection.execute(
            """
            INSERT INTO bronze_objects (
                table_batch_id, source_table, batch_id, object_key, manifest_key, schema_version,
                row_count, content_sha256, logical_hash, watermark_before, watermark_after,
                status, committed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'COMMITTED', %s)
            """,
            (
                commit.object.table_batch_id,
                commit.run.source_table,
                commit.run.batch_id,
                commit.object.object_key,
                commit.object.manifest_key,
                commit.object.schema_version,
                commit.object.row_count,
                commit.object.content_sha256,
                commit.object.logical_hash,
                Jsonb(commit.run.watermark_before.as_metadata_json()),
                Jsonb(commit.object.watermark_after.as_metadata_json()),
                current_time,
            ),
        )
        updated_run = connection.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = %s,
                rows_extracted = %s,
                rows_valid = %s,
                rows_rejected = %s,
                rows_loaded = %s,
                status = 'SUCCESS'
            WHERE run_id = %s AND source_table = %s AND status = 'RUNNING'
            """,
            (
                current_time,
                commit.rows_extracted,
                commit.rows_valid,
                commit.rows_rejected,
                commit.rows_loaded,
                commit.run.run_id,
                commit.run.source_table,
            ),
        )
        if updated_run.rowcount != 1:
            raise PipelineRunStateError("Only a RUNNING pipeline run can be committed")
        updated_watermark = connection.execute(
            """
            UPDATE watermarks
            SET watermark_timestamp = %s,
                watermark_keys = %s,
                version = version + 1,
                updated_at = %s
            WHERE pipeline_name = %s
              AND source_table = %s
              AND version = %s
              AND watermark_timestamp IS NOT DISTINCT FROM %s
              AND watermark_keys = %s
            """,
            (
                commit.object.watermark_after.timestamp,
                Jsonb(commit.object.watermark_after.as_json()),
                current_time,
                commit.expected_watermark.pipeline_name,
                commit.expected_watermark.source_table,
                commit.expected_watermark.version,
                commit.expected_watermark.cursor.timestamp,
                Jsonb(commit.expected_watermark.cursor.as_json()),
            ),
        )
        if updated_watermark.rowcount != 1:
            raise WatermarkConflictError("Watermark changed before metadata commit")


def record_failed_run(
    settings: PostgresSettings,
    run: PipelineRun,
    *,
    error_type: str,
    error_message: str | None = None,
    now: datetime | None = None,
) -> None:
    """RUNNING 수집을 FAILED로 종료하며 Watermark는 변경하지 않는다."""
    _assert_nonempty(error_type, "error_type", 64)
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        updated_run = connection.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = %s,
                status = 'FAILED',
                error_type = %s,
                error_message = %s
            WHERE run_id = %s AND source_table = %s AND status = 'RUNNING'
            """,
            (current_time, error_type, error_message, run.run_id, run.source_table),
        )
        if updated_run.rowcount != 1:
            raise PipelineRunStateError("Only a RUNNING pipeline run can fail")


def record_success_no_data_run(
    settings: PostgresSettings, run: PipelineRun, *, now: datetime | None = None
) -> None:
    """빈 고정 범위의 RUNNING 수집을 Object·Watermark 없이 성공 종료한다."""
    if run.extract_upper_bound is not None:
        raise ValueError("SUCCESS_NO_DATA requires an empty extract_upper_bound")
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        updated_run = connection.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = %s,
                status = 'SUCCESS_NO_DATA'
            WHERE run_id = %s AND source_table = %s AND status = 'RUNNING'
            """,
            (current_time, run.run_id, run.source_table),
        )
        if updated_run.rowcount != 1:
            raise PipelineRunStateError("Only a RUNNING pipeline run can finish with no data")


def record_skipped_already_committed_run(
    settings: PostgresSettings,
    run: PipelineRun,
    *,
    row_count: int,
    now: datetime | None = None,
) -> None:
    """이미 Commit된 같은 범위의 RUNNING 재실행을 Object 생성 없이 Skip으로 종료한다."""
    if row_count < 0:
        raise ValueError("row_count must be non-negative")
    current_time = _utc_now(now)
    with settings.pipeline_connection() as connection, connection.transaction():
        updated_run = connection.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = %s,
                rows_extracted = %s,
                rows_valid = %s,
                rows_loaded = %s,
                status = 'SKIPPED_ALREADY_COMMITTED'
            WHERE run_id = %s AND source_table = %s AND status = 'RUNNING'
            """,
            (current_time, row_count, row_count, row_count, run.run_id, run.source_table),
        )
        if updated_run.rowcount != 1:
            raise PipelineRunStateError("Only a RUNNING pipeline run can be skipped")


def _get_or_create_watermark(
    connection: psycopg.Connection,
    pipeline_name: str,
    source_table: str,
    current_time: datetime,
) -> Watermark:
    """현재 Transaction 안에서 초기 Watermark를 만들고 Lock된 Snapshot을 읽는다."""
    connection.execute(
        """
        INSERT INTO watermarks (pipeline_name, source_table, watermark_keys, updated_at)
        VALUES (%s, %s, '[]'::jsonb, %s)
        ON CONFLICT (pipeline_name, source_table) DO NOTHING
        """,
        (pipeline_name, source_table, current_time),
    )
    row = connection.execute(
        """
        SELECT watermark_timestamp, watermark_keys, version
        FROM watermarks
        WHERE pipeline_name = %s AND source_table = %s
        FOR UPDATE
        """,
        (pipeline_name, source_table),
    ).fetchone()
    if row is None:
        raise RuntimeError("Watermark row is missing")
    return Watermark(
        pipeline_name=pipeline_name,
        source_table=source_table,
        cursor=CursorPosition(timestamp=row[0], keys=tuple(row[1])),
        version=row[2],
    )


def _cursor_json(cursor: CursorPosition | None) -> Jsonb | None:
    """선택적인 Upper Bound Cursor를 Metadata JSONB 값으로 변환한다."""
    if cursor is None:
        return None
    return Jsonb(cursor.as_metadata_json())


def _assert_nonempty(value: str, name: str, maximum_length: int | None = None) -> None:
    """공백이 아닌 문자열과 선택적인 저장 길이 제한을 확인한다."""
    if not value or not value.strip():
        raise ValueError(f"{name} must not be empty")
    if maximum_length is not None and len(value) > maximum_length:
        raise ValueError(f"{name} must contain at most {maximum_length} characters")


def _assert_sha256(value: str, name: str) -> None:
    """소문자 SHA-256 Hex 값인지 확인한다."""
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex value")


def _assert_utc(value: datetime, name: str) -> None:
    """시각이 UTC Offset을 포함하고 UTC로 정규화됐는지 확인한다."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be normalized to UTC")


def _utc_now(value: datetime | None) -> datetime:
    """주입된 시각 또는 현재 시각을 UTC로 정규화해 반환한다."""
    if value is None:
        return datetime.now(UTC)
    _assert_utc(value, "now")
    return value
