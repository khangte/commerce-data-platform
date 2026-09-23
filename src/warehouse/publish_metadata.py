"""mart_publish_runs 상태 전이를 PostgreSQL Transaction으로 기록한다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file
from src.warehouse.errors import PublishInProgressError, PublishStateError

BUILDING = "BUILDING"
PUBLISHING = "PUBLISHING"
PUBLISHED = "PUBLISHED"
FAILED = "FAILED"
ACTIVE_STATUSES = (BUILDING, PUBLISHING)
ACTIVE_PUBLISH_INDEX = "mart_publish_runs_single_active_idx"
ERROR_MESSAGE_LIMIT = 4000

_RECORD_COLUMNS = (
    "publish_run_id, status, started_at, previous_publish_run_id, mart_hashes, error_type"
)


@dataclass(frozen=True)
class PublishRun:
    """새 Publish Run을 시작할 때 필요한 식별 정보를 담는다."""

    publish_run_id: uuid.UUID
    pipeline_name: str
    batch_id: str | None = None
    dag_run_id: str | None = None


@dataclass(frozen=True)
class PublishRecord:
    """저장된 Publish Run 한 건의 상태 요약을 담는다."""

    publish_run_id: uuid.UUID
    status: str
    started_at: datetime
    previous_publish_run_id: uuid.UUID | None
    mart_hashes: dict[str, str] | None
    error_type: str | None


def ensure_publish_metadata(settings: PostgresSettings) -> None:
    """mart_publish_runs Table과 Index를 멱등적으로 준비한다."""
    with settings.pipeline_connection() as connection:
        apply_sql_file(connection, "sql/metadata/005_create_mart_publish_runs.sql")


def start_publish_run(
    settings: PostgresSettings, run: PublishRun, *, now: datetime
) -> PublishRecord:
    """BUILDING Row를 만들고, 다른 활성 Run이 있으면 PublishInProgressError를 낸다."""
    try:
        with settings.pipeline_connection() as connection:
            row = connection.execute(
                f"""
                INSERT INTO mart_publish_runs (
                    publish_run_id, pipeline_name, batch_id, dag_run_id, status,
                    previous_publish_run_id, started_at
                )
                SELECT %s, %s, %s, %s, 'BUILDING', (
                    SELECT publish_run_id FROM mart_publish_runs
                    WHERE status = 'PUBLISHED'
                    ORDER BY finished_at DESC, started_at DESC
                    LIMIT 1
                ), %s
                RETURNING {_RECORD_COLUMNS}
                """,
                (run.publish_run_id, run.pipeline_name, run.batch_id, run.dag_run_id, now),
            ).fetchone()
    except psycopg.errors.UniqueViolation as error:
        if error.diag.constraint_name == ACTIVE_PUBLISH_INDEX:
            raise PublishInProgressError("Another mart publish run is active") from error
        raise
    return _record(row)


def record_dbt_result(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    *,
    invocation_id: str | None,
    tests_passed: int,
    tests_failed: int,
) -> None:
    """BUILDING Run에 dbt Invocation ID와 Test 통과·실패 수를 기록한다."""
    _transition(
        settings,
        publish_run_id,
        (BUILDING,),
        "dbt_invocation_id = %s, tests_passed = %s, tests_failed = %s",
        (invocation_id, tests_passed, tests_failed),
    )


def mark_publishing(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    *,
    mart_hashes: dict[str, str],
    mart_row_counts: dict[str, int],
) -> None:
    """Swap 직전 Hash와 Row 수를 저장하고 BUILDING을 PUBLISHING으로 바꾼다."""
    _transition(
        settings,
        publish_run_id,
        (BUILDING,),
        "status = 'PUBLISHING', mart_hashes = %s, mart_row_counts = %s",
        (Jsonb(mart_hashes), Jsonb(mart_row_counts)),
    )


def mark_published(settings: PostgresSettings, publish_run_id: uuid.UUID, *, now: datetime) -> None:
    """Swap이 끝난 PUBLISHING Run을 PUBLISHED로 종료한다."""
    _transition(
        settings, publish_run_id, (PUBLISHING,), "status = 'PUBLISHED', finished_at = %s", (now,)
    )


def mark_failed(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    *,
    error_type: str,
    error_message: str,
    failed_path: str | None,
    now: datetime,
) -> None:
    """활성 Run을 Error Type·메시지·격리 경로와 함께 FAILED로 종료한다."""
    _transition(
        settings,
        publish_run_id,
        ACTIVE_STATUSES,
        "status = 'FAILED', error_type = %s, error_message = %s, failed_path = %s,"
        " finished_at = %s",
        (error_type, error_message[:ERROR_MESSAGE_LIMIT], failed_path, now),
    )


def get_publish_run(settings: PostgresSettings, publish_run_id: uuid.UUID) -> PublishRecord | None:
    """Publish Run 한 건을 조회하고 없으면 None을 반환한다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            f"SELECT {_RECORD_COLUMNS} FROM mart_publish_runs WHERE publish_run_id = %s",
            (publish_run_id,),
        ).fetchone()
    return None if row is None else _record(row)


def get_latest_publish_run(settings: PostgresSettings) -> PublishRecord | None:
    """가장 최근에 완료된 PUBLISHED Run을 조회한다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            f"""
            SELECT {_RECORD_COLUMNS}
            FROM mart_publish_runs
            WHERE status = 'PUBLISHED'
            ORDER BY finished_at DESC, started_at DESC
            LIMIT 1
            """
        ).fetchone()
    return None if row is None else _record(row)


def stale_active_runs(
    settings: PostgresSettings, *, older_than: datetime
) -> tuple[PublishRecord, ...]:
    """기준 시각 이전에 시작해 아직 활성 상태인 Run을 반환한다."""
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT {_RECORD_COLUMNS} FROM mart_publish_runs
            WHERE status IN ('BUILDING', 'PUBLISHING') AND started_at < %s
            ORDER BY started_at
            """,
            (older_than,),
        ).fetchall()
    return tuple(_record(row) for row in rows)


def assert_no_active_publish(settings: PostgresSettings) -> None:
    """활성 Publish Run이 있으면 PublishInProgressError를 낸다."""
    ensure_publish_metadata(settings)
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            "SELECT publish_run_id FROM mart_publish_runs"
            " WHERE status IN ('BUILDING', 'PUBLISHING') LIMIT 1"
        ).fetchone()
    if row is not None:
        raise PublishInProgressError(f"Mart publish run {row[0]} is active")


def _transition(
    settings: PostgresSettings,
    publish_run_id: uuid.UUID,
    expected: tuple[str, ...],
    assignments: str,
    values: tuple[object, ...],
) -> None:
    """기대 상태일 때만 한 Row를 갱신하고, 아니면 PublishStateError를 낸다."""
    with settings.pipeline_connection() as connection:
        cursor = connection.execute(
            f"UPDATE mart_publish_runs SET {assignments}"
            " WHERE publish_run_id = %s AND status = ANY(%s)",
            (*values, publish_run_id, list(expected)),
        )
        if cursor.rowcount != 1:
            raise PublishStateError(
                f"Publish run {publish_run_id} is not in {', '.join(expected)}"
            )


def _record(row: tuple) -> PublishRecord:
    """SELECT 결과 Tuple을 PublishRecord로 바꾼다."""
    return PublishRecord(
        publish_run_id=row[0],
        status=row[1],
        started_at=row[2],
        previous_publish_run_id=row[3],
        mart_hashes=row[4],
        error_type=row[5],
    )
