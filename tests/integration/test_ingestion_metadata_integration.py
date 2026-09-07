"""실제 PostgreSQL 증분 수집 Metadata의 상태 전이와 원자적 Commit을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.common.database import PostgresSettings
from src.generator.ids import logical_hash
from src.ingestion.metadata import (
    CursorPosition,
    PipelineRun,
    TableCommit,
    VerifiedBronzeObject,
    WatermarkConflictError,
    commit_table_run,
    ensure_ingestion_metadata,
    get_or_create_watermark,
    record_failed_run,
    record_started_run,
)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_table_commit_moves_object_run_and_watermark_together() -> None:
    """성공 Commit은 Object·Run 상태와 Watermark를 하나의 Transaction으로 전진시킨다."""
    settings = PostgresSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_ingestion_{uuid.uuid4().hex}"
    run = _run(pipeline_name, now)
    object = _object(run, _after_cursor(now))

    ensure_ingestion_metadata(settings)
    try:
        initial = get_or_create_watermark(
            settings, run.pipeline_name, run.source_table, now=now
        )
        assert initial.cursor == CursorPosition(None)
        record_started_run(settings, run, now=now)
        commit_table_run(
            settings,
            TableCommit(
                run=run,
                object=object,
                expected_watermark=initial,
                rows_extracted=3,
                rows_valid=3,
                rows_rejected=0,
                rows_loaded=3,
            ),
            now=now + timedelta(minutes=1),
        )

        with settings.pipeline_connection() as connection:
            object_row = connection.execute(
                """
                SELECT status, row_count, watermark_before, watermark_after
                FROM bronze_objects
                WHERE table_batch_id = %s
                """,
                (object.table_batch_id,),
            ).fetchone()
            run_row = connection.execute(
                """
                SELECT status, rows_extracted, rows_valid, rows_rejected, rows_loaded
                FROM pipeline_runs
                WHERE run_id = %s AND source_table = %s
                """,
                (run.run_id, run.source_table),
            ).fetchone()
            watermark_row = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys, version
                FROM watermarks
                WHERE pipeline_name = %s AND source_table = %s
                """,
                (run.pipeline_name, run.source_table),
            ).fetchone()

        assert object_row == (
            "COMMITTED",
            3,
            run.watermark_before.as_metadata_json(),
            object.watermark_after.as_metadata_json(),
        )
        assert run_row == ("SUCCESS", 3, 3, 0, 3)
        assert watermark_row == (
            object.watermark_after.timestamp,
            object.watermark_after.as_json(),
            initial.version + 1,
        )
    finally:
        _delete_test_metadata(settings, pipeline_name, run, object)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_watermark_conflict_rolls_back_object_and_success_state() -> None:
    """CAS 충돌이면 Object와 성공 상태는 남기지 않고 Run을 실패로 종료할 수 있다."""
    settings = PostgresSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_ingestion_{uuid.uuid4().hex}"
    run = _run(pipeline_name, now)
    object = _object(run, _after_cursor(now))

    ensure_ingestion_metadata(settings)
    try:
        initial = get_or_create_watermark(
            settings, run.pipeline_name, run.source_table, now=now
        )
        record_started_run(settings, run, now=now)
        with settings.pipeline_connection() as connection:
            connection.execute(
                """
                UPDATE watermarks
                SET version = version + 1
                WHERE pipeline_name = %s AND source_table = %s
                """,
                (run.pipeline_name, run.source_table),
            )
            connection.commit()

        with pytest.raises(WatermarkConflictError):
            commit_table_run(
                settings,
                TableCommit(
                    run=run,
                    object=object,
                    expected_watermark=initial,
                    rows_extracted=3,
                    rows_valid=3,
                    rows_rejected=0,
                    rows_loaded=3,
                ),
                now=now + timedelta(minutes=1),
            )

        with settings.pipeline_connection() as connection:
            assert connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (object.table_batch_id,),
            ).fetchone()[0] == 0
            assert connection.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = %s AND source_table = %s",
                (run.run_id, run.source_table),
            ).fetchone()[0] == "RUNNING"

        record_failed_run(
            settings,
            run,
            error_type="WATERMARK_CONFLICT",
            now=now + timedelta(minutes=2),
        )
        with settings.pipeline_connection() as connection:
            assert connection.execute(
                "SELECT status, error_type FROM pipeline_runs WHERE run_id = %s AND source_table = %s",
                (run.run_id, run.source_table),
            ).fetchone() == ("FAILED", "WATERMARK_CONFLICT")
    finally:
        _delete_test_metadata(settings, pipeline_name, run, object)


def _run(pipeline_name: str, now: datetime) -> PipelineRun:
    """Metadata 상태 전이 테스트에 사용할 고정 범위 Run을 반환한다."""
    return PipelineRun(
        run_id=uuid.uuid4(),
        pipeline_name=pipeline_name,
        source_table="orders",
        batch_id=f"{pipeline_name}__20260907T000000Z",
        logical_date=now,
        attempt_number=1,
        watermark_before=CursorPosition(None),
        extract_upper_bound=_after_cursor(now),
    )


def _after_cursor(now: datetime) -> CursorPosition:
    """초기 Watermark를 전진시키는 `orders` Composite Cursor를 반환한다."""
    return CursorPosition(now + timedelta(seconds=1), ("order-0001",))


def _object(run: PipelineRun, watermark_after: CursorPosition) -> VerifiedBronzeObject:
    """검증을 끝낸 것처럼 취급할 고유한 Bronze Object 증적을 반환한다."""
    table_batch_id = f"{run.batch_id}__{run.source_table}"
    logical_rows = {"run_id": str(run.run_id), "orders": ["order-0001", "order-0002", "order-0003"]}
    return VerifiedBronzeObject(
        table_batch_id=table_batch_id,
        object_key=f"bronze/orders/batch_id={run.batch_id}/data.parquet",
        manifest_key=f"bronze/orders/batch_id={run.batch_id}/manifest.json",
        schema_version=1,
        row_count=3,
        content_sha256="a" * 64,
        logical_hash=logical_hash(logical_rows),
        watermark_after=watermark_after,
    )


def _delete_test_metadata(
    settings: PostgresSettings,
    pipeline_name: str,
    run: PipelineRun,
    object: VerifiedBronzeObject,
) -> None:
    """통합 테스트가 만든 정확한 Metadata Row를 역순으로 정리한다."""
    with settings.pipeline_connection() as connection:
        connection.execute("DELETE FROM bronze_objects WHERE table_batch_id = %s", (object.table_batch_id,))
        connection.execute(
            "DELETE FROM pipeline_runs WHERE run_id = %s AND source_table = %s",
            (run.run_id, run.source_table),
        )
        connection.execute(
            "DELETE FROM watermarks WHERE pipeline_name = %s AND source_table = %s",
            (pipeline_name, run.source_table),
        )
        connection.commit()
