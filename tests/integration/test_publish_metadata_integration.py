"""mart_publish_runs 상태 전이와 단일 활성 Run 제약을 실제 PostgreSQL에서 검증한다."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from src.common.database import PostgresSettings
from src.warehouse import publish_metadata as metadata
from src.warehouse.duckdb_files import FileIdentity
from src.warehouse.errors import PublishInProgressError, PublishStateError
from src.warehouse.publish_metadata import (
    BUILDING,
    FAILED,
    PUBLISHED,
    PublishRun,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
    mark_failed,
    mark_published,
    mark_publishing,
    record_dbt_result,
    stale_active_runs,
    start_publish_run,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
        reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
    ),
]

NOW = datetime(2026, 9, 18, tzinfo=UTC)


@pytest.fixture
def settings() -> Iterator[PostgresSettings]:
    """활성 Publish가 없는 상태에서 테스트하고, 만든 Row를 정리한다."""
    settings = PostgresSettings.from_environment()
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")
    yield settings
    with settings.pipeline_connection() as connection:
        connection.execute("DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'")


def _run() -> PublishRun:
    """테스트 전용 Pipeline 이름을 가진 Publish Run을 만든다."""
    return PublishRun(uuid.uuid4(), f"test_publish_{uuid.uuid4().hex[:8]}", "batch-1", "dag-run")


def _after_latest_publish(settings: PostgresSettings) -> datetime:
    """실제 Publish Run이 있어도 Test Run이 항상 최신이 되도록 기준 시각을 잡는다."""
    with settings.pipeline_connection() as connection:
        latest = connection.execute(
            "SELECT max(finished_at) FROM mart_publish_runs WHERE status = 'PUBLISHED'"
        ).fetchone()[0]
    return max(latest or NOW, NOW) + timedelta(minutes=1)


def test_full_success_transition_links_previous_published_run(settings: PostgresSettings) -> None:
    """전역 최신 PUBLISHED 계약에서 다음 Run이 직전 PUBLISHED Run을 가리킨다."""
    base = _after_latest_publish(settings)
    first = start_publish_run(settings, _run(), now=base)
    assert first.status == BUILDING
    record_dbt_result(
        settings, first.publish_run_id, invocation_id="inv-1", tests_passed=10, tests_failed=0
    )
    mark_publishing(
        settings,
        first.publish_run_id,
        mart_hashes={"dimensions.dim_date": "abc"},
        mart_row_counts={"dimensions.dim_date": 3},
        published_identity=FileIdentity(123, 456),
    )
    mark_published(settings, first.publish_run_id, now=base + timedelta(minutes=1))
    stored = get_publish_run(settings, first.publish_run_id)
    assert stored is not None
    assert stored.published_identity == FileIdentity(123, 456)
    assert stored.status == PUBLISHED
    assert stored.mart_hashes == {"dimensions.dim_date": "abc"}

    second = start_publish_run(settings, _run(), now=base + timedelta(minutes=2))
    assert second.previous_publish_run_id == first.publish_run_id
    mark_failed(
        settings,
        second.publish_run_id,
        error_type="DBT_TEST_ERROR",
        error_message="x" * 5000,
        failed_path="failed/x.duckdb",
        now=NOW,
    )
    failed = get_publish_run(settings, second.publish_run_id)
    assert failed is not None
    assert failed.status == FAILED
    assert failed.error_type == "DBT_TEST_ERROR"


def test_second_active_run_is_rejected(settings: PostgresSettings) -> None:
    """BUILDING Run이 있으면 두 번째 Run 시작과 Rebaseline Guard가 모두 거부된다."""
    active = start_publish_run(settings, _run(), now=NOW)
    with pytest.raises(PublishInProgressError):
        start_publish_run(settings, _run(), now=NOW)
    with pytest.raises(PublishInProgressError):
        assert_no_active_publish(settings)
    mark_failed(
        settings,
        active.publish_run_id,
        error_type="UNKNOWN_ERROR",
        error_message="cleanup",
        failed_path=None,
        now=NOW,
    )
    assert_no_active_publish(settings)


def test_invalid_transition_raises_state_error(settings: PostgresSettings) -> None:
    """BUILDING에서 곧바로 PUBLISHED로 가는 전이는 거부된다."""
    active = start_publish_run(settings, _run(), now=NOW)
    with pytest.raises(PublishStateError):
        mark_published(settings, active.publish_run_id, now=NOW)
    mark_failed(
        settings,
        active.publish_run_id,
        error_type="UNKNOWN_ERROR",
        error_message="cleanup",
        failed_path=None,
        now=NOW,
    )


def test_publishing_requires_hash_object(settings: PostgresSettings) -> None:
    """PUBLISHING 상태는 mart_hashes JSON Object 없이 저장될 수 없다."""
    active = start_publish_run(settings, _run(), now=NOW)
    with settings.pipeline_connection() as connection, pytest.raises(psycopg.IntegrityError):
        connection.execute(
            "UPDATE mart_publish_runs SET status = 'PUBLISHING' WHERE publish_run_id = %s",
            (active.publish_run_id,),
        )
    mark_failed(
        settings,
        active.publish_run_id,
        error_type="UNKNOWN_ERROR",
        error_message="cleanup",
        failed_path=None,
        now=NOW,
    )


def test_stale_active_runs_returns_only_old_active_rows(settings: PostgresSettings) -> None:
    """기준 시각보다 먼저 시작한 활성 Run만 Stale로 반환한다."""
    old = start_publish_run(settings, _run(), now=NOW - timedelta(hours=2))
    stale = stale_active_runs(settings, stale_before=NOW - timedelta(hours=1))
    assert [record.publish_run_id for record in stale] == [old.publish_run_id]
    assert stale_active_runs(settings, stale_before=NOW - timedelta(hours=3)) == ()
    mark_failed(
        settings,
        old.publish_run_id,
        error_type="UNKNOWN_ERROR",
        error_message="cleanup",
        failed_path=None,
        now=NOW,
    )


def test_publish_metadata_migration_is_idempotent(settings: PostgresSettings) -> None:
    """기존 테이블을 보존하며 열 추가를 반복해도 중복이 생기지 않는다."""
    ensure_publish_metadata(settings)
    ensure_publish_metadata(settings)
    with settings.pipeline_connection() as connection:
        columns = connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'mart_publish_runs' AND column_name IN ('published_inode', 'published_mtime_ns', 'heartbeat_at') "
            "ORDER BY column_name"
        ).fetchall()
    assert [row[0] for row in columns] == ["heartbeat_at", "published_inode", "published_mtime_ns"]


def _heartbeat(settings, run_id):
    """저장된 생존 신호 시각을 직접 조회한다."""
    with settings.pipeline_connection() as connection:
        return connection.execute(
            "SELECT heartbeat_at FROM mart_publish_runs WHERE publish_run_id = %s", (run_id,),
        ).fetchone()[0]


def test_start_sets_heartbeat_to_start_time(settings):
    """새 실행의 시작 시각과 생존 신호는 같다."""
    record = start_publish_run(settings, _run(), now=NOW)
    assert _heartbeat(settings, record.publish_run_id) == record.started_at


def test_touch_updates_only_active_runs(settings):
    """활성 실행만 생존 신호를 갱신한다."""
    record = start_publish_run(settings, _run(), now=NOW)
    later = NOW + timedelta(seconds=30)
    assert metadata.touch_publish_run(settings, record.publish_run_id, now=later)
    assert _heartbeat(settings, record.publish_run_id) == later
    mark_publishing(settings, record.publish_run_id, mart_hashes={}, mart_row_counts={}, published_identity=FileIdentity(1, 2))
    assert metadata.touch_publish_run(settings, record.publish_run_id, now=later)
    mark_failed(settings, record.publish_run_id, error_type="UNKNOWN_ERROR", error_message="cleanup", failed_path=None, now=later)
    assert not metadata.touch_publish_run(settings, record.publish_run_id, now=later + timedelta(seconds=30))
    assert _heartbeat(settings, record.publish_run_id) == later


def test_stale_uses_heartbeat_not_start(settings):
    """오래 실행 중이어도 생존 신호가 최신이면 복구하지 않는다."""
    record = start_publish_run(settings, _run(), now=NOW - timedelta(hours=2))
    metadata.touch_publish_run(settings, record.publish_run_id, now=NOW)
    assert stale_active_runs(settings, stale_before=NOW - timedelta(minutes=5)) == ()


def test_stale_falls_back_to_started_at_when_heartbeat_null(settings):
    """이전 버전 행은 시작 시각으로 만료를 판단한다."""
    record = start_publish_run(settings, _run(), now=NOW - timedelta(hours=2))
    with settings.pipeline_connection() as connection:
        connection.execute("UPDATE mart_publish_runs SET heartbeat_at = NULL WHERE publish_run_id = %s", (record.publish_run_id,))
    assert [row.publish_run_id for row in stale_active_runs(settings, stale_before=NOW - timedelta(minutes=5))] == [record.publish_run_id]


def test_close_stale_run_skips_refreshed_heartbeat(settings):
    """후보 조회 후 갱신된 생존 신호가 조건부 종료를 막는다."""
    record = start_publish_run(settings, _run(), now=NOW - timedelta(hours=2))
    metadata.touch_publish_run(settings, record.publish_run_id, now=NOW)
    assert not metadata.close_stale_run(settings, record.publish_run_id, published=False, stale_before=NOW - timedelta(minutes=5), now=NOW)
    assert get_publish_run(settings, record.publish_run_id).status == BUILDING


def test_close_stale_run_failed_sets_abandoned(settings):
    """만료된 행을 먼저 닫고 격리 경로는 실패 행에만 기록한다."""
    record = start_publish_run(settings, _run(), now=NOW - timedelta(hours=2))
    assert metadata.close_stale_run(settings, record.publish_run_id, published=False, stale_before=NOW - timedelta(minutes=5), now=NOW)
    metadata.record_failed_path(settings, record.publish_run_id, "failed/test.duckdb")
    with settings.pipeline_connection() as connection:
        row = connection.execute("SELECT status, error_type, error_message, finished_at, failed_path FROM mart_publish_runs WHERE publish_run_id = %s", (record.publish_run_id,)).fetchone()
    assert row == (FAILED, "UNKNOWN_ERROR", "abandoned", NOW, "failed/test.duckdb")
    assert not metadata.close_stale_run(settings, record.publish_run_id, published=False, stale_before=NOW, now=NOW)


@pytest.mark.parametrize("pipeline,dag_run_id,publishing,matches", [
    ("test_publish_reclaim", "dr1", False, True),
    ("test_publish_reclaim", "dr2", False, False),
    ("test_publish_other", "dr1", False, False),
    ("test_publish_reclaim", None, False, False),
    ("test_publish_reclaim", "dr1", True, False),
])
def test_close_dag_run_building_returns_only_closed_runs(settings, pipeline, dag_run_id, publishing, matches):
    """같은 실행의 BUILDING만 생존 신호와 무관하게 닫고 종료된 식별자만 반환한다."""
    run = PublishRun(uuid.uuid4(), pipeline, dag_run_id=dag_run_id)
    start_publish_run(settings, run, now=NOW)
    metadata.touch_publish_run(settings, run.publish_run_id, now=NOW)
    if publishing:
        mark_publishing(settings, run.publish_run_id, mart_hashes={}, mart_row_counts={}, published_identity=FileIdentity(1, 2))
    closed = metadata.close_dag_run_building(settings, pipeline_name="test_publish_reclaim", dag_run_id="dr1", now=NOW)
    assert closed == ((run.publish_run_id,) if matches else ())
    with settings.pipeline_connection() as connection:
        row = connection.execute("SELECT status,error_type,error_message,finished_at FROM mart_publish_runs WHERE publish_run_id=%s", (run.publish_run_id,)).fetchone()
    if matches:
        assert row == (FAILED, "UNKNOWN_ERROR", metadata.RECLAIMED_MESSAGE, NOW)
    else:
        assert row == ("PUBLISHING" if publishing else BUILDING, None, None, None)
    assert metadata.close_dag_run_building(settings, pipeline_name="test_publish_reclaim", dag_run_id="dr1", now=NOW) == ()
