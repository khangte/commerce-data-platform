"""mart_publish_runs 상태 전이와 단일 활성 Run 제약을 실제 PostgreSQL에서 검증한다."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from src.common.database import PostgresSettings
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
    )
    mark_published(settings, first.publish_run_id, now=base + timedelta(minutes=1))
    stored = get_publish_run(settings, first.publish_run_id)
    assert stored is not None
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
    stale = stale_active_runs(settings, older_than=NOW - timedelta(hours=1))
    assert [record.publish_run_id for record in stale] == [old.publish_run_id]
    assert stale_active_runs(settings, older_than=NOW - timedelta(hours=3)) == ()
    mark_failed(
        settings,
        old.publish_run_id,
        error_type="UNKNOWN_ERROR",
        error_message="cleanup",
        failed_path=None,
        now=NOW,
    )
