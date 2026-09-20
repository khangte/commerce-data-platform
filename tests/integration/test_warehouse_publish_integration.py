"""Build-then-swap Publish의 성공·실패·복구 시나리오를 실제 PostgreSQL로 검증한다."""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.ingestion.metadata import ensure_ingestion_metadata
from src.warehouse.dbt_runner import DbtRunResult
from src.warehouse.errors import (
    DBT_TEST_ERROR,
    PublishedWalError,
    PublishInProgressError,
    WarehouseBuildError,
)
from src.warehouse.mart_hash import MartTarget, mart_logical_hashes, mart_row_counts
from src.warehouse.publish import (
    ABANDONED_MESSAGE,
    WarehousePaths,
    prepare_warehouse_build,
    publish_warehouse,
    recover_incomplete_publishes,
)
from src.warehouse.publish_metadata import (
    FAILED,
    PUBLISHED,
    PublishRun,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
    mark_failed,
    mark_publishing,
    start_publish_run,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
        reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
    ),
]

DIM_DATE_TARGET = MartTarget("dimensions", "dim_date", ("date_key",))
TARGETS = (DIM_DATE_TARGET,)


@pytest.fixture
def settings() -> Iterator[PostgresSettings]:
    """활성 Publish가 없을 때만 실행하고, 테스트 Run Row를 정리한다."""
    settings = PostgresSettings.from_environment()
    ensure_ingestion_metadata(settings)
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
    return PublishRun(uuid.uuid4(), f"test_publish_{uuid.uuid4().hex[:8]}")


def _dbt_writing(
    rows: int, returncode: int = 0, error_type: str | None = None
) -> Callable[[Path, Path], DbtRunResult]:
    """Build 파일에 dim_date를 rows개로 쓰는 가짜 dbt Runner를 만든다."""

    def runner(build_path: Path, target_path: Path) -> DbtRunResult:
        """Build 파일에 Mart Table 하나를 쓰고 지정한 결과를 반환한다."""
        with duckdb.connect(str(build_path)) as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS dimensions")
            connection.execute(
                "CREATE OR REPLACE TABLE dimensions.dim_date AS "
                f"SELECT range AS date_key FROM range({rows})"
            )
        failed = () if error_type is None else ("test.fake.gate",)
        return DbtRunResult(
            returncode, "fake-invocation", 1, len(failed), error_type, failed, "fake output"
        )

    return runner


def _published_hash(paths: WarehousePaths) -> dict[str, str]:
    """Published 파일의 dim_date Hash를 계산한다."""
    return mart_logical_hashes(paths.published, TARGETS)


def test_failed_build_never_reaches_the_published_file(
    settings: PostgresSettings, tmp_path: Path
) -> None:
    """성공 → 실패 → 성공 순서에서 실패 Build는 Published 파일을 바꾸지 않는다."""
    paths = WarehousePaths.under(tmp_path)

    first = publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    first_hash = _published_hash(paths)
    assert mart_row_counts(paths.published, TARGETS) == {"dimensions.dim_date": 3}

    failing = _run()
    with pytest.raises(WarehouseBuildError) as raised:
        publish_warehouse(
            settings,
            paths,
            failing,
            dbt_runner=_dbt_writing(5, 1, DBT_TEST_ERROR),
            targets=TARGETS,
        )
    assert raised.value.error_type == DBT_TEST_ERROR
    assert _published_hash(paths) == first_hash
    assert paths.failed_file(failing.publish_run_id).is_file()
    assert not paths.build_file(failing.publish_run_id).exists()
    failed_record = get_publish_run(settings, failing.publish_run_id)
    assert failed_record is not None
    assert (failed_record.status, failed_record.error_type) == (FAILED, DBT_TEST_ERROR)

    third = publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(7), targets=TARGETS)
    assert third.previous_publish_run_id == first.publish_run_id
    assert third.changed_relations == ("dimensions.dim_date",)
    assert mart_row_counts(paths.published, TARGETS) == {"dimensions.dim_date": 7}
    assert get_publish_run(settings, third.publish_run_id).status == PUBLISHED


def test_published_wal_stops_the_build_as_configuration_error(
    settings: PostgresSettings, tmp_path: Path
) -> None:
    """Published 파일에 WAL이 있으면 복사하지 않고 CONFIGURATION_ERROR로 끝낸다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    paths.published_wal.write_bytes(b"pending")
    run = _run()

    with pytest.raises(PublishedWalError):
        prepare_warehouse_build(settings, paths, run)

    record = get_publish_run(settings, run.publish_run_id)
    assert record is not None
    assert (record.status, record.error_type) == (FAILED, "CONFIGURATION_ERROR")


def test_fresh_active_run_blocks_a_new_build(settings: PostgresSettings, tmp_path: Path) -> None:
    """1시간이 지나지 않은 활성 Run이 있으면 새 Build를 시작하지 않는다."""
    paths = WarehousePaths.under(tmp_path)
    active = start_publish_run(settings, _run(), now=datetime.now(UTC))
    blocked = _run()
    try:
        with pytest.raises(PublishInProgressError):
            prepare_warehouse_build(settings, paths, blocked)
        assert get_publish_run(settings, blocked.publish_run_id) is None
    finally:
        mark_failed(
            settings,
            active.publish_run_id,
            error_type="UNKNOWN_ERROR",
            error_message="cleanup",
            failed_path=None,
            now=datetime.now(UTC),
        )


def test_recovery_confirms_a_publishing_run_whose_hash_matches(
    settings: PostgresSettings, tmp_path: Path
) -> None:
    """Swap 후 기록 전에 멈춘 PUBLISHING Run은 Hash가 같으면 PUBLISHED로 확정한다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    now = datetime.now(UTC)
    stuck = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    mark_publishing(
        settings,
        stuck.publish_run_id,
        mart_hashes=_published_hash(paths),
        mart_row_counts=mart_row_counts(paths.published, TARGETS),
    )
    paths.build_dir.mkdir(exist_ok=True)
    paths.build_file(stuck.publish_run_id).write_bytes(b"leftover")

    recovered = recover_incomplete_publishes(settings, paths, now=now)

    assert recovered == (stuck.publish_run_id,)
    record = get_publish_run(settings, stuck.publish_run_id)
    assert record is not None
    assert record.status == PUBLISHED
    assert not paths.build_file(stuck.publish_run_id).exists()


def test_recovery_fails_an_abandoned_building_run(settings: PostgresSettings, tmp_path: Path) -> None:
    """오래된 BUILDING Run은 UNKNOWN_ERROR abandoned로 닫고 Build를 격리한다."""
    paths = WarehousePaths.under(tmp_path)
    now = datetime.now(UTC)
    abandoned = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    paths.build_dir.mkdir(parents=True)
    paths.build_file(abandoned.publish_run_id).write_bytes(b"partial")

    recovered = recover_incomplete_publishes(settings, paths, now=now)

    assert recovered == (abandoned.publish_run_id,)
    record = get_publish_run(settings, abandoned.publish_run_id)
    assert record is not None
    assert (record.status, record.error_type) == (FAILED, "UNKNOWN_ERROR")
    assert paths.failed_file(abandoned.publish_run_id).read_bytes() == b"partial"
    with settings.pipeline_connection() as connection:
        message = connection.execute(
            "SELECT error_message FROM mart_publish_runs WHERE publish_run_id = %s",
            (abandoned.publish_run_id,),
        ).fetchone()[0]
    assert message == ABANDONED_MESSAGE
