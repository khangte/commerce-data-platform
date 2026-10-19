"""Build-then-swap Publish의 성공·실패·복구 시나리오를 실제 PostgreSQL로 검증한다."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.ingestion.control.metadata import ensure_ingestion_metadata
from src.warehouse import publish as publisher
from src.warehouse import publish_metadata as metadata
from src.warehouse.dbt_runner import DbtRunResult
from src.warehouse.duckdb_files import FileIdentity, file_identity
from src.warehouse.errors import (
    DBT_TEST_ERROR,
    PublishedWalError,
    PublishInProgressError,
    PublishStateError,
    WarehouseBuildError,
)
from src.warehouse.mart_hash import MartTarget, mart_logical_hashes, mart_row_counts
from src.warehouse.publish import (
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
    assert first.published_identity == file_identity(paths.published)
    assert get_publish_run(settings, first.publish_run_id).published_identity == first.published_identity
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
    """Lease가 지나지 않은 활성 Run이 있으면 새 Build를 시작하지 않는다."""
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


def test_recovery_confirms_a_publishing_run_whose_file_identity_matches(
    settings: PostgresSettings, tmp_path: Path
) -> None:
    """교체 후 멈춘 PUBLISHING은 해시와 무관하게 파일 식별값으로 확정한다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    now = datetime.now(UTC)
    stuck = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    mark_publishing(
        settings,
        stuck.publish_run_id,
        mart_hashes={"dimensions.dim_date": "0" * 64},
        mart_row_counts=mart_row_counts(paths.published, TARGETS),
        published_identity=file_identity(paths.published),
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
    assert message == metadata.ABANDONED_MESSAGE


@pytest.mark.parametrize("without_identity", [False, True])
def test_recovery_fails_a_publishing_run_with_unusable_file_identity(
    settings: PostgresSettings, tmp_path: Path, without_identity: bool
) -> None:
    """올바른 해시가 있어도 수정 시각 불일치나 식별값 누락이면 복구를 거부한다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    now = datetime.now(UTC)
    stuck = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    actual = file_identity(paths.published)
    mark_publishing(
        settings, stuck.publish_run_id,
        mart_hashes=_published_hash(paths),
        mart_row_counts=mart_row_counts(paths.published, TARGETS),
        published_identity=actual if without_identity else FileIdentity(actual.inode, 1),
    )
    if without_identity:
        with settings.pipeline_connection() as connection:
            connection.execute(
                "UPDATE mart_publish_runs SET published_inode = NULL, published_mtime_ns = NULL "
                "WHERE publish_run_id = %s", (stuck.publish_run_id,),
            )
    assert recover_incomplete_publishes(settings, paths, now=now) == (stuck.publish_run_id,)
    record = get_publish_run(settings, stuck.publish_run_id)
    assert record is not None
    assert (record.status, record.error_type) == (FAILED, "UNKNOWN_ERROR")


def test_live_old_run_is_not_recovered(settings, tmp_path):
    """오래된 실행도 최신 생존 신호가 있으면 파일과 활성 상태를 보존한다."""
    paths = WarehousePaths.under(tmp_path)
    now = datetime.now(UTC)
    active = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    paths.build_dir.mkdir(parents=True)
    paths.build_file(active.publish_run_id).write_bytes(b"live")
    metadata.touch_publish_run(settings, active.publish_run_id, now=now)
    assert recover_incomplete_publishes(settings, paths, now=now) == ()
    assert get_publish_run(settings, active.publish_run_id).status == "BUILDING"
    assert paths.build_file(active.publish_run_id).read_bytes() == b"live"


@pytest.mark.parametrize("publishing", [False, True])
def test_recovery_skips_run_refreshed_after_lookup(settings, tmp_path, monkeypatch, publishing):
    """후보 조회 직후 소유자가 갱신하면 두 복구 경로 모두 파일을 보존한다."""
    paths = WarehousePaths.under(tmp_path)
    now = datetime.now(UTC)
    stuck = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    paths.build_dir.mkdir(parents=True)
    paths.build_file(stuck.publish_run_id).write_bytes(b"live")
    target = paths.target_dir(stuck.publish_run_id)
    target.mkdir()
    (target / "keep").write_text("keep")
    if publishing:
        paths.published.write_bytes(b"published")
        mark_publishing(settings, stuck.publish_run_id, mart_hashes={}, mart_row_counts={}, published_identity=file_identity(paths.published))
    lookup = publisher.stale_active_runs
    def refresh_after_lookup(*args, **kwargs):
        """복구 판단과 UPDATE 사이의 생존 신호 갱신을 재현한다."""
        records = lookup(*args, **kwargs)
        assert records
        metadata.touch_publish_run(settings, stuck.publish_run_id, now=now)
        return records
    monkeypatch.setattr(publisher, "stale_active_runs", refresh_after_lookup)
    assert recover_incomplete_publishes(settings, paths, now=now) == ()
    assert paths.build_file(stuck.publish_run_id).read_bytes() == b"live"
    assert (target / "keep").is_file()
    assert not paths.failed_file(stuck.publish_run_id).exists()
    assert get_publish_run(settings, stuck.publish_run_id).status == ("PUBLISHING" if publishing else "BUILDING")


def test_zombie_cannot_publish_after_takeover(settings, tmp_path):
    """인수된 소유자의 기록·교체 전이는 거부되고 게시 파일은 그대로다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    before = paths.published.read_bytes(), file_identity(paths.published)
    now = datetime.now(UTC)
    stuck = start_publish_run(settings, _run(), now=now - timedelta(hours=2))
    paths.build_file(stuck.publish_run_id).write_bytes(b"zombie")
    assert recover_incomplete_publishes(settings, paths, now=now) == (stuck.publish_run_id,)
    with pytest.raises(PublishStateError):
        metadata.record_dbt_result(settings, stuck.publish_run_id, invocation_id="zombie", tests_passed=1, tests_failed=0)
    with pytest.raises(PublishStateError):
        mark_publishing(settings, stuck.publish_run_id, mart_hashes={}, mart_row_counts={}, published_identity=FileIdentity(1, 2))
    assert (paths.published.read_bytes(), file_identity(paths.published)) == before


@contextmanager
def _running_heartbeat_owner(settings, tmp_path, owner):
    """준비를 마친 자식의 생존 신호를 관측하고 종료를 보장한다."""
    child_code = """
import sys, time, uuid
from datetime import timedelta
from pathlib import Path
from src.common.database import PostgresSettings
from src.warehouse.publish import WarehousePaths, prepare_warehouse_build
from src.warehouse.publish_metadata import PublishRun
from src.warehouse.publish_heartbeat import publish_heartbeat
settings = PostgresSettings.from_environment()
run = PublishRun(uuid.UUID(sys.argv[2]), sys.argv[3], dag_run_id=sys.argv[4] or None)
prepare_warehouse_build(settings, WarehousePaths.under(Path(sys.argv[1])), run)
with publish_heartbeat(settings, run.publish_run_id, interval=timedelta(seconds=0.2)):
    time.sleep(60)
"""
    child = subprocess.Popen([sys.executable, "-c", child_code, str(tmp_path), str(owner.publish_run_id), owner.pipeline_name, owner.dag_run_id or ""], cwd=Path(__file__).resolve().parents[2], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        seen = set()
        deadline = time.monotonic() + 15
        while len(seen) < 3 and time.monotonic() < deadline:
            assert child.poll() is None, "자식 실행이 생존 신호 관측 전에 종료됨"
            with settings.pipeline_connection() as connection:
                row = connection.execute("SELECT heartbeat_at FROM mart_publish_runs WHERE publish_run_id = %s", (owner.publish_run_id,)).fetchone()
            if row:
                seen.add(row[0])
            time.sleep(0.05)
        assert len(seen) >= 3
        yield child
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)


def test_sigkilled_owner_is_taken_over_after_lease(settings, tmp_path, monkeypatch):
    """실제 자식의 주기 갱신·SIGKILL·만료 전 차단·만료 뒤 인수를 검증한다."""
    monkeypatch.setattr(publisher, "PUBLISH_LEASE", timedelta(seconds=2))
    paths = WarehousePaths.under(tmp_path)
    owner = _run()
    with _running_heartbeat_owner(settings, tmp_path, owner) as child:
        os.kill(child.pid, signal.SIGKILL)
        assert child.wait(timeout=5) == -signal.SIGKILL
        new = _run()
        with pytest.raises(PublishInProgressError):
            prepare_warehouse_build(settings, paths, new)
        time.sleep(3)
        prepare_warehouse_build(settings, paths, new)
        assert get_publish_run(settings, new.publish_run_id).status == "BUILDING"
        assert paths.failed_file(owner.publish_run_id).is_file()
        with settings.pipeline_connection() as connection:
            row = connection.execute("SELECT status, error_type, error_message FROM mart_publish_runs WHERE publish_run_id = %s", (owner.publish_run_id,)).fetchone()
        assert row == (FAILED, "UNKNOWN_ERROR", "abandoned")


@pytest.mark.parametrize("sigkill", [False, True])
def test_retry_reclaims_own_dag_run_building_row(settings, tmp_path, sigkill):
    """같은 DagRun의 재시도는 생존 신호가 신선한 이전 시도를 즉시 회수한다."""
    paths = WarehousePaths.under(tmp_path)
    owner = PublishRun(uuid.uuid4(), "test_publish_reclaim", dag_run_id="dr1")
    if sigkill:
        with _running_heartbeat_owner(settings, tmp_path, owner) as child:
            os.kill(child.pid, signal.SIGKILL)
            assert child.wait(timeout=5) == -signal.SIGKILL
    else:
        start_publish_run(settings, owner, now=datetime.now(UTC))
        metadata.touch_publish_run(settings, owner.publish_run_id, now=datetime.now(UTC))
        paths.build_dir.mkdir(parents=True)
        paths.build_file(owner.publish_run_id).write_bytes(b"partial")
    target = paths.target_dir(owner.publish_run_id)
    target.mkdir()
    (target / "leftover").write_text("leftover")
    new = PublishRun(uuid.uuid4(), owner.pipeline_name, dag_run_id=owner.dag_run_id)
    prepare_warehouse_build(settings, paths, new)
    assert get_publish_run(settings, new.publish_run_id).status == "BUILDING"
    assert paths.failed_file(owner.publish_run_id).is_file()
    assert not target.exists()
    with settings.pipeline_connection() as connection:
        row = connection.execute("SELECT status,error_type,error_message,failed_path FROM mart_publish_runs WHERE publish_run_id=%s", (owner.publish_run_id,)).fetchone()
    assert row == (FAILED, "UNKNOWN_ERROR", metadata.RECLAIMED_MESSAGE, str(paths.failed_file(owner.publish_run_id)))


@pytest.mark.parametrize("old_pipeline,old_dag,new_pipeline,new_dag,publishing", [
    ("test_publish_reclaim", "dr1", "test_publish_reclaim", "dr2", False),
    ("test_publish_reclaim", None, "test_publish_reclaim", None, False),
    ("test_publish_other", "dr1", "test_publish_reclaim", "dr1", False),
    ("test_publish_reclaim", "dr1", "test_publish_reclaim", "dr1", True),
])
def test_retry_does_not_reclaim_unrelated_or_publishing_rows(settings, tmp_path, old_pipeline, old_dag, new_pipeline, new_dag, publishing):
    """다른 실행·Pipeline·CLI·교체 중 행에는 기존 Lease 차단을 유지한다."""
    paths = WarehousePaths.under(tmp_path)
    owner = PublishRun(uuid.uuid4(), old_pipeline, dag_run_id=old_dag)
    start_publish_run(settings, owner, now=datetime.now(UTC))
    paths.build_dir.mkdir(parents=True)
    paths.build_file(owner.publish_run_id).write_bytes(b"keep")
    if publishing:
        mark_publishing(settings, owner.publish_run_id, mart_hashes={}, mart_row_counts={}, published_identity=FileIdentity(1, 2))
    with pytest.raises(PublishInProgressError):
        prepare_warehouse_build(settings, paths, PublishRun(uuid.uuid4(), new_pipeline, dag_run_id=new_dag))
    assert get_publish_run(settings, owner.publish_run_id).status == ("PUBLISHING" if publishing else "BUILDING")
    assert paths.build_file(owner.publish_run_id).read_bytes() == b"keep"
    assert not paths.failed_file(owner.publish_run_id).exists()


def test_reclaimed_zombie_cannot_publish(settings, tmp_path):
    """같은 실행에 회수된 이전 소유자도 게시 파일을 변경하지 못한다."""
    paths = WarehousePaths.under(tmp_path)
    publish_warehouse(settings, paths, _run(), dbt_runner=_dbt_writing(3), targets=TARGETS)
    before = paths.published.read_bytes(), file_identity(paths.published)
    owner = PublishRun(uuid.uuid4(), "test_publish_reclaim", dag_run_id="dr1")
    start_publish_run(settings, owner, now=datetime.now(UTC))
    paths.build_file(owner.publish_run_id).write_bytes(b"zombie")
    prepare_warehouse_build(settings, paths, PublishRun(uuid.uuid4(), owner.pipeline_name, dag_run_id=owner.dag_run_id))
    with pytest.raises(PublishStateError):
        metadata.record_dbt_result(settings, owner.publish_run_id, invocation_id="zombie", tests_passed=1, tests_failed=0)
    with pytest.raises(PublishStateError):
        mark_publishing(settings, owner.publish_run_id, mart_hashes={}, mart_row_counts={}, published_identity=FileIdentity(1, 2))
    assert (paths.published.read_bytes(), file_identity(paths.published)) == before
