"""build_warehouse의 도메인 부분 게시 분기를 가짜 dbt Runner와 합성 DuckDB로 검증한다."""

from __future__ import annotations

import logging
import os
import uuid
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from src.ingestion.errors import DBT_TEST_ERROR
from src.warehouse import publish
from src.warehouse.dbt_runner import DbtRunResult
from src.warehouse.errors import DBT_BUILD_ERROR, WarehouseBuildError
from src.warehouse.publish import FAILED_BUILD_RETENTION, WarehousePaths, build_warehouse
from src.warehouse.publish_metadata import _RECORD_COLUMNS
from tests.test_warehouse_domain_gate import (
    FCT_ORDER_TEST,
    FCT_SUB_TEST,
    INVOCATION,
    _all_models,
    _create_warehouse,
    _test,
    _test_id,
    _write_target,
)

SETTINGS = object()
FAILED_TEST = _test_id("a")
BOUNDARY_TEST = _test_id("boundary")
ORDER_FAILURE = {FAILED_TEST: FCT_ORDER_TEST}
BOUNDARY_NODE = _test("boundary", "fct_order", "dim_customer")


class Recorder:
    """메타데이터 호출과 Heartbeat 안에서 실행됐는지를 기록한다."""

    def __init__(self) -> None:
        self.failed_paths: list[str] = []
        self.held: list[list[str]] = []
        self.failures: list[dict] = []
        self.in_heartbeat = False
        self.held_in_heartbeat = False


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    """Publish가 쓰는 PostgreSQL 기록 함수를 메모리 기록으로 바꾼다."""
    state = Recorder()

    @contextmanager
    def _heartbeat(*_: object, **__: object):
        state.in_heartbeat = True
        yield
        state.in_heartbeat = False

    def _held(_: object, __: uuid.UUID, domains: list[str]) -> None:
        state.held.append(list(domains))
        state.held_in_heartbeat = state.in_heartbeat

    monkeypatch.setattr(publish, "publish_heartbeat", _heartbeat)
    monkeypatch.setattr(publish, "record_dbt_result", lambda *_, **__: None)
    monkeypatch.setattr(publish, "record_failed_path", lambda _, __, path: state.failed_paths.append(path))
    monkeypatch.setattr(publish, "record_held_domains", _held)
    monkeypatch.setattr(publish, "mark_failed", lambda *_, **kwargs: state.failures.append(kwargs))
    return state


@pytest.fixture
def paths(tmp_path: Path) -> WarehousePaths:
    """직전 게시 값 old와 이번 Build 값 new를 가진 Warehouse 경로를 만든다."""
    paths = WarehousePaths.under(tmp_path)
    paths.build_dir.mkdir(parents=True)
    _create_warehouse(paths.published, "old", "BIGINT")
    return paths


@pytest.fixture
def run_id(paths: WarehousePaths) -> uuid.UUID:
    """Build 파일이 준비된 Publish Run ID를 만든다."""
    run_id = uuid.uuid4()
    _create_warehouse(paths.build_file(run_id), "new", "BIGINT")
    return run_id


def _runner(
    nodes: dict[str, dict] = ORDER_FAILURE,
    results: dict[str, str] | None = None,
    compiled: dict[str, str] | None = None,
    *,
    error_type: str = DBT_TEST_ERROR,
    warned: tuple[str, ...] = (),
):
    """Target Directory에 dbt 산출물을 쓰고 지정한 실패 결과를 돌려주는 Runner를 만든다."""
    statuses = {FAILED_TEST: "fail"} if results is None else results

    def run(_: Path, target: Path) -> DbtRunResult:
        _write_target(target, {**_all_models(), **nodes}, statuses, compiled)
        return DbtRunResult(
            1, INVOCATION, 3, 1, error_type, (FAILED_TEST,), "dbt output", len(warned), warned_nodes=warned
        )

    return run


def _value(path: Path, relation: str) -> str:
    """합성 Table의 값 표식을 읽는다."""
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(f"SELECT DISTINCT value FROM {relation}").fetchone()[0]


def _build(paths: WarehousePaths, run_id: uuid.UUID, runner, **kwargs) -> DbtRunResult:
    return build_warehouse(SETTINGS, paths, run_id, dbt_runner=runner, **kwargs)


def test_shared_record_columns_exclude_held_domains() -> None:
    """마이그레이션 전 DB의 공용 Reader가 새 컬럼 때문에 깨지면 안 된다."""
    assert "held_domains" not in _RECORD_COLUMNS


def test_attributed_failure_reverts_domain_and_keeps_pre_revert_copy(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder
) -> None:
    """귀속·재검증이 통과하면 예외 없이 해당 도메인만 되돌리고 원본 복사본을 남긴다."""
    result = _build(paths, run_id, _runner())

    assert result.error_type == DBT_TEST_ERROR
    build = paths.build_file(run_id)
    assert _value(build, "facts.fct_order") == "old"
    assert _value(build, "facts.fct_subscription_payment") == "new"
    assert _value(paths.failed_file(run_id), "facts.fct_order") == "new"
    assert recorder.failed_paths == [str(paths.failed_file(run_id))]
    assert recorder.held == [["orders"]]
    assert recorder.held_in_heartbeat
    assert recorder.failures == []
    assert paths.target_dir(run_id).exists()


def test_copy_and_wal_are_retained_within_failed_retention(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder
) -> None:
    """복사본에 WAL을 포함하고, 복사본을 최신으로 두어 보존 개수를 지킨다."""
    paths.failed_dir.mkdir()
    for index in range(FAILED_BUILD_RETENTION):
        stale = paths.failed_dir / f"stale-{index}.duckdb"
        stale.write_bytes(b"x")
        os.utime(stale, (1000 + index, 1000 + index))
    wal = publish.wal_path(paths.build_file(run_id))

    def run(path: Path, target: Path) -> DbtRunResult:
        result = _runner()(path, target)
        wal.write_bytes(b"")
        return result

    _build(paths, run_id, run)

    assert publish.wal_path(paths.failed_file(run_id)).exists()
    remaining = {path.name for path in paths.failed_dir.glob("*.duckdb")}
    assert len(remaining) == FAILED_BUILD_RETENTION
    assert paths.failed_file(run_id).name in remaining
    assert "stale-0.duckdb" not in remaining


def test_unattributable_failure_follows_existing_path(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder
) -> None:
    """귀속할 수 없으면 복사·되돌리기 없이 기존 격리 경로로 끝난다."""
    runner = _runner(
        {FAILED_TEST: FCT_ORDER_TEST, _test_id("b"): FCT_SUB_TEST},
        {FAILED_TEST: "fail", _test_id("b"): "fail"},
    )
    with pytest.raises(WarehouseBuildError) as raised:
        _build(paths, run_id, runner)

    assert raised.value.error_type == DBT_TEST_ERROR
    assert recorder.failed_paths == [] and recorder.held == []
    assert recorder.failures[0]["failed_path"] == str(paths.failed_file(run_id))
    assert "fallback" not in recorder.failures[0]["error_message"]
    assert not paths.build_file(run_id).exists()
    assert _value(paths.failed_file(run_id), "facts.fct_order") == "new"


def test_boundary_failure_blocks_and_keeps_pre_revert_file(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder
) -> None:
    """재검증이 실패하면 전체 차단하고 failed/에는 되돌리기 전 파일을 남긴다."""
    runner = _runner(
        {**ORDER_FAILURE, BOUNDARY_TEST: BOUNDARY_NODE},
        {FAILED_TEST: "fail", BOUNDARY_TEST: "skipped"},
        {BOUNDARY_TEST: "SELECT 1 FROM facts.fct_order"},
    )
    with pytest.raises(WarehouseBuildError) as raised:
        _build(paths, run_id, runner)

    assert raised.value.error_type == DBT_TEST_ERROR
    assert recorder.held == []
    assert recorder.failures[0]["error_type"] == DBT_TEST_ERROR
    assert "fallback" in recorder.failures[0]["error_message"]
    assert BOUNDARY_TEST in recorder.failures[0]["error_message"]
    assert not paths.build_file(run_id).exists()
    assert _value(paths.failed_file(run_id), "facts.fct_order") == "new"
    assert not paths.target_dir(run_id).exists()


def test_revert_error_blocks_and_keeps_pre_revert_file(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder
) -> None:
    """Published를 열 수 없어 되돌리기가 실패해도 전체 차단과 원본 보존을 지킨다."""
    paths.published.unlink()
    with pytest.raises(WarehouseBuildError) as raised:
        _build(paths, run_id, _runner())

    assert raised.value.error_type == DBT_TEST_ERROR
    assert "fallback" in recorder.failures[0]["error_message"]
    assert not paths.build_file(run_id).exists()
    assert _value(paths.failed_file(run_id), "facts.fct_order") == "new"


def test_held_record_error_blocks_and_keeps_pre_revert_file(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """되돌린 뒤 보류 기록이 실패하면 되돌린 파일이 원본 복사본을 덮으면 안 된다."""

    def _raise(*_: object) -> None:
        raise RuntimeError("metadata down")

    monkeypatch.setattr(publish, "record_held_domains", _raise)
    with pytest.raises(WarehouseBuildError):
        _build(paths, run_id, _runner())

    assert "metadata down" in recorder.failures[0]["error_message"]
    assert _value(paths.failed_file(run_id), "facts.fct_order") == "new"
    assert not paths.build_file(run_id).exists()


def test_copy_error_blocks_and_isolates_the_untouched_build(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """복사가 실패하면 증거를 잃지 않도록 Build를 지우지 않고 기존 격리로 옮긴다."""

    def _raise(*_: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(publish.shutil, "copyfile", _raise)
    with pytest.raises(WarehouseBuildError):
        _build(paths, run_id, _runner())

    assert "disk full" in recorder.failures[0]["error_message"]
    assert recorder.failures[0]["failed_path"] == str(paths.failed_file(run_id))
    assert _value(paths.failed_file(run_id), "facts.fct_order") == "new"


def test_switch_off_keeps_existing_failure_path(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder
) -> None:
    """partial_publish=False면 귀속 가능한 실패도 기존처럼 전체 차단한다."""
    with pytest.raises(WarehouseBuildError):
        _build(paths, run_id, _runner(), partial_publish=False)

    assert recorder.failed_paths == [] and recorder.held == []
    assert "fallback" not in recorder.failures[0]["error_message"]
    assert _value(paths.failed_file(run_id), "facts.fct_order") == "new"


def test_non_test_error_is_not_partially_published(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder
) -> None:
    """Test 실패가 아닌 dbt 오류는 귀속을 시도하지 않는다."""
    with pytest.raises(WarehouseBuildError) as raised:
        _build(paths, run_id, _runner(error_type=DBT_BUILD_ERROR))

    assert raised.value.error_type == DBT_BUILD_ERROR
    assert recorder.failed_paths == [] and recorder.held == []


def test_partial_publish_logs_nodes_and_domains_on_one_line(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    """부분 게시는 경고·실패 Node와 보류 도메인을 경고 한 줄로 남긴다."""
    with caplog.at_level(logging.WARNING, logger=publish.__name__):
        _build(paths, run_id, _runner(warned=("test.p.w",)))

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert all(text in message for text in ("test.p.w", FAILED_TEST, "orders"))


def test_warned_nodes_are_logged_without_partial_publish(
    paths: WarehousePaths, run_id: uuid.UUID, recorder: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    """성공 Run도 warn Node를 남기고, warn이 없으면 경고를 남기지 않는다."""

    def run(warned: tuple[str, ...]):
        return lambda *_: DbtRunResult(0, INVOCATION, 3, 0, None, (), "", len(warned), warned_nodes=warned)

    with caplog.at_level(logging.WARNING, logger=publish.__name__):
        _build(paths, run_id, run(()))
        assert caplog.records == []
        _build(paths, run_id, run(("test.p.w",)))

    assert "test.p.w" in caplog.records[0].getMessage()
