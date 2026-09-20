"""Publish 경로 규칙과 실패 Build 격리·보존 규칙을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from src.warehouse.publish import (
    FAILED_BUILD_RETENTION,
    WarehousePaths,
    _isolate_build,
    _prune_failed_builds,
)

NOW = datetime(2026, 9, 18, tzinfo=UTC)


def test_paths_under_root_follow_the_spec_layout(tmp_path: Path) -> None:
    """Published·Build·Failed 경로가 Spec의 Directory 구조를 따른다."""
    paths = WarehousePaths.under(tmp_path)
    run_id = uuid.UUID(int=1)
    assert paths.published == tmp_path / "warehouse.duckdb"
    assert paths.published_wal == tmp_path / "warehouse.duckdb.wal"
    assert paths.build_file(run_id) == tmp_path / "build" / f"{run_id}.duckdb"
    assert paths.failed_file(run_id) == tmp_path / "failed" / f"{run_id}.duckdb"
    assert paths.target_dir(run_id) == tmp_path / "build" / f"{run_id}-target"


def test_isolate_build_moves_file_and_wal_and_touches_mtime(tmp_path: Path) -> None:
    """실패 Build와 WAL을 failed/로 옮기고 mtime을 격리 시각으로 맞춘다."""
    paths = WarehousePaths.under(tmp_path)
    run_id = uuid.uuid4()
    paths.build_dir.mkdir()
    paths.build_file(run_id).write_bytes(b"db")
    paths.build_file(run_id).with_name(f"{run_id}.duckdb.wal").write_bytes(b"wal")

    isolated = _isolate_build(paths, run_id, now=NOW)

    assert isolated == paths.failed_file(run_id)
    assert isolated.read_bytes() == b"db"
    assert isolated.with_name(f"{run_id}.duckdb.wal").read_bytes() == b"wal"
    assert not paths.build_file(run_id).exists()
    assert isolated.stat().st_mtime == NOW.timestamp()


def test_isolate_build_returns_none_when_build_is_missing(tmp_path: Path) -> None:
    """Build 파일이 없으면 아무것도 옮기지 않고 None을 반환한다."""
    assert _isolate_build(WarehousePaths.under(tmp_path), uuid.uuid4(), now=NOW) is None


def test_prune_keeps_only_the_newest_failed_builds(tmp_path: Path) -> None:
    """failed/에는 mtime 기준 최신 3개 Build와 그 WAL만 남는다."""
    failed_dir = tmp_path / "failed"
    failed_dir.mkdir()
    for index in range(5):
        build = failed_dir / f"run-{index}.duckdb"
        build.write_bytes(b"db")
        build.with_name(f"run-{index}.duckdb.wal").write_bytes(b"wal")
        os.utime(build, (1_000 + index, 1_000 + index))

    _prune_failed_builds(failed_dir)

    remaining = sorted(path.name for path in failed_dir.glob("*.duckdb"))
    assert FAILED_BUILD_RETENTION == 3
    assert remaining == ["run-2.duckdb", "run-3.duckdb", "run-4.duckdb"]
    assert not (failed_dir / "run-0.duckdb.wal").exists()
