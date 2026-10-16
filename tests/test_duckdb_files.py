"""DuckDB 파일 경로와 원자 교체 전후의 식별값을 검증한다."""

import os
from pathlib import Path

from src.warehouse.duckdb_files import file_identity, wal_path


def test_file_identity_survives_os_replace(tmp_path: Path) -> None:
    """원자 교체는 inode와 수정 시각을 보존한다."""
    source = tmp_path / "build.duckdb"
    source.write_bytes(b"x")
    before = file_identity(source)
    target = tmp_path / "warehouse.duckdb"
    os.replace(source, target)
    assert file_identity(target) == before


def test_file_identity_changes_with_mtime(tmp_path: Path) -> None:
    """같은 inode라도 수정 시각이 바뀌면 다른 파일 식별값이다."""
    path = tmp_path / "warehouse.duckdb"
    path.write_bytes(b"x")
    before = file_identity(path)
    os.utime(path, ns=(before.mtime_ns + 1_000, before.mtime_ns + 1_000))
    after = file_identity(path)
    assert after.inode == before.inode
    assert after != before


def test_wal_path_appends_wal_suffix() -> None:
    """WAL 경로는 파일명 뒤에 .wal을 붙인다."""
    assert wal_path(Path("/x/warehouse.duckdb")) == Path("/x/warehouse.duckdb.wal")
