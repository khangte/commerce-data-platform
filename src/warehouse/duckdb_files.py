"""DuckDB 파일의 경로·동기화·식별값을 공유한다."""

from __future__ import annotations

import os
from pathlib import Path
from typing import NamedTuple

import duckdb

from src.common.database import PROJECT_ROOT

DEFAULT_WAREHOUSE_ROOT = PROJECT_ROOT / "data" / "warehouse"


class FileIdentity(NamedTuple):
    """파일을 구별하는 inode와 나노초 수정 시각을 담는다."""

    inode: int
    mtime_ns: int


def file_identity(path: Path) -> FileIdentity:
    """파일의 inode와 나노초 수정 시각을 반환한다."""
    stat = os.stat(path)
    return FileIdentity(stat.st_ino, stat.st_mtime_ns)


def checkpoint(warehouse_path: Path) -> None:
    """WAL 내용을 본 파일에 반영하고 연결을 닫는다."""
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CHECKPOINT")


def fsync_directory(directory: Path) -> None:
    """Rename 결과가 Disk에 남도록 Directory를 fsync한다."""
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def wal_path(warehouse_path: Path) -> Path:
    """DuckDB 파일의 WAL 경로를 반환한다."""
    return warehouse_path.with_name(f"{warehouse_path.name}.wal")


