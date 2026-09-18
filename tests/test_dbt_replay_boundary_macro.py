"""Bronze Replay 경계 Macro의 값 검증과 Catalog 절단을 확인한다."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_boundary_excludes_objects_committed_after_the_as_of_timestamp(tmp_path: Path) -> None:
    """경계보다 늦게 Commit된 Object는 Bronze 입력 목록에서 빠진다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(warehouse_path, bronze_as_of="2026-09-10T00:00:00Z")

    assert result.returncode == 0, _combined_output(result)
    assert "bronze/orders/early.parquet" in result.stdout
    assert "bronze/orders/late.parquet" not in result.stdout


def test_boundary_absent_keeps_every_committed_object(tmp_path: Path) -> None:
    """경계를 주지 않으면 지금과 같이 Commit된 Object를 모두 읽는다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(warehouse_path)

    assert result.returncode == 0, _combined_output(result)
    assert "bronze/orders/early.parquet" in result.stdout
    assert "bronze/orders/late.parquet" in result.stdout


def test_boundary_rejects_a_timestamp_without_utc_offset(tmp_path: Path) -> None:
    """UTC Offset이 없는 값은 Build 전에 차단한다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(warehouse_path, bronze_as_of="2026-09-10 00:00:00")

    assert result.returncode != 0
    assert "REPLAY_BOUNDARY_ERROR" in _combined_output(result)


def test_boundary_rejects_a_non_timestamp_value(tmp_path: Path) -> None:
    """Timestamp가 아닌 값은 Build 전에 차단한다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    result = _run_validate_catalog(warehouse_path, bronze_as_of="yesterday")

    assert result.returncode != 0
    assert "REPLAY_BOUNDARY_ERROR" in _combined_output(result)


def _create_catalog(warehouse_path: Path) -> None:
    """경계 앞뒤로 하나씩 Commit된 최소 Bronze Catalog를 만든다."""
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            """
            CREATE TABLE control.bronze_files (
                source_table VARCHAR NOT NULL,
                object_key VARCHAR PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                batch_id VARCHAR NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                row_count BIGINT NOT NULL,
                logical_hash VARCHAR NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO control.bronze_files VALUES
            ('orders', 'bronze/orders/early.parquet', 3, 'batch-1',
             timestamptz '2026-09-09 00:00:00+00', 1, 'a'),
            ('orders', 'bronze/orders/late.parquet', 3, 'batch-2',
             timestamptz '2026-09-11 00:00:00+00', 1, 'b')
            """
        )


def _run_validate_catalog(
    warehouse_path: Path, *, bronze_as_of: str | None = None
) -> subprocess.CompletedProcess[str]:
    """선택한 경계로 dbt Catalog 사전 검증 Operation을 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_BUCKET": "test-bucket",
        "SEAWEEDFS_ACCESS_KEY": "test-access-key",
        "SEAWEEDFS_SECRET_KEY": "test-secret-key",
    }
    command = [
        str(Path(sys.executable).with_name("dbt")),
        "run-operation",
        "validate_bronze_catalog",
        "--project-dir",
        "dbt",
        "--profiles-dir",
        "dbt",
    ]
    if bronze_as_of is not None:
        command += ["--vars", json.dumps({"bronze_as_of": bronze_as_of})]
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    """dbt 버전에 따라 달라지는 표준 출력·오류 출력을 함께 비교한다."""
    return f"{result.stdout}\n{result.stderr}"
