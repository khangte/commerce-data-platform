"""재계산 경계 Watermark 매크로의 생성과 조회 경계를 검증한다."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_watermark_table_is_created_and_starts_empty(tmp_path: Path) -> None:
    """Watermark Table이 없으면 만들고, 비어 있으면 하한이 빈 문자열이 된다."""
    warehouse_path = tmp_path / "warehouse.duckdb"

    result = _run_operation(warehouse_path, "ensure_processed_batch_watermark")

    assert result.returncode == 0, _combined_output(result)

    with duckdb.connect(str(warehouse_path)) as connection:
        rows = connection.execute(
            "SELECT coalesce(max(processed_batch_id), '') FROM control.dbt_processed_batch"
        ).fetchall()

    assert rows == [("",)]


def test_watermark_table_creation_is_idempotent(tmp_path: Path) -> None:
    """이미 기록이 있는 Watermark Table은 다시 만들어도 값을 잃지 않는다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    assert _run_operation(warehouse_path, "ensure_processed_batch_watermark").returncode == 0
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute(
            "INSERT INTO control.dbt_processed_batch (processed_batch_id, invocation_id)"
            " VALUES ('warehouse_pipeline__20260909T030000Z', 'invocation-1')"
        )

    result = _run_operation(warehouse_path, "ensure_processed_batch_watermark")

    assert result.returncode == 0, _combined_output(result)
    with duckdb.connect(str(warehouse_path)) as connection:
        rows = connection.execute(
            "SELECT max(processed_batch_id) FROM control.dbt_processed_batch"
        ).fetchall()

    assert rows == [("warehouse_pipeline__20260909T030000Z",)]


def _run_operation(warehouse_path: Path, operation: str) -> subprocess.CompletedProcess[str]:
    """격리된 Warehouse에 dbt Operation 하나를 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_BUCKET": "test-bucket",
        "SEAWEEDFS_ACCESS_KEY": "test-access-key",
        "SEAWEEDFS_SECRET_KEY": "test-secret-key",
    }
    return subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "run-operation",
            operation,
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    """dbt 버전에 따라 달라지는 표준 출력·오류 출력을 함께 비교한다."""
    return f"{result.stdout}\n{result.stderr}"
