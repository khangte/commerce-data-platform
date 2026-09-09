"""dbt Bronze File Catalog Macro의 입력 경계를 검증한다."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_catalog_macro_handles_empty_catalog_and_rejects_unsupported_version(tmp_path) -> None:
    """빈 목록은 안전하게 처리하고 지원하지 않는 Version은 dbt 실행 전에 차단한다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)

    empty_result = _run_validate_catalog(warehouse_path)

    assert empty_result.returncode == 0, empty_result.stderr
    assert "Validated Bronze catalog relation for orders" in empty_result.stdout
    assert "where false" in empty_result.stdout

    connection = duckdb.connect(str(warehouse_path))
    try:
        connection.execute(
            """
            INSERT INTO control.bronze_files VALUES
            ('orders', 'bronze/orders/committed.parquet', 1, 'batch-1', now(), 1, 'a'),
            ('orders', 'bronze/orders/unsupported.parquet', 2, 'batch-2', now(), 1, 'b')
            """
        )
    finally:
        connection.close()

    invalid_result = _run_validate_catalog(warehouse_path)

    assert invalid_result.returncode != 0
    assert "SOURCE_CONTRACT_ERROR: unsupported schema_version(s)=2" in _combined_output(invalid_result)


def test_catalog_macro_renders_committed_object_as_explicit_parquet_list(tmp_path) -> None:
    """Commit된 Object Key만 `read_parquet([...])` 목록에 포함하고 Glob을 만들지 않는다."""
    warehouse_path = tmp_path / "warehouse.duckdb"
    _create_catalog(warehouse_path)
    connection = duckdb.connect(str(warehouse_path))
    try:
        connection.execute(
            """
            INSERT INTO control.bronze_files VALUES
            ('orders', 'bronze/orders/committed.parquet', 1, 'batch-1', now(), 1, 'a')
            """
        )
    finally:
        connection.close()

    result = _run_validate_catalog(warehouse_path)

    assert result.returncode == 0, result.stderr
    assert "s3://test-bucket/bronze/orders/committed.parquet" in result.stdout
    assert "bronze/orders/*.parquet" not in result.stdout


def _create_catalog(warehouse_path: Path) -> None:
    """dbt Macro 검증에 필요한 최소 DuckDB File Catalog를 생성한다."""
    connection = duckdb.connect(str(warehouse_path))
    try:
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
    finally:
        connection.close()


def _run_validate_catalog(warehouse_path: Path) -> subprocess.CompletedProcess[str]:
    """테스트 Warehouse를 대상으로 dbt Catalog 사전 검증 Operation을 실행한다."""
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
            "validate_bronze_catalog",
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
