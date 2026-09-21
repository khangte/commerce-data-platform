"""Benchmark 실험이 공유하는 DuckDB S3(SeaweedFS) 읽기 설정."""

from __future__ import annotations

import duckdb

from src.ingestion.storage import SeaweedFSSettings


def configure_s3(connection: duckdb.DuckDBPyConnection, storage: SeaweedFSSettings) -> None:
    """SeaweedFS S3 API를 읽기 위한 DuckDB httpfs 설정을 적용한다."""

    def _escaped(value: str) -> str:
        """SQL 문자열 리터럴에 넣을 수 있게 홑따옴표를 이스케이프한다."""
        return value.replace("'", "''")

    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(f"SET s3_endpoint='{_escaped(f'{storage.host}:{storage.port}')}'")
    connection.execute("SET s3_region='us-east-1'")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_use_ssl=false")
    connection.execute(f"SET s3_access_key_id='{_escaped(storage.access_key)}'")
    connection.execute(f"SET s3_secret_access_key='{_escaped(storage.secret_key)}'")
