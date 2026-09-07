"""실제 SeaweedFS S3 API의 Path-style Bronze 호환성을 검증한다."""

from __future__ import annotations

import io
import os
import uuid

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.ingestion.storage import SeaweedFSSettings, ensure_bucket, seaweedfs_s3_client

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set RUN_SEAWEEDFS_INTEGRATION=1 after starting the Phase 3 SeaweedFS container.",
)
def test_seaweedfs_supports_path_style_bronze_object_lifecycle_and_duckdb_read() -> None:
    """Bucket·PUT/GET/HEAD/LIST/DELETE와 DuckDB Parquet Read를 임시 Prefix에서 검증한다."""
    settings = SeaweedFSSettings.from_environment()
    client = seaweedfs_s3_client(settings)
    key = f"_smoke/{uuid.uuid4()}/orders.parquet"
    parquet_bytes = _parquet_bytes()

    ensure_bucket(settings)
    try:
        client.put_object(Bucket=settings.bucket, Key=key, Body=b"temporary-overwrite-observation")
        client.put_object(Bucket=settings.bucket, Key=key, Body=parquet_bytes)

        actual_bytes = client.get_object(Bucket=settings.bucket, Key=key)["Body"].read()
        head = client.head_object(Bucket=settings.bucket, Key=key)
        listed = client.list_objects_v2(Bucket=settings.bucket, Prefix=key)

        assert actual_bytes == parquet_bytes
        assert head["ContentLength"] == len(parquet_bytes)
        assert [item["Key"] for item in listed["Contents"]] == [key]
        assert _duckdb_order_ids(settings, key) == ["order-0001", "order-0002"]
    finally:
        client.delete_object(Bucket=settings.bucket, Key=key)

    assert client.list_objects_v2(Bucket=settings.bucket, Prefix=key).get("Contents", []) == []


def _parquet_bytes() -> bytes:
    """DuckDB Read 검증에 사용할 최소 Bronze 호환 Parquet Byte를 반환한다."""
    buffer = io.BytesIO()
    pq.write_table(
        pa.table({"order_id": ["order-0001", "order-0002"], "amount": [10, 20]}),
        buffer,
        compression="zstd",
    )
    return buffer.getvalue()


def _duckdb_order_ids(settings: SeaweedFSSettings, key: str) -> list[str]:
    """SeaweedFS Path-style S3 Object를 DuckDB `read_parquet`으로 읽은 Key를 반환한다."""
    connection = duckdb.connect()
    try:
        _load_httpfs(connection)
        connection.execute("SET s3_endpoint = ?", [f"{settings.host}:{settings.port}"])
        connection.execute("SET s3_url_style = 'path'")
        connection.execute("SET s3_use_ssl = false")
        connection.execute("SET s3_access_key_id = ?", [settings.access_key])
        connection.execute("SET s3_secret_access_key = ?", [settings.secret_key])
        rows = connection.execute(
            "SELECT order_id FROM read_parquet(?) ORDER BY order_id",
            [f"s3://{settings.bucket}/{key}"],
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        connection.close()


def _load_httpfs(connection: duckdb.DuckDBPyConnection) -> None:
    """설치된 DuckDB HTTPFS Extension을 불러오고 없으면 한 번 설치한다."""
    try:
        connection.execute("LOAD httpfs")
    except duckdb.Error:
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
