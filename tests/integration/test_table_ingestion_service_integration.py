"""- `orders` 외 Table도 공통 Commit 서비스로 Bronze까지 수집하는지 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.service import TableIngestionRequest, ingest_table, table_object_keys
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_customers_table_uses_the_same_bronze_commit_protocol_as_orders(tmp_path) -> None:
    """- Mutable `customers`도 공통 Writer·Manifest·Metadata CAS로 3개 Row를 Commit한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_customers_service_{uuid.uuid4().hex}"
    request = TableIngestionRequest.for_dag_run(
        source_table="customers",
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=2,
    )
    _set_customers_watermark(postgres, pipeline_name, now)

    try:
        result = ingest_table(postgres, storage, request, local_directory=tmp_path, now=now)

        assert result.status == "SUCCESS"
        assert result.row_count == 3
        assert result.rows_rejected == 0
        assert result.object_key is not None and result.object_key.startswith("bronze/customers/")
        with postgres.pipeline_connection() as connection:
            run_row = connection.execute(
                """
                SELECT rows_extracted, rows_valid, rows_rejected, rows_loaded, status
                FROM pipeline_runs WHERE run_id = %s AND source_table = 'customers'
                """,
                (result.run.run_id,),
            ).fetchone()
            object_row = connection.execute(
                "SELECT row_count, status FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__customers",),
            ).fetchone()

        assert run_row == (3, 3, 0, 3, "SUCCESS")
        assert object_row == (3, "COMMITTED")
    finally:
        _cleanup(postgres, storage, pipeline_name, request)


def _set_customers_watermark(settings: PostgresSettings, pipeline_name: str, now: datetime) -> None:
    """- 여러 Page를 만들도록 최신 세 Customer 직전 Cursor를 Watermark로 설정한다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT updated_at, customer_id FROM customers
            ORDER BY updated_at DESC, customer_id COLLATE "C" DESC OFFSET 3 LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain at least four customers")
    cursor = CursorPosition(row[0], (row[1],))
    get_or_create_watermark(settings, pipeline_name, "customers", now=now)
    with settings.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks SET watermark_timestamp = %s, watermark_keys = %s
            WHERE pipeline_name = %s AND source_table = 'customers'
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), pipeline_name),
        )
        connection.commit()


def _cleanup(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    request: TableIngestionRequest,
) -> None:
    """- 테스트가 만든 정확한 Customer Metadata와 Final Object만 정리한다."""
    object_key, manifest_key = table_object_keys("customers", request.batch_id, request.logical_date)
    client = seaweedfs_s3_client(storage)
    for key in (manifest_key, object_key):
        client.delete_object(Bucket=storage.bucket, Key=key)
    with postgres.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM bronze_objects WHERE table_batch_id = %s",
            (f"{request.batch_id}__customers",),
        )
        connection.execute(
            "DELETE FROM pipeline_runs WHERE pipeline_name = %s AND source_table = 'customers'",
            (pipeline_name,),
        )
        connection.execute(
            "DELETE FROM watermarks WHERE pipeline_name = %s AND source_table = 'customers'",
            (pipeline_name,),
        )
        connection.commit()
