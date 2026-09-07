"""실제 PostgreSQL·SeaweedFS에서 `orders` Commit Protocol 전체를 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.ingestion.batch import BatchIdentityConflictError
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.service import OrdersIngestionRequest, ingest_orders, orders_object_keys
from src.ingestion.storage import SeaweedFSSettings, ensure_bucket, seaweedfs_s3_client

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_orders_service_commits_verified_manifest_object_run_and_watermark(tmp_path) -> None:
    """다섯 Row 실행은 검증된 Object·Manifest와 Metadata CAS를 모두 완료한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_orders_service_{uuid.uuid4().hex}"
    request = OrdersIngestionRequest.for_dag_run(
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=2,
    )
    lower_bound = _lower_bound_before_five_latest_rows(postgres)
    _set_watermark(postgres, pipeline_name, lower_bound, now)

    result = None
    try:
        result = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)

        assert result.status == "SUCCESS"
        assert result.row_count == 5
        assert result.object_key is not None
        assert result.manifest_key is not None
        manifest = (
            seaweedfs_s3_client(storage)
            .get_object(Bucket=storage.bucket, Key=result.manifest_key)["Body"]
            .read()
        )
        assert b'"object_state":"VERIFIED"' in manifest

        with postgres.pipeline_connection() as connection:
            run_row = connection.execute(
                """
                SELECT status, rows_extracted, rows_valid, rows_rejected, rows_loaded
                FROM pipeline_runs WHERE run_id = %s AND source_table = 'orders'
                """,
                (result.run.run_id,),
            ).fetchone()
            object_row = connection.execute(
                """
                SELECT status, row_count, object_key, manifest_key
                FROM bronze_objects WHERE table_batch_id = %s
                """,
                (f"{request.batch_id}__orders",),
            ).fetchone()
            watermark_row = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys, version
                FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'
                """,
                (pipeline_name,),
            ).fetchone()

        assert run_row == ("SUCCESS", 5, 5, 0, 5)
        assert object_row == ("COMMITTED", 5, result.object_key, result.manifest_key)
        assert watermark_row == (
            result.run.extract_upper_bound.timestamp,
            result.run.extract_upper_bound.as_json(),
            1,
        )
    finally:
        _delete_test_rows_and_objects(postgres, storage, pipeline_name, request)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_orders_service_keeps_watermark_when_final_object_already_exists(tmp_path) -> None:
    """Final Key 충돌 실패는 Object를 덮어쓰지 않고 Watermark를 유지한 채 Run을 실패시킨다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_orders_failure_{uuid.uuid4().hex}"
    request = OrdersIngestionRequest.for_dag_run(
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=2,
    )
    lower_bound = _lower_bound_before_five_latest_rows(postgres)
    _set_watermark(postgres, pipeline_name, lower_bound, now)
    object_key, _ = orders_object_keys(request.batch_id, request.logical_date)
    ensure_bucket(storage)
    client = seaweedfs_s3_client(storage)
    client.put_object(Bucket=storage.bucket, Key=object_key, Body=b"must-not-be-overwritten")

    try:
        with pytest.raises(FileExistsError):
            ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)

        assert (
            client.get_object(Bucket=storage.bucket, Key=object_key)["Body"].read()
            == b"must-not-be-overwritten"
        )
        with postgres.pipeline_connection() as connection:
            watermark_row = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys, version
                FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'
                """,
                (pipeline_name,),
            ).fetchone()
            run_row = connection.execute(
                """
                SELECT status, error_type FROM pipeline_runs
                WHERE pipeline_name = %s AND source_table = 'orders'
                """,
                (pipeline_name,),
            ).fetchone()

        assert watermark_row == (lower_bound.timestamp, lower_bound.as_json(), 0)
        assert run_row == ("FAILED", "FileExistsError")
    finally:
        _delete_test_rows_and_objects(postgres, storage, pipeline_name, request)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_orders_service_finishes_an_empty_range_without_object_or_watermark_change(
    tmp_path,
) -> None:
    """최대 Cursor에서 시작한 Empty Batch는 Object 없이 SUCCESS_NO_DATA로 종료한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_orders_empty_{uuid.uuid4().hex}"
    request = OrdersIngestionRequest.for_dag_run(
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
    )
    maximum_cursor = _maximum_cursor(postgres)
    _set_watermark(postgres, pipeline_name, maximum_cursor, now)

    try:
        result = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)

        assert result.status == "SUCCESS_NO_DATA"
        assert result.object_key is None
        with postgres.pipeline_connection() as connection:
            run_row = connection.execute(
                """
                SELECT status, rows_extracted FROM pipeline_runs
                WHERE run_id = %s AND source_table = 'orders'
                """,
                (result.run.run_id,),
            ).fetchone()
            object_count = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__orders",),
            ).fetchone()[0]
            watermark_row = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys, version
                FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'
                """,
                (pipeline_name,),
            ).fetchone()

        assert run_row == ("SUCCESS_NO_DATA", 0)
        assert object_count == 0
        assert watermark_row == (maximum_cursor.timestamp, maximum_cursor.as_json(), 0)
    finally:
        _delete_test_rows_and_objects(postgres, storage, pipeline_name, request)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_orders_service_reuses_a_committed_standard_batch_without_new_object_or_watermark(
    tmp_path,
) -> None:
    """같은 표준 Batch 재실행은 Source 추출·Object 생성 없이 Catalog Object를 재사용한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_orders_reuse_{uuid.uuid4().hex}"
    request = OrdersIngestionRequest.for_dag_run(
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=2,
    )
    _set_watermark(postgres, pipeline_name, _lower_bound_before_five_latest_rows(postgres), now)

    try:
        first = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        reused = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)

        assert first.status == "SUCCESS"
        assert reused.status == "SKIPPED_ALREADY_COMMITTED"
        assert reused.object_key == first.object_key
        assert reused.manifest_key == first.manifest_key
        with postgres.pipeline_connection() as connection:
            object_count = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__orders",),
            ).fetchone()[0]
            watermark_row = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys, version
                FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'
                """,
                (pipeline_name,),
            ).fetchone()
            statuses = connection.execute(
                """
                SELECT status FROM pipeline_runs
                WHERE pipeline_name = %s AND source_table = 'orders'
                ORDER BY started_at, run_id
                """,
                (pipeline_name,),
            ).fetchall()

        assert object_count == 1
        assert watermark_row == (
            first.run.extract_upper_bound.timestamp,
            first.run.extract_upper_bound.as_json(),
            1,
        )
        assert {row[0] for row in statuses} == {"SUCCESS", "SKIPPED_ALREADY_COMMITTED"}
    finally:
        _delete_test_rows_and_objects(postgres, storage, pipeline_name, request)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_orders_service_rejects_a_standard_batch_when_the_current_range_differs(tmp_path) -> None:
    """같은 Batch라도 현재 Watermark가 다르면 Object 없이 Conflict Run으로 종료한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 7, tzinfo=UTC)
    pipeline_name = f"test_orders_identity_conflict_{uuid.uuid4().hex}"
    request = OrdersIngestionRequest.for_dag_run(
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=2,
    )
    lower_bound = _lower_bound_before_five_latest_rows(postgres)
    _set_watermark(postgres, pipeline_name, lower_bound, now)

    try:
        first = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        _set_watermark(postgres, pipeline_name, lower_bound, now)

        with pytest.raises(BatchIdentityConflictError, match="range"):
            ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)

        with postgres.pipeline_connection() as connection:
            object_count = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__orders",),
            ).fetchone()[0]
            run_rows = connection.execute(
                """
                SELECT status, error_type FROM pipeline_runs
                WHERE pipeline_name = %s AND source_table = 'orders'
                ORDER BY started_at, run_id
                """,
                (pipeline_name,),
            ).fetchall()
            watermark_row = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys
                FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'
                """,
                (pipeline_name,),
            ).fetchone()

        assert first.status == "SUCCESS"
        assert object_count == 1
        assert set(run_rows) == {("SUCCESS", None), ("FAILED", "BATCH_IDENTITY_CONFLICT")}
        assert watermark_row == (lower_bound.timestamp, lower_bound.as_json())
    finally:
        _delete_test_rows_and_objects(postgres, storage, pipeline_name, request)


def _lower_bound_before_five_latest_rows(settings: PostgresSettings) -> CursorPosition:
    """작고 여러 Page인 실제 범위를 만들 최신 다섯 Row 직전 Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT updated_at, order_id FROM orders
            ORDER BY updated_at DESC, order_id COLLATE "C" DESC OFFSET 5 LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain at least six orders")
    return CursorPosition(row[0], (row[1],))


def _maximum_cursor(settings: PostgresSettings) -> CursorPosition:
    """Empty Range를 만들 수 있는 실제 `orders` 최대 Composite Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT updated_at, order_id FROM orders
            ORDER BY updated_at DESC, order_id COLLATE "C" DESC LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("The seeded source must contain orders")
    return CursorPosition(row[0], (row[1],))


def _set_watermark(
    settings: PostgresSettings, pipeline_name: str, cursor: CursorPosition, now: datetime
) -> None:
    """통합 테스트 전용 Pipeline의 초기 Watermark를 작은 Source 범위 직전으로 옮긴다."""
    get_or_create_watermark(settings, pipeline_name, "orders", now=now)
    with settings.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks
            SET watermark_timestamp = %s, watermark_keys = %s
            WHERE pipeline_name = %s AND source_table = 'orders'
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), pipeline_name),
        )
        connection.commit()


def _delete_test_rows_and_objects(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    request: OrdersIngestionRequest,
) -> None:
    """테스트가 만든 정확한 Metadata와 Final Object만 역순으로 정리한다."""
    object_key, manifest_key = orders_object_keys(request.batch_id, request.logical_date)
    client = seaweedfs_s3_client(storage)
    for key in (manifest_key, object_key):
        client.delete_object(Bucket=storage.bucket, Key=key)
    with postgres.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM bronze_objects WHERE table_batch_id = %s", (f"{request.batch_id}__orders",)
        )
        connection.execute(
            "DELETE FROM pipeline_runs WHERE pipeline_name = %s AND source_table = 'orders'",
            (pipeline_name,),
        )
        connection.execute(
            "DELETE FROM watermarks WHERE pipeline_name = %s AND source_table = 'orders'",
            (pipeline_name,),
        )
        connection.commit()
