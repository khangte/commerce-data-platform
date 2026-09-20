"""Corruption Matrix의 Ingestion 5종이 격리·Commit·Watermark 규칙을 지키는지 검증한다 (AC-08)."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.ingestion.corruption import (
    BROKEN_REFERENCE,
    DUPLICATE_PRIMARY_KEY,
    INVALID_STATUS,
    NEGATIVE_NUMERIC,
    NULL_PRIMARY_KEY,
    CorruptionPlan,
)
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.service import (
    TableIngestionRequest,
    ingest_table,
    quarantine_object_keys,
    table_object_keys,
)
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client
from src.ingestion.tables import TableConfig, table_config

pytestmark = pytest.mark.integration

BATCH_ROWS = 20
CORRUPTED_ORDINAL = 1

CORRUPTION_MATRIX = (
    pytest.param("orders", DUPLICATE_PRIMARY_KEY, {"BATCH_DUPLICATE": 1}, id="duplicate"),
    pytest.param(
        "orders", NULL_PRIMARY_KEY, {"REQUIRED_NULL": 1, "KEY_NULL": 1}, id="null-key"
    ),
    pytest.param("order_items", BROKEN_REFERENCE, {"BROKEN_REFERENCE": 1}, id="broken-fk"),
    pytest.param("orders", INVALID_STATUS, {"STATUS_DOMAIN_INVALID": 1}, id="invalid-status"),
    pytest.param(
        "order_items", NEGATIVE_NUMERIC, {"NUMERIC_RANGE_INVALID": 1}, id="negative-value"
    ),
)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
@pytest.mark.parametrize(("source_table", "kind", "expected_errors"), CORRUPTION_MATRIX)
def test_corruption_is_quarantined_while_valid_rows_commit(
    tmp_path, source_table: str, kind: str, expected_errors: dict[str, int]
) -> None:
    """오염 1건은 격리되고 19건은 Commit되며 Source는 그대로이고 Watermark는 전진한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    config = table_config(source_table)
    now = datetime.now(UTC)
    pipeline_name = f"test_corruption_{uuid.uuid4().hex}"
    request = TableIngestionRequest.for_dag_run(
        source_table=source_table,
        dag_id=f"warehouse_{uuid.uuid4().hex}",
        logical_date=now,
        pipeline_name=pipeline_name,
        page_size=BATCH_ROWS,
        corruption_plan=CorruptionPlan({CORRUPTED_ORDINAL: kind}),
    )
    source_before = _latest_source_rows(postgres, config)
    newest_cursor = _cursor_at_offset(postgres, config, 0)
    start_cursor = _cursor_at_offset(postgres, config, BATCH_ROWS)
    _set_watermark(postgres, pipeline_name, config, start_cursor, now)

    try:
        result = ingest_table(postgres, storage, request, local_directory=tmp_path, now=now)

        assert result.status == "SUCCESS"
        assert (result.row_count, result.rows_rejected, result.rows_corrupted) == (19, 1, 1)
        with postgres.pipeline_connection() as connection:
            quarantine = connection.execute(
                """
                SELECT object_key, row_count, error_counts FROM quarantine_batches
                WHERE table_batch_id = %s
                """,
                (f"{request.batch_id}__{source_table}",),
            ).fetchone()
            run_counts = connection.execute(
                """
                SELECT rows_extracted, rows_valid, rows_rejected, rows_loaded, status
                FROM pipeline_runs WHERE pipeline_name = %s AND source_table = %s
                """,
                (pipeline_name, source_table),
            ).fetchone()
            watermark = connection.execute(
                """
                SELECT watermark_timestamp, watermark_keys FROM watermarks
                WHERE pipeline_name = %s AND source_table = %s
                """,
                (pipeline_name, source_table),
            ).fetchone()
        assert quarantine == (result.quarantine_object_key, 1, expected_errors)
        assert run_counts == (BATCH_ROWS, 19, 1, 19, "SUCCESS")
        assert watermark == (newest_cursor.timestamp, newest_cursor.as_json())
        assert _latest_source_rows(postgres, config) == source_before
    finally:
        _cleanup(postgres, storage, pipeline_name, request)


def _cursor_at_offset(
    settings: PostgresSettings, config: TableConfig, offset: int
) -> CursorPosition:
    """최신순으로 offset번째 Row의 Composite Cursor를 읽는다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            f"""
            SELECT {", ".join(config.cursor_columns)}
            FROM {config.source_table}
            ORDER BY {_order_by(config, descending=True)}
            OFFSET %s LIMIT 1
            """,
            (offset,),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"The seeded source needs more than {offset} {config.source_table}")
    return CursorPosition(row[0], tuple(row[1:]))


def _latest_source_rows(settings: PostgresSettings, config: TableConfig) -> list[tuple]:
    """Source 무변경 비교용으로 최신 BATCH_ROWS개 Row 전체를 읽는다."""
    with settings.source_connection() as connection:
        return connection.execute(
            f"""
            SELECT * FROM {config.source_table}
            ORDER BY {_order_by(config, descending=True)}
            LIMIT %s
            """,
            (BATCH_ROWS,),
        ).fetchall()


def _set_watermark(
    settings: PostgresSettings,
    pipeline_name: str,
    config: TableConfig,
    cursor: CursorPosition,
    now: datetime,
) -> None:
    """최신 BATCH_ROWS개 Row만 추출되도록 Watermark를 그 직전 Cursor로 설정한다."""
    get_or_create_watermark(settings, pipeline_name, config.source_table, now=now)
    with settings.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks SET watermark_timestamp = %s, watermark_keys = %s
            WHERE pipeline_name = %s AND source_table = %s
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), pipeline_name, config.source_table),
        )
        connection.commit()


def _order_by(config: TableConfig, *, descending: bool = False) -> str:
    """문자열 Key의 C Collation을 보존한 Cursor SQL ORDER BY를 만든다."""
    suffix = " DESC" if descending else ""
    key_columns = [
        f'{column} COLLATE "C"' if column == "order_id" else column
        for column in config.cursor_columns
    ]
    return ", ".join(f"{column}{suffix}" for column in key_columns)


def _cleanup(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    request: TableIngestionRequest,
) -> None:
    """테스트가 만든 Bronze·Quarantine Object와 Metadata만 정리한다."""
    table = request.source_table
    keys = (
        *table_object_keys(table, request.batch_id, request.logical_date),
        *quarantine_object_keys(table, request.batch_id, request.logical_date),
    )
    client = seaweedfs_s3_client(storage)
    for key in keys:
        client.delete_object(Bucket=storage.bucket, Key=key)
    table_batch_id = f"{request.batch_id}__{table}"
    with postgres.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM quarantine_batches WHERE table_batch_id = %s", (table_batch_id,)
        )
        connection.execute(
            "DELETE FROM bronze_objects WHERE table_batch_id = %s", (table_batch_id,)
        )
        connection.execute(
            "DELETE FROM pipeline_runs WHERE pipeline_name = %s AND source_table = %s",
            (pipeline_name, table),
        )
        connection.execute(
            "DELETE FROM watermarks WHERE pipeline_name = %s AND source_table = %s",
            (pipeline_name, table),
        )
        connection.commit()
