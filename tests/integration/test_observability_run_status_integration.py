"""Observability SQL 한 번으로 성공·빈·실패·재실행 Batch가 구분되는지 검증한다 (AC-13)."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.ingestion.metadata import ensure_ingestion_metadata
from src.warehouse.publish_metadata import ensure_publish_metadata

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
QUERY_PATH = PROJECT_ROOT / "sql" / "validation" / "observability_run_status.sql"
FIXTURE_TIME = datetime(2000, 1, 1, tzinfo=UTC)


@pytest.fixture
def settings() -> PostgresSettings:
    """Metadata Table을 준비한 PostgreSQL 설정을 반환한다."""
    if os.environ.get("RUN_POSTGRES_INTEGRATION") != "1":
        pytest.skip("Set RUN_POSTGRES_INTEGRATION=1 after starting PostgreSQL.")
    postgres = PostgresSettings.from_environment()
    ensure_ingestion_metadata(postgres)
    ensure_publish_metadata(postgres)
    return postgres


def test_observability_query_distinguishes_run_outcomes(settings: PostgresSettings) -> None:
    """네 가지 상태와 Publish 실패가 run_outcome과 Publish Column에 드러난다."""
    pipeline_name = f"test_observability_{uuid.uuid4().hex}"
    runs = (
        ("b1_success", "orders", 1, "SUCCESS", 10, None),
        ("b1_success", "payments", 1, "SUCCESS", 5, None),
        ("b2_empty", "orders", 1, "SUCCESS_NO_DATA", 0, None),
        ("b2_empty", "payments", 1, "SUCCESS_NO_DATA", 0, None),
        ("b3_ingest_failed", "orders", 1, "FAILED", 0, "SOURCE_CONNECTION_ERROR"),
        ("b3_ingest_failed", "payments", 1, "SUCCESS", 5, None),
        ("b4_rerun", "orders", 1, "FAILED", 0, "SOURCE_CONNECTION_ERROR"),
        ("b4_rerun", "orders", 2, "SUCCESS", 10, None),
        ("b5_skipped", "orders", 1, "SKIPPED_ALREADY_COMMITTED", 0, None),
        ("b6_publish_failed", "orders", 1, "SUCCESS", 10, None),
    )
    publishes = (
        ("b1_success", "PUBLISHED", None),
        ("b6_publish_failed", "FAILED", "DBT_TEST_ERROR"),
    )
    try:
        _insert_pipeline_runs(settings, pipeline_name, runs)
        _insert_publish_runs(settings, pipeline_name, publishes)
        rows = _query(settings, pipeline_name)
    finally:
        _cleanup(settings, pipeline_name)

    assert rows == [
        (f"{pipeline_name}:b1_success", "SUCCESS", 2, 1, 15, "PUBLISHED", None),
        (f"{pipeline_name}:b2_empty", "EMPTY", 2, 1, 0, None, None),
        (f"{pipeline_name}:b3_ingest_failed", "FAILED", 2, 1, 5, None, None),
        (f"{pipeline_name}:b4_rerun", "RERUN", 1, 2, 10, None, None),
        (f"{pipeline_name}:b5_skipped", "RERUN", 1, 1, 0, None, None),
        (f"{pipeline_name}:b6_publish_failed", "FAILED", 1, 1, 10, "FAILED", "DBT_TEST_ERROR"),
    ]


def _query(settings: PostgresSettings, pipeline_name: str) -> list[tuple]:
    """Observability SQL을 Subquery로 감싸 테스트 Pipeline의 Row만 읽는다."""
    sql = QUERY_PATH.read_text(encoding="utf-8")
    with settings.pipeline_connection() as connection:
        return connection.execute(
            f"""
            SELECT batch_id, run_outcome, table_count, max_attempt_number,
                   rows_extracted, publish_status, publish_error_type
            FROM ({sql}) AS run_status
            WHERE pipeline_name = %s
            ORDER BY batch_id
            """,
            (pipeline_name,),
        ).fetchall()


def _insert_pipeline_runs(
    settings: PostgresSettings, pipeline_name: str, runs: tuple[tuple, ...]
) -> None:
    """Fixture Ingestion Run을 pipeline_runs에 넣는다."""
    with settings.pipeline_connection() as connection:
        for index, (batch, table, attempt, status, extracted, error_type) in enumerate(runs):
            started_at = FIXTURE_TIME + timedelta(minutes=index)
            connection.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, source_table, pipeline_name, batch_id, logical_date,
                    attempt_number, started_at, finished_at, watermark_before,
                    rows_extracted, rows_valid, rows_loaded, status, error_type, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid.uuid4(),
                    table,
                    pipeline_name,
                    f"{pipeline_name}:{batch}",
                    FIXTURE_TIME,
                    attempt,
                    started_at,
                    started_at + timedelta(seconds=30),
                    Jsonb({}),
                    extracted,
                    extracted,
                    extracted,
                    status,
                    error_type,
                    None if error_type is None else "fixture failure",
                ),
            )
        connection.commit()


def _insert_publish_runs(
    settings: PostgresSettings, pipeline_name: str, publishes: tuple[tuple, ...]
) -> None:
    """Fixture Publish Run을 종료 상태로 mart_publish_runs에 넣는다."""
    with settings.pipeline_connection() as connection:
        for batch, status, error_type in publishes:
            published = status == "PUBLISHED"
            connection.execute(
                """
                INSERT INTO mart_publish_runs (
                    publish_run_id, pipeline_name, batch_id, status, error_type,
                    error_message, mart_hashes, mart_row_counts, started_at, finished_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid.uuid4(),
                    pipeline_name,
                    f"{pipeline_name}:{batch}",
                    status,
                    error_type,
                    None if error_type is None else "fixture failure",
                    Jsonb({}) if published else None,
                    Jsonb({}) if published else None,
                    FIXTURE_TIME,
                    FIXTURE_TIME + timedelta(minutes=1),
                ),
            )
        connection.commit()


def _cleanup(settings: PostgresSettings, pipeline_name: str) -> None:
    """테스트 Pipeline 이름으로 만든 Fixture Row만 지운다."""
    with settings.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM mart_publish_runs WHERE pipeline_name = %s", (pipeline_name,)
        )
        connection.execute("DELETE FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,))
        connection.commit()
