"""Generator 실행 Metadata Schema와 실행 이력 기록을 제공한다."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import psycopg
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file
from src.generator.config import GeneratorConfig


def ensure_generator_metadata(settings: PostgresSettings) -> None:
    """Generator 실행 이력 테이블을 멱등적으로 준비한다."""
    with settings.pipeline_connection() as connection:
        apply_sql_file(connection, "sql/metadata/002_create_generator_metadata.sql")


def record_started_run(
    connection: psycopg.Connection, generator_run_id: uuid.UUID, config: GeneratorConfig
) -> None:
    """변경 Transaction 전에 실행 입력과 RUNNING 상태를 기록한다."""
    connection.execute(
        """
        INSERT INTO generator_runs (
            generator_run_id, source_snapshot_id, random_seed, logical_date, order_count,
            anomaly_profile, generator_version, started_at, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'RUNNING')
        """,
        (
            generator_run_id,
            config.source_snapshot_id,
            config.random_seed,
            config.logical_date,
            config.order_count,
            config.anomaly_profile,
            config.generator_version,
            datetime.now(UTC),
        ),
    )
    connection.commit()


def record_finished_run(
    settings: PostgresSettings,
    generator_run_id: uuid.UUID,
    *,
    status: str,
    result_counts: dict[str, int] | None = None,
    logical_content_hash: str | None = None,
    error_message: str | None = None,
) -> None:
    """성공 또는 실패한 실행의 결과와 종료 상태를 기록한다."""
    if status not in {"SUCCESS", "FAILED"}:
        raise ValueError("Generator completion status must be SUCCESS or FAILED")
    if status == "SUCCESS" and (result_counts is None or logical_content_hash is None):
        raise ValueError("Successful generator runs require result_counts and logical_content_hash")

    with settings.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE generator_runs
            SET finished_at = %s,
                result_counts = %s,
                logical_hash = %s,
                status = %s,
                error_message = %s
            WHERE generator_run_id = %s
            """,
            (
                datetime.now(UTC),
                Jsonb(result_counts) if result_counts is not None else None,
                logical_content_hash,
                status,
                error_message,
                generator_run_id,
            ),
        )
        connection.commit()
