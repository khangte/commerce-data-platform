"""Source 변경과 함께 커밋되는 Generator 실행 결과를 관리한다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import psycopg
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file
from src.generator.config import GeneratorConfig


@dataclass(frozen=True)
class CommittedResult:
    """Source에 커밋된 실행의 복구 가능한 결과다."""

    generator_run_id: uuid.UUID
    result_counts: dict[str, int]
    logical_content_hash: str


def ensure_generator_commits(settings: PostgresSettings) -> None:
    """Source 실행 마커 테이블을 준비한다."""
    with settings.source_connection() as connection:
        apply_sql_file(connection, "sql/source/004_create_generator_commits.sql")


def committed_result(settings: PostgresSettings, config: GeneratorConfig) -> CommittedResult | None:
    """동일 결정성 입력의 Source 커밋 결과를 조회한다."""
    with settings.source_connection() as connection:
        row = connection.execute(
            """
            SELECT generator_run_id, result_counts, logical_hash
            FROM generator_commits
            WHERE source_snapshot_id = %s AND random_seed = %s AND logical_date = %s
              AND order_count = %s AND anomaly_profile = %s AND generator_version = %s
            """,
            (
                config.source_snapshot_id, config.random_seed, config.logical_date,
                config.order_count, config.anomaly_profile, config.generator_version,
            ),
        ).fetchone()
    return None if row is None else CommittedResult(row[0], dict(row[1]), row[2])


def record_source_commit(
    connection: psycopg.Connection,
    generator_run_id: uuid.UUID,
    config: GeneratorConfig,
    result_counts: dict[str, int],
    logical_content_hash: str,
) -> None:
    """Source 변경과 같은 트랜잭션에 실행 결과를 기록한다."""
    connection.execute(
        """
        INSERT INTO generator_commits (
            generator_run_id, source_snapshot_id, random_seed, logical_date, order_count,
            anomaly_profile, generator_version, result_counts, logical_hash
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            generator_run_id, config.source_snapshot_id, config.random_seed,
            config.logical_date, config.order_count, config.anomaly_profile,
            config.generator_version, Jsonb(result_counts), logical_content_hash,
        ),
    )
