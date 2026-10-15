"""Bronze 배치의 커밋 여부와 스키마 계약을 메타데이터로 확인한다."""

from __future__ import annotations

from dataclasses import dataclass

from src.common.database import PostgresSettings
from src.ingestion.contract.schema import assert_supported_schema_version
from src.ingestion.contract.tables import table_config
from src.ingestion.control.batch import BatchIdentity, get_committed_table_batch


class BronzeCommitVerificationError(RuntimeError):
    """테이블 배치의 커밋 메타데이터가 계약과 어긋날 때 발생한다."""


@dataclass(frozen=True)
class VerifiedTableBatch:
    """재확인을 통과한 한 Table Batch의 Commit 증적이다. Bronze Object 없는 SUCCESS_NO_DATA도 포함한다."""

    source_table: str
    object_key: str | None
    row_count: int


def verify_bronze_commit(
    postgres: PostgresSettings,
    batch: BatchIdentity,
    source_tables: tuple[str, ...],
) -> tuple[VerifiedTableBatch, ...]:
    """테이블별 COMMITTED 또는 최신 SUCCESS_NO_DATA 상태와 커밋 스키마 계약을 확인한다."""
    verified: list[VerifiedTableBatch] = []
    for source_table in source_tables:
        identity = batch.table_batch(source_table)
        existing = get_committed_table_batch(postgres, identity)
        if existing is None:
            if _latest_run_status(postgres, batch.batch_id, source_table) == "SUCCESS_NO_DATA":
                verified.append(
                    VerifiedTableBatch(source_table=source_table, object_key=None, row_count=0)
                )
                continue
            raise BronzeCommitVerificationError(
                f"{source_table} has no COMMITTED bronze object for batch {batch.batch_id}"
            )
        config = table_config(source_table)
        assert_supported_schema_version(existing.schema_version)
        if existing.schema_version != config.schema_version:
            raise BronzeCommitVerificationError(
                f"{source_table} committed schema_version differs from the table contract"
            )

        verified.append(
            VerifiedTableBatch(
                source_table=source_table, object_key=existing.object_key, row_count=existing.row_count
            )
        )
    return tuple(verified)


def _latest_run_status(postgres: PostgresSettings, batch_id: str, source_table: str) -> str | None:
    """COMMITTED Object가 없는 Table의 최신 Pipeline Run 상태를 조회한다."""
    with postgres.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT status FROM pipeline_runs
            WHERE batch_id = %s AND source_table = %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (batch_id, source_table),
        ).fetchone()
    return row[0] if row is not None else None
