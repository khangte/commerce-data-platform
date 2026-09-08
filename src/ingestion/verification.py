"""COMMITTED Bronze Batch를 Metadata·Manifest·Object 증적으로 재확인한다."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from src.common.database import PostgresSettings
from src.ingestion.batch import BatchIdentity, get_committed_table_batch
from src.ingestion.bronze import table_logical_hash_bytes
from src.ingestion.manifest import parse_bronze_manifest_payload
from src.ingestion.schema import assert_supported_schema_version
from src.ingestion.storage import (
    SeaweedFSSettings,
    read_object_bytes,
    stored_object_from_head,
    verify_parquet_object,
)
from src.ingestion.tables import table_config


class BronzeCommitVerificationError(RuntimeError):
    """Batch의 어느 Table이든 Metadata·Manifest·Object 증적이 어긋날 때 발생한다."""


@dataclass(frozen=True)
class VerifiedTableBatch:
    """재확인을 통과한 한 Table Batch의 Commit 증적이다. Bronze Object 없는 SUCCESS_NO_DATA도 포함한다."""

    source_table: str
    object_key: str | None
    row_count: int


def verify_bronze_commit(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    batch: BatchIdentity,
    source_tables: tuple[str, ...],
) -> tuple[VerifiedTableBatch, ...]:
    """지정 Table 전부가 COMMITTED이고 Manifest·Object·Hash·Row Count가 일치하는지 확인한다."""
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

        manifest_object = stored_object_from_head(storage, existing.manifest_key)
        manifest_bytes = read_object_bytes(storage, existing.manifest_key)
        if hashlib.sha256(manifest_bytes).hexdigest() != manifest_object.content_sha256:
            raise BronzeCommitVerificationError(f"{source_table} manifest checksum differs from HEAD")
        try:
            manifest = parse_bronze_manifest_payload(
                manifest_bytes, expected_object_key=existing.object_key
            )
        except ValueError as error:
            raise BronzeCommitVerificationError(f"{source_table} manifest is invalid: {error}") from error

        verified_object = verify_parquet_object(storage, stored_object_from_head(storage, existing.object_key))
        if verified_object.row_count != existing.row_count or verified_object.row_count != manifest["row_count"]:
            raise BronzeCommitVerificationError(f"{source_table} row_count differs across Metadata/Manifest/Object")
        if verified_object.content_sha256 != manifest["content_sha256"]:
            raise BronzeCommitVerificationError(f"{source_table} object checksum differs from the manifest")

        data_bytes = read_object_bytes(storage, existing.object_key)
        if table_logical_hash_bytes(data_bytes, config) != manifest["logical_hash"]:
            raise BronzeCommitVerificationError(f"{source_table} logical hash differs from the manifest")

        if existing.watermark_after.as_metadata_json() != manifest["extract_upper_bound"]:
            raise BronzeCommitVerificationError(f"{source_table} watermark differs from the manifest")

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
