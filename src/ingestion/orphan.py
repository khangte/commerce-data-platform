"""VERIFIED Object 중 Metadata Commit이 없는 Orphan의 탐지·보수적 재조정을 제공한다."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime

from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.ingestion.bronze import table_logical_hash_bytes
from src.ingestion.manifest import parse_bronze_manifest_payload
from src.ingestion.schema import assert_supported_schema_version
from src.ingestion.storage import (
    BRONZE_PREFIX,
    QUARANTINE_PREFIX,
    SeaweedFSSettings,
    list_object_keys,
    read_object_bytes,
    stored_object_from_head,
    verify_parquet_object,
)
from src.ingestion.tables import table_config


class OrphanReconciliationError(RuntimeError):
    """Object·Manifest·Metadata 계약이 맞지 않아 자동 재조정을 거부할 때 발생한다."""


@dataclass(frozen=True)
class OrphanCandidate:
    """Final Bronze Object와 대응 Manifest Key로 식별한 Orphan 후보다."""

    object_key: str
    manifest_key: str | None


def find_orphan_candidates(
    settings: PostgresSettings, storage: SeaweedFSSettings
) -> tuple[OrphanCandidate, ...]:
    """Storage Final Object 중 Metadata COMMITTED가 없는 후보만 찾는다."""
    with settings.pipeline_connection() as connection:
        committed = {
            row[0]
            for row in connection.execute(
                "SELECT object_key FROM bronze_objects WHERE status = 'COMMITTED'"
            ).fetchall()
        }
    keys = set(list_object_keys(storage, f"{BRONZE_PREFIX}/"))
    return tuple(
        OrphanCandidate(
            key,
            f"{key.rsplit('/', 1)[0]}/manifest.json"
            if f"{key.rsplit('/', 1)[0]}/manifest.json" in keys
            else None,
        )
        for key in sorted(keys)
        if key.endswith("/data.parquet") and key not in committed
    )


def reconcile_orphan(
    settings: PostgresSettings,
    storage: SeaweedFSSettings,
    candidate: OrphanCandidate,
    *,
    now: datetime | None = None,
) -> None:
    """모든 증적이 일치하는 Reject 0건 Orphan만 Metadata·Watermark에 안전하게 재조정한다."""
    current_time = _utc_now(now)
    if candidate.manifest_key is None:
        raise OrphanReconciliationError(
            "Orphan without a VERIFIED manifest requires manual handling"
        )
    manifest = _load_manifest(storage, candidate)
    verified = verify_parquet_object(
        storage, stored_object_from_head(storage, candidate.object_key)
    )
    if (
        verified.size != manifest["object_size"]
        or verified.content_sha256 != manifest["content_sha256"]
    ):
        raise OrphanReconciliationError(
            "Object HEAD or checksum differs from the VERIFIED manifest"
        )
    if verified.row_count != manifest["row_count"]:
        raise OrphanReconciliationError("Object row count differs from the VERIFIED manifest")
    config = table_config(manifest["source_table"])
    assert_supported_schema_version(manifest["schema_version"])
    if config.schema_version != manifest["schema_version"]:
        raise OrphanReconciliationError("Manifest schema version differs from the table contract")
    data_bytes = read_object_bytes(storage, candidate.object_key)
    if table_logical_hash_bytes(data_bytes, config) != manifest["logical_hash"]:
        raise OrphanReconciliationError("Object logical hash differs from the VERIFIED manifest")
    if _quarantine_object_exists(storage, manifest):
        raise OrphanReconciliationError("Orphans with rejects require manual reconciliation")
    table_batch_id = f"{manifest['batch_id']}__{manifest['source_table']}"
    with settings.pipeline_connection() as connection, connection.transaction():
        run = connection.execute(
            """
            SELECT status, pipeline_name, watermark_before, extract_upper_bound FROM pipeline_runs
            WHERE run_id = %s AND source_table = %s FOR UPDATE
            """,
            (manifest["run_id"], manifest["source_table"]),
        ).fetchone()
        if run is None or run[0] != "RUNNING":
            raise OrphanReconciliationError(
                "Only a crash-interrupted RUNNING pipeline run can be reconciled"
            )
        if run[2] != manifest["watermark_before"] or run[3] != manifest["extract_upper_bound"]:
            raise OrphanReconciliationError("Pipeline run range differs from the orphan manifest")
        watermark = connection.execute(
            """
            SELECT watermark_timestamp, watermark_keys, version FROM watermarks
            WHERE pipeline_name = %s AND source_table = %s FOR UPDATE
            """,
            (run[1], manifest["source_table"]),
        ).fetchone()
        if (
            watermark is None
            or _cursor_json(watermark[0], watermark[1]) != manifest["watermark_before"]
        ):
            raise OrphanReconciliationError(
                "Current watermark differs from the orphan manifest lower bound"
            )
        quarantine = connection.execute(
            "SELECT 1 FROM quarantine_batches WHERE table_batch_id = %s", (table_batch_id,)
        ).fetchone()
        if quarantine is not None:
            raise OrphanReconciliationError("Orphans with rejects require manual reconciliation")
        existing = connection.execute(
            "SELECT 1 FROM bronze_objects WHERE table_batch_id = %s OR object_key = %s",
            (table_batch_id, candidate.object_key),
        ).fetchone()
        if existing is not None:
            raise OrphanReconciliationError(
                "Bronze metadata already exists for the orphan candidate"
            )
        connection.execute(
            """
            INSERT INTO bronze_objects (
                table_batch_id, source_table, batch_id, object_key, manifest_key, schema_version,
                row_count, content_sha256, logical_hash, watermark_before, watermark_after, status, committed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'COMMITTED', %s)
            """,
            (
                table_batch_id,
                manifest["source_table"],
                manifest["batch_id"],
                candidate.object_key,
                candidate.manifest_key,
                manifest["schema_version"],
                manifest["row_count"],
                manifest["content_sha256"],
                manifest["logical_hash"],
                Jsonb(manifest["watermark_before"]),
                Jsonb(manifest["extract_upper_bound"]),
                current_time,
            ),
        )
        connection.execute(
            """
            UPDATE pipeline_runs SET finished_at = %s, rows_extracted = %s, rows_valid = %s,
                rows_rejected = 0, rows_loaded = %s, status = 'SUCCESS', error_type = NULL, error_message = NULL
            WHERE run_id = %s AND source_table = %s
            """,
            (
                current_time,
                manifest["row_count"],
                manifest["row_count"],
                manifest["row_count"],
                manifest["run_id"],
                manifest["source_table"],
            ),
        )
        updated = connection.execute(
            """
            UPDATE watermarks SET watermark_timestamp = %s, watermark_keys = %s, version = version + 1, updated_at = %s
            WHERE pipeline_name = %s AND source_table = %s AND version = %s
            """,
            (
                manifest["extract_upper_bound"]["timestamp"],
                Jsonb(manifest["extract_upper_bound"]["keys"]),
                current_time,
                run[1],
                manifest["source_table"],
                watermark[2],
            ),
        )
        if updated.rowcount != 1:
            raise OrphanReconciliationError("Watermark changed during orphan reconciliation")


def _load_manifest(storage: SeaweedFSSettings, candidate: OrphanCandidate) -> dict[str, object]:
    """VERIFIED Bronze Manifest의 재조정에 필요한 필드와 Type을 검증한다."""
    if candidate.manifest_key is None:
        raise OrphanReconciliationError("manifest key is missing")
    manifest_object = stored_object_from_head(storage, candidate.manifest_key)
    manifest_bytes = read_object_bytes(storage, candidate.manifest_key)
    if hashlib.sha256(manifest_bytes).hexdigest() != manifest_object.content_sha256:
        raise OrphanReconciliationError("manifest checksum differs from HEAD")
    try:
        return parse_bronze_manifest_payload(manifest_bytes, expected_object_key=candidate.object_key)
    except ValueError as error:
        raise OrphanReconciliationError("Orphan manifest is invalid") from error


def _quarantine_object_exists(storage: SeaweedFSSettings, manifest: dict[str, object]) -> bool:
    """Bronze보다 먼저 게시되는 같은 Batch의 Quarantine Parquet 존재 여부를 확인한다."""
    logical_date = _utc_datetime(manifest["logical_date"])
    object_key = (
        f"{QUARANTINE_PREFIX}/{manifest['source_table']}/"
        f"ingestion_date={logical_date.date().isoformat()}/batch_id={manifest['batch_id']}/records.parquet"
    )
    return object_key in set(list_object_keys(storage, object_key))


def _cursor_json(timestamp: datetime | None, keys: list[object]) -> dict[str, object]:
    """Metadata Watermark Row를 Manifest와 비교할 JSON Cursor로 바꾼다."""
    return {"timestamp": timestamp.isoformat() if timestamp is not None else None, "keys": keys}


def _utc_now(value: datetime | None) -> datetime:
    """주입된 UTC 시각 또는 현재 UTC 시각을 반환한다."""
    result = value or datetime.now(UTC)
    if result.tzinfo is None or result.utcoffset().total_seconds() != 0:
        raise ValueError("Reconciliation time must be normalized to UTC")
    return result
