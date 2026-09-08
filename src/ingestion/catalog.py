"""Metadata의 Commit된 Bronze Object만 DuckDB File Catalog로 동기화한다."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from src.common.database import PostgresSettings
from src.ingestion.schema import assert_supported_schema_version


@dataclass(frozen=True)
class BronzeCatalogEntry:
    """DuckDB Bronze File Catalog의 한 Commit된 Object 행이다."""

    source_table: str
    object_key: str
    schema_version: int
    batch_id: str
    committed_at: object
    row_count: int
    logical_hash: str


def sync_bronze_catalog(settings: PostgresSettings, database_path: Path) -> tuple[BronzeCatalogEntry, ...]:
    """Metadata COMMITTED Object만 `control.bronze_files`에 원자적으로 다시 동기화한다."""
    entries = _committed_entries(settings)
    for entry in entries:
        assert_supported_schema_version(entry.schema_version)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS control")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS control.bronze_files (
                source_table VARCHAR NOT NULL,
                object_key VARCHAR PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                batch_id VARCHAR NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                row_count BIGINT NOT NULL,
                logical_hash VARCHAR NOT NULL
            )
            """
        )
        connection.execute("BEGIN TRANSACTION")
        connection.execute("DELETE FROM control.bronze_files")
        if entries:
            connection.executemany(
                """
                INSERT INTO control.bronze_files VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        entry.source_table,
                        entry.object_key,
                        entry.schema_version,
                        entry.batch_id,
                        entry.committed_at,
                        entry.row_count,
                        entry.logical_hash,
                    )
                    for entry in entries
                ],
            )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return entries


def _committed_entries(settings: PostgresSettings) -> tuple[BronzeCatalogEntry, ...]:
    """Metadata Source of Truth에서 Commit된 Object만 정렬해 읽는다."""
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            """
            SELECT source_table, object_key, schema_version, batch_id, committed_at, row_count, logical_hash
            FROM bronze_objects
            WHERE status = 'COMMITTED'
            ORDER BY source_table, committed_at, object_key
            """
        ).fetchall()
    entries = tuple(BronzeCatalogEntry(*row) for row in rows)
    return entries
