"""Published Warehouse에서 Mart 전용 DuckDB Serving 파일을 원자적으로 만든다."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.warehouse.publish import DEFAULT_WAREHOUSE_ROOT, _fsync_directory, _wal_path
from src.warehouse.publish_metadata import PUBLISHED, get_latest_publish_run, get_publish_run

SERVING_FILE_NAME = "mart.duckdb"
SERVING_SCHEMAS = ("dimensions", "facts", "metrics")
STORAGE_VERSION = "v1.0.0"


@dataclass(frozen=True)
class ServingPaths:
    """Serving Published·Build 파일 위치를 한곳에서 정한다."""

    serving: Path
    build_dir: Path

    @classmethod
    def under(cls, root: Path) -> ServingPaths:
        """Serving Root 아래 Spec 기본 Directory 구조를 만든다."""
        return cls(root / SERVING_FILE_NAME, root / "build")

    def build_file(self, export_id: uuid.UUID) -> Path:
        """Serving Export 한 건의 임시 Build 파일 경로를 반환한다."""
        return self.build_dir / f"{export_id}.duckdb"


@dataclass(frozen=True)
class ServingExportResult:
    """Serving Export가 만든 파일과 Publish 증적을 담는다."""

    export_id: uuid.UUID
    row_counts: dict[str, int]
    serving_path: Path


def export_serving_mart(
    published: Path,
    paths: ServingPaths,
    *,
    publish_run_id: uuid.UUID,
    mart_hashes: Mapping[str, str],
    storage_version: str = STORAGE_VERSION,
    now: datetime,
) -> ServingExportResult:
    """Published Mart만 복사해 Serving 파일을 원자 교체하고 Publish Hash를 기록한다."""
    if _wal_path(published).exists():
        raise ValueError(f"Published warehouse has a WAL: {_wal_path(published)}")
    if not published.is_file():
        raise FileNotFoundError(f"Published warehouse does not exist: {published}")

    export_id = uuid.uuid4()
    build_path = paths.build_file(export_id)
    paths.build_dir.mkdir(parents=True, exist_ok=True)
    versioned_path: Path | None = None
    try:
        row_counts = _copy_mart_objects(published, build_path, storage_version)
        _record_manifest(
            build_path,
            export_id=export_id,
            publish_run_id=publish_run_id,
            mart_hashes=mart_hashes,
            row_counts=row_counts,
            storage_version=storage_version,
            now=now,
        )
        _validate_serving_file(build_path)
        _checkpoint(build_path)
        if _wal_path(build_path).exists():
            raise ValueError(f"Serving build has a WAL: {_wal_path(build_path)}")
        paths.serving.parent.mkdir(parents=True, exist_ok=True)
        versions_dir = paths.serving.parent / "exports"
        versions_dir.mkdir(parents=True, exist_ok=True)
        versioned_path = versions_dir / f"{export_id}.duckdb"
        os.link(build_path, versioned_path)
        os.replace(build_path, paths.serving)
        _fsync_directory(versions_dir)
        _fsync_directory(paths.serving.parent)
    except Exception:
        build_path.unlink(missing_ok=True)
        _wal_path(build_path).unlink(missing_ok=True)
        if versioned_path is not None:
            versioned_path.unlink(missing_ok=True)
        raise
    return ServingExportResult(export_id=export_id, row_counts=row_counts, serving_path=paths.serving)


def prune_serving_exports(versions_dir: Path, keep: int = 3) -> None:
    """재지정 결과를 확인한 뒤 오래된 Export 버전 파일을 정리한다."""
    versions = sorted(
        versions_dir.glob("*.duckdb"), key=lambda path: path.stat().st_mtime_ns, reverse=True
    )
    for old_version in versions[keep:]:
        old_version.unlink()
    _fsync_directory(versions_dir)


def _copy_mart_objects(published: Path, build_path: Path, storage_version: str) -> dict[str, int]:
    """Published 파일의 Mart Table·View를 독립된 Serving Table로 복사한다."""
    row_counts: dict[str, int] = {}
    with duckdb.connect() as connection:
        connection.execute(
            f"ATTACH {_sql_string(build_path)} AS serving (STORAGE_VERSION {_sql_string(storage_version)})"
        )
        connection.execute(f"ATTACH {_sql_string(published)} AS published (READ_ONLY)")
        for schema in SERVING_SCHEMAS:
            connection.execute(f"CREATE SCHEMA serving.{_identifier(schema)}")
            objects = connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = 'published' AND table_schema = ?
                ORDER BY table_name
                """,
                [schema],
            ).fetchall()
            for (name,) in objects:
                source = f"published.{_identifier(schema)}.{_identifier(name)}"
                target = f"serving.{_identifier(schema)}.{_identifier(name)}"
                connection.execute(f"CREATE TABLE {target} AS SELECT * FROM {source}")
                row_counts[f"{schema}.{name}"] = connection.execute(
                    f"SELECT count(*) FROM {target}"
                ).fetchone()[0]
        connection.execute("DETACH published")
        connection.execute("DETACH serving")
    return row_counts


def _record_manifest(
    build_path: Path,
    *,
    export_id: uuid.UUID,
    publish_run_id: uuid.UUID,
    mart_hashes: Mapping[str, str],
    row_counts: Mapping[str, int],
    storage_version: str,
    now: datetime,
) -> None:
    """Serving 파일 자체에 Export와 Publish의 추적 증적을 한 행으로 남긴다."""
    with duckdb.connect(str(build_path)) as connection:
        connection.execute(
            """
            CREATE TABLE serving_manifest (
                export_id UUID NOT NULL,
                publish_run_id UUID NOT NULL,
                exported_at TIMESTAMPTZ NOT NULL,
                storage_version VARCHAR NOT NULL,
                mart_result_hashes JSON NOT NULL,
                row_counts JSON NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO serving_manifest
            VALUES (?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON))
            """,
            [
                export_id,
                publish_run_id,
                now.astimezone(UTC),
                storage_version,
                json.dumps(dict(mart_hashes), ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                json.dumps(dict(row_counts), ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            ],
        )


def _validate_serving_file(build_path: Path) -> None:
    """Serving 파일에 Mart 외 Schema가 새지 않았는지 확인한다."""
    with duckdb.connect(str(build_path), read_only=True) as connection:
        schemas = {
            row[0]
            for row in connection.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name IN ('dimensions', 'facts', 'metrics', 'control', 'staging', 'intermediate')
                """
            ).fetchall()
        }
        if schemas != set(SERVING_SCHEMAS):
            raise ValueError(f"Serving schema boundary is invalid: {sorted(schemas)}")
        manifest = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()
        if manifest != [("serving_manifest",)]:
            raise ValueError(f"Serving main schema is invalid: {manifest}")


def _checkpoint(path: Path) -> None:
    """Serving Build의 WAL을 본 파일에 반영하고 연결을 닫는다."""
    with duckdb.connect(str(path)) as connection:
        connection.execute("CHECKPOINT")


def _identifier(value: str) -> str:
    """DuckDB 식별자를 SQL에 안전하게 넣는다."""
    return '"' + value.replace('"', '""') + '"'


def _sql_string(path_or_value: Path | str) -> str:
    """파일 경로 또는 값을 DuckDB 문자열 Literal로 안전하게 넣는다."""
    return "'" + str(path_or_value).replace("'", "''") + "'"


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """Serving Export CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="Export the published Mart as a serving DuckDB file")
    parser.add_argument("export", nargs="?", default="export")
    parser.add_argument("--published", type=Path, default=DEFAULT_WAREHOUSE_ROOT / "warehouse.duckdb")
    parser.add_argument("--serving-root", type=Path, default=PROJECT_ROOT / "data" / "serving")
    parser.add_argument("--publish-run-id", type=uuid.UUID)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI로 최신 Published Run의 Mart를 Serving 파일로 내보낸다."""
    arguments = _parse_arguments(argv)
    settings = PostgresSettings.from_environment()
    record = (
        get_publish_run(settings, arguments.publish_run_id)
        if arguments.publish_run_id is not None
        else get_latest_publish_run(settings)
    )
    if record is None or record.status != PUBLISHED or record.mart_hashes is None:
        raise ValueError("No published mart run with recorded hashes exists")
    result = export_serving_mart(
        arguments.published,
        ServingPaths.under(arguments.serving_root),
        publish_run_id=record.publish_run_id,
        mart_hashes=record.mart_hashes,
        now=datetime.now(UTC),
    )
    print(
        json.dumps(
            {
                "export_id": str(result.export_id),
                "publish_run_id": str(record.publish_run_id),
                "row_counts": result.row_counts,
                "serving_path": str(result.serving_path),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0
