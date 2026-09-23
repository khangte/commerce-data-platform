"""Build-then-swap 방식으로 검증된 Warehouse 파일만 Published 경로에 교체한다."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path

import duckdb

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.ingestion.catalog import sync_bronze_catalog
from src.ingestion.errors import classify_error
from src.warehouse.dbt_runner import DbtRunResult, run_dbt_build
from src.warehouse.errors import (
    DBT_BUILD_ERROR,
    UNKNOWN_ERROR,
    PublishedWalError,
    PublishStateError,
    WarehouseBuildError,
)
from src.warehouse.mart_hash import (
    MART_HASH_TARGETS,
    MartTarget,
    mart_logical_hashes,
    mart_row_counts,
    mismatched_relations,
    target_for,
)
from src.warehouse.publish_metadata import (
    PUBLISHING,
    PublishRecord,
    PublishRun,
    ensure_publish_metadata,
    get_publish_run,
    mark_failed,
    mark_published,
    mark_publishing,
    record_dbt_result,
    stale_active_runs,
    start_publish_run,
)

DEFAULT_WAREHOUSE_ROOT = PROJECT_ROOT / "data" / "warehouse"
PUBLISHED_FILE_NAME = "warehouse.duckdb"
FAILED_BUILD_RETENTION = 3
PUBLISH_STALE_AFTER = timedelta(hours=1)
ABANDONED_MESSAGE = "abandoned"
FAILED_NODE_LIMIT = 20

DbtRunner = Callable[[Path, Path], DbtRunResult]


@dataclass(frozen=True)
class WarehousePaths:
    """Published·Build·Failed Warehouse 파일 위치를 한곳에서 정한다."""

    published: Path
    build_dir: Path
    failed_dir: Path

    @classmethod
    def under(cls, root: Path) -> WarehousePaths:
        """Warehouse Root 아래 Spec 기본 Directory 구조를 만든다."""
        return cls(root / PUBLISHED_FILE_NAME, root / "build", root / "failed")

    @property
    def published_wal(self) -> Path:
        """Published 파일의 WAL 경로를 반환한다."""
        return _wal_path(self.published)

    def build_file(self, publish_run_id: uuid.UUID) -> Path:
        """Publish Run의 Build 파일 경로를 반환한다."""
        return self.build_dir / f"{publish_run_id}.duckdb"

    def failed_file(self, publish_run_id: uuid.UUID) -> Path:
        """Publish Run의 실패 Build 격리 경로를 반환한다."""
        return self.failed_dir / f"{publish_run_id}.duckdb"

    def target_dir(self, publish_run_id: uuid.UUID) -> Path:
        """Publish Run 전용 dbt --target-path를 반환한다."""
        return self.build_dir / f"{publish_run_id}-target"


@dataclass(frozen=True)
class PublishOutcome:
    """Publish 성공 결과와 직전 Publish 대비 바뀐 Relation을 담는다."""

    publish_run_id: uuid.UUID
    previous_publish_run_id: uuid.UUID | None
    mart_hashes: dict[str, str]
    mart_row_counts: dict[str, int]
    changed_relations: tuple[str, ...]


def recover_incomplete_publishes(
    settings: PostgresSettings, paths: WarehousePaths, *, now: datetime
) -> tuple[uuid.UUID, ...]:
    """1시간 넘게 활성 상태인 Run을 PUBLISHED 확정 또는 abandoned FAILED로 닫는다."""
    recovered: list[uuid.UUID] = []
    for record in stale_active_runs(settings, older_than=now - PUBLISH_STALE_AFTER):
        if record.status == PUBLISHING and _published_matches(paths.published, record.mart_hashes):
            mark_published(settings, record.publish_run_id, now=now)
            paths.build_file(record.publish_run_id).unlink(missing_ok=True)
            shutil.rmtree(paths.target_dir(record.publish_run_id), ignore_errors=True)
        else:
            _fail(settings, paths, record.publish_run_id, UNKNOWN_ERROR, ABANDONED_MESSAGE, now)
        recovered.append(record.publish_run_id)
    return tuple(recovered)


def prepare_warehouse_build(
    settings: PostgresSettings,
    paths: WarehousePaths,
    run: PublishRun,
    *,
    now: datetime | None = None,
) -> Path:
    """활성 Run을 잡고 Published 파일을 복사한 Build 파일에 Catalog를 동기화한다."""
    now = now or datetime.now(UTC)
    ensure_publish_metadata(settings)
    recover_incomplete_publishes(settings, paths, now=now)
    start_publish_run(settings, run, now=now)
    build_path = paths.build_file(run.publish_run_id)
    try:
        if paths.published_wal.exists():
            raise PublishedWalError(f"Published warehouse has a WAL: {paths.published_wal}")
        paths.build_dir.mkdir(parents=True, exist_ok=True)
        if paths.published.is_file():
            shutil.copyfile(paths.published, build_path)
        sync_bronze_catalog(settings, build_path)
    except Exception as error:
        _fail(settings, paths, run.publish_run_id, classify_error(error), str(error))
        raise
    return build_path


def build_warehouse(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    *,
    dbt_runner: DbtRunner = run_dbt_build,
) -> DbtRunResult:
    """Build 파일에 dbt build를 실행하고 실패하면 격리 후 WarehouseBuildError를 낸다."""
    try:
        result = dbt_runner(paths.build_file(publish_run_id), paths.target_dir(publish_run_id))
        record_dbt_result(
            settings,
            publish_run_id,
            invocation_id=result.invocation_id,
            tests_passed=result.tests_passed,
            tests_failed=result.tests_failed,
        )
    except Exception as error:
        _fail(settings, paths, publish_run_id, classify_error(error), str(error))
        raise
    if not result.succeeded:
        error_type = result.error_type or DBT_BUILD_ERROR
        summary = _failure_summary(error_type, result)
        _fail(settings, paths, publish_run_id, error_type, summary)
        raise WarehouseBuildError(error_type, summary)
    return result


def publish_build(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    *,
    targets: tuple[MartTarget, ...] = MART_HASH_TARGETS,
    now: datetime | None = None,
) -> PublishOutcome:
    """검증된 Build 파일을 CHECKPOINT·Hash 기록 후 Published 경로로 원자 교체한다."""
    record = get_publish_run(settings, publish_run_id)
    if record is None:
        raise PublishStateError(f"Publish run {publish_run_id} does not exist")
    build_path = paths.build_file(publish_run_id)
    try:
        _checkpoint(build_path)
        if _wal_path(build_path).exists():
            raise PublishedWalError(f"Build warehouse still has a WAL: {build_path}")
        hashes = mart_logical_hashes(build_path, targets)
        counts = mart_row_counts(build_path, targets)
        mark_publishing(settings, publish_run_id, mart_hashes=hashes, mart_row_counts=counts)
        os.replace(build_path, paths.published)
        _fsync_directory(paths.published.parent)
        mark_published(settings, publish_run_id, now=now or datetime.now(UTC))
    except Exception as error:
        if build_path.exists():
            _fail(settings, paths, publish_run_id, classify_error(error), str(error))
        raise
    shutil.rmtree(paths.target_dir(publish_run_id), ignore_errors=True)
    return PublishOutcome(
        publish_run_id=publish_run_id,
        previous_publish_run_id=record.previous_publish_run_id,
        mart_hashes=hashes,
        mart_row_counts=counts,
        changed_relations=mismatched_relations(_previous_hashes(settings, record), hashes),
    )


def publish_warehouse(
    settings: PostgresSettings,
    paths: WarehousePaths,
    run: PublishRun,
    *,
    dbt_runner: DbtRunner = run_dbt_build,
    targets: tuple[MartTarget, ...] = MART_HASH_TARGETS,
) -> PublishOutcome:
    """준비·dbt build·Publish를 한 Process에서 순서대로 실행한다."""
    prepare_warehouse_build(settings, paths, run)
    build_warehouse(settings, paths, run.publish_run_id, dbt_runner=dbt_runner)
    return publish_build(settings, paths, run.publish_run_id, targets=targets)


def _previous_hashes(settings: PostgresSettings, record: PublishRecord) -> dict[str, str]:
    """직전 PUBLISHED Run의 Hash를 반환하고, 없으면 빈 Dict를 반환한다."""
    if record.previous_publish_run_id is None:
        return {}
    previous = get_publish_run(settings, record.previous_publish_run_id)
    return {} if previous is None else dict(previous.mart_hashes or {})


def _published_matches(published: Path, recorded: dict[str, str] | None) -> bool:
    """Published 파일 Hash가 기록된 Hash와 같으면 True다."""
    if not recorded or not published.is_file():
        return False
    try:
        targets = tuple(target_for(relation) for relation in recorded)
        return mart_logical_hashes(published, targets) == recorded
    except (KeyError, duckdb.Error):
        return False


def _fail(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    error_type: str,
    message: str,
    now: datetime | None = None,
) -> None:
    """Build를 격리하고 Run을 FAILED로 기록한 뒤 오래된 실패 Build를 정리한다."""
    now = now or datetime.now(UTC)
    failed_path = _isolate_build(paths, publish_run_id, now=now)
    mark_failed(
        settings,
        publish_run_id,
        error_type=error_type,
        error_message=message or error_type,
        failed_path=None if failed_path is None else str(failed_path),
        now=now,
    )
    _prune_failed_builds(paths.failed_dir)
    shutil.rmtree(paths.target_dir(publish_run_id), ignore_errors=True)


def _isolate_build(
    paths: WarehousePaths, publish_run_id: uuid.UUID, *, now: datetime
) -> Path | None:
    """Build 파일과 WAL을 failed/로 옮기고 mtime을 격리 시각으로 맞춘다."""
    source = paths.build_file(publish_run_id)
    if not source.exists():
        return None
    paths.failed_dir.mkdir(parents=True, exist_ok=True)
    target = paths.failed_file(publish_run_id)
    os.replace(source, target)
    if _wal_path(source).exists():
        os.replace(_wal_path(source), _wal_path(target))
    os.utime(target, (now.timestamp(), now.timestamp()))
    return target


def _prune_failed_builds(failed_dir: Path, keep: int = FAILED_BUILD_RETENTION) -> None:
    """mtime 기준 최신 keep개만 남기고 오래된 실패 Build와 WAL을 지운다."""
    if not failed_dir.is_dir():
        return
    builds = sorted(
        failed_dir.glob("*.duckdb"), key=lambda path: path.stat().st_mtime, reverse=True
    )
    for stale in builds[keep:]:
        stale.unlink(missing_ok=True)
        _wal_path(stale).unlink(missing_ok=True)


def _checkpoint(warehouse_path: Path) -> None:
    """WAL 내용을 본 파일에 반영하고 연결을 닫는다."""
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CHECKPOINT")


def _fsync_directory(directory: Path) -> None:
    """Rename 결과가 Disk에 남도록 Directory를 fsync한다."""
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _wal_path(warehouse_path: Path) -> Path:
    """DuckDB 파일의 WAL 경로를 반환한다."""
    return warehouse_path.with_name(f"{warehouse_path.name}.wal")


def _failure_summary(error_type: str, result: DbtRunResult) -> str:
    """실패 Node 목록과 dbt 출력 끝부분으로 오류 메시지를 만든다."""
    nodes = ", ".join(result.failed_nodes[:FAILED_NODE_LIMIT]) or "none"
    return f"{error_type}: failed nodes: {nodes}\n{result.output}"


def main(argv: list[str] | None = None) -> int:
    """Publish 또는 복구를 실행하고 결과를 JSON 한 줄로 출력한다."""
    parser = argparse.ArgumentParser(description="Build, test and publish the mart warehouse.")
    parser.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE_ROOT)
    parser.add_argument("--pipeline-name", default="manual_publish")
    parser.add_argument("--dbt-vars", help="YAML/JSON string passed to dbt --vars")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="dbt build에 --full-refresh를 전달한다",
    )
    parser.add_argument("--recover-only", action="store_true")
    args = parser.parse_args(argv)

    settings = PostgresSettings.from_environment()
    paths = WarehousePaths.under(args.warehouse_root)
    if args.recover_only:
        ensure_publish_metadata(settings)
        recovered = recover_incomplete_publishes(settings, paths, now=datetime.now(UTC))
        print(json.dumps({"recovered": [str(run_id) for run_id in recovered]}))
        return 0

    extra_args: list[str] = []
    if args.full_refresh:
        extra_args.append("--full-refresh")
    if args.dbt_vars is not None:
        extra_args.extend(("--vars", args.dbt_vars))
    runner: DbtRunner = run_dbt_build
    if extra_args:
        runner = partial(run_dbt_build, extra_args=tuple(extra_args))
    run = PublishRun(uuid.uuid4(), args.pipeline_name)
    try:
        outcome = publish_warehouse(settings, paths, run, dbt_runner=runner)
    except WarehouseBuildError as error:
        print(
            json.dumps(
                {
                    "publish_run_id": str(run.publish_run_id),
                    "status": "FAILED",
                    "error_type": error.error_type,
                }
            )
        )
        return 1
    print(
        json.dumps(
            {
                "publish_run_id": str(outcome.publish_run_id),
                "status": "PUBLISHED",
                "previous_publish_run_id": (
                    None
                    if outcome.previous_publish_run_id is None
                    else str(outcome.previous_publish_run_id)
                ),
                "changed_relations": list(outcome.changed_relations),
                "mart_row_counts": outcome.mart_row_counts,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
