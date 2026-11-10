"""Build-then-swap 방식으로 검증된 Warehouse 파일만 Published 경로에 교체한다."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path

import yaml

from src.common.database import PostgresSettings
from src.ingestion.bronze.catalog import sync_bronze_catalog
from src.ingestion.errors import DBT_TEST_ERROR, classify_error
from src.warehouse.dbt_runner import DbtRunResult, run_dbt_build
from src.warehouse.dbt_vars import dbt_vars_json
from src.warehouse.domain_gate import attribute_failure, boundary_failures, revert_domain
from src.warehouse.duckdb_files import (
    DEFAULT_WAREHOUSE_ROOT,
    PUBLISHED_FILE_NAME,
    FileIdentity,
    checkpoint,
    file_identity,
    fsync_directory,
    wal_path,
)
from src.warehouse.errors import (
    DBT_BUILD_ERROR,
    PublishedWalError,
    PublishStateError,
    WarehouseBuildError,
)
from src.warehouse.mart_hash import (
    MART_HASH_TARGETS,
    MartTarget,
    mart_hashes_and_row_counts,
    mismatched_relations,
)
from src.warehouse.publish_heartbeat import publish_heartbeat
from src.warehouse.publish_metadata import (
    PUBLISHING,
    PublishRecord,
    PublishRun,
    close_dag_run_building,
    close_stale_run,
    ensure_publish_metadata,
    get_held_domains,
    get_publish_run,
    mark_failed,
    mark_published,
    mark_publishing,
    record_dbt_result,
    record_failed_path,
    record_held_domains,
    stale_active_runs,
    start_publish_run,
)

FAILED_BUILD_RETENTION = 3
PUBLISH_LEASE = timedelta(minutes=5)
PUBLISH_HEARTBEAT_INTERVAL = timedelta(seconds=30)
FAILED_NODE_LIMIT = 20

DbtRunner = Callable[[Path, Path], DbtRunResult]

logger = logging.getLogger(__name__)


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
        return wal_path(self.published)

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
    published_identity: FileIdentity
    held_domains: tuple[str, ...] = ()


def recover_incomplete_publishes(
    settings: PostgresSettings, paths: WarehousePaths, *, now: datetime,
    reclaim: PublishRun | None = None,
) -> tuple[uuid.UUID, ...]:
    """같은 DagRun 잔재와 만료 행을 먼저 닫고 인수한 파일만 정리한다."""
    recovered: list[uuid.UUID] = []
    if reclaim is not None and reclaim.dag_run_id is not None:
        for publish_run_id in close_dag_run_building(
            settings, pipeline_name=reclaim.pipeline_name, dag_run_id=reclaim.dag_run_id, now=now,
        ):
            _clean_failed_recovery(settings, paths, publish_run_id, now=now)
            recovered.append(publish_run_id)
    stale_before = now - PUBLISH_LEASE
    for record in stale_active_runs(settings, stale_before=stale_before):
        published = record.status == PUBLISHING and _published_identity_matches(
            paths.published, record.published_identity
        )
        if not close_stale_run(
            settings, record.publish_run_id, published=published,
            stale_before=stale_before, now=now,
        ):
            continue
        if published:
            paths.build_file(record.publish_run_id).unlink(missing_ok=True)
            shutil.rmtree(paths.target_dir(record.publish_run_id), ignore_errors=True)
        else:
            _clean_failed_recovery(settings, paths, record.publish_run_id, now=now)
        recovered.append(record.publish_run_id)
    return tuple(recovered)


def prepare_warehouse_build(
    settings: PostgresSettings,
    paths: WarehousePaths,
    run: PublishRun,
) -> Path:
    """활성 Run을 잡고 Published 파일을 복사한 Build 파일에 Catalog를 동기화한다."""
    now = datetime.now(UTC)
    ensure_publish_metadata(settings)
    recover_incomplete_publishes(settings, paths, now=now, reclaim=run)
    start_publish_run(settings, run, now=now)
    with publish_heartbeat(settings, run.publish_run_id, interval=PUBLISH_HEARTBEAT_INTERVAL):
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
    partial_publish: bool = True,
) -> DbtRunResult:
    """Build 파일에 dbt build를 실행하고 실패하면 격리 후 WarehouseBuildError를 낸다.

    partial_publish가 참이고 Test 실패가 한 도메인에 귀속되면 그 도메인만 직전 게시 값으로
    되돌려 예외 없이 반환한다. 되돌리기·재검증이 실패하면 전체 차단으로 물러난다.
    """
    held_domains: tuple[str, ...] = ()
    fallback_reason: str | None = None
    try:
        with publish_heartbeat(settings, publish_run_id, interval=PUBLISH_HEARTBEAT_INTERVAL):
            result = dbt_runner(paths.build_file(publish_run_id), paths.target_dir(publish_run_id))
            record_dbt_result(
                settings,
                publish_run_id,
                invocation_id=result.invocation_id,
                tests_passed=result.tests_passed,
                tests_failed=result.tests_failed,
                tests_warned=result.tests_warned,
            )
            domain = None
            if partial_publish and result.error_type == DBT_TEST_ERROR and result.invocation_id:
                domain = attribute_failure(paths.target_dir(publish_run_id))
            if domain is not None:
                fallback_reason = _hold_domain(settings, paths, publish_run_id, domain, result)
                held_domains = () if fallback_reason else (domain,)
    except Exception as error:
        _fail(settings, paths, publish_run_id, classify_error(error), str(error))
        raise
    if result.warned_nodes or held_domains:
        logger.warning(
            "dbt warned_nodes=%s failed_nodes=%s held_domains=%s",
            list(result.warned_nodes),
            list(result.failed_nodes) if held_domains else [],
            list(held_domains),
        )
    if held_domains:
        return result
    if not result.succeeded:
        error_type = result.error_type or DBT_BUILD_ERROR
        summary = _failure_summary(error_type, result)
        if fallback_reason:
            # 오류 메시지는 뒤가 잘리므로 물러난 사유를 dbt 출력보다 앞에 둔다.
            summary = f"{error_type}: partial publish fallback: {fallback_reason}\n{summary}"
        _fail(settings, paths, publish_run_id, error_type, summary)
        raise WarehouseBuildError(error_type, summary)
    return result


def publish_build(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    *,
    targets: tuple[MartTarget, ...] = MART_HASH_TARGETS,
) -> PublishOutcome:
    """검증된 Build 파일을 CHECKPOINT·Hash 기록 후 Published 경로로 원자 교체한다."""
    record = get_publish_run(settings, publish_run_id)
    if record is None:
        raise PublishStateError(f"Publish run {publish_run_id} does not exist")
    held_domains = get_held_domains(settings, publish_run_id)
    build_path = paths.build_file(publish_run_id)
    try:
        with publish_heartbeat(settings, publish_run_id, interval=PUBLISH_HEARTBEAT_INTERVAL):
            checkpoint(build_path)
            if wal_path(build_path).exists():
                raise PublishedWalError(f"Build warehouse still has a WAL: {build_path}")
            hashes, counts = mart_hashes_and_row_counts(build_path, targets)
            identity = file_identity(build_path)
            mark_publishing(
                settings, publish_run_id, mart_hashes=hashes, mart_row_counts=counts,
                published_identity=identity,
            )
            os.replace(build_path, paths.published)
            fsync_directory(paths.published.parent)
            mark_published(settings, publish_run_id, now=datetime.now(UTC))
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
        published_identity=identity,
        changed_relations=mismatched_relations(_previous_hashes(settings, record), hashes),
        held_domains=held_domains,
    )


def publish_warehouse(
    settings: PostgresSettings,
    paths: WarehousePaths,
    run: PublishRun,
    *,
    dbt_runner: DbtRunner = run_dbt_build,
    targets: tuple[MartTarget, ...] = MART_HASH_TARGETS,
    partial_publish: bool = True,
) -> PublishOutcome:
    """준비·dbt build·Publish를 한 Process에서 순서대로 실행한다."""
    prepare_warehouse_build(settings, paths, run)
    build_warehouse(
        settings, paths, run.publish_run_id, dbt_runner=dbt_runner, partial_publish=partial_publish,
    )
    return publish_build(settings, paths, run.publish_run_id, targets=targets)


def _previous_hashes(settings: PostgresSettings, record: PublishRecord) -> dict[str, str]:
    """직전 PUBLISHED Run의 Hash를 반환하고, 없으면 빈 Dict를 반환한다."""
    if record.previous_publish_run_id is None:
        return {}
    previous = get_publish_run(settings, record.previous_publish_run_id)
    return {} if previous is None else dict(previous.mart_hashes or {})


def _published_identity_matches(published: Path, recorded: FileIdentity | None) -> bool:
    """게시 파일과 기록된 inode·수정 시각이 모두 같을 때만 참이다."""
    return recorded is not None and published.is_file() and file_identity(published) == recorded


def _clean_failed_recovery(
    settings: PostgresSettings, paths: WarehousePaths, publish_run_id: uuid.UUID, *, now: datetime,
) -> None:
    """행 종료가 확정된 회수 실행의 파일을 격리하고 잔여물을 정리한다."""
    failed_path = _isolate_build(paths, publish_run_id, now=now)
    if failed_path is not None:
        record_failed_path(settings, publish_run_id, str(failed_path))
    _prune_failed_builds(paths.failed_dir)
    shutil.rmtree(paths.target_dir(publish_run_id), ignore_errors=True)


def _fail(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    error_type: str,
    message: str,
) -> None:
    """Build를 격리하고 Run을 FAILED로 기록한 뒤 오래된 실패 Build를 정리한다."""
    now = datetime.now(UTC)
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
    if wal_path(source).exists():
        os.replace(wal_path(source), wal_path(target))
    os.utime(target, (now.timestamp(), now.timestamp()))
    return target


def _copy_build_to_failed(paths: WarehousePaths, publish_run_id: uuid.UUID) -> Path:
    """Build 파일과 WAL을 failed/로 복사하고 mtime을 복사 시각으로 맞춘다."""
    source = paths.build_file(publish_run_id)
    target = paths.failed_file(publish_run_id)
    paths.failed_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    if wal_path(source).exists():
        shutil.copyfile(wal_path(source), wal_path(target))
    os.utime(target)
    return target


def _hold_domain(
    settings: PostgresSettings,
    paths: WarehousePaths,
    publish_run_id: uuid.UUID,
    domain: str,
    result: DbtRunResult,
) -> str | None:
    """실패 도메인을 직전 게시 값으로 되돌리고, 전체 차단해야 하면 그 사유를 반환한다.

    복사 전에 실패하면 Build가 유일한 증거이므로 남겨서 기존 격리 경로로 옮기게 한다.
    되돌리기를 시작한 뒤 실패하면 failed/의 원본을 덮지 않도록 Build를 지운다.
    """
    build_path = paths.build_file(publish_run_id)
    target_dir = paths.target_dir(publish_run_id)
    try:
        failed_path = _copy_build_to_failed(paths, publish_run_id)
        record_failed_path(settings, publish_run_id, str(failed_path))
        _prune_failed_builds(paths.failed_dir)
    except Exception as error:  # noqa: BLE001 - 어떤 실패든 전체 차단으로 물러나는 경계다.
        return f"{domain}: pre-revert copy failed: {error}"
    try:
        revert_domain(build_path, paths.published, domain, target_dir, result.invocation_id)
        failures = boundary_failures(build_path, domain, target_dir)
        if not failures:
            record_held_domains(settings, publish_run_id, [domain])
            return None
        reason = f"{domain}: boundary tests failed: {', '.join(failures[:FAILED_NODE_LIMIT])}"
    except Exception as error:  # noqa: BLE001 - 어떤 실패든 전체 차단으로 물러나는 경계다.
        reason = f"{domain}: revert failed: {error}"
    build_path.unlink(missing_ok=True)
    wal_path(build_path).unlink(missing_ok=True)
    return reason


def _prune_failed_builds(failed_dir: Path, keep: int = FAILED_BUILD_RETENTION) -> None:
    """mtime 기준 최신 keep개만 남기고 오래된 실패 Build와 WAL을 지운다."""
    if not failed_dir.is_dir():
        return
    builds = sorted(
        failed_dir.glob("*.duckdb"), key=lambda path: path.stat().st_mtime, reverse=True
    )
    for stale in builds[keep:]:
        stale.unlink(missing_ok=True)
        wal_path(stale).unlink(missing_ok=True)


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
    parser.add_argument(
        "--no-partial-publish",
        action="store_true",
        help="도메인 부분 게시를 끄고 Test 실패를 전체 차단한다",
    )
    parser.add_argument("--recover-only", action="store_true")
    args = parser.parse_args(argv)
    variables = None
    if args.dbt_vars is not None:
        try:
            variables = yaml.safe_load(args.dbt_vars)
            if not isinstance(variables, dict) or any(not isinstance(key, str) for key in variables):
                raise ValueError("--dbt-vars must be a YAML mapping with string keys")
            dbt_vars_json(variables)
        except (yaml.YAMLError, ValueError, TypeError) as error:
            parser.error(str(error))

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
    runner: DbtRunner = run_dbt_build
    if extra_args or variables is not None:
        runner = partial(run_dbt_build, extra_args=tuple(extra_args), dbt_vars=variables)
    run = PublishRun(uuid.uuid4(), args.pipeline_name)
    try:
        outcome = publish_warehouse(
            settings, paths, run, dbt_runner=runner, partial_publish=not args.no_partial_publish,
        )
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
                "held_domains": list(outcome.held_domains),
            }
        )
    )
    print(
        "Serving은 갱신되지 않았다. uv run python -m src.serving export로 "
        "Export와 Metabase 재지정을 실행한다.", file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
