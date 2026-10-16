"""서빙 내보내기·Metabase 재지정과 최신 상태 확인을 실행한다."""

import argparse
import json
import sys
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.serving.export import ServingPaths, export_serving_mart
from src.serving.freshness import check_serving_freshness
from src.serving.metabase_repoint import repoint_and_prune_serving
from src.serving.metabase_settings import (
    host_metabase_url,
    metabase_setting,
    require_metabase_setting,
)
from src.warehouse.duckdb_files import DEFAULT_WAREHOUSE_ROOT
from src.warehouse.publish_metadata import PUBLISHED, get_latest_publish_run, get_publish_run


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """내보내기와 상태 확인의 하위 명령 및 경로 인자를 해석한다."""
    parser = argparse.ArgumentParser(description="Export the serving mart or check its freshness")
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export", help="서빙 내보내기와 Metabase 재지정")
    export.add_argument("--published", type=Path, default=DEFAULT_WAREHOUSE_ROOT / "warehouse.duckdb")
    export.add_argument("--serving-root", type=Path, default=PROJECT_ROOT / "data" / "serving")
    export.add_argument("--publish-run-id", type=uuid.UUID)
    commands.add_parser("status", help="Metabase가 최신 게시 실행을 읽는지 확인")
    return parser.parse_args(argv)


def _export(arguments: argparse.Namespace) -> int:
    """게시 파일 내보내기 뒤 Metabase 재지정과 조건부 정리를 수행한다."""
    settings = PostgresSettings.from_environment()
    record = (
        get_publish_run(settings, arguments.publish_run_id)
        if arguments.publish_run_id is not None
        else get_latest_publish_run(settings)
    )
    if record is None or record.status != PUBLISHED or record.mart_hashes is None:
        raise ValueError("No published mart run with recorded hashes exists")
    if record.published_identity is None:
        raise ValueError(
            f"Publish run {record.publish_run_id} has no recorded file identity; run a new publish"
        )
    result = export_serving_mart(
        arguments.published,
        ServingPaths.under(arguments.serving_root),
        publish_run_id=record.publish_run_id,
        mart_hashes=record.mart_hashes,
        published_identity=record.published_identity,
        now=datetime.now(UTC),
    )
    status = repoint_and_prune_serving(
        str(result.export_id), host_metabase_url(), metabase_setting("METABASE_API_KEY"),
        metabase_setting("METABASE_SERVING_DATABASE_ID"), arguments.serving_root / "exports",
    )
    print(json.dumps({
        "export_id": str(result.export_id),
        "publish_run_id": str(record.publish_run_id),
        "row_counts": result.row_counts,
        "serving_path": str(result.serving_path),
        "metabase_repoint": status,
    }, ensure_ascii=False, sort_keys=True))
    if status == "SKIPPED":
        print(
            "Metabase 재지정 건너뜀: METABASE_API_KEY 또는 METABASE_SERVING_DATABASE_ID가 비어 있다. "
            "python -m src.serving status로 확인한다.", file=sys.stderr,
        )
    return 1 if status == "FAILED" else 0


def _status() -> int:
    """실제 연결의 최신 여부를 JSON과 종료 코드로 반환하고 오류 상세를 숨긴다."""
    try:
        database_id = int(require_metabase_setting("METABASE_SERVING_DATABASE_ID"))
        result = check_serving_freshness(
            PostgresSettings.from_environment(), host_metabase_url(),
            require_metabase_setting("METABASE_API_KEY"), database_id,
        )
        print(json.dumps(asdict(result) | {"fresh": result.fresh}, ensure_ascii=False, sort_keys=True))
        return 0 if result.fresh else 1
    except (RuntimeError, ValueError, HTTPError, URLError, OSError, KeyError) as error:
        if isinstance(error, HTTPError):
            detail = f"HTTP {error.code}"
        elif isinstance(error, RuntimeError):
            detail = f"{type(error).__name__}: {error}"
        else:
            detail = type(error).__name__
        print(f"Serving 상태 확인 실패: {detail}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    """서빙 CLI 하위 명령을 해당 실행 경로로 전달한다."""
    arguments = _parse_arguments(argv)
    return _export(arguments) if arguments.command == "export" else _status()
