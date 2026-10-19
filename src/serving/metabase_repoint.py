"""Metabase의 Serving 연결을 Export별 파일 경로로 재지정한다."""

from __future__ import annotations

import json
import sys
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.warehouse.duckdb_files import fsync_directory

LIVE_MANIFEST_QUERY = (
    "SELECT CAST(export_id AS VARCHAR), CAST(publish_run_id AS VARCHAR) FROM serving_manifest"
)

_API_ERRORS = (HTTPError, URLError, OSError, HTTPException, ValueError, TypeError, KeyError)


def _failure_message(error: Exception, subject: str, action: str) -> str:
    """Metabase 요청 예외를 단계(대상·동작)별 한 줄 로그 메시지로 바꾼다."""
    if isinstance(error, HTTPError):
        return f"Metabase {subject} {action} 실패: HTTP {error.code}"
    if isinstance(error, URLError):
        return "Metabase 서빙 파일 재지정 실패: 서버에 연결할 수 없음"
    if isinstance(error, (OSError, HTTPException)):
        return f"Metabase {subject} 요청 실패(시간 초과 또는 연결 끊김): {type(error).__name__}"
    return f"Metabase {subject} 설정 또는 응답 형식 오류: {type(error).__name__}"


def _request_json(url: str, api_key: str, method: str, payload: dict | None = None) -> dict:
    """API Key를 헤더로만 보내고 Metabase JSON 응답을 읽는다."""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=data,
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        method=method,
    )
    with urlopen(request, timeout=15) as response:
        return json.load(response)


def repoint_metabase_serving(
    export_id: str, metabase_url: str, api_key: str, database_id: str
) -> str:
    """Serving 경로를 바꾸고 Manifest를 확인해 SUCCESS·SKIPPED·FAILED를 반환한다."""
    if not all((metabase_url, api_key, database_id)):
        return "SKIPPED"
    try:
        numeric_id = int(database_id)
        base = metabase_url.rstrip("/")
        database_url = f"{base}/api/database/{numeric_id}"
        database = _request_json(database_url, api_key, "GET")
    except _API_ERRORS as error:
        print(_failure_message(error, "데이터베이스", "조회"))
        return "FAILED"

    try:
        details = dict(database["details"])
        details["database_file"] = f"/serving/exports/{export_id}.duckdb"
        _request_json(database_url, api_key, "PUT", {"details": details})
        live_export_id, _ = read_live_serving_manifest(base, api_key, numeric_id)
        if live_export_id != export_id:
            print("Metabase 서빙 매니페스트의 export_id가 새 내보내기와 일치하지 않음")
            return "FAILED"
    except _API_ERRORS as error:
        print(_failure_message(error, "서빙 파일", "재지정"))
        return "FAILED"
    try:
        _request_json(f"{base}/api/database/{numeric_id}/sync_schema", api_key, "POST")
    except _API_ERRORS as error:
        detail = f"HTTP {error.code}" if isinstance(error, HTTPError) else type(error).__name__
        print(
            f"경고: Metabase 스키마 동기화 요청 실패({detail}). 재지정은 성공했고 정기 Sync가 반영한다.",
            file=sys.stderr,
        )
    return "SUCCESS"


def repoint_and_prune_serving(
    export_id: str, metabase_url: str, api_key: str, database_id: str, versions_dir: Path
) -> str:
    """재지정 성공 또는 설정 누락 때에만 오래된 버전 파일을 정리한다."""
    status = repoint_metabase_serving(export_id, metabase_url, api_key, database_id)
    if status in ("SUCCESS", "SKIPPED"):
        prune_serving_exports(versions_dir)
    return status


def metabase_repoint_summary_status(serving_export: dict | None, status: str | None) -> str:
    """Export 및 재지정 결과를 Summary 상태 코드로 매핑한다."""
    if serving_export is None:
        return "NOT_REQUESTED"
    return status or "FAILED"


SERVING_EXPORT_RETENTION = 3


def prune_serving_exports(versions_dir: Path, keep: int = SERVING_EXPORT_RETENTION) -> None:
    """재지정 결과를 확인한 뒤 오래된 Export 버전 파일을 정리한다."""
    versions = sorted(
        versions_dir.glob("*.duckdb"), key=lambda path: path.stat().st_mtime_ns, reverse=True
    )
    for old_version in versions[keep:]:
        old_version.unlink()
    fsync_directory(versions_dir)


def read_live_serving_manifest(
    metabase_url: str, api_key: str, database_id: int
) -> tuple[str, str]:
    """Metabase가 실제 읽는 서빙 매니페스트의 두 식별자를 조회한다."""
    result = _request_json(
        f"{metabase_url.rstrip('/')}/api/dataset", api_key, "POST",
        {"database": database_id, "type": "native", "native": {"query": LIVE_MANIFEST_QUERY}},
    )
    if result.get("status") == "failed":
        raise ValueError("Metabase serving manifest query failed")
    rows = result["data"]["rows"]
    if (
        not isinstance(rows, list) or len(rows) != 1
        or not isinstance(rows[0], list) or len(rows[0]) != 2
    ):
        raise ValueError("Serving manifest must have exactly one row")
    return rows[0][0], rows[0][1]
