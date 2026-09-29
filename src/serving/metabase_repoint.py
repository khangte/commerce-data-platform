"""Metabase의 Serving 연결을 Export별 파일 경로로 재지정한다."""

from __future__ import annotations

import json
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.serving.export import prune_serving_exports


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
    except HTTPError as error:
        print(f"Metabase database request failed: HTTP {error.code}")
        return "FAILED"
    except URLError:
        print("Metabase Serving repoint failed: server unreachable")
        return "FAILED"
    except (TimeoutError, OSError, HTTPException) as error:
        print(f"Metabase database 요청 실패(Timeout 또는 연결 끊김): {type(error).__name__}")
        return "FAILED"
    except (ValueError, TypeError, KeyError) as error:
        print(f"Metabase database 설정 또는 응답 형식 오류: {type(error).__name__}")
        return "FAILED"

    try:
        details = dict(database["details"])
        details["database_file"] = f"/serving/exports/{export_id}.duckdb"
        _request_json(database_url, api_key, "PUT", {"details": details})
        result = _request_json(
            f"{base}/api/dataset",
            api_key,
            "POST",
            {
                "database": numeric_id,
                "type": "native",
                "native": {"query": "SELECT CAST(export_id AS VARCHAR) FROM serving_manifest"},
            },
        )
        rows = result["data"]["rows"]
        if rows != [[export_id]]:
            print("Metabase Serving Manifest does not match the new Export")
            return "FAILED"
    except HTTPError as error:
        print(f"Metabase Serving repoint failed: HTTP {error.code}")
        return "FAILED"
    except URLError:
        print("Metabase Serving repoint failed: server unreachable")
        return "FAILED"
    except (TimeoutError, OSError, HTTPException) as error:
        print(f"Metabase Serving 요청 실패(Timeout 또는 연결 끊김): {type(error).__name__}")
        return "FAILED"
    except (ValueError, TypeError, KeyError) as error:
        print(f"Metabase Serving 설정 또는 응답 형식 오류: {type(error).__name__}")
        return "FAILED"
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
