"""Metabase Serving 재지정의 성공과 건너뜀 및 실패를 검증한다."""

from __future__ import annotations

import uuid
from http.client import IncompleteRead
from urllib.error import URLError

from src.serving.metabase_repoint import (
    metabase_repoint_summary_status,
    repoint_and_prune_serving,
    repoint_metabase_serving,
)


def test_repoint_updates_only_database_file_and_checks_manifest(monkeypatch) -> None:
    """기존 상세 설정을 보존하고 새 파일의 Export ID를 확인한다."""
    export_id = uuid.uuid4()
    calls = []

    def fake_request(url, api_key, method, payload=None):
        """Metabase 응답을 대신하고 요청 본문을 기록한다."""
        calls.append((url, api_key, method, payload))
        if method == "GET":
            return {"details": {"database_file": "/serving/mart.duckdb", "read_only": True}}
        if url.endswith("/api/dataset"):
            return {"data": {"rows": [[str(export_id)]]}}
        return {}

    monkeypatch.setattr("src.serving.metabase_repoint._request_json", fake_request)
    assert repoint_metabase_serving(str(export_id), "http://metabase:3000", "secret", "2") == "SUCCESS"
    assert calls[1][3] == {"details": {"database_file": f"/serving/exports/{export_id}.duckdb", "read_only": True}}
    assert calls[2][3]["native"]["query"] == "SELECT CAST(export_id AS VARCHAR) FROM serving_manifest"
    assert all(call[1] == "secret" for call in calls)


def test_repoint_skips_missing_configuration_and_fails_when_server_unreachable(monkeypatch) -> None:
    """설정 누락만 SKIPPED이며 설정된 서버의 접속 불능은 FAILED다."""
    assert repoint_metabase_serving(str(uuid.uuid4()), "", "secret", "2") == "SKIPPED"

    def unreachable(url, api_key, method, payload=None):
        """연결 실패를 재현한다."""
        raise URLError("connection refused")

    monkeypatch.setattr("src.serving.metabase_repoint._request_json", unreachable)
    assert repoint_metabase_serving(str(uuid.uuid4()), "http://metabase:3000", "secret", "2") == "FAILED"


def test_summary_maps_export_and_repoint_results() -> None:
    """Export 부재와 재지정 실패를 각각 NOT_REQUESTED와 FAILED로 구분한다."""
    assert metabase_repoint_summary_status(None, None) == "NOT_REQUESTED"
    assert metabase_repoint_summary_status({"export_id": "x"}, None) == "FAILED"
    assert metabase_repoint_summary_status({"export_id": "x"}, "SKIPPED") == "SKIPPED"
    assert metabase_repoint_summary_status({"export_id": "x"}, "SUCCESS") == "SUCCESS"


def test_repoint_prunes_only_after_success_or_missing_configuration(monkeypatch, tmp_path) -> None:
    """FAILED는 네 버전을 모두 남기고 SUCCESS·SKIPPED만 세 버전으로 정리한다."""
    for status in ("FAILED", "SUCCESS", "SKIPPED"):
        versions_dir = tmp_path / status
        versions_dir.mkdir()
        for index in range(4):
            path = versions_dir / f"{index}.duckdb"
            path.write_bytes(b"version")
            path.touch()

        def fake_repoint(*args, result=status):
            """각 재지정 결과를 반환한다."""
            return result

        monkeypatch.setattr("src.serving.metabase_repoint.repoint_metabase_serving", fake_repoint)
        assert repoint_and_prune_serving("x", "url", "key", "2", versions_dir) == status
        assert len(list(versions_dir.glob("*.duckdb"))) == (4 if status == "FAILED" else 3)


def test_repoint_reports_failed_on_manifest_mismatch(monkeypatch) -> None:
    """재지정 뒤 이전 Export가 보이면 FAILED로 반환한다."""
    def fake_request(url, api_key, method, payload=None):
        """이전 Manifest를 반환한다."""
        if method == "GET":
            return {"details": {"database_file": "/serving/mart.duckdb"}}
        if url.endswith("/api/dataset"):
            return {"data": {"rows": [[str(uuid.uuid4())]]}}
        return {}

    monkeypatch.setattr("src.serving.metabase_repoint._request_json", fake_request)
    assert repoint_metabase_serving(str(uuid.uuid4()), "http://metabase:3000", "secret", "2") == "FAILED"


def test_repoint_reports_failed_on_incomplete_responses(monkeypatch, capsys) -> None:
    """GET과 PUT 중 불완전한 HTTP 응답을 받으면 FAILED로 기록한다."""
    for failing_method in ("GET", "PUT"):
        def incomplete(url, api_key, method, payload=None, failed_method=failing_method):
            """선택한 요청에서 HTTP 본문 절단을 재현한다."""
            if method == failed_method:
                raise IncompleteRead(b"partial", 5)
            return {"details": {"database_file": "/serving/mart.duckdb"}}

        monkeypatch.setattr("src.serving.metabase_repoint._request_json", incomplete)
        assert repoint_metabase_serving(str(uuid.uuid4()), "http://metabase:3000", "secret", "2") == "FAILED"
        assert "요청 실패(Timeout 또는 연결 끊김)" in capsys.readouterr().out


def test_repoint_reports_failed_on_timeout(monkeypatch, capsys) -> None:
    """응답을 읽다가 제한 시간을 넘기면 FAILED로 기록한다."""
    def timeout(url, api_key, method, payload=None):
        """소켓 제한 시간 초과를 재현한다."""
        raise TimeoutError("timed out")

    monkeypatch.setattr("src.serving.metabase_repoint._request_json", timeout)
    assert repoint_metabase_serving(str(uuid.uuid4()), "http://metabase:3000", "secret", "2") == "FAILED"
    assert "요청 실패(Timeout 또는 연결 끊김)" in capsys.readouterr().out


def test_repoint_identifies_response_format_errors(monkeypatch, capsys) -> None:
    """GET과 PUT의 잘못된 응답은 설정 또는 응답 형식 오류로 기록한다."""
    for failing_method in ("GET", "PUT"):
        def invalid_response(url, api_key, method, payload=None, failed_method=failing_method):
            """선택한 요청의 응답 형식 오류를 재현한다."""
            if method == failed_method:
                raise ValueError("invalid response")
            return {"details": {"database_file": "/serving/mart.duckdb"}}

        monkeypatch.setattr("src.serving.metabase_repoint._request_json", invalid_response)
        assert repoint_metabase_serving(str(uuid.uuid4()), "http://metabase:3000", "secret", "2") == "FAILED"
        assert "설정 또는 응답 형식 오류" in capsys.readouterr().out
