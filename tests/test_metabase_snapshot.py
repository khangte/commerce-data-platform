"""Collection 기준 Snapshot과 쓰기 보호를 검증한다."""

import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest


@pytest.fixture
def snapshot(monkeypatch):
    """Snapshot 모듈과 가짜 인증 설정을 준비한다."""
    spec = importlib.util.spec_from_file_location("snapshot", Path(__file__).parents[1] / "scripts/metabase_snapshot.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "require_metabase_setting", lambda *args: "snapshot-test-secret")
    monkeypatch.setattr(module, "host_metabase_url", lambda: "http://localhost:3000")
    monkeypatch.setattr(module, "has_uncommitted_changes", lambda path: False)
    return module


@pytest.fixture
def transport(snapshot, monkeypatch):
    """여러 모델과 중복 대상을 포함한 응답을 준비한다."""
    def respond(request, timeout):
        """주소에 맞는 JSON 응답을 반환한다."""
        url = request.full_url
        if "/collection/5/" in url:
            result = {"data": [{"model": "metric", "id": 40}, {"model": "card", "id": 43}, {"model": "dataset", "id": 40}]}
        elif "/collection/6/" in url:
            result = {"data": [{"model": "card", "id": 46}, {"model": "dashboard", "id": 2}, {"model": "collection", "id": 9}]}
        elif "/card/" in url:
            result = {"id": int(url.rsplit("/", 1)[1]), "dataset_query": {"database": 2, "stages": []}, "view_count": 7, "result_metadata": []}
        else:
            result = {"id": 2, "dashcards": [{"id": 8, "card_id": 46, "card": {}}, {"id": 3, "card_id": 40, "card": {}}]}
        return io.BytesIO(json.dumps(result).encode())
    mock = Mock(side_effect=respond)
    monkeypatch.setattr(snapshot, "urlopen", mock)
    return mock


def test_snapshot_collects_cards_metrics_and_dashboards_from_collections(snapshot, transport, tmp_path):
    """지원 모델을 중복 없이 모으고 배포 필드만 저장한다."""
    out = tmp_path / "export"
    assert snapshot.main(["--output-dir", str(out)]) == 0
    assert sorted(p.name for p in out.iterdir()) == ["card-40.json", "card-43.json", "card-46.json", "dashboard-2.json"]
    saved = json.loads((out / "card-40.json").read_text())
    assert "view_count" not in saved and "result_metadata" not in saved
    assert saved["dataset_query"] == {"database": 2, "stages": []}
    dash = json.loads((out / "dashboard-2.json").read_text())
    assert [d["id"] for d in dash["dashcards"]] == [3, 8]
    assert all("card" not in d for d in dash["dashcards"])
    assert not any("/collection/9" in call.args[0].full_url for call in transport.call_args_list)


def test_snapshot_refuses_dirty_output_without_force(snapshot, transport, tmp_path, monkeypatch, capsys):
    """변경된 출력은 HTTP 전에 거부하고 명시적 강제 실행을 허용한다."""
    monkeypatch.setattr(snapshot, "has_uncommitted_changes", lambda path: True)
    args = ["--output-dir", str(tmp_path / "export")]
    assert snapshot.main(args) == 1
    transport.assert_not_called()
    assert "--force" in capsys.readouterr().err
    assert snapshot.main([*args, "--force"]) == 0


def test_snapshot_writes_nothing_when_any_get_fails(snapshot, transport, tmp_path, capsys):
    """마지막 조회 실패도 기존 파일을 보존한다."""
    out = tmp_path / "export"
    out.mkdir()
    (out / "card-46.json").write_text("old")
    respond = transport.side_effect
    def fail_dashboard(request, timeout):
        """카드 조회 이후 Dashboard에서 실패한다."""
        if "/dashboard/" in request.full_url:
            raise HTTPError(request.full_url, 500, "snapshot-test-secret", {}, None)
        return respond(request, timeout)
    transport.side_effect = fail_dashboard
    assert snapshot.main(["--output-dir", str(out)]) == 1
    assert (out / "card-46.json").read_text() == "old"
    assert sorted(p.name for p in out.iterdir()) == ["card-46.json"]
    captured = capsys.readouterr()
    assert "snapshot-test-secret" not in captured.out + captured.err


def test_snapshot_removes_stale_exports_only(snapshot, transport, tmp_path):
    """대상에서 빠진 JSON만 지운다."""
    for name in ("card-99.json", "dashboard-98.json", "notes.txt"):
        (tmp_path / name).write_text("old")
    assert snapshot.main(["--output-dir", str(tmp_path)]) == 0
    assert not (tmp_path / "card-99.json").exists()
    assert not (tmp_path / "dashboard-98.json").exists()
    assert (tmp_path / "notes.txt").exists()


def test_snapshot_sends_key_only_in_header(snapshot, transport, tmp_path, capsys):
    """인증 키가 주소와 출력에 포함되지 않는다."""
    assert snapshot.main(["--output-dir", str(tmp_path)]) == 0
    assert all(call.args[0].get_header("X-api-key") == "snapshot-test-secret" for call in transport.call_args_list)
    assert all("snapshot-test-secret" not in call.args[0].full_url for call in transport.call_args_list)
    captured = capsys.readouterr()
    assert "snapshot-test-secret" not in captured.out + captured.err
