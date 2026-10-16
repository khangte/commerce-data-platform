"""Metabase 카드 가져오기의 읽기 기본값과 제한된 쓰기 계약을 검증한다."""

import importlib.util
import json
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/metabase_import.py"


@pytest.fixture
def importer(tmp_path, monkeypatch):
    """격리된 Export와 모킹한 HTTP 전송을 쓰는 스크립트를 준비한다."""
    spec = importlib.util.spec_from_file_location("metabase_import", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(module, "EXPORT_DIR", tmp_path / "export")
    module.EXPORT_DIR.mkdir()
    monkeypatch.setenv("METABASE_API_KEY", "test-secret")
    monkeypatch.setenv("METABASE_URL", "http://metabase.test:3000/")
    return module


def prepare_cards(importer, monkeypatch, ids=(49,)):
    """서로 다른 SQL과 설명을 가진 저장·운영 카드를 준비하고 HTTP를 가로챈다."""
    exports = {}
    live = {}
    for card_id in ids:
        exports[card_id] = {
            "id": card_id,
            "database_id": 2,
            "description": "배송 완료만 집계",
            "dataset_query": {"database": 2, "native": {"query": "SELECT 'DELIVERED'"}},
        }
        live[card_id] = deepcopy(exports[card_id])
        live[card_id]["description"] = "운영 설명"
        live[card_id]["dataset_query"]["native"]["query"] = "SELECT 'ALL'"
        (importer.EXPORT_DIR / f"card-{card_id}.json").write_text(
            json.dumps(exports[card_id], ensure_ascii=False)
        )

    def respond(request, timeout):
        """GET에는 운영 카드를, PUT에는 요청한 카드 응답을 돌려준다."""
        card_id = int(request.full_url.rsplit("/", 1)[1])
        return BytesIO(json.dumps(live[card_id]).encode())

    transport = Mock(side_effect=respond)
    monkeypatch.setattr(importer, "urlopen", transport)
    return exports, live, transport


def test_default_dry_run_only_gets_and_prints_query_diff(importer, monkeypatch, capsys):
    """여러 카드를 GET만 하며 실제로 바꿀 쿼리의 차이만 출력한다."""
    _, _, transport = prepare_cards(importer, monkeypatch, (49, 50, 51))
    assert importer.main(["49", "50", "51"]) == 0
    requests = [call.args[0] for call in transport.call_args_list]
    assert [request.get_method() for request in requests] == ["GET"] * 3
    assert [request.full_url for request in requests] == [
        f"http://metabase.test:3000/api/card/{card_id}" for card_id in (49, 50, 51)
    ]
    assert all(request.data is None for request in requests)
    assert all(request.get_header("X-api-key") == "test-secret" for request in requests)
    output = capsys.readouterr().out
    assert "DELIVERED" in output and "ALL" in output and "dataset_query" in output
    assert "운영 설명" not in output and "배송 완료만 집계" not in output
    assert "test-secret" not in output


def test_apply_puts_only_dataset_query_after_all_gets(importer, monkeypatch):
    """명시적으로 요청한 쓰기는 카드 쿼리만 보내고 다른 필드를 보존한다."""
    exports, _, transport = prepare_cards(importer, monkeypatch, (49, 50))
    assert importer.main(["49", "50", "--apply"]) == 0
    requests = [call.args[0] for call in transport.call_args_list]
    assert [request.get_method() for request in requests] == ["GET", "GET", "PUT", "PUT"]
    assert [json.loads(request.data) for request in requests[2:]] == [
        {"dataset_query": exports[card_id]["dataset_query"]} for card_id in (49, 50)
    ]


@pytest.mark.parametrize("side,field", [
    ("export", "id"), ("live", "id"),
    ("export", "database_id"), ("live", "database_id"),
    ("export", "query_database"), ("live", "query_database"),
    ("live", "database"),
])
def test_identity_mismatch_aborts_all_writes(importer, monkeypatch, side, field, capsys):
    """뒤 카드의 식별자가 다르면 앞 카드도 쓰지 않고 전체 반영을 중단한다."""
    exports, live, transport = prepare_cards(importer, monkeypatch, (49, 50))
    card = exports[50] if side == "export" else live[50]
    if field == "database":
        card["database_id"] = card["dataset_query"]["database"] = 999
    elif field == "query_database":
        card["dataset_query"]["database"] = 999
    else:
        card[field] = 999
    (importer.EXPORT_DIR / "card-50.json").write_text(json.dumps(exports[50]))
    assert importer.main(["49", "50", "--apply"]) == 1
    assert all(call.args[0].get_method() == "GET" for call in transport.call_args_list)
    assert "불일치" in capsys.readouterr().err


def test_missing_key_fails_without_http(importer, monkeypatch, capsys):
    """환경 변수와 루트 .env에 키가 없으면 네트워크 요청 없이 실패한다."""
    monkeypatch.delenv("METABASE_API_KEY")
    transport = Mock()
    monkeypatch.setattr(importer, "urlopen", transport)
    assert importer.main(["49"]) == 1
    transport.assert_not_called()
    assert "METABASE_API_KEY가 필요합니다" in capsys.readouterr().err


def test_empty_env_key_uses_root_dotenv(importer, monkeypatch, tmp_path):
    """다른 작업 디렉터리에서도 루트 .env의 키를 헤더로 사용한다."""
    monkeypatch.setenv("METABASE_API_KEY", "")
    (importer.PROJECT_ROOT / ".env").write_text("METABASE_API_KEY=local-key\n")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    _, _, transport = prepare_cards(importer, monkeypatch)
    assert importer.main(["49"]) == 0
    assert transport.call_args.args[0].get_header("X-api-key") == "local-key"


def test_unchanged_query_makes_no_put(importer, monkeypatch):
    """쿼리가 같으면 설명 차이만으로 쓰기를 하지 않는다."""
    exports, live, transport = prepare_cards(importer, monkeypatch)
    live[49]["dataset_query"] = exports[49]["dataset_query"]
    assert importer.main(["49", "--apply"]) == 0
    assert [call.args[0].get_method() for call in transport.call_args_list] == ["GET"]


def test_http_error_is_clear_and_hides_credentials(importer, monkeypatch, capsys):
    """권한 오류는 HTTP 상태만 보여주고 비밀값·응답 본문을 출력하지 않는다."""
    prepare_cards(importer, monkeypatch)
    monkeypatch.setattr(importer, "urlopen", Mock(side_effect=HTTPError(
        "http://metabase.test", 403, "test-secret", {}, None,
    )))
    assert importer.main(["49"]) == 1
    output = capsys.readouterr()
    assert "403" in output.err
    assert "test-secret" not in output.err + output.out
