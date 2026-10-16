"""Metabase 카드 가져오기의 읽기 기본값과 제한된 쓰기 계약을 검증한다."""

import importlib.util
import json
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import HTTPError, URLError

import pytest

from src.serving.freshness import ServingFreshness

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
    monkeypatch.setattr(module, "PostgresSettings", SimpleNamespace(
        from_environment=Mock(return_value=object()),
    ), raising=False)
    monkeypatch.setattr(module, "check_serving_freshness", Mock(
        return_value=ServingFreshness(2, "e1", "p1", "p1"),
    ), raising=False)
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
    ("export", "query_database"), ("live", "database_id"),
])
def test_identity_mismatch_aborts_all_writes(importer, monkeypatch, side, field, capsys):
    """뒤 카드의 식별자가 다르면 앞 카드도 쓰지 않고 전체 반영을 중단한다."""
    exports, live, transport = prepare_cards(importer, monkeypatch, (49, 50))
    card = exports[50] if side == "export" else live[50]
    if field == "query_database":
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


def test_apply_aborts_before_put_when_serving_is_stale(importer, monkeypatch, capsys):
    """모든 카드를 조회한 뒤 실제 연결이 오래됐으면 첫 쓰기 전에 중단한다."""
    _, _, transport = prepare_cards(importer, monkeypatch, (49, 50))
    importer.check_serving_freshness.return_value = ServingFreshness(
        2, "old-export", "old-published", "latest-published",
    )
    assert importer.main(["49", "50", "--apply"]) == 1
    assert [call.args[0].get_method() for call in transport.call_args_list] == ["GET", "GET"]
    error = capsys.readouterr().err
    assert "old-published" in error and "latest-published" in error and "src.serving export" in error


def test_apply_with_no_changes_still_aborts_when_serving_is_stale(importer, monkeypatch):
    """쿼리 차이가 없어도 오래된 서빙 파일을 성공으로 오인하지 않는다."""
    exports, live, transport = prepare_cards(importer, monkeypatch)
    live[49]["dataset_query"] = exports[49]["dataset_query"]
    importer.check_serving_freshness.return_value = ServingFreshness(2, "e0", "p0", "p1")
    assert importer.main(["49", "--apply"]) == 1
    importer.check_serving_freshness.assert_called_once()
    assert [call.args[0].get_method() for call in transport.call_args_list] == ["GET"]


def test_apply_checks_each_database_once(importer, monkeypatch):
    """같은 DB의 여러 카드와 중복 카드 인자도 검사 한 번으로 처리한다."""
    prepare_cards(importer, monkeypatch, (49, 50))
    assert importer.main(["50", "49", "49", "--apply"]) == 0
    importer.check_serving_freshness.assert_called_once_with(
        importer.PostgresSettings.from_environment.return_value,
        "http://metabase.test:3000", "test-secret", 2,
    )


def test_dry_run_skips_freshness_check(importer, monkeypatch):
    """기본 모의 실행은 카드 조회만 수행하고 게시 메타데이터를 요구하지 않는다."""
    prepare_cards(importer, monkeypatch)
    assert importer.main(["49"]) == 0
    importer.check_serving_freshness.assert_not_called()
    importer.PostgresSettings.from_environment.assert_not_called()


@pytest.mark.parametrize("error,expected", [
    (HTTPError("http://mb.test", 403, "test-secret", {}, None), "403"),
    (URLError("test-secret"), "연결할 수 없습니다"),
])
def test_freshness_http_failure_aborts_and_hides_credentials(
    importer, monkeypatch, capsys, error, expected,
):
    """상태 조회의 HTTP·연결 오류도 쓰기를 막고 비밀값을 출력하지 않는다."""
    _, _, transport = prepare_cards(importer, monkeypatch)
    importer.check_serving_freshness.side_effect = error
    assert importer.main(["49", "--apply"]) == 1
    assert [call.args[0].get_method() for call in transport.call_args_list] == ["GET"]
    output = capsys.readouterr()
    assert expected in output.err and "test-secret" not in output.out + output.err


def test_apply_checks_databases_in_order_before_any_put(importer, monkeypatch):
    """모든 DB를 오름차순으로 검사한 뒤에만 카드 쓰기를 시작한다."""
    exports, live, transport = prepare_cards(importer, monkeypatch, (49, 50, 51))
    for card_id in (49, 51):
        for card in (exports[card_id], live[card_id]):
            card["database_id"] = card["dataset_query"]["database"] = 3
        (importer.EXPORT_DIR / f"card-{card_id}.json").write_text(json.dumps(exports[card_id]))

    def check(settings, base, key, database_id):
        """각 DB 검사 시점에도 모든 전송이 조회뿐인지 확인한다."""
        assert [call.args[0].get_method() for call in transport.call_args_list] == ["GET"] * 3
        return ServingFreshness(database_id, "e1", "p1", "p1")

    importer.check_serving_freshness.side_effect = check
    assert importer.main(["49", "50", "51", "--apply"]) == 0
    assert [call.args[3] for call in importer.check_serving_freshness.call_args_list] == [2, 3]


def test_code_defect_in_export_is_not_swallowed(importer, monkeypatch):
    """필수 쿼리 누락은 코드 결함으로 예외를 그대로 전파한다."""
    prepare_cards(importer, monkeypatch)
    (importer.EXPORT_DIR / "card-49.json").write_text(json.dumps({"id": 49, "database_id": 2}))
    with pytest.raises(KeyError):
        importer.main(["49"])
