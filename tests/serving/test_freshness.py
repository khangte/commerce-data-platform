"""Metabase가 실제 읽는 내보내기와 최신 게시 실행의 일치를 검증한다."""

import json
from io import BytesIO
from types import SimpleNamespace

import pytest

from src.serving import freshness, metabase_repoint


def test_read_live_manifest_posts_query_with_header_key_only(monkeypatch) -> None:
    """조회 쿼리에는 DB만 전달하고 인증값은 HTTP 헤더에만 담는다."""
    requests = []

    def respond(request, timeout):
        """실제 요청 객체를 기록하고 매니페스트 한 행을 반환한다."""
        requests.append(request)
        return BytesIO(json.dumps({"data": {"rows": [["e1", "p1"]]}}).encode())

    monkeypatch.setattr(metabase_repoint, "urlopen", respond)
    assert metabase_repoint.read_live_serving_manifest(
        "http://mb.test/", "fake-api-credential", 2
    ) == ("e1", "p1")
    request, = requests
    assert request.full_url == "http://mb.test/api/dataset"
    assert request.get_method() == "POST"
    assert request.get_header("X-api-key") == "fake-api-credential"
    assert json.loads(request.data) == {
        "database": 2,
        "type": "native",
        "native": {"query": metabase_repoint.LIVE_MANIFEST_QUERY},
    }
    assert "fake-api-credential" not in request.full_url + request.data.decode()


def test_failed_query_status_raises(monkeypatch) -> None:
    """실패 응답의 상세 오류를 노출하지 않고 매니페스트 조회를 거부한다."""
    monkeypatch.setattr(metabase_repoint, "_request_json", lambda *args: {
        "status": "failed", "error": "Binder Error with sensitive response",
    })
    with pytest.raises(ValueError, match="Metabase serving manifest query failed") as raised:
        metabase_repoint.read_live_serving_manifest("http://mb.test", "fake", 2)
    assert "Binder" not in str(raised.value)


@pytest.mark.parametrize("rows", [None, [], [["a", "b"], ["c", "d"]], [["a"]], [["a", "b", "c"]]])
def test_manifest_row_count_must_be_one(monkeypatch, rows) -> None:
    """정확히 두 값의 한 행이 아니면 상태 비교에 사용하지 않는다."""
    monkeypatch.setattr(metabase_repoint, "_request_json", lambda *args: {"data": {"rows": rows}})
    with pytest.raises(ValueError, match="Serving manifest must have exactly one row"):
        metabase_repoint.read_live_serving_manifest("http://mb.test", "fake", 2)


@pytest.mark.parametrize("latest_id,expected", [("p1", True), ("p2", False), (None, False)])
def test_freshness_compares_live_with_latest_published(monkeypatch, latest_id, expected) -> None:
    """최신 게시가 있고 실제 연결 실행과 같을 때만 최신 상태다."""
    monkeypatch.setattr(metabase_repoint, "_request_json", lambda *args: {
        "data": {"rows": [["e1", "p1"]]},
    })
    latest = None if latest_id is None else SimpleNamespace(publish_run_id=latest_id)
    monkeypatch.setattr(freshness, "get_latest_publish_run", lambda _: latest)
    result = freshness.check_serving_freshness(object(), "http://mb.test", "fake", 2)
    assert result.database_id == 2
    assert (result.live_export_id, result.live_publish_run_id) == ("e1", "p1")
    assert result.latest_publish_run_id == latest_id
    assert result.fresh is expected
