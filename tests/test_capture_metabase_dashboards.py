"""Metabase Dashboard 캡처 스크립트의 재실행 계약을 검증한다."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/capture_metabase_dashboards.py"


def load_capture_module():
    """경로로 캡처 스크립트를 import한다."""
    spec = importlib.util.spec_from_file_location("capture_metabase_dashboards", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dashboard_url_uses_the_dashboard_path_without_the_api_key():
    """Dashboard URL이 비밀값을 Query String에 넣지 않는지 검증한다."""
    module = load_capture_module()

    assert module.dashboard_url("http://localhost:3000/", 2) == "http://localhost:3000/dashboard/2"


def test_dashboard_ids_match_the_documented_sales_product_customer_dashboards():
    """캡처 대상이 문서화된 세 Dashboard인지 검증한다."""
    module = load_capture_module()

    assert module.DASHBOARD_IDS == (2, 3, 4)


class FakePage:
    """가상 시계와 응답 도착·로딩 표시 소멸을 재현한다."""
    def __init__(self, responses, arrive=True):
        """첫 응답과 선택적 후속 응답을 준비한다."""
        self.responses = responses
        self.arrive = arrive
        self.waits = []
        self.now = 0
        self.polls = 0
    def wait_for_timeout(self, milliseconds):
        """두 번째 폴링에 응답을 추가하고 시간을 진행한다."""
        self.waits.append(milliseconds)
        self.now += milliseconds / 1000
        self.polls += 1
        if self.arrive and self.polls == 2:
            self.responses[11] = SimpleNamespace(json=lambda: {"status": "completed"})
    def locator(self, selector):
        """세 번째 폴링까지 로딩 표시를 유지한다."""
        return SimpleNamespace(count=lambda: int(self.polls < 3))


def test_wait_returns_after_all_queries_complete_and_spinners_gone(monkeypatch):
    """모든 카드 응답과 로딩 표시 소멸 후 안정화만 기다린다."""
    module = load_capture_module()
    responses = {10: SimpleNamespace(json=lambda: {"status": "completed"})}
    page = FakePage(responses)
    monkeypatch.setattr(module.time, "monotonic", lambda: page.now)
    module.wait_for_dashboard(page, 4, {10, 11}, responses, 2000)
    assert page.polls == 4
    assert page.waits == [100, 100, 100, module.RENDER_SETTLE_MS]


def test_wait_raises_on_failed_card_query(monkeypatch):
    """HTTP 성공이어도 쿼리 실패이면 카드 식별자만 담아 중단한다."""
    module = load_capture_module()
    responses = {11: SimpleNamespace(json=lambda: {"status": "failed", "error": "capture-test-secret"})}
    page = FakePage(responses)
    monkeypatch.setattr(module.time, "monotonic", lambda: page.now)
    with pytest.raises(RuntimeError, match="Dashboard 4.*11") as error:
        module.wait_for_dashboard(page, 4, {11}, responses, 2000)
    assert "capture-test-secret" not in str(error.value)
    assert not page.waits


def test_wait_times_out_with_counts(monkeypatch):
    """응답 누락은 상한에서 수신·기대 수를 남기고 중단한다."""
    module = load_capture_module()
    responses = {10: SimpleNamespace(json=lambda: {"status": "completed"})}
    page = FakePage(responses, arrive=False)
    monkeypatch.setattr(module.time, "monotonic", lambda: page.now)
    with pytest.raises(TimeoutError, match="1/2"):
        module.wait_for_dashboard(page, 4, {10, 11}, responses, 300)
    assert page.now <= 0.3


def test_loading_indicator_timeout_shares_query_deadline(monkeypatch):
    """로딩 표시도 같은 제한 시간을 사용한다."""
    module = load_capture_module()
    responses = {10: SimpleNamespace(json=lambda: {"status": "completed"})}
    page = FakePage(responses, arrive=False)
    page.locator = lambda selector: SimpleNamespace(count=lambda: 1)
    monkeypatch.setattr(module.time, "monotonic", lambda: page.now)
    with pytest.raises(TimeoutError, match="1/1"):
        module.wait_for_dashboard(page, 4, {10}, responses, 300)
    assert page.now <= 0.3


def test_query_path_pattern_ignores_other_paths():
    """Dashboard 카드 쿼리 경로에서 두 식별자를 추출한다."""
    module = load_capture_module()
    match = module.QUERY_PATH_PATTERN.fullmatch("/api/dashboard/4/dashcard/7/card/59/query")
    assert match.groupdict() == {"dashboard": "4", "dashcard": "7"}
    assert not module.QUERY_PATH_PATTERN.fullmatch("/api/card/59/query")
    assert not module.QUERY_PATH_PATTERN.fullmatch("/api/dashboard/4")


def test_record_response_filters_dashboard_and_keeps_latest():
    """다른 화면 응답은 제외하고 같은 배치 카드의 최신 응답을 보존한다."""
    module = load_capture_module()
    responses = {}
    for url in ("http://localhost/api/dashboard/3/dashcard/7/card/59/query", "http://localhost/api/card/59/query"):
        module.record_query_response(SimpleNamespace(url=url), 4, responses)
    assert not responses
    first = SimpleNamespace(url="http://localhost/api/dashboard/4/dashcard/7/card/59/query?foo=bar")
    second = SimpleNamespace(url=first.url)
    module.record_query_response(first, 4, responses)
    module.record_query_response(second, 4, responses)
    assert responses == {7: second}


@pytest.mark.parametrize("env,expected", [("", "http://localhost:3000"), ("http://example.test:3000/", "http://example.test:3000")])
def test_default_metabase_url_falls_back_when_env_empty(monkeypatch, env, expected):
    """빈 환경 변수는 공통 기본 주소로 대체하고 끝 슬래시를 제거한다."""
    module = load_capture_module()
    monkeypatch.setenv("METABASE_URL", env)
    assert module.parse_args([]).metabase_url == expected
    assert module.parse_args(["--metabase-url", "http://explicit.test/"]).metabase_url == "http://explicit.test"


def test_expected_query_dashcards_ignores_text_and_rejects_http_error():
    """쿼리 없는 텍스트 배치를 제외하고 조회 오류를 명확히 알린다."""
    module = load_capture_module()
    response = SimpleNamespace(ok=True, json=lambda: {"dashcards": [{"id": 10, "card_id": 20}, {"id": 11, "card_id": None}]})
    context = SimpleNamespace(request=SimpleNamespace(get=lambda url: response))
    assert module.expected_query_dashcards(context, "http://localhost:3000/", 4) == {10}
    response.ok = False
    response.status = 403
    with pytest.raises(RuntimeError, match="403"):
        module.expected_query_dashcards(context, "http://localhost:3000/", 4)


def test_failed_query_saves_no_png_and_removes_response_handler(monkeypatch, tmp_path):
    """캡처 경로에서도 실패 카드의 PNG를 저장하지 않고 핸들러를 정리한다."""
    from unittest.mock import MagicMock

    module = load_capture_module()
    page = MagicMock()
    context = MagicMock()
    browser = MagicMock()
    playwright = MagicMock()
    playwright.chromium.launch.return_value = browser
    browser.new_context.return_value = context
    context.new_page.return_value = page
    manager = MagicMock()
    manager.__enter__.return_value = playwright
    monkeypatch.setattr(module, "sync_playwright", lambda: manager)
    monkeypatch.setattr(module, "DASHBOARD_IDS", (4,))
    monkeypatch.setattr(module, "expected_query_dashcards", lambda *args: {11})
    response = SimpleNamespace(
        url="http://localhost:3000/api/dashboard/4/dashcard/11/card/59/query",
        json=lambda: {"status": "failed", "error": "capture-test-secret"},
    )
    def navigate(*args, **kwargs):
        """이동 중 등록된 핸들러에 실패 응답을 전달한다."""
        page.on.call_args.args[1](response)
    page.goto.side_effect = navigate
    with pytest.raises(RuntimeError, match="Dashboard 4.*11") as error:
        module.capture_dashboards("http://localhost:3000", "capture-test-secret", tmp_path, 2000)
    assert "capture-test-secret" not in str(error.value)
    page.screenshot.assert_not_called()
    page.remove_listener.assert_called_once_with("response", page.on.call_args.args[1])
    context.close.assert_called_once()
    browser.close.assert_called_once()
    assert browser.new_context.call_args.kwargs["extra_http_headers"] == {"X-API-Key": "capture-test-secret"}
    assert "capture-test-secret" not in page.goto.call_args.args[0]
