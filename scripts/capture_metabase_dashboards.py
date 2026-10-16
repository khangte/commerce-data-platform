"""Metabase Dashboard 화면을 API Key 인증으로 재캡처한다."""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from playwright.sync_api import BrowserContext, Page, Response, sync_playwright
from playwright.sync_api import Error as PlaywrightError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.serving.metabase_settings import host_metabase_url, require_metabase_setting

DASHBOARD_IDS = (2, 3, 4)
VIEWPORT = {"width": 1440, "height": 1200}
MAX_VIEWPORT_HEIGHT = 16_000
MAX_FIT_ATTEMPTS = 3
OVERFLOW_SCRIPT = """() => Math.max(0, ...Array.from(document.querySelectorAll('*'))
    .filter(e => e !== document.documentElement && e !== document.body &&
        ['auto', 'scroll'].includes(getComputedStyle(e).overflowY))
    .map(e => e.scrollHeight - e.clientHeight))"""
DEFAULT_OUTPUT_DIR = Path("docs/bi/screenshots")
DEFAULT_TIMEOUT_MS = 60_000
RENDER_SETTLE_MS = 500
LOADING_SELECTOR = '[data-testid="loading-indicator"]:visible'
QUERY_PATH_PATTERN = re.compile(
    r"/api/dashboard/(?P<dashboard>\d+)/dashcard/(?P<dashcard>\d+)/card/\d+/query$"
)


def dashboard_url(metabase_url: str, dashboard_id: int) -> str:
    """Dashboard 식별자에 대응하는 화면 URL을 만든다."""
    return urljoin(f"{metabase_url.rstrip('/')}/", f"dashboard/{dashboard_id}")


def assert_api_key_access(context: BrowserContext, metabase_url: str) -> None:
    """API Key가 현재 Metabase 사용자 조회를 허용하는지 확인한다."""
    response = context.request.get(urljoin(f"{metabase_url.rstrip('/')}/", "api/user/current"))
    if not response.ok:
        raise RuntimeError(f"Metabase API Key 인증에 실패했습니다: HTTP {response.status}")


def expected_query_dashcards(context: BrowserContext, metabase_url: str, dashboard_id: int) -> set[int]:
    """Dashboard에서 실제 쿼리를 실행하는 배치 카드 식별자를 구한다."""
    response = context.request.get(urljoin(f"{metabase_url.rstrip('/')}/", f"api/dashboard/{dashboard_id}"))
    if not response.ok:
        raise RuntimeError(f"Dashboard {dashboard_id} 조회 실패: HTTP {response.status}")
    return {card["id"] for card in response.json()["dashcards"] if card.get("card_id")}


def record_query_response(response: Response, dashboard_id: int, responses: dict[int, Response]) -> None:
    """현재 Dashboard의 배치 카드별 최신 응답만 본문을 읽지 않고 기록한다."""
    match = QUERY_PATH_PATTERN.fullmatch(urlsplit(response.url).path)
    if match and int(match["dashboard"]) == dashboard_id:
        responses[int(match["dashcard"])] = response


def remaining_dashboard_time(
    deadline: float, dashboard_id: int, expected: set[int], responses: dict[int, Response],
) -> float:
    """렌더링과 높이 조정이 공유하는 남은 시간을 확인한다."""
    remaining = (deadline - time.monotonic()) * 1000
    if remaining <= 0:
        received = len(expected.intersection(responses))
        raise TimeoutError(f"Dashboard {dashboard_id} 렌더링 시간 초과: 쿼리 응답 {received}/{len(expected)}")
    return remaining


def wait_for_loading(page: Page, remaining_ms: Callable[[], float]) -> None:
    """보이는 로딩 표시가 사라진 뒤 남은 시간 안에서 차트를 안정화한다."""
    while page.locator(LOADING_SELECTOR).count():
        page.wait_for_timeout(min(100, remaining_ms()))
    if remaining_ms() < RENDER_SETTLE_MS:
        raise TimeoutError("차트 안정화 대기 시간 초과")
    page.wait_for_timeout(RENDER_SETTLE_MS)
    remaining_ms()


def wait_for_dashboard(
    page: Page, dashboard_id: int, expected: set[int], responses: dict[int, Response], timeout_ms: int,
    *, remaining_ms: Callable[[], float] | None = None,
) -> None:
    """쿼리 완료와 보이는 로딩 표시 소멸을 같은 제한 시간 안에 확인한다."""
    if remaining_ms is None:
        remaining_ms = partial(
            remaining_dashboard_time, time.monotonic() + timeout_ms / 1000,
            dashboard_id, expected, responses,
        )
    while not expected.issubset(responses):
        page.wait_for_timeout(min(100, remaining_ms()))
    for dashcard_id in sorted(expected):
        remaining_ms()
        try:
            status = responses[dashcard_id].json()["status"]
        except (PlaywrightError, ValueError, KeyError, TypeError):
            raise RuntimeError(f"Dashboard {dashboard_id} dashcard {dashcard_id} 쿼리 응답 확인 실패") from None
        if status != "completed":
            raise RuntimeError(f"Dashboard {dashboard_id} dashcard {dashcard_id} 쿼리 실패")
    wait_for_loading(page, remaining_ms)


def fit_viewport_to_content(
    page: Page, dashboard_id: int, remaining_ms: Callable[[], float],
) -> int:
    """내부 스크롤 초과 높이만큼 화면을 늘리고 잘림이 남으면 중단한다."""
    for _ in range(MAX_FIT_ATTEMPTS):
        remaining_ms()
        overflow = page.evaluate(OVERFLOW_SCRIPT)
        if overflow <= 0:
            return page.viewport_size["height"]
        height = page.viewport_size["height"] + overflow
        if height > MAX_VIEWPORT_HEIGHT:
            raise RuntimeError(f"Dashboard {dashboard_id} 필요한 높이 {height}px가 최대 {MAX_VIEWPORT_HEIGHT}px를 초과함")
        page.set_viewport_size({"width": VIEWPORT["width"], "height": height})
        wait_for_loading(page, remaining_ms)
    remaining_ms()
    overflow = page.evaluate(OVERFLOW_SCRIPT)
    if overflow > 0:
        raise RuntimeError(f"Dashboard {dashboard_id} 하단 잘림 해소 실패: 남은 높이 {overflow}px")
    return page.viewport_size["height"]


def capture_dashboards(
    metabase_url: str,
    api_key: str,
    output_dir: Path,
    timeout_ms: int,
) -> None:
    """Sales·Product·Customer Dashboard PNG를 지정 경로에 기록한다."""
    output_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            viewport=VIEWPORT,
            device_scale_factor=1,
            extra_http_headers={"X-API-Key": api_key},
        )
        try:
            context.set_default_timeout(timeout_ms)
            assert_api_key_access(context, metabase_url)
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            for dashboard_id in DASHBOARD_IDS:
                started = time.monotonic()
                deadline = started + timeout_ms / 1000
                responses = {}

                collect = partial(record_query_response, dashboard_id=dashboard_id, responses=responses)

                page.on("response", collect)
                try:
                    page.set_viewport_size(VIEWPORT)
                    expected = expected_query_dashcards(context, metabase_url, dashboard_id)
                    remaining_ms = partial(remaining_dashboard_time, deadline, dashboard_id, expected, responses)
                    page.goto(dashboard_url(metabase_url, dashboard_id), wait_until="domcontentloaded", timeout=remaining_ms())
                    page.locator("main").wait_for(state="visible", timeout=remaining_ms())
                    wait_for_dashboard(page, dashboard_id, expected, responses, timeout_ms, remaining_ms=remaining_ms)
                    height = fit_viewport_to_content(page, dashboard_id, remaining_ms)
                    page.screenshot(
                        path=str(output_dir / f"dashboard-{dashboard_id}.png"),
                        full_page=True,
                        timeout=remaining_ms(),
                    )
                    print(f"dashboard-{dashboard_id}.png {time.monotonic() - started:.1f}s {height}px")
                finally:
                    page.remove_listener("response", collect)
        except PlaywrightError:
            raise RuntimeError("Metabase 화면 요청 또는 캡처에 실패했습니다.") from None
        finally:
            context.close()
            browser.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """재캡처 실행에 필요한 Command-line 인자를 해석한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metabase-url",
        default=host_metabase_url(),
        help="Metabase base URL (기본값: METABASE_URL 또는 http://localhost:3000)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="PNG 출력 디렉터리 (기본값: docs/bi/screenshots)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
        help="화면 대기 제한 시간 밀리초 (기본값: 60000)",
    )
    args = parser.parse_args(argv)
    args.metabase_url = args.metabase_url.rstrip("/")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    """API Key 인증 후 세 Dashboard를 headless Chromium으로 캡처한다."""
    args = parse_args(argv)
    if args.timeout_ms <= 0:
        raise ValueError("--timeout-ms는 0보다 커야 합니다.")

    capture_dashboards(
        metabase_url=args.metabase_url,
        api_key=require_metabase_setting("METABASE_API_KEY", PROJECT_ROOT / ".env"),
        output_dir=args.output_dir,
        timeout_ms=args.timeout_ms,
    )


if __name__ == "__main__":
    main()
