"""Metabase Dashboard 화면을 API Key 인증으로 재캡처한다."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import BrowserContext, Page, sync_playwright

DASHBOARD_IDS = (2, 3, 4)
DEFAULT_METABASE_URL = "http://localhost:3000"
DEFAULT_OUTPUT_DIR = Path("docs/bi/screenshots")
DEFAULT_TIMEOUT_MS = 60_000
DASHBOARD_RENDER_WAIT_MS = 8_000


def dashboard_url(metabase_url: str, dashboard_id: int) -> str:
    """Dashboard 식별자에 대응하는 화면 URL을 만든다."""
    return urljoin(f"{metabase_url.rstrip('/')}/", f"dashboard/{dashboard_id}")


def load_api_key() -> str:
    """환경 변수 또는 로컬 .env에서 Metabase API Key를 읽는다."""
    api_key = os.environ.get("METABASE_API_KEY", "").strip()
    if api_key:
        return api_key

    env_path = Path(".env")
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("METABASE_API_KEY="):
                return line.partition("=")[2].strip()

    raise RuntimeError("METABASE_API_KEY가 필요합니다.")


def assert_api_key_access(context: BrowserContext, metabase_url: str) -> None:
    """API Key가 현재 Metabase 사용자 조회를 허용하는지 확인한다."""
    response = context.request.get(urljoin(f"{metabase_url.rstrip('/')}/", "api/user/current"))
    if not response.ok:
        raise RuntimeError(f"Metabase API Key 인증에 실패했습니다: HTTP {response.status}")


def wait_for_dashboard(page: Page, timeout_ms: int) -> None:
    """Dashboard의 기본 화면과 카드 요청이 안정화될 때까지 기다린다."""
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    page.locator("main").wait_for(state="visible", timeout=timeout_ms)
    page.wait_for_timeout(DASHBOARD_RENDER_WAIT_MS)


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
            viewport={"width": 1440, "height": 1200},
            device_scale_factor=1,
            extra_http_headers={"X-API-Key": api_key},
        )
        try:
            assert_api_key_access(context, metabase_url)
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            for dashboard_id in DASHBOARD_IDS:
                page.goto(dashboard_url(metabase_url, dashboard_id), wait_until="domcontentloaded")
                wait_for_dashboard(page, timeout_ms)
                page.screenshot(
                    path=str(output_dir / f"dashboard-{dashboard_id}.png"),
                    full_page=True,
                )
        finally:
            context.close()
            browser.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """재캡처 실행에 필요한 Command-line 인자를 해석한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metabase-url",
        default=os.environ.get("METABASE_URL", DEFAULT_METABASE_URL),
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
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """API Key 인증 후 세 Dashboard를 headless Chromium으로 캡처한다."""
    args = parse_args(argv)
    if args.timeout_ms <= 0:
        raise ValueError("--timeout-ms는 0보다 커야 합니다.")

    capture_dashboards(
        metabase_url=args.metabase_url,
        api_key=load_api_key(),
        output_dir=args.output_dir,
        timeout_ms=args.timeout_ms,
    )


if __name__ == "__main__":
    main()
