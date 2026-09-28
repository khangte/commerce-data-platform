"""Metabase Dashboard 캡처 스크립트의 재실행 계약을 검증한다."""

import importlib.util
from pathlib import Path

SCRIPT_PATH = Path("scripts/capture_metabase_dashboards.py")


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


def test_dashboard_capture_waits_for_card_rendering():
    """하단 카드가 골격 화면으로 저장되지 않을 만큼 렌더링 시간을 기다리는지 검증한다."""
    module = load_capture_module()

    assert module.DASHBOARD_RENDER_WAIT_MS >= 8_000
