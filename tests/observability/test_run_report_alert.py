"""Run report 판정 중 Task 실패로 드러낼 코드만 고르는 alert_failures를 검증한다."""

import pytest

from src.observability.run_report import alert_failures


@pytest.mark.parametrize(
    "code", ["PUBLISH_PARTIAL:subscriptions", "DATA_STALE:orders", "PUBLISH_FAILED"]
)
def test_alert_failures_selects_publish_gate_codes(code):
    """부분 게시·도메인 지연·전체 차단 코드는 고른다."""
    assert alert_failures([code]) == [code]


@pytest.mark.parametrize(
    "code", ["DATA_STALE", "DBT_TEST_WARN", "PUBLISH_SKIPPED", "SERVING_EXPORT_FAILED"]
)
def test_alert_failures_ignores_other_codes(code):
    """주문 사건 시각 지연·경고·Build 이전 실패·내보내기 실패는 고르지 않는다."""
    assert alert_failures([code]) == []


def test_alert_failures_keeps_order_and_handles_empty():
    """섞인 목록에서 순서를 유지해 고르고 빈 목록은 빈 목록이다."""
    assert alert_failures([]) == []
    assert alert_failures(
        ["DBT_TEST_WARN", "PUBLISH_PARTIAL:orders", "DATA_STALE", "DATA_STALE:orders"]
    ) == ["PUBLISH_PARTIAL:orders", "DATA_STALE:orders"]
