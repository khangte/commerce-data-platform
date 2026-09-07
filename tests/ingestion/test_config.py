"""증분 수집 환경 설정 계약을 검증한다."""

from __future__ import annotations

import pytest

from src.ingestion.config import DEFAULT_INGESTION_PAGE_SIZE, ingestion_page_size


def test_ingestion_page_size_uses_default_when_not_configured() -> None:
    """Page Size가 없으면 계약상 기본값 50,000을 사용한다."""
    assert ingestion_page_size({}) == DEFAULT_INGESTION_PAGE_SIZE


def test_ingestion_page_size_reads_a_positive_integer() -> None:
    """환경 변수의 양의 정수 Page Size를 반환한다."""
    assert ingestion_page_size({"INGESTION_PAGE_SIZE": "2"}) == 2


@pytest.mark.parametrize("value", ("0", "-1", "not-a-number"))
def test_ingestion_page_size_rejects_invalid_values(value: str) -> None:
    """0·음수·정수가 아닌 Page Size를 거부한다."""
    with pytest.raises(ValueError, match="INGESTION_PAGE_SIZE"):
        ingestion_page_size({"INGESTION_PAGE_SIZE": value})
