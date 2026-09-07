"""증분 수집 실행 설정을 환경 변수에서 읽고 검증한다."""

from __future__ import annotations

from collections.abc import Mapping

from src.common.database import environment_values

DEFAULT_INGESTION_PAGE_SIZE = 50_000


def ingestion_page_size(values: Mapping[str, str] | None = None) -> int:
    """`INGESTION_PAGE_SIZE` 또는 기본값 50,000을 양의 정수로 반환한다."""
    raw_value = (values if values is not None else environment_values()).get(
        "INGESTION_PAGE_SIZE", str(DEFAULT_INGESTION_PAGE_SIZE)
    )
    try:
        page_size = int(raw_value)
    except ValueError as error:
        raise ValueError("INGESTION_PAGE_SIZE must be an integer") from error
    if page_size <= 0:
        raise ValueError("INGESTION_PAGE_SIZE must be greater than zero")
    return page_size
