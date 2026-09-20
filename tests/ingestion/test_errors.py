"""Error Type 분류 규칙을 검증한다."""

from __future__ import annotations

import pytest

from src.ingestion.errors import (
    CONFIGURATION_ERROR,
    DBT_BUILD_ERROR,
    DBT_TEST_ERROR,
    LEASE_UNAVAILABLE,
    UNKNOWN_ERROR,
    classify_error,
    is_retryable,
)
from src.warehouse.errors import (
    PublishedWalError,
    PublishInProgressError,
    WarehouseBuildError,
)


def test_unknown_exception_is_classified_as_unknown_error() -> None:
    """등록되지 않은 예외는 CONFIGURATION_ERROR가 아니라 UNKNOWN_ERROR로 분류된다."""
    assert classify_error(RuntimeError("boom")) == UNKNOWN_ERROR
    assert not is_retryable(RuntimeError("boom"))


@pytest.mark.parametrize("error_type", [DBT_BUILD_ERROR, DBT_TEST_ERROR])
def test_warehouse_build_error_keeps_its_own_type(error_type: str) -> None:
    """WarehouseBuildError는 생성 시 받은 Error Type을 그대로 반환한다."""
    assert classify_error(WarehouseBuildError(error_type, "failed")) == error_type


def test_publish_in_progress_is_retryable_lease_unavailable() -> None:
    """다른 Publish가 진행 중이면 재시도 가능한 LEASE_UNAVAILABLE이다."""
    error = PublishInProgressError("active publish")
    assert classify_error(error) == LEASE_UNAVAILABLE
    assert is_retryable(error)


def test_published_wal_is_configuration_error() -> None:
    """Published 파일에 WAL이 남아 있으면 운영 설정 오류로 분류한다."""
    assert classify_error(PublishedWalError("wal")) == CONFIGURATION_ERROR


def test_warehouse_build_error_rejects_empty_type() -> None:
    """빈 Error Type으로는 WarehouseBuildError를 만들 수 없다."""
    with pytest.raises(ValueError):
        WarehouseBuildError("", "failed")
