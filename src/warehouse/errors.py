"""Warehouse Build·Publish 단계의 Error Type 상수와 예외를 정의한다."""

from __future__ import annotations

DBT_BUILD_ERROR = "DBT_BUILD_ERROR"
DBT_TEST_ERROR = "DBT_TEST_ERROR"
UNKNOWN_ERROR = "UNKNOWN_ERROR"


class WarehouseBuildError(RuntimeError):
    """dbt Build 또는 Test 실패를 분류된 Error Type과 함께 전달한다."""

    def __init__(self, error_type: str, message: str) -> None:
        """Error Type과 요약 메시지를 받아 예외를 만든다."""
        if not error_type:
            raise ValueError("error_type must not be empty")
        super().__init__(message)
        self.error_type = error_type


class PublishedWalError(RuntimeError):
    """Published 파일 또는 CHECKPOINT 후 Build 파일에 WAL이 남아 Publish를 거부할 때 발생한다."""


class PublishInProgressError(RuntimeError):
    """다른 활성 Publish Run이 있어 새 Run을 시작할 수 없을 때 발생한다."""


class PublishStateError(RuntimeError):
    """Publish Run 상태 전이가 기대 상태와 맞지 않을 때 발생한다."""
