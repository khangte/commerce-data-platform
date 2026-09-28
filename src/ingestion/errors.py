"""예외를 Retryable/Non-retryable Error Type으로 분류하는 단일 함수를 제공한다."""

from __future__ import annotations

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from src.generator.errors import SourceCursorRegressionError
from src.generator.lease import LeaseOwnershipLostError, LeaseUnavailableError
from src.ingestion.batch import BatchIdentityConflictError
from src.ingestion.lease import TableLeaseOwnershipLostError, TableLeaseUnavailableError
from src.ingestion.metadata import WatermarkConflictError
from src.ingestion.quarantine import RejectRateExceededError
from src.ingestion.schema import SourceContractError as BronzeSchemaContractError
from src.ingestion.validation import SourceContractError as BatchValidationContractError
from src.ingestion.verification import BronzeCommitVerificationError
from src.warehouse.errors import (
    DBT_BUILD_ERROR,
    DBT_TEST_ERROR,
    UNKNOWN_ERROR,
    PublishedWalError,
    PublishInProgressError,
    WarehouseBuildError,
)

SOURCE_CONNECTION_ERROR = "SOURCE_CONNECTION_ERROR"
LEASE_UNAVAILABLE = "LEASE_UNAVAILABLE"
CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
SOURCE_CONTRACT_ERROR = "SOURCE_CONTRACT_ERROR"
VALIDATION_THRESHOLD_EXCEEDED = "VALIDATION_THRESHOLD_EXCEEDED"
OBJECT_VERIFICATION_ERROR = "OBJECT_VERIFICATION_ERROR"
WATERMARK_CONFLICT = "WATERMARK_CONFLICT"
BATCH_IDENTITY_CONFLICT = "BATCH_IDENTITY_CONFLICT"
LEASE_OWNERSHIP_LOST = "LEASE_OWNERSHIP_LOST"
OBJECT_STORAGE_ERROR = "OBJECT_STORAGE_ERROR"
WAREHOUSE_ERROR_TYPES = frozenset({DBT_BUILD_ERROR, DBT_TEST_ERROR, UNKNOWN_ERROR})

_RETRYABLE_HTTP_STATUS = frozenset({429, *range(500, 600)})

# 문서 "재시도 가능" 목록: 일시적 연결 오류와 다른 활성 실행 종료를 기다리는 LeaseUnavailableError.
RETRYABLE_ERROR_TYPES = frozenset({SOURCE_CONNECTION_ERROR, LEASE_UNAVAILABLE})

_CONNECTION_EXCEPTION_TYPES: tuple[type[Exception], ...] = (
    psycopg.OperationalError,
    ConnectionError,
    TimeoutError,
)

_LEASE_UNAVAILABLE_EXCEPTION_TYPES: tuple[type[Exception], ...] = (
    LeaseUnavailableError,
    TableLeaseUnavailableError,
    PublishInProgressError,
)

_LEASE_OWNERSHIP_LOST_EXCEPTION_TYPES: tuple[type[Exception], ...] = (
    LeaseOwnershipLostError,
    TableLeaseOwnershipLostError,
)


def classify_error(error: Exception) -> str:
    """예외 하나를 문서 정의 Retryable/Non-retryable Error Type 문자열로 변환한다."""
    if isinstance(error, _LEASE_UNAVAILABLE_EXCEPTION_TYPES):
        return LEASE_UNAVAILABLE
    if isinstance(error, _LEASE_OWNERSHIP_LOST_EXCEPTION_TYPES):
        return LEASE_OWNERSHIP_LOST
    if isinstance(error, _CONNECTION_EXCEPTION_TYPES):
        return SOURCE_CONNECTION_ERROR
    if isinstance(error, (BronzeSchemaContractError, BatchValidationContractError, SourceCursorRegressionError)):
        return SOURCE_CONTRACT_ERROR
    if isinstance(error, RejectRateExceededError):
        return VALIDATION_THRESHOLD_EXCEEDED
    if isinstance(error, BronzeCommitVerificationError):
        return OBJECT_VERIFICATION_ERROR
    if isinstance(error, WatermarkConflictError):
        return WATERMARK_CONFLICT
    if isinstance(error, BatchIdentityConflictError):
        return BATCH_IDENTITY_CONFLICT
    if isinstance(error, WarehouseBuildError):
        return error.error_type
    if isinstance(error, PublishedWalError):
        return CONFIGURATION_ERROR
    if isinstance(error, (ClientError, BotoCoreError)):
        return OBJECT_STORAGE_ERROR
    return UNKNOWN_ERROR


def _object_storage_http_status(error: Exception) -> int | None:
    """ClientError의 HTTP 상태 코드를 방어적으로 꺼낸다."""
    response = getattr(error, "response", None) or {}
    return response.get("ResponseMetadata", {}).get("HTTPStatusCode")


def is_retryable(error: Exception) -> bool:
    """분류된 Error Type이 문서 정의 재시도 가능 목록에 속하는지 반환한다.

    `OBJECT_STORAGE_ERROR`는 예외다 — 코드 하나가 재시도 가능한 5xx/429와
    재시도 불가능한 4xx를 모두 포함하므로, `RETRYABLE_ERROR_TYPES` 소속 여부가
    아니라 HTTP 상태 코드로 가른다(ADR-018).
    """
    error_type = classify_error(error)
    if error_type == OBJECT_STORAGE_ERROR:
        status = _object_storage_http_status(error)
        return status in _RETRYABLE_HTTP_STATUS
    return error_type in RETRYABLE_ERROR_TYPES
