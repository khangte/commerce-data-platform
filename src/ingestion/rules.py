"""PRD §11 수집 검증 Rule의 Code·순서·범위를 한곳에 정의한다."""

from __future__ import annotations

from dataclasses import dataclass

ROW = "ROW"
BATCH = "BATCH"


@dataclass(frozen=True)
class ValidationRule:
    """검증 Rule 하나의 오류 Code, 검증 순서, Row·Batch 범위를 담는다."""

    code: str
    order: int
    scope: str


SCHEMA_MISMATCH = ValidationRule("SCHEMA_MISMATCH", 1, BATCH)
REQUIRED_NULL = ValidationRule("REQUIRED_NULL", 2, ROW)
TYPE_MISMATCH = ValidationRule("TYPE_MISMATCH", 3, ROW)
KEY_NULL = ValidationRule("KEY_NULL", 4, ROW)
BATCH_DUPLICATE = ValidationRule("BATCH_DUPLICATE", 5, ROW)
STATUS_DOMAIN_INVALID = ValidationRule("STATUS_DOMAIN_INVALID", 6, ROW)
NUMERIC_RANGE_INVALID = ValidationRule("NUMERIC_RANGE_INVALID", 7, ROW)
BROKEN_REFERENCE = ValidationRule("BROKEN_REFERENCE", 8, ROW)
CURSOR_OUT_OF_RANGE = ValidationRule("CURSOR_OUT_OF_RANGE", 9, BATCH)

VALIDATION_RULES: tuple[ValidationRule, ...] = (
    SCHEMA_MISMATCH,
    REQUIRED_NULL,
    TYPE_MISMATCH,
    KEY_NULL,
    BATCH_DUPLICATE,
    STATUS_DOMAIN_INVALID,
    NUMERIC_RANGE_INVALID,
    BROKEN_REFERENCE,
    CURSOR_OUT_OF_RANGE,
)
ROW_ERROR_CODES = frozenset(rule.code for rule in VALIDATION_RULES if rule.scope == ROW)
