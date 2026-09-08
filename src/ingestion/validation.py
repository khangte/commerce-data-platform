"""Source Page의 Schema·Type·Key·Domain·범위를 순서대로 검증한다."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import pyarrow as pa

from src.ingestion.extract import SourcePage, SourceRecord
from src.ingestion.metadata import CursorPosition
from src.ingestion.tables import TableConfig


@dataclass(frozen=True)
class RejectedRecord:
    """Quarantine으로 보낼 Record와 Batch 내 순번·오류 Code를 보관한다."""

    record: SourceRecord
    ordinal: int
    error_codes: tuple[str, ...]


@dataclass(frozen=True)
class ValidatedPage:
    """한 Source Page의 Valid Record와 Row 단위 Reject를 분리한다."""

    valid_records: tuple[SourceRecord, ...]
    rejected_records: tuple[RejectedRecord, ...]


class SourceContractError(RuntimeError):
    """Schema 또는 고정 Cursor 범위를 위반한 Batch 계약 오류다."""


@dataclass
class ValidationPipeline:
    """고정 Cursor 범위 안에서 Batch 중복을 추적하는 Page 검증기다."""

    config: TableConfig
    watermark_before: CursorPosition
    extract_upper_bound: CursorPosition
    _seen_primary_keys: set[tuple[object, ...]] = field(default_factory=set, init=False)
    _next_ordinal: int = field(default=0, init=False)

    def validate_page(
        self, page: SourcePage, *, broken_reference_cursors: frozenset[tuple[object, ...]] = frozenset()
    ) -> ValidatedPage:
        """정의된 순서로 Page를 검사해 Valid와 Reject를 분리하고 계약 오류는 Batch 오류로 승격한다."""
        if not page.records or page.records[0].config != self.config:
            raise ValueError("Validation page must use the pipeline table config")
        valid: list[SourceRecord] = []
        rejected: list[RejectedRecord] = []
        for record in page.records:
            ordinal = self._next_ordinal
            self._next_ordinal += 1
            error_codes = self._validate_record(record, broken_reference_cursors)
            if error_codes:
                rejected.append(RejectedRecord(record, ordinal, tuple(error_codes)))
            else:
                valid.append(record)
        return ValidatedPage(tuple(valid), tuple(rejected))

    def _validate_record(
        self, record: SourceRecord, broken_reference_cursors: frozenset[tuple[object, ...]]
    ) -> list[str]:
        """Schema부터 Cursor 범위까지의 Record 오류를 정해진 순서로 수집한다."""
        values = record.values
        errors = _schema_and_type_errors(self.config, values)
        if "SCHEMA_MISMATCH" in errors:
            raise SourceContractError("Source schema differs from the configured contract")
        if not _cursor_in_range(record.cursor, self.watermark_before, self.extract_upper_bound):
            raise SourceContractError("Source record cursor is outside the fixed extraction range")
        primary_key = tuple(values.get(column) for column in self.config.primary_key_columns)
        if any(value is None for value in primary_key):
            errors.append("KEY_NULL")
        elif primary_key in self._seen_primary_keys:
            errors.append("BATCH_DUPLICATE")
        else:
            self._seen_primary_keys.add(primary_key)
        errors.extend(_domain_and_numeric_errors(self.config, values))
        if record.cursor.keys in broken_reference_cursors:
            errors.append("BROKEN_REFERENCE")
        return errors


def _schema_and_type_errors(config: TableConfig, values: Mapping[str, object]) -> list[str]:
    """Source Column 집합·필수값·Arrow Type 호환성 오류를 순서대로 반환한다."""
    if tuple(values) != config.source_column_names:
        return ["SCHEMA_MISMATCH"]
    errors: list[str] = []
    for column in config.source_columns:
        value = values[column.name]
        if value is None:
            if not column.nullable:
                errors.append("REQUIRED_NULL")
            continue
        if not _matches_arrow_type(value, column.type):
            errors.append("TYPE_MISMATCH")
    return errors


def _domain_and_numeric_errors(config: TableConfig, values: Mapping[str, object]) -> list[str]:
    """Source Status Domain과 Numeric 최소값 위반을 검증한다."""
    errors: list[str] = []
    for column, domain in config.status_domains.items():
        if values.get(column) is not None and values[column] not in domain:
            errors.append("STATUS_DOMAIN_INVALID")
    for column, minimum in config.numeric_minimums.items():
        value = values.get(column)
        field_type = config.source_schema.field(column).type
        if value is not None and _matches_arrow_type(value, field_type) and value < minimum:
            errors.append("NUMERIC_RANGE_INVALID")
    return errors


def _matches_arrow_type(value: object, type: pa.DataType) -> bool:
    """PostgreSQL Driver 값이 지정 Arrow Source Type과 호환되는지 확인한다."""
    if pa.types.is_string(type):
        return isinstance(value, str)
    if pa.types.is_integer(type):
        return isinstance(value, int) and not isinstance(value, bool)
    if pa.types.is_decimal(type):
        return isinstance(value, Decimal)
    if pa.types.is_timestamp(type):
        return isinstance(value, datetime) and value.tzinfo is not None
    raise TypeError(f"Unsupported validation Arrow type: {type}")


def _cursor_in_range(cursor: CursorPosition, lower: CursorPosition, upper: CursorPosition) -> bool:
    """Cursor가 `(lower, upper]` 고정 추출 범위에 있는지 확인한다."""
    if cursor.timestamp is None:
        return False
    candidate = (cursor.timestamp, cursor.keys)
    if lower.timestamp is not None and candidate <= (lower.timestamp, lower.keys):
        return False
    return candidate <= (upper.timestamp, upper.keys)
