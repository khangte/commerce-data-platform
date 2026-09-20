"""테스트·검증용 결정적 In-memory Pipeline Corruption을 제공한다."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from src.ingestion.extract import SourceRecord

NULL_PRIMARY_KEY = "NULL_PRIMARY_KEY"
INVALID_STATUS = "INVALID_STATUS"
NEGATIVE_NUMERIC = "NEGATIVE_NUMERIC"
TYPE_MISMATCH = "TYPE_MISMATCH"
BROKEN_REFERENCE = "BROKEN_REFERENCE"
DUPLICATE_PRIMARY_KEY = "DUPLICATE_PRIMARY_KEY"
SUPPORTED_CORRUPTION_KINDS = frozenset(
    {
        NULL_PRIMARY_KEY,
        INVALID_STATUS,
        NEGATIVE_NUMERIC,
        TYPE_MISMATCH,
        BROKEN_REFERENCE,
        DUPLICATE_PRIMARY_KEY,
    }
)


@dataclass(frozen=True)
class CorruptionPlan:
    """Batch 내 추출 순번별로 적용할 결정적 Corruption 종류를 보관한다."""

    rules: Mapping[int, str]

    def __post_init__(self) -> None:
        """음수 순번과 지원하지 않는 Corruption 종류를 막는다."""
        if any(ordinal < 0 or kind not in SUPPORTED_CORRUPTION_KINDS for ordinal, kind in self.rules.items()):
            raise ValueError("Corruption rules must use non-negative ordinals and supported kinds")

    def apply(self, record: SourceRecord, ordinal: int) -> SourceRecord:
        """지정 순번에만 Source를 쓰지 않는 복제본 Corruption을 적용한다."""
        kind = self.rules.get(ordinal)
        if kind is None:
            return record
        if kind == DUPLICATE_PRIMARY_KEY:
            raise ValueError("DUPLICATE_PRIMARY_KEY needs the previous record; use apply_page")
        values = dict(record.values)
        if kind == NULL_PRIMARY_KEY:
            values[record.config.primary_key_columns[0]] = None
        elif kind == INVALID_STATUS:
            column = next(iter(record.config.status_domains), None)
            if column is None:
                raise ValueError("INVALID_STATUS requires a configured status domain")
            values[column] = "__invalid_status__"
        elif kind == NEGATIVE_NUMERIC:
            column = next(iter(record.config.numeric_minimums), None)
            if column is None:
                raise ValueError("NEGATIVE_NUMERIC requires a configured numeric minimum")
            values[column] = -1
        elif kind == TYPE_MISMATCH:
            values[record.config.primary_key_columns[-1]] = object()
        elif kind == BROKEN_REFERENCE:
            column = _child_reference_column(record)
            values[column] = "__missing_parent__"
        return SourceRecord(record.config, MappingProxyType(values), cursor_override=record.cursor)

    def apply_page(
        self, records: tuple[SourceRecord, ...], first_ordinal: int
    ) -> tuple[SourceRecord, ...]:
        """Page 전체에 Corruption을 적용하고 Duplicate는 직전 출력 Record PK를 복사한다."""
        output: list[SourceRecord] = []
        for index, record in enumerate(records):
            ordinal = first_ordinal + index
            if self.rules.get(ordinal) != DUPLICATE_PRIMARY_KEY:
                output.append(self.apply(record, ordinal))
                continue
            if not output:
                raise ValueError("DUPLICATE_PRIMARY_KEY requires a preceding record in the page")
            output.append(_with_primary_key_of(record, output[-1]))
        return tuple(output)


def _child_reference_column(record: SourceRecord) -> str:
    """Broken Reference를 만들 수 있는 Child FK Column을 반환한다."""
    if record.config.source_table in {"order_items", "order_payments"}:
        return "order_id"
    raise ValueError("BROKEN_REFERENCE requires a child table record")


def _with_primary_key_of(record: SourceRecord, previous: SourceRecord) -> SourceRecord:
    """Record의 PK Column 값만 직전 Record 값으로 바꾼 복제본을 만든다."""
    values = dict(record.values)
    for column in record.config.primary_key_columns:
        values[column] = previous.values[column]
    return SourceRecord(record.config, MappingProxyType(values), cursor_override=record.cursor)
