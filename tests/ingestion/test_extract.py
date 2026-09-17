"""Postgres UUID Column을 가진 Source Table의 Cursor 식·Row 변환을 검증한다."""

from __future__ import annotations

import uuid
from types import MappingProxyType

import pyarrow as pa

from src.ingestion.extract import SourceRecord, _cursor_expression
from src.ingestion.tables import TableConfig, _text, _timestamp, _uuid

UUID_CURSOR_TABLE = TableConfig(
    source_table="uuid_cursor_fixture",
    primary_key_columns=("subscription_id",),
    cursor_timestamp_column="updated_at",
    cursor_key_columns=("subscription_id",),
    source_columns=(
        _uuid("subscription_id", nullable=False),
        _timestamp("updated_at", nullable=False),
    ),
)

TEXT_CURSOR_TABLE = TableConfig(
    source_table="text_cursor_fixture",
    primary_key_columns=("customer_id",),
    cursor_timestamp_column="updated_at",
    cursor_key_columns=("customer_id",),
    source_columns=(
        _text("customer_id", nullable=False),
        _timestamp("updated_at", nullable=False),
    ),
)


def test_cursor_expression_omits_collate_for_uuid_columns() -> None:
    """UUID PK Cursor 식은 Postgres가 거부하는 COLLATE를 붙이지 않는다."""
    assert _cursor_expression(UUID_CURSOR_TABLE) == "(updated_at, subscription_id)"


def test_cursor_expression_keeps_collate_for_text_columns() -> None:
    """문자열 PK Cursor 식은 기존과 같이 결정적 정렬을 위해 COLLATE "C"를 유지한다."""
    assert _cursor_expression(TEXT_CURSOR_TABLE) == '(updated_at, customer_id COLLATE "C")'


def test_source_record_normalizes_uuid_value_to_str_for_arrow_compatibility() -> None:
    """uuid.UUID 원본 값은 pyarrow string Array로 바로 변환되지 않아 str로 정규화해야 한다."""
    raw_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    record = SourceRecord(
        config=UUID_CURSOR_TABLE,
        values=MappingProxyType({"subscription_id": raw_uuid, "updated_at": None}),
    )

    normalized = record.arrow_compatible_values()

    assert isinstance(normalized["subscription_id"], str)
    assert normalized["subscription_id"] == str(raw_uuid)
    # 정규화된 값은 실제로 pyarrow string Array를 만들 수 있어야 한다.
    pa.array([normalized["subscription_id"]], type=pa.string())


def test_source_record_leaves_non_uuid_values_unchanged() -> None:
    """UUID가 아닌 값은 정규화 과정에서 그대로 유지된다."""
    record = SourceRecord(
        config=TEXT_CURSOR_TABLE,
        values=MappingProxyType({"customer_id": "abc-123", "updated_at": None}),
    )

    normalized = record.arrow_compatible_values()

    assert normalized["customer_id"] == "abc-123"
