"""행 단위 Canonical JSON 직렬화와 누적 SHA-256 Hash를 계산한다.

Mart Hash와 Benchmark Result Hash가 같은 정의를 공유하도록 이 모듈에 둔다.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from collections.abc import Iterable

ROW_BATCH_SIZE = 10_000


def canonical_row_json(row: dict[str, object]) -> str:
    """컬럼 이름을 정렬하고 값 표현을 고정한 행 JSON을 반환한다."""
    return json.dumps(
        row,
        default=json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def json_default(value: object) -> str:
    """JSON 기본 형식에 없는 Warehouse 값의 결정적 문자열 표현을 반환한다."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            if value == datetime.min:  # noqa: DTZ901
                return "-infinity"
            if value == datetime.max:  # noqa: DTZ901
                return "infinity"
            raise ValueError("Mart timestamp must include a UTC offset")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Unsupported mart value: {type(value).__name__}")


def hash_cursor_rows(cursor, *, batch_size: int = ROW_BATCH_SIZE) -> tuple[str, int]:
    """이미 정렬된 DuckDB Cursor를 Canonical JSON 누적 SHA-256으로 Hash한다."""
    column_names = [descriptor[0] for descriptor in cursor.description]
    digest = hashlib.sha256()
    row_count = 0
    while True:
        rows: Iterable[tuple[object, ...]] = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            payload = canonical_row_json(dict(zip(column_names, row, strict=True)))
            digest.update(payload.encode("utf-8"))
            digest.update(b"\n")
            row_count += 1
    return digest.hexdigest(), row_count
