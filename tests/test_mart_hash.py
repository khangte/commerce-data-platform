"""Mart Logical Hash의 값 표현과 정렬 무관성을 검증한다."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

from src.warehouse.mart_hash import (
    MART_HASH_TARGETS,
    MartTarget,
    _canonical_row_json,
    mart_logical_hash,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_canonical_row_json_normalizes_deterministic_values() -> None:
    """Timestamp는 UTC ISO, Decimal은 고정 소수점, UUID는 문자열, None은 null로 적는다."""
    row = {
        "paid_at": datetime(2026, 9, 18, 3, 0, tzinfo=timezone(timedelta(hours=9))),
        "amount": Decimal("10.50"),
        "payment_id": UUID("2f1b3d4e-5a6b-7c8d-9e0f-1a2b3c4d5e6f"),
        "failure_code": None,
        "billing_date": date(2026, 9, 18),
    }

    assert _canonical_row_json(row) == (
        '{"amount":"10.50",'
        '"billing_date":"2026-09-18",'
        '"failure_code":null,'
        '"paid_at":"2026-09-17T18:00:00+00:00",'
        '"payment_id":"2f1b3d4e-5a6b-7c8d-9e0f-1a2b3c4d5e6f"}'
    )


def test_canonical_row_json_rejects_a_naive_timestamp() -> None:
    """Timezone 없는 Timestamp는 Hash를 실행 환경에 의존하게 만들므로 막는다."""
    with pytest.raises(ValueError, match="UTC offset"):
        _canonical_row_json({"paid_at": datetime(2026, 9, 18, 3, 0)})  # noqa: DTZ001


def test_hash_ignores_physical_row_order() -> None:
    """같은 논리 내용이면 저장 순서가 달라도 Hash가 같다."""
    target = MartTarget("main", "sample", ("id",))
    first = _sample_connection([(2, "b"), (1, "a")])
    second = _sample_connection([(1, "a"), (2, "b")])

    try:
        assert mart_logical_hash(first, target) == mart_logical_hash(second, target)
    finally:
        first.close()
        second.close()


def test_hash_changes_when_one_value_changes() -> None:
    """값이 하나라도 다르면 Hash가 달라진다."""
    target = MartTarget("main", "sample", ("id",))
    first = _sample_connection([(1, "a"), (2, "b")])
    second = _sample_connection([(1, "a"), (2, "B")])

    try:
        assert mart_logical_hash(first, target) != mart_logical_hash(second, target)
    finally:
        first.close()
        second.close()


def test_hash_targets_cover_every_materialized_mart() -> None:
    """Mart를 추가하고 Hash 대상 목록에 넣지 않으면 조용히 비교에서 빠지므로 막는다."""
    expected = set()
    for schema in ("dimensions", "facts"):
        for path in (PROJECT_ROOT / "dbt/models/marts" / schema).glob("*.sql"):
            expected.add(f"{schema}.{path.stem}")

    assert {target.relation for target in MART_HASH_TARGETS} == expected


def test_hash_targets_declare_an_order_key() -> None:
    """정렬 Key가 없으면 행 순서가 Hash를 좌우하므로 모든 대상이 Key를 가진다."""
    for target in MART_HASH_TARGETS:
        assert target.order_by, target.relation


def _sample_connection(rows: list[tuple[int, str]]) -> duckdb.DuckDBPyConnection:
    """지정한 순서로 행을 넣은 메모리 DuckDB 연결을 만든다."""
    connection = duckdb.connect()
    connection.execute("CREATE TABLE sample (id INTEGER, label VARCHAR)")
    connection.executemany("INSERT INTO sample VALUES (?, ?)", rows)
    return connection
