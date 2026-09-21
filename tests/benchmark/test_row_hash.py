"""공유 Canonical Row Hash 모듈의 안정성과 값 변환을 검증한다."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import duckdb

from src.common.row_hash import canonical_row_json, hash_cursor_rows


def test_known_row_hashes_to_a_stable_digest() -> None:
    """정해진 값 조합의 행 하나는 항상 같은 Digest로 Hash된다."""
    row = {
        "paid_at": datetime(2026, 9, 18, 3, 0, tzinfo=timezone(timedelta(hours=9))),
        "amount": Decimal("10.50"),
        "payment_id": UUID("2f1b3d4e-5a6b-7c8d-9e0f-1a2b3c4d5e6f"),
        "failure_code": None,
        "billing_date": date(2026, 9, 18),
    }

    payload = canonical_row_json(row)

    assert payload == (
        '{"amount":"10.50",'
        '"billing_date":"2026-09-18",'
        '"failure_code":null,'
        '"paid_at":"2026-09-17T18:00:00+00:00",'
        '"payment_id":"2f1b3d4e-5a6b-7c8d-9e0f-1a2b3c4d5e6f"}'
    )


def test_hash_cursor_rows_returns_digest_and_row_count() -> None:
    """정렬된 Cursor를 순회해 누적 Hash와 행 수를 함께 반환한다."""
    connection = duckdb.connect()
    connection.execute("CREATE TABLE sample (id INTEGER, label VARCHAR)")
    connection.executemany("INSERT INTO sample VALUES (?, ?)", [(1, "a"), (2, "b")])

    cursor = connection.execute("SELECT * FROM sample ORDER BY id")
    digest, row_count = hash_cursor_rows(cursor)

    assert row_count == 2
    assert digest == hash_cursor_rows(
        connection.execute("SELECT * FROM sample ORDER BY id")
    )[0]
    connection.close()
