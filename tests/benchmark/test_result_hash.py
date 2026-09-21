"""Query Result Hash와 Arm 간 정확성 Gate를 검증한다."""

from __future__ import annotations

import duckdb
import pytest

from src.benchmark.result_hash import (
    ResultHashMismatch,
    assert_arms_match,
    query_result_hash,
)


def _connection() -> duckdb.DuckDBPyConnection:
    """정렬 순서를 뒤섞은 Sample Table을 가진 In-memory 연결을 만든다."""
    connection = duckdb.connect()
    connection.execute("CREATE TABLE sample (id INTEGER, label VARCHAR)")
    connection.executemany("INSERT INTO sample VALUES (?, ?)", [(2, "b"), (1, "a")])
    return connection


def test_same_rows_in_different_physical_order_hash_equal() -> None:
    """물리 저장 순서가 달라도 같은 논리 결과면 같은 Hash가 나온다."""
    first = _connection()
    second = duckdb.connect()
    second.execute("CREATE TABLE sample (id INTEGER, label VARCHAR)")
    second.executemany("INSERT INTO sample VALUES (?, ?)", [(1, "a"), (2, "b")])

    first_hash, first_count = query_result_hash(first, "SELECT * FROM sample ORDER BY id")
    second_hash, second_count = query_result_hash(second, "SELECT * FROM sample ORDER BY id")

    assert first_hash == second_hash
    assert first_count == second_count == 2


def test_changed_value_hashes_differently() -> None:
    """값이 하나라도 바뀌면 Hash가 달라진다."""
    connection = _connection()
    connection.execute("UPDATE sample SET label = 'B' WHERE id = 2")

    changed_hash, _ = query_result_hash(connection, "SELECT * FROM sample ORDER BY id")
    baseline_hash, _ = query_result_hash(
        _connection(), "SELECT * FROM sample ORDER BY id"
    )

    assert changed_hash != baseline_hash


def test_missing_order_by_raises() -> None:
    """결정적이지 않은 결과는 Hash 의미가 없으므로 `ORDER BY` 누락을 거부한다."""
    connection = _connection()
    with pytest.raises(ValueError, match="ORDER BY"):
        query_result_hash(connection, "SELECT * FROM sample")


def test_assert_arms_match_passes_when_all_hashes_agree() -> None:
    """모든 Arm의 Hash가 같으면 통과한다."""
    assert_arms_match({"full": "aaa", "incremental": "aaa"})


def test_assert_arms_match_raises_with_both_arm_names_and_hashes() -> None:
    """Hash가 다르면 두 Arm 이름과 각각의 Hash를 담아 예외를 낸다."""
    with pytest.raises(ResultHashMismatch) as excinfo:
        assert_arms_match({"full": "aaa", "incremental": "bbb"})

    assert excinfo.value.hashes == {"full": "aaa", "incremental": "bbb"}
