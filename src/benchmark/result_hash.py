"""임의 DuckDB Query 결과의 Logical Hash를 계산하고 Arm 간 정확성 Gate를 강제한다."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from src.common.row_hash import hash_cursor_rows

if TYPE_CHECKING:
    import duckdb


def query_result_hash(
    connection: duckdb.DuckDBPyConnection, sql: str, params: Sequence[object] = ()
) -> tuple[str, int]:
    """정렬 보장된 Query 결과를 `(Hash, 행 수)`로 반환한다. `ORDER BY` 없는 SQL은 거부한다."""
    if "order by" not in sql.lower():
        raise ValueError("query_result_hash requires a deterministic ORDER BY")
    cursor = connection.execute(sql, params)
    return hash_cursor_rows(cursor)


class ResultHashMismatch(Exception):
    """서로 다른 Arm의 Result Hash가 불일치할 때 두 Arm 이름과 Hash를 함께 담는다."""

    def __init__(self, hashes: Mapping[str, str]) -> None:
        self.hashes = dict(hashes)
        detail = ", ".join(f"{arm}={digest}" for arm, digest in sorted(self.hashes.items()))
        super().__init__(f"Result hash mismatch across arms: {detail}")


def assert_arms_match(hashes: Mapping[str, str]) -> None:
    """모든 Arm의 Result Hash가 같은지 확인한다. 하나라도 다르면 정확성 Gate를 어긴다."""
    if len(set(hashes.values())) > 1:
        raise ResultHashMismatch(hashes)
