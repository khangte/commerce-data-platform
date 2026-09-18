"""Mart Table의 Logical Hash를 계산하고 Incremental과 Full Refresh 결과를 비교한다."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import duckdb

ROW_BATCH_SIZE = 10_000
DIFF_LIMIT = 20


@dataclass(frozen=True)
class MartTarget:
    """Hash 비교 대상 Mart 하나의 Schema, Model 이름, 정렬 Key를 담는다."""

    schema: str
    model: str
    order_by: tuple[str, ...]

    @property
    def relation(self) -> str:
        """Warehouse에서 조회할 Relation 이름을 반환한다."""
        return f"{self.schema}.{self.model}"


MART_HASH_TARGETS: tuple[MartTarget, ...] = (
    MartTarget("dimensions", "dim_customer", ("customer_key",)),
    MartTarget("dimensions", "dim_date", ("date_key",)),
    MartTarget("dimensions", "dim_product", ("product_id",)),
    MartTarget("dimensions", "dim_seller", ("seller_id",)),
    MartTarget("dimensions", "dim_subscription", ("subscription_key",)),
    MartTarget("facts", "fact_orders", ("order_id",)),
    MartTarget("facts", "fact_order_items", ("order_id", "order_item_id")),
    MartTarget("facts", "fact_payments", ("order_id", "payment_sequence")),
    MartTarget("facts", "fact_subscription_payments", ("payment_id",)),
)


def mart_logical_hash(connection: duckdb.DuckDBPyConnection, target: MartTarget) -> str:
    """Mart 한 개를 Key 정렬 Canonical JSON 누적 SHA-256 Hash로 변환한다."""
    order_by = ", ".join(f'"{column}"' for column in target.order_by)
    cursor = connection.execute(f"SELECT * FROM {target.relation} ORDER BY {order_by}")
    column_names = [descriptor[0] for descriptor in cursor.description]
    digest = hashlib.sha256()
    while True:
        rows = cursor.fetchmany(ROW_BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            payload = _canonical_row_json(dict(zip(column_names, row, strict=True)))
            digest.update(payload.encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


def mart_logical_hashes(
    warehouse_path: Path, targets: tuple[MartTarget, ...] = MART_HASH_TARGETS
) -> dict[str, str]:
    """Hash 대상 Mart 전부를 Relation 이름 기준 Hash Dict로 반환한다."""
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        return {
            target.relation: mart_logical_hash(connection, target)
            for target in targets
        }


def target_for(relation: str) -> MartTarget:
    """Relation 이름에 해당하는 Hash 대상 정의를 반환한다."""
    for target in MART_HASH_TARGETS:
        if target.relation == relation:
            return target
    raise KeyError(f"Unknown mart relation: {relation}")


def mismatched_relations(left: dict[str, str], right: dict[str, str]) -> tuple[str, ...]:
    """두 Hash Dict에서 값이 다르거나 한쪽에만 있는 Relation 이름을 정렬해 반환한다."""
    relations = set(left) | set(right)
    return tuple(sorted(name for name in relations if left.get(name) != right.get(name)))


def describe_mart_difference(
    left_path: Path, right_path: Path, target: MartTarget, *, limit: int = DIFF_LIMIT
) -> str:
    """Hash가 다른 Mart의 행 수, 한쪽에만 있는 Key, 값이 다른 Key를 사람이 읽을 형태로 만든다."""
    left_rows = _keyed_rows(left_path, target)
    right_rows = _keyed_rows(right_path, target)
    only_left = sorted(set(left_rows) - set(right_rows))
    only_right = sorted(set(right_rows) - set(left_rows))
    changed = sorted(
        key for key in set(left_rows) & set(right_rows) if left_rows[key] != right_rows[key]
    )
    lines = [
        f"{target.relation}: left {len(left_rows)} rows, right {len(right_rows)} rows",
        f"  only in left ({len(only_left)}): {_format_keys(only_left, limit)}",
        f"  only in right ({len(only_right)}): {_format_keys(only_right, limit)}",
        f"  changed ({len(changed)}): {_format_keys(changed, limit)}",
    ]
    for key in changed[:limit]:
        lines.append(f"  {_format_key(key)} left : {left_rows[key]}")
        lines.append(f"  {_format_key(key)} right: {right_rows[key]}")
    return "\n".join(lines)


def _keyed_rows(warehouse_path: Path, target: MartTarget) -> dict[tuple[str, ...], str]:
    """Mart를 정렬 Key Tuple에서 행 Canonical JSON으로 가는 Dict로 읽는다."""
    order_by = ", ".join(f'"{column}"' for column in target.order_by)
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        cursor = connection.execute(f"SELECT * FROM {target.relation} ORDER BY {order_by}")
        column_names = [descriptor[0] for descriptor in cursor.description]
        rows: dict[tuple[str, ...], str] = {}
        for row in cursor.fetchall():
            values = dict(zip(column_names, row, strict=True))
            key = tuple(str(values[column]) for column in target.order_by)
            rows[key] = _canonical_row_json(values)
    return rows


def _format_keys(keys: list[tuple[str, ...]], limit: int) -> str:
    """진단 출력에 넣을 Key 목록을 상한까지만 한 줄로 만든다."""
    if not keys:
        return "-"
    shown = ", ".join(_format_key(key) for key in keys[:limit])
    if len(keys) > limit:
        return f"{shown}, ... (+{len(keys) - limit})"
    return shown


def _format_key(key: tuple[str, ...]) -> str:
    """Key Tuple을 사람이 읽을 한 덩어리 문자열로 만든다."""
    return "|".join(key)


def _canonical_row_json(row: dict[str, object]) -> str:
    """컬럼 이름을 정렬하고 값 표현을 고정한 행 JSON을 반환한다."""
    return json.dumps(
        row,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_default(value: object) -> str:
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


def main(
    argv: list[str] | None = None, *, targets: tuple[MartTarget, ...] = MART_HASH_TARGETS
) -> int:
    """Mart Hash를 출력하거나 두 Warehouse의 Hash를 비교한다."""
    parser = argparse.ArgumentParser(description="Compare mart logical hashes between warehouses")
    parser.add_argument("warehouse", type=Path, help="Warehouse DuckDB file to hash")
    parser.add_argument(
        "--compare", type=Path, default=None, help="Second warehouse to compare against"
    )
    arguments = parser.parse_args(argv)

    left = mart_logical_hashes(arguments.warehouse, targets)
    if arguments.compare is None:
        print(json.dumps(left, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    right = mart_logical_hashes(arguments.compare, targets)
    mismatched = mismatched_relations(left, right)
    if not mismatched:
        print(f"All {len(left)} mart hashes match")
        return 0

    print(f"Mismatched marts: {', '.join(mismatched)}")
    for relation in mismatched:
        target = next(item for item in targets if item.relation == relation)
        print(describe_mart_difference(arguments.warehouse, arguments.compare, target))
    return 1


if __name__ == "__main__":
    sys.exit(main())
