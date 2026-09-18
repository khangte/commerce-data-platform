"""Mart Table의 Logical Hash를 계산하고 Incremental과 Full Refresh 결과를 비교한다."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import duckdb

ROW_BATCH_SIZE = 10_000


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


def mart_logical_hashes(warehouse_path: Path) -> dict[str, str]:
    """Hash 대상 Mart 전부를 Relation 이름 기준 Hash Dict로 반환한다."""
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        return {
            target.relation: mart_logical_hash(connection, target)
            for target in MART_HASH_TARGETS
        }


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
            raise ValueError("Mart timestamp must include a UTC offset")
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Unsupported mart value: {type(value).__name__}")
