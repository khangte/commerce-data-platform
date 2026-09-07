"""Child Page가 같은 Source Snapshot의 Parent Key를 참조하는지 검증한다."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from src.ingestion.extract import SourcePage, TableSnapshot


@dataclass(frozen=True)
class ParentReference:
    """Child Source Column과 단일 PK Parent Table의 참조 계약이다."""

    child_column: str
    parent_table: str
    parent_column: str


@dataclass(frozen=True)
class BrokenReference:
    """현재 Snapshot에서 찾지 못한 Child Record의 Parent Key 증적이다."""

    child_table: str
    child_cursor: tuple[str | int, ...]
    child_column: str
    parent_table: str
    missing_key: str | int


CHILD_PARENT_REFERENCES: dict[str, tuple[ParentReference, ...]] = {
    "order_items": (
        ParentReference("order_id", "orders", "order_id"),
        ParentReference("product_id", "products", "product_id"),
        ParentReference("seller_id", "sellers", "seller_id"),
    ),
    "order_payments": (ParentReference("order_id", "orders", "order_id"),),
}


def find_broken_parent_references(
    snapshot: TableSnapshot, page: SourcePage
) -> tuple[BrokenReference, ...]:
    """동일 Read-only Snapshot에서 Child Page의 누락 Parent Key를 모두 반환한다."""
    if page.records and page.records[0].config != snapshot.config:
        raise ValueError("Source page config must match the snapshot config")
    references = CHILD_PARENT_REFERENCES.get(snapshot.config.source_table, ())
    broken: list[BrokenReference] = []
    for reference in references:
        keys = {record.values[reference.child_column] for record in page.records}
        present_keys = _parent_keys(snapshot, reference, keys)
        for record in page.records:
            key = record.values[reference.child_column]
            if key not in present_keys:
                if not isinstance(key, (str, int)) or isinstance(key, bool):
                    raise TypeError("Parent reference keys must be strings or integers")
                broken.append(
                    BrokenReference(
                        child_table=snapshot.config.source_table,
                        child_cursor=record.cursor.keys,
                        child_column=reference.child_column,
                        parent_table=reference.parent_table,
                        missing_key=key,
                    )
                )
    return tuple(broken)


def _parent_keys(
    snapshot: TableSnapshot, reference: ParentReference, keys: Iterable[object]
) -> set[str | int]:
    """Snapshot Connection에서 대상 Parent Key만 읽어 Lookup Set으로 반환한다."""
    query_keys = tuple(keys)
    if not query_keys:
        return set()
    rows = snapshot.connection.execute(
        f"""
        SELECT {reference.parent_column}
        FROM {reference.parent_table}
        WHERE {reference.parent_column} = ANY(%s)
        """,
        (list(query_keys),),
    ).fetchall()
    values = {row[0] for row in rows}
    if any(not isinstance(value, (str, int)) or isinstance(value, bool) for value in values):
        raise TypeError("Parent reference keys must be strings or integers")
    return values
