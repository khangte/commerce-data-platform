"""결정적인 Customer 시나리오와 Membership 변경 계획을 만든다."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

import psycopg

from src.generator.config import GeneratorConfig
from src.generator.ids import deterministic_uuid, logical_hash

MEMBERSHIP_LEVELS = frozenset({"bronze", "silver", "gold"})
ADDRESS_CATALOG = (
    ("sao paulo", "SP"),
    ("rio de janeiro", "RJ"),
    ("curitiba", "PR"),
    ("belo horizonte", "MG"),
)


@dataclass(frozen=True)
class CustomerAddress:
    """Customer Source Record에 저장할 원본 호환 주소다."""

    city: str
    state: str

    def __post_init__(self) -> None:
        """Source Schema의 도시와 주 코드 제약을 사전에 확인한다."""
        if not self.city or len(self.city) > 128:
            raise ValueError("customer_city must contain 1 to 128 characters")
        if len(self.state) != 2 or self.state != self.state.upper():
            raise ValueError("customer_state must be a two-character uppercase code")


@dataclass(frozen=True)
class CustomerRecord:
    """Orders가 참조할 주문 시점 Customer Record 계획이다."""

    customer_id: str
    customer_unique_id: str
    address: CustomerAddress
    membership_level: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """Customer Source Row의 식별자·Membership·UTC 시각 계약을 확인한다."""
        if not self.customer_id or len(self.customer_id) > 64:
            raise ValueError("customer_id must contain 1 to 64 characters")
        if not self.customer_unique_id or len(self.customer_unique_id) > 64:
            raise ValueError("customer_unique_id must contain 1 to 64 characters")
        if self.membership_level not in MEMBERSHIP_LEVELS:
            raise ValueError(f"Unsupported membership_level: {self.membership_level}")
        _assert_utc_timestamp(self.created_at, "created_at")
        _assert_utc_timestamp(self.updated_at, "updated_at")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must be greater than or equal to created_at")


@dataclass(frozen=True)
class CustomerMutationResult:
    """Customer Source 저장 결과의 Insert·Update·Skip 건수다."""

    inserted: int
    updated: int
    skipped: int


def new_customer_record(config: GeneratorConfig, order_ordinal: int) -> CustomerRecord:
    """새 인물과 해당 주문의 새 Customer Record를 결정적으로 만든다."""
    _assert_positive_ordinal(order_ordinal)
    customer_unique_id = _deterministic_id(config, "customer-unique", order_ordinal)
    return _new_order_customer_record(
        config,
        customer_unique_id=customer_unique_id,
        order_ordinal=order_ordinal,
        address=_address_for(config, "new-customer", order_ordinal),
        record_kind="new-customer-record",
    )


def repurchase_customer_record(
    config: GeneratorConfig, existing_records: Iterable[CustomerRecord], order_ordinal: int
) -> CustomerRecord:
    """기존 인물을 결정적으로 선택해 재구매용 새 Customer Record를 만든다."""
    _assert_positive_ordinal(order_ordinal)
    latest_records = _latest_record_by_unique_id(existing_records)
    if not latest_records:
        raise ValueError("A repurchase scenario requires at least one existing customer")
    customer_unique_id = _choose_customer_unique_id(config, latest_records, order_ordinal)
    existing = latest_records[customer_unique_id]
    return _new_order_customer_record(
        config,
        customer_unique_id=customer_unique_id,
        order_ordinal=order_ordinal,
        address=existing.address,
        record_kind="repurchase-customer-record",
    )


def address_change_customer_record(
    config: GeneratorConfig, existing: CustomerRecord, order_ordinal: int
) -> CustomerRecord:
    """과거 Record를 보존하고 변경된 주소를 가진 새 Customer Record를 만든다."""
    _assert_positive_ordinal(order_ordinal)
    return _new_order_customer_record(
        config,
        customer_unique_id=existing.customer_unique_id,
        order_ordinal=order_ordinal,
        address=_changed_address_for(config, existing.address, order_ordinal),
        record_kind="address-change-customer-record",
    )


def membership_level(delivered_order_count: int) -> str:
    """Seed와 같은 완료 주문 기준으로 Customer Membership을 계산한다."""
    if delivered_order_count < 0:
        raise ValueError("delivered_order_count must be zero or greater")
    if delivered_order_count >= 15:
        return "gold"
    if delivered_order_count >= 5:
        return "silver"
    return "bronze"


def membership_change_records(
    config: GeneratorConfig,
    records: Iterable[CustomerRecord],
    delivered_order_count: int,
) -> tuple[CustomerRecord, ...]:
    """한 인물의 모든 Customer Record에 동일 Membership 변경을 계획한다."""
    record_list = tuple(records)
    if not record_list:
        raise ValueError("Membership changes require at least one customer record")
    unique_ids = {record.customer_unique_id for record in record_list}
    if len(unique_ids) != 1:
        raise ValueError("Membership changes must contain exactly one customer_unique_id")

    target_level = membership_level(delivered_order_count)
    changed_records = []
    for record in sorted(record_list, key=lambda value: value.customer_id):
        if record.membership_level == target_level:
            changed_records.append(record)
            continue
        if config.logical_date <= record.updated_at:
            raise ValueError("logical_date must be greater than updated_at for a membership change")
        changed_records.append(
            replace(record, membership_level=target_level, updated_at=config.logical_date)
        )
    return tuple(changed_records)


def fetch_customer_records(connection: psycopg.Connection) -> tuple[CustomerRecord, ...]:
    """재구매 선택에 필요한 현재 Customer Record를 안정된 순서로 읽는다."""
    rows = connection.execute(
        """
        SELECT customer_id, customer_unique_id, customer_city, customer_state,
               membership_level, created_at, updated_at
        FROM customers
        ORDER BY customer_unique_id COLLATE "C", created_at, customer_id COLLATE "C"
        """
    )
    return tuple(
        CustomerRecord(
            customer_id=row[0],
            customer_unique_id=row[1],
            address=CustomerAddress(city=row[2], state=row[3]),
            membership_level=row[4],
            created_at=row[5],
            updated_at=row[6],
        )
        for row in rows
    )


def persist_customer_records(
    connection: psycopg.Connection, records: Iterable[CustomerRecord]
) -> CustomerMutationResult:
    """Customer Record를 멱등적으로 저장하고 단조 증가하지 않는 변경을 거부한다."""
    inserted = 0
    updated = 0
    skipped = 0
    for record in records:
        existing = _find_customer_record(connection, record.customer_id)
        if existing is None:
            connection.execute(
                """
                INSERT INTO customers (
                    customer_id, customer_unique_id, customer_city, customer_state,
                    membership_level, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                _customer_parameters(record),
            )
            inserted += 1
            continue
        if existing == record:
            skipped += 1
            continue
        if existing.customer_unique_id != record.customer_unique_id:
            raise ValueError("customer_id cannot be reassigned to another customer_unique_id")
        if record.updated_at <= existing.updated_at:
            raise ValueError("Customer changes require an updated_at greater than the current value")
        connection.execute(
            """
            UPDATE customers
            SET customer_city = %s,
                customer_state = %s,
                membership_level = %s,
                updated_at = %s
            WHERE customer_id = %s
            """,
            (
                record.address.city,
                record.address.state,
                record.membership_level,
                record.updated_at,
                record.customer_id,
            ),
        )
        updated += 1
    return CustomerMutationResult(inserted=inserted, updated=updated, skipped=skipped)


def _new_order_customer_record(
    config: GeneratorConfig,
    *,
    customer_unique_id: str,
    order_ordinal: int,
    address: CustomerAddress,
    record_kind: str,
) -> CustomerRecord:
    """주문 시점 Customer Record의 공통 필드를 결정적으로 구성한다."""
    return CustomerRecord(
        customer_id=_deterministic_id(config, record_kind, customer_unique_id, order_ordinal),
        customer_unique_id=customer_unique_id,
        address=address,
        membership_level="bronze",
        created_at=config.logical_date,
        updated_at=config.logical_date,
    )


def _find_customer_record(
    connection: psycopg.Connection, customer_id: str
) -> CustomerRecord | None:
    """저장 전 동일 Customer Record를 Lock으로 보호해 읽는다."""
    row = connection.execute(
        """
        SELECT customer_id, customer_unique_id, customer_city, customer_state,
               membership_level, created_at, updated_at
        FROM customers
        WHERE customer_id = %s
        FOR UPDATE
        """,
        (customer_id,),
    ).fetchone()
    if row is None:
        return None
    return CustomerRecord(
        customer_id=row[0],
        customer_unique_id=row[1],
        address=CustomerAddress(city=row[2], state=row[3]),
        membership_level=row[4],
        created_at=row[5],
        updated_at=row[6],
    )


def _customer_parameters(record: CustomerRecord) -> tuple[object, ...]:
    """INSERT에 사용할 Customer Record의 Source Column 순서를 반환한다."""
    return (
        record.customer_id,
        record.customer_unique_id,
        record.address.city,
        record.address.state,
        record.membership_level,
        record.created_at,
        record.updated_at,
    )


def _latest_record_by_unique_id(records: Iterable[CustomerRecord]) -> dict[str, CustomerRecord]:
    """동일 인물의 여러 주문 Record 중 최신 주소 Record만 남긴다."""
    latest_records: dict[str, CustomerRecord] = {}
    for record in records:
        previous = latest_records.get(record.customer_unique_id)
        if previous is None or _record_sort_key(record) > _record_sort_key(previous):
            latest_records[record.customer_unique_id] = record
    return latest_records


def _choose_customer_unique_id(
    config: GeneratorConfig,
    latest_records: dict[str, CustomerRecord],
    order_ordinal: int,
) -> str:
    """정렬된 기존 인물 목록에서 재구매 대상을 결정적으로 선택한다."""
    candidates = tuple(sorted(latest_records))
    selector_hash = logical_hash(
        {
            "generator_inputs": config.deterministic_inputs(),
            "entity": "repurchase-customer-selection",
            "order_ordinal": order_ordinal,
        }
    )
    return candidates[int(selector_hash, 16) % len(candidates)]


def _address_for(config: GeneratorConfig, address_kind: str, ordinal: int) -> CustomerAddress:
    """Config와 순번으로 주소 카탈로그 항목 하나를 결정적으로 선택한다."""
    selector_hash = logical_hash(
        {
            "generator_inputs": config.deterministic_inputs(),
            "address_kind": address_kind,
            "ordinal": ordinal,
        }
    )
    city, state = ADDRESS_CATALOG[int(selector_hash, 16) % len(ADDRESS_CATALOG)]
    return CustomerAddress(city=city, state=state)


def _changed_address_for(
    config: GeneratorConfig, previous_address: CustomerAddress, order_ordinal: int
) -> CustomerAddress:
    """이전 주소와 다른 결정적 주소를 선택한다."""
    alternatives = tuple(address for address in ADDRESS_CATALOG if address != _address_tuple(previous_address))
    selector_hash = logical_hash(
        {
            "generator_inputs": config.deterministic_inputs(),
            "address_kind": "address-change",
            "order_ordinal": order_ordinal,
            "previous_address": _address_tuple(previous_address),
        }
    )
    city, state = alternatives[int(selector_hash, 16) % len(alternatives)]
    return CustomerAddress(city=city, state=state)


def _deterministic_id(config: GeneratorConfig, entity_name: str, *components: object) -> str:
    """Source VARCHAR 식별자에 맞는 32자리 UUIDv5 Hex 문자열을 반환한다."""
    return deterministic_uuid(entity_name, config.deterministic_inputs(), *components).hex


def _record_sort_key(record: CustomerRecord) -> tuple[datetime, datetime, str]:
    """같은 인물의 최신 Customer Record를 고르는 안정적인 정렬 Key를 반환한다."""
    return record.created_at, record.updated_at, record.customer_id


def _address_tuple(address: CustomerAddress) -> tuple[str, str]:
    """주소 비교와 Hash 입력에 사용할 불변 Tuple을 반환한다."""
    return address.city, address.state


def _assert_positive_ordinal(order_ordinal: int) -> None:
    """주문 순번이 1부터 시작하는 정수인지 확인한다."""
    if isinstance(order_ordinal, bool) or not isinstance(order_ordinal, int) or order_ordinal < 1:
        raise ValueError("order_ordinal must be an integer greater than or equal to 1")


def _assert_utc_timestamp(value: datetime, field_name: str) -> None:
    """Customer Row 시각이 UTC로 정규화됐는지 확인한다."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be normalized to UTC")
