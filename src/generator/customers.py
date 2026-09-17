"""결정적인 계정 Customer와 사람 단위 구독 상태·거래 실적 등급 변경 계획을 만든다."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from uuid import UUID

import psycopg

from src.generator.config import GeneratorConfig
from src.generator.ids import deterministic_uuid, logical_hash

MEMBERSHIP_TIERS = frozenset({"BRONZE", "SILVER", "GOLD"})
SUBSCRIPTION_STATUSES = frozenset(
    {"ACTIVE", "PAYMENT_FAILED", "CANCEL_REQUESTED", "CHURNED"}
)
SUBSCRIPTION_PERIOD = timedelta(days=30)
PAYMENT_FAILURE_GRACE_PERIOD = timedelta(days=7)
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
    """Orders가 참조할 불변 주문 계정 Record 계획이다."""

    customer_id: str
    customer_unique_id: str
    address: CustomerAddress
    created_at: datetime

    def __post_init__(self) -> None:
        """계정 식별자와 UTC 생성 시각의 Source 계약을 확인한다."""
        if not self.customer_id or len(self.customer_id) > 64:
            raise ValueError("customer_id must contain 1 to 64 characters")
        if not self.customer_unique_id or len(self.customer_unique_id) > 64:
            raise ValueError("customer_unique_id must contain 1 to 64 characters")
        _assert_utc_timestamp(self.created_at, "created_at")


@dataclass(frozen=True)
class SubscriptionRecord:
    """구독 계약 1건의 현재 운영 상태 Source Record 계획이다."""

    subscription_id: UUID
    customer_unique_id: str
    subscription_status: str
    auto_renew_enabled: bool
    subscription_started_at: datetime
    current_period_started_at: datetime | None
    current_period_ends_at: datetime | None
    billing_due_at: datetime | None
    next_payment_attempt_at: datetime | None
    payment_failed_at: datetime | None
    cancel_requested_at: datetime | None
    ended_at: datetime | None
    status_changed_at: datetime
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """계약 키·상태·자동갱신 시각의 Source 계약을 확인한다."""
        if not self.customer_unique_id or len(self.customer_unique_id) > 64:
            raise ValueError("customer_unique_id must contain 1 to 64 characters")
        if self.subscription_status not in SUBSCRIPTION_STATUSES:
            raise ValueError(f"Unsupported subscription_status: {self.subscription_status}")
        for name, value in (
            ("subscription_started_at", self.subscription_started_at),
            ("status_changed_at", self.status_changed_at),
            ("created_at", self.created_at),
            ("updated_at", self.updated_at),
        ):
            _assert_utc_timestamp(value, name)
        for name, value in _subscription_timestamps(self):
            if value is not None:
                _assert_utc_timestamp(value, name)
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must be greater than or equal to created_at")
        if self.current_period_started_at and self.current_period_ends_at and (
            self.current_period_ends_at <= self.current_period_started_at
        ):
            raise ValueError("current period must end after it starts")
        if self.subscription_status == "ACTIVE" and (
            self.current_period_started_at is None
            or self.current_period_ends_at is None
            or self.billing_due_at is None
            or self.next_payment_attempt_at is None
        ):
            raise ValueError("ACTIVE requires a current period and next payment schedule")
        if self.subscription_status == "PAYMENT_FAILED" and (
            self.payment_failed_at is None
        ):
            raise ValueError("PAYMENT_FAILED requires payment_failed_at")
        if self.subscription_status == "CANCEL_REQUESTED" and (
            self.auto_renew_enabled
            or self.cancel_requested_at is None
            or self.next_payment_attempt_at is not None
        ):
            raise ValueError("CANCEL_REQUESTED disables renewal and has no next payment attempt")
        if self.subscription_status == "CHURNED" and (
            self.auto_renew_enabled
            or self.ended_at is None
            or self.billing_due_at is not None
            or self.next_payment_attempt_at is not None
        ):
            raise ValueError("CHURNED must be ended with no renewal schedule")


@dataclass(frozen=True)
class MembershipTierRecord:
    """한 사람의 거래 실적 등급 Source Record 계획이다. customer_membership_tiers 축이다."""

    customer_unique_id: str
    membership_tier: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """사람 키·등급·시각의 Source 계약을 확인한다."""
        if not self.customer_unique_id or len(self.customer_unique_id) > 64:
            raise ValueError("customer_unique_id must contain 1 to 64 characters")
        if self.membership_tier not in MEMBERSHIP_TIERS:
            raise ValueError(f"Unsupported membership_tier: {self.membership_tier}")
        _assert_utc_timestamp(self.created_at, "created_at")
        _assert_utc_timestamp(self.updated_at, "updated_at")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must be greater than or equal to created_at")


@dataclass(frozen=True)
class CustomerMutationResult:
    """불변 Customer 계정 저장 결과의 Insert·Update·Skip 건수다."""

    inserted: int
    updated: int
    skipped: int


@dataclass(frozen=True)
class AxisMutationResult:
    """구독 또는 등급 축 Source Record 저장 결과의 Insert·Update·Skip 건수다."""

    inserted: int
    updated: int
    skipped: int


def new_customer_record(config: GeneratorConfig, order_ordinal: int) -> CustomerRecord:
    """새 인물과 해당 주문의 새 Customer 계정을 결정적으로 만든다."""
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
    """기존 인물을 결정적으로 선택해 재구매용 새 Customer 계정을 만든다."""
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
    """과거 계정을 보존하고 변경 주소를 가진 새 Customer 계정을 만든다."""
    _assert_positive_ordinal(order_ordinal)
    return _new_order_customer_record(
        config,
        customer_unique_id=existing.customer_unique_id,
        order_ordinal=order_ordinal,
        address=_changed_address_for(config, existing.address, order_ordinal),
        record_kind="address-change-customer-record",
    )


def new_subscription_record(customer: CustomerRecord) -> SubscriptionRecord:
    """새 계정의 사람 키로 최초 활성 구독 계약 Record를 만든다."""
    return new_subscription_record_for_customer(customer.customer_unique_id, customer.created_at)


def new_subscription_record_for_customer(
    customer_unique_id: str, subscription_started_at: datetime
) -> SubscriptionRecord:
    """사람 키와 시작 시각으로 최초 활성 구독 계약 Record를 만든다."""
    return SubscriptionRecord(
        subscription_id=deterministic_uuid(
            "subscription-contract", customer_unique_id, subscription_started_at
        ),
        customer_unique_id=customer_unique_id,
        subscription_status="ACTIVE",
        auto_renew_enabled=True,
        subscription_started_at=subscription_started_at,
        current_period_started_at=subscription_started_at,
        current_period_ends_at=subscription_started_at + SUBSCRIPTION_PERIOD,
        billing_due_at=subscription_started_at + SUBSCRIPTION_PERIOD,
        next_payment_attempt_at=subscription_started_at + SUBSCRIPTION_PERIOD,
        payment_failed_at=None,
        cancel_requested_at=None,
        ended_at=None,
        status_changed_at=subscription_started_at,
        created_at=subscription_started_at,
        updated_at=subscription_started_at,
    )


def new_membership_tier_record(customer: CustomerRecord) -> MembershipTierRecord:
    """새 계정의 사람 키로 최초 BRONZE 등급 Record를 만든다."""
    return MembershipTierRecord(
        customer_unique_id=customer.customer_unique_id,
        membership_tier="BRONZE",
        created_at=customer.created_at,
        updated_at=customer.created_at,
    )


def membership_tier(delivered_order_count: int) -> str:
    """Seed와 같은 완료 주문 기준으로 사람 단위 거래 실적 등급을 계산한다."""
    if delivered_order_count < 0:
        raise ValueError("delivered_order_count must be zero or greater")
    if delivered_order_count >= 15:
        return "GOLD"
    if delivered_order_count >= 5:
        return "SILVER"
    return "BRONZE"


def membership_tier_change_records(
    config: GeneratorConfig,
    records: Iterable[MembershipTierRecord],
    delivered_order_count: int,
) -> tuple[MembershipTierRecord, ...]:
    """한 사람의 거래 실적 등급만 단조 증가한 변경 시각으로 갱신한다."""
    record_list = tuple(records)
    if not record_list:
        raise ValueError("Membership tier changes require at least one tier record")
    unique_ids = {record.customer_unique_id for record in record_list}
    if len(unique_ids) != 1:
        raise ValueError("Membership tier changes must contain exactly one customer_unique_id")
    if len(record_list) != 1:
        raise ValueError("Membership tier changes require exactly one person-grain record")
    record = record_list[0]
    target_tier = membership_tier(delivered_order_count)
    if record.membership_tier == target_tier:
        return (record,)
    if config.logical_date <= record.updated_at:
        raise ValueError("logical_date must be greater than updated_at for a membership change")
    return (replace(record, membership_tier=target_tier, updated_at=config.logical_date),)


def subscription_transition_records(
    config: GeneratorConfig, records: Iterable[SubscriptionRecord], next_status: str
) -> tuple[SubscriptionRecord, ...]:
    """한 사람의 허용된 구독 상태 전이를 현재 논리 시각으로 계획한다."""
    record_list = tuple(records)
    if len(record_list) != 1:
        raise ValueError("Subscription transitions require exactly one person-grain record")
    record = record_list[0]
    if config.logical_date <= record.updated_at:
        raise ValueError("logical_date must be greater than updated_at for a subscription transition")
    if next_status not in _allowed_subscription_targets(record.subscription_status):
        raise ValueError(
            f"Unsupported subscription transition: {record.subscription_status} -> {next_status}"
        )
    return (_transition_subscription_record(record, next_status, config.logical_date),)


def fetch_customer_records(connection: psycopg.Connection) -> tuple[CustomerRecord, ...]:
    """재구매 선택에 필요한 현재 Customer 계정을 안정된 순서로 읽는다."""
    rows = connection.execute(
        """
        SELECT customer_id, customer_unique_id, customer_city, customer_state, created_at
        FROM customers
        ORDER BY customer_unique_id COLLATE "C", created_at, customer_id COLLATE "C"
        """
    )
    return tuple(
        CustomerRecord(row[0], row[1], CustomerAddress(row[2], row[3]), row[4]) for row in rows
    )


def fetch_subscription_record(
    connection: psycopg.Connection, customer_unique_id: str
) -> SubscriptionRecord | None:
    """한 고객의 유효 구독 계약 상태 Record를 읽는다."""
    row = connection.execute(
        """
        SELECT subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
               subscription_started_at, current_period_started_at, current_period_ends_at,
               billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
               ended_at, status_changed_at, created_at, updated_at
        FROM customer_subscriptions
        WHERE customer_unique_id = %s
          AND subscription_status <> 'CHURNED'
        """,
        (customer_unique_id,),
    ).fetchone()
    return None if row is None else SubscriptionRecord(*row)


def fetch_membership_tier_record(
    connection: psycopg.Connection, customer_unique_id: str
) -> MembershipTierRecord | None:
    """변경할 사람의 현재 거래 실적 등급 Record를 읽는다."""
    row = connection.execute(
        """
        SELECT customer_unique_id, membership_tier, created_at, updated_at
        FROM customer_membership_tiers
        WHERE customer_unique_id = %s
        """,
        (customer_unique_id,),
    ).fetchone()
    return None if row is None else MembershipTierRecord(*row)


def persist_customer_records(
    connection: psycopg.Connection, records: Iterable[CustomerRecord]
) -> CustomerMutationResult:
    """불변 Customer 계정을 멱등적으로 저장하고 기존 값 변경을 거부한다."""
    inserted = 0
    skipped = 0
    for record in records:
        existing = _find_customer_record(connection, record.customer_id)
        if existing is None:
            connection.execute(
                """
                INSERT INTO customers (
                    customer_id, customer_unique_id, customer_city, customer_state, created_at
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                _customer_parameters(record),
            )
            inserted += 1
        elif existing == record:
            skipped += 1
        else:
            raise ValueError("Customer accounts are immutable after creation")
    return CustomerMutationResult(inserted=inserted, updated=0, skipped=skipped)


def ensure_subscription_records(
    connection: psycopg.Connection, records: Iterable[SubscriptionRecord]
) -> AxisMutationResult:
    """새 구독 계약 Record만 저장하고 같은 계약은 보존한다."""
    inserted = 0
    skipped = 0
    for record in records:
        existing = _find_subscription_record(connection, record.subscription_id)
        if existing is None:
            connection.execute(
                """
                INSERT INTO customer_subscriptions (
                    subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
                    subscription_started_at, current_period_started_at, current_period_ends_at,
                    billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
                    ended_at, status_changed_at, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                _subscription_parameters(record),
            )
            inserted += 1
        else:
            skipped += 1
    return AxisMutationResult(inserted=inserted, updated=0, skipped=skipped)


def ensure_membership_tier_records(
    connection: psycopg.Connection, records: Iterable[MembershipTierRecord]
) -> AxisMutationResult:
    """새 사람의 최초 등급 Record만 저장하고 기존 등급은 보존한다."""
    inserted = 0
    skipped = 0
    for record in records:
        existing = _find_membership_tier_record(connection, record.customer_unique_id)
        if existing is None:
            connection.execute(
                """
                INSERT INTO customer_membership_tiers (
                    customer_unique_id, membership_tier, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s)
                """,
                _membership_tier_parameters(record),
            )
            inserted += 1
        else:
            skipped += 1
    return AxisMutationResult(inserted=inserted, updated=0, skipped=skipped)


def persist_subscription_records(
    connection: psycopg.Connection, records: Iterable[SubscriptionRecord]
) -> AxisMutationResult:
    """구독 계약 상태 Record를 멱등 저장하고 역행 변경을 거부한다."""
    inserted = 0
    updated = 0
    skipped = 0
    for record in records:
        existing = _find_subscription_record(connection, record.subscription_id)
        if existing is None:
            connection.execute(
                """
                INSERT INTO customer_subscriptions (
                    subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
                    subscription_started_at, current_period_started_at, current_period_ends_at,
                    billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
                    ended_at, status_changed_at, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                _subscription_parameters(record),
            )
            inserted += 1
        elif existing == record:
            skipped += 1
        else:
            if record.created_at != existing.created_at:
                raise ValueError("Subscription created_at cannot change")
            if record.updated_at <= existing.updated_at:
                raise ValueError(
                    "Subscription changes require an updated_at greater than the current value"
                )
            connection.execute(
                """
                UPDATE customer_subscriptions
                SET subscription_status = %s, auto_renew_enabled = %s,
                    current_period_started_at = %s, current_period_ends_at = %s,
                    billing_due_at = %s, next_payment_attempt_at = %s,
                    payment_failed_at = %s, cancel_requested_at = %s, ended_at = %s,
                    status_changed_at = %s, updated_at = %s
                WHERE subscription_id = %s
                """,
                _subscription_update_parameters(record),
            )
            updated += 1
    return AxisMutationResult(inserted=inserted, updated=updated, skipped=skipped)


def persist_membership_tier_records(
    connection: psycopg.Connection, records: Iterable[MembershipTierRecord]
) -> AxisMutationResult:
    """사람 단위 등급 Record를 멱등 저장하고 역행 변경을 거부한다."""
    inserted = 0
    updated = 0
    skipped = 0
    for record in records:
        existing = _find_membership_tier_record(connection, record.customer_unique_id)
        if existing is None:
            connection.execute(
                """
                INSERT INTO customer_membership_tiers (
                    customer_unique_id, membership_tier, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s)
                """,
                _membership_tier_parameters(record),
            )
            inserted += 1
        elif existing == record:
            skipped += 1
        else:
            if record.created_at != existing.created_at:
                raise ValueError("Membership tier created_at cannot change")
            if record.updated_at <= existing.updated_at:
                raise ValueError(
                    "Membership tier changes require an updated_at greater than the current value"
                )
            connection.execute(
                """
                UPDATE customer_membership_tiers
                SET membership_tier = %s, updated_at = %s
                WHERE customer_unique_id = %s
                """,
                (record.membership_tier, record.updated_at, record.customer_unique_id),
            )
            updated += 1
    return AxisMutationResult(inserted=inserted, updated=updated, skipped=skipped)


def _new_order_customer_record(
    config: GeneratorConfig,
    *,
    customer_unique_id: str,
    order_ordinal: int,
    address: CustomerAddress,
    record_kind: str,
) -> CustomerRecord:
    """주문 시점 불변 Customer 계정의 공통 필드를 결정적으로 구성한다."""
    return CustomerRecord(
        customer_id=_deterministic_id(config, record_kind, customer_unique_id, order_ordinal),
        customer_unique_id=customer_unique_id,
        address=address,
        created_at=config.logical_date,
    )


def _find_customer_record(
    connection: psycopg.Connection, customer_id: str
) -> CustomerRecord | None:
    """저장 전 동일 Customer 계정을 Lock으로 보호해 읽는다."""
    row = connection.execute(
        """
        SELECT customer_id, customer_unique_id, customer_city, customer_state, created_at
        FROM customers WHERE customer_id = %s FOR UPDATE
        """,
        (customer_id,),
    ).fetchone()
    return (
        None
        if row is None
        else CustomerRecord(row[0], row[1], CustomerAddress(row[2], row[3]), row[4])
    )


def _find_subscription_record(
    connection: psycopg.Connection, subscription_id: UUID
) -> SubscriptionRecord | None:
    """저장 전 동일 구독 계약 상태를 Lock으로 보호해 읽는다."""
    row = connection.execute(
        """
        SELECT subscription_id, customer_unique_id, subscription_status, auto_renew_enabled,
               subscription_started_at, current_period_started_at, current_period_ends_at,
               billing_due_at, next_payment_attempt_at, payment_failed_at, cancel_requested_at,
               ended_at, status_changed_at, created_at, updated_at
        FROM customer_subscriptions WHERE subscription_id = %s FOR UPDATE
        """,
        (subscription_id,),
    ).fetchone()
    return None if row is None else SubscriptionRecord(*row)


def _find_membership_tier_record(
    connection: psycopg.Connection, customer_unique_id: str
) -> MembershipTierRecord | None:
    """저장 전 동일 사람 등급을 Lock으로 보호해 읽는다."""
    row = connection.execute(
        """
        SELECT customer_unique_id, membership_tier, created_at, updated_at
        FROM customer_membership_tiers WHERE customer_unique_id = %s FOR UPDATE
        """,
        (customer_unique_id,),
    ).fetchone()
    return None if row is None else MembershipTierRecord(*row)


def _customer_parameters(record: CustomerRecord) -> tuple[object, ...]:
    """INSERT에 사용할 Customer 계정 Source Column 순서를 반환한다."""
    return (
        record.customer_id,
        record.customer_unique_id,
        record.address.city,
        record.address.state,
        record.created_at,
    )


def _subscription_parameters(record: SubscriptionRecord) -> tuple[object, ...]:
    """INSERT에 사용할 구독 계약 상태 Source Column 순서를 반환한다."""
    return (
        record.subscription_id,
        record.customer_unique_id,
        record.subscription_status,
        record.auto_renew_enabled,
        record.subscription_started_at,
        record.current_period_started_at,
        record.current_period_ends_at,
        record.billing_due_at,
        record.next_payment_attempt_at,
        record.payment_failed_at,
        record.cancel_requested_at,
        record.ended_at,
        record.status_changed_at,
        record.created_at,
        record.updated_at,
    )


def _subscription_update_parameters(record: SubscriptionRecord) -> tuple[object, ...]:
    """UPDATE에 사용할 구독 계약 상태 Source Column 순서를 반환한다."""
    return (
        record.subscription_status,
        record.auto_renew_enabled,
        record.current_period_started_at,
        record.current_period_ends_at,
        record.billing_due_at,
        record.next_payment_attempt_at,
        record.payment_failed_at,
        record.cancel_requested_at,
        record.ended_at,
        record.status_changed_at,
        record.updated_at,
        record.subscription_id,
    )


def _membership_tier_parameters(record: MembershipTierRecord) -> tuple[object, ...]:
    """INSERT에 사용할 사람 등급 Source Column 순서를 반환한다."""
    return (
        record.customer_unique_id,
        record.membership_tier,
        record.created_at,
        record.updated_at,
    )


def _subscription_timestamps(
    record: SubscriptionRecord,
) -> tuple[tuple[str, datetime | None], ...]:
    """구독 상태 Record의 선택 시각 Column 이름과 값을 반환한다."""
    return (
        ("current_period_started_at", record.current_period_started_at),
        ("current_period_ends_at", record.current_period_ends_at),
        ("billing_due_at", record.billing_due_at),
        ("next_payment_attempt_at", record.next_payment_attempt_at),
        ("payment_failed_at", record.payment_failed_at),
        ("cancel_requested_at", record.cancel_requested_at),
        ("ended_at", record.ended_at),
    )


def _allowed_subscription_targets(status: str) -> frozenset[str]:
    """현재 구독 상태에서 허용되는 다음 상태 집합을 반환한다."""
    return {
        "ACTIVE": frozenset({"ACTIVE", "PAYMENT_FAILED", "CANCEL_REQUESTED"}),
        "PAYMENT_FAILED": frozenset({"ACTIVE", "CANCEL_REQUESTED", "CHURNED"}),
        "CANCEL_REQUESTED": frozenset({"ACTIVE", "CHURNED"}),
        "CHURNED": frozenset(),
    }[status]


def _transition_subscription_record(
    record: SubscriptionRecord, next_status: str, mutation_time: datetime
) -> SubscriptionRecord:
    """상태별 시각 정책을 적용해 다음 구독 상태 Record를 만든다."""
    if next_status == "ACTIVE":
        return replace(
            record,
            subscription_status="ACTIVE",
            auto_renew_enabled=True,
            current_period_started_at=mutation_time,
            current_period_ends_at=mutation_time + SUBSCRIPTION_PERIOD,
            billing_due_at=mutation_time + SUBSCRIPTION_PERIOD,
            next_payment_attempt_at=mutation_time + SUBSCRIPTION_PERIOD,
            payment_failed_at=None,
            cancel_requested_at=None,
            ended_at=None,
            status_changed_at=mutation_time,
            updated_at=mutation_time,
        )
    if next_status == "PAYMENT_FAILED":
        return replace(
            record,
            subscription_status="PAYMENT_FAILED",
            current_period_ends_at=mutation_time + PAYMENT_FAILURE_GRACE_PERIOD,
            billing_due_at=record.billing_due_at or mutation_time,
            next_payment_attempt_at=mutation_time + timedelta(days=2),
            payment_failed_at=mutation_time,
            cancel_requested_at=None,
            status_changed_at=mutation_time,
            updated_at=mutation_time,
        )
    if next_status == "CANCEL_REQUESTED":
        return replace(
            record,
            subscription_status="CANCEL_REQUESTED",
            auto_renew_enabled=False,
            next_payment_attempt_at=None,
            cancel_requested_at=mutation_time,
            status_changed_at=mutation_time,
            updated_at=mutation_time,
        )
    if next_status == "CHURNED":
        if record.current_period_ends_at is None or mutation_time < record.current_period_ends_at:
            raise ValueError("CHURNED requires logical_date at or after current_period_ends_at")
        return replace(
            record,
            subscription_status="CHURNED",
            auto_renew_enabled=False,
            billing_due_at=None,
            next_payment_attempt_at=None,
            ended_at=mutation_time,
            status_changed_at=mutation_time,
            updated_at=mutation_time,
        )
    raise ValueError(f"Unsupported subscription target: {next_status}")


def _latest_record_by_unique_id(records: Iterable[CustomerRecord]) -> dict[str, CustomerRecord]:
    """동일 인물의 여러 주문 계정 중 최신 주소 계정만 남긴다."""
    latest_records: dict[str, CustomerRecord] = {}
    for record in records:
        previous = latest_records.get(record.customer_unique_id)
        if previous is None or _record_sort_key(record) > _record_sort_key(previous):
            latest_records[record.customer_unique_id] = record
    return latest_records


def _choose_customer_unique_id(
    config: GeneratorConfig, latest_records: dict[str, CustomerRecord], order_ordinal: int
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
    alternatives = tuple(
        address for address in ADDRESS_CATALOG if address != _address_tuple(previous_address)
    )
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


def _record_sort_key(record: CustomerRecord) -> tuple[datetime, str]:
    """같은 인물의 최신 Customer 계정을 고르는 안정적인 정렬 Key를 반환한다."""
    return record.created_at, record.customer_id


def _address_tuple(address: CustomerAddress) -> tuple[str, str]:
    """주소 비교와 Hash 입력에 사용할 불변 Tuple을 반환한다."""
    return address.city, address.state


def _assert_positive_ordinal(order_ordinal: int) -> None:
    """주문 순번이 1부터 시작하는 정수인지 확인한다."""
    if isinstance(order_ordinal, bool) or not isinstance(order_ordinal, int) or order_ordinal < 1:
        raise ValueError("order_ordinal must be an integer greater than zero")


def _assert_utc_timestamp(value: datetime, name: str) -> None:
    """Source 저장 시각이 UTC-aware datetime인지 확인한다."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be normalized to UTC")
