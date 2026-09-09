"""결정적 Order·Item·Payment 묶음의 생성과 원자적 Source 저장을 제공한다."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg

from src.common.database import PostgresSettings
from src.generator.config import GeneratorConfig
from src.generator.customers import (
    CustomerMutationResult,
    CustomerRecord,
    MembershipMutationResult,
    ensure_membership_records,
    new_membership_record,
    persist_customer_records,
)
from src.generator.ids import deterministic_uuid, logical_hash

PAYMENT_TYPES = ("credit_card", "boleto", "voucher")


@dataclass(frozen=True)
class ProductReference:
    """Order Item 생성에 사용할 기존 Source Product 식별자다."""

    product_id: str


@dataclass(frozen=True)
class SellerReference:
    """Order Item 생성에 사용할 기존 Source Seller 식별자다."""

    seller_id: str


@dataclass(frozen=True)
class OrderCatalog:
    """결정적 Order Bundle에 사용할 정렬된 Product·Seller 후보 묶음이다."""

    products: tuple[ProductReference, ...]
    sellers: tuple[SellerReference, ...]

    def __post_init__(self) -> None:
        """Order 생성에 필요한 Product와 Seller 후보가 존재하는지 확인한다."""
        if not self.products:
            raise ValueError("Order generation requires at least one product")
        if not self.sellers:
            raise ValueError("Order generation requires at least one seller")


@dataclass(frozen=True)
class OrderRecord:
    """Source `orders` Table에 저장할 신규 Order Row다."""

    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: datetime
    order_approved_at: datetime | None
    order_delivered_carrier_date: datetime | None
    order_delivered_customer_date: datetime | None
    order_estimated_delivery_date: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class OrderItemRecord:
    """Source `order_items` Table에 저장할 Composite Key Item Row다."""

    order_id: str
    order_item_id: int
    product_id: str
    seller_id: str
    price: Decimal
    freight_value: Decimal
    created_at: datetime


@dataclass(frozen=True)
class PaymentRecord:
    """Source `order_payments` Table에 저장할 순차 결제 Row다."""

    order_id: str
    payment_sequential: int
    payment_type: str
    payment_installments: int | None
    payment_value: Decimal
    payment_status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class OrderBundle:
    """하나의 Customer·Order·Item·Payment Transaction 단위다."""

    customer: CustomerRecord
    order: OrderRecord
    items: tuple[OrderItemRecord, ...]
    payments: tuple[PaymentRecord, ...]

    def __post_init__(self) -> None:
        """Bundle 내부 FK와 Composite Key·Payment 합계 계약을 확인한다."""
        if self.order.customer_id != self.customer.customer_id:
            raise ValueError("Order customer_id must reference the bundle customer")
        if not self.items:
            raise ValueError("Order bundles require at least one item")
        if not self.payments:
            raise ValueError("Order bundles require at least one payment")
        expected_item_ids = tuple(range(1, len(self.items) + 1))
        if tuple(item.order_item_id for item in self.items) != expected_item_ids:
            raise ValueError("order_item_id values must be sequential from 1")
        expected_payment_ids = tuple(range(1, len(self.payments) + 1))
        if tuple(payment.payment_sequential for payment in self.payments) != expected_payment_ids:
            raise ValueError("payment_sequential values must be sequential from 1")
        if any(item.order_id != self.order.order_id for item in self.items):
            raise ValueError("All items must reference the bundle order")
        if any(payment.order_id != self.order.order_id for payment in self.payments):
            raise ValueError("All payments must reference the bundle order")
        item_total = sum((item.price + item.freight_value for item in self.items), Decimal("0.00"))
        payment_total = sum((payment.payment_value for payment in self.payments), Decimal("0.00"))
        if item_total != payment_total:
            raise ValueError("Payment total must equal item price plus freight total")


@dataclass(frozen=True)
class OrderBundleMutationResult:
    """Order Bundle 저장 결과의 Entity별 Insert·Skip 건수다."""

    customer: CustomerMutationResult
    membership: MembershipMutationResult
    orders_inserted: int
    orders_skipped: int
    items_inserted: int
    items_skipped: int
    payments_inserted: int
    payments_skipped: int


def fetch_order_catalog(connection: psycopg.Connection) -> OrderCatalog:
    """기존 Source에서 정렬된 Product·Seller 후보를 읽어 반환한다."""
    products = tuple(
        ProductReference(product_id=row[0])
        for row in connection.execute(
            'SELECT product_id FROM products ORDER BY product_id COLLATE "C"'
        )
    )
    sellers = tuple(
        SellerReference(seller_id=row[0])
        for row in connection.execute(
            'SELECT seller_id FROM sellers ORDER BY seller_id COLLATE "C"'
        )
    )
    return OrderCatalog(products=products, sellers=sellers)


def new_order_bundle(
    config: GeneratorConfig,
    customer: CustomerRecord,
    catalog: OrderCatalog,
    order_ordinal: int,
) -> OrderBundle:
    """Config·Customer·Catalog에서 결정적으로 생성한 신규 Order Bundle을 반환한다."""
    _assert_positive_ordinal(order_ordinal)
    order_id = _deterministic_id(config, "order", customer.customer_id, order_ordinal)
    order = OrderRecord(
        order_id=order_id,
        customer_id=customer.customer_id,
        order_status="created",
        order_purchase_timestamp=config.logical_date,
        order_approved_at=None,
        order_delivered_carrier_date=None,
        order_delivered_customer_date=None,
        order_estimated_delivery_date=config.logical_date + timedelta(days=7),
        created_at=config.logical_date,
        updated_at=config.logical_date,
    )
    item_count = 1 + _selector(config, "item-count", order_ordinal) % 3
    items = tuple(
        _new_item_record(config, catalog, order_id, order_ordinal, item_ordinal)
        for item_ordinal in range(1, item_count + 1)
    )
    payment = PaymentRecord(
        order_id=order_id,
        payment_sequential=1,
        payment_type=PAYMENT_TYPES[
            _selector(config, "payment-type", order_ordinal) % len(PAYMENT_TYPES)
        ],
        payment_installments=None,
        payment_value=sum((item.price + item.freight_value for item in items), Decimal("0.00")),
        payment_status="pending",
        created_at=config.logical_date,
        updated_at=config.logical_date,
    )
    payment = _with_payment_installments(payment, config, order_ordinal)
    return OrderBundle(customer=customer, order=order, items=items, payments=(payment,))


def apply_order_bundle(
    settings: PostgresSettings, bundle: OrderBundle
) -> OrderBundleMutationResult:
    """Customer·Order·Item·Payment Bundle을 하나의 Source Transaction으로 저장한다."""
    with settings.source_connection() as connection, connection.transaction():
        return persist_order_bundle(connection, bundle)


def persist_order_bundle(
    connection: psycopg.Connection, bundle: OrderBundle
) -> OrderBundleMutationResult:
    """외부 Transaction 안에서 Order Bundle을 멱등적으로 저장한다."""
    customer_result = persist_customer_records(connection, (bundle.customer,))
    membership_result = ensure_membership_records(
        connection, (new_membership_record(bundle.customer),)
    )
    orders_inserted, orders_skipped = _persist_order(connection, bundle.order)
    items_inserted, items_skipped = _persist_items(connection, bundle.items)
    payments_inserted, payments_skipped = _persist_payments(connection, bundle.payments)
    return OrderBundleMutationResult(
        customer=customer_result,
        membership=membership_result,
        orders_inserted=orders_inserted,
        orders_skipped=orders_skipped,
        items_inserted=items_inserted,
        items_skipped=items_skipped,
        payments_inserted=payments_inserted,
        payments_skipped=payments_skipped,
    )


def _new_item_record(
    config: GeneratorConfig,
    catalog: OrderCatalog,
    order_id: str,
    order_ordinal: int,
    item_ordinal: int,
) -> OrderItemRecord:
    """Product·Seller·금액이 결정된 신규 Order Item을 반환한다."""
    product = catalog.products[
        _selector(config, "item-product", order_ordinal, item_ordinal) % len(catalog.products)
    ]
    seller = catalog.sellers[
        _selector(config, "item-seller", order_ordinal, item_ordinal) % len(catalog.sellers)
    ]
    return OrderItemRecord(
        order_id=order_id,
        order_item_id=item_ordinal,
        product_id=product.product_id,
        seller_id=seller.seller_id,
        price=_amount(
            config, "item-price", order_ordinal, item_ordinal, minimum_cents=1_000, span=99_001
        ),
        freight_value=_amount(
            config, "item-freight", order_ordinal, item_ordinal, minimum_cents=100, span=9_901
        ),
        created_at=config.logical_date,
    )


def _with_payment_installments(
    payment: PaymentRecord, config: GeneratorConfig, order_ordinal: int
) -> PaymentRecord:
    """결제 수단에 맞는 결정적 할부 횟수가 반영된 Payment를 반환한다."""
    if payment.payment_type != "credit_card":
        return payment
    return PaymentRecord(
        order_id=payment.order_id,
        payment_sequential=payment.payment_sequential,
        payment_type=payment.payment_type,
        payment_installments=1 + _selector(config, "payment-installments", order_ordinal) % 12,
        payment_value=payment.payment_value,
        payment_status=payment.payment_status,
        created_at=payment.created_at,
        updated_at=payment.updated_at,
    )


def _persist_order(connection: psycopg.Connection, order: OrderRecord) -> tuple[int, int]:
    """신규 Order Insert 또는 동일 Order Skip 결과를 반환한다."""
    desired = _order_parameters(order)
    existing = connection.execute(
        """
        SELECT order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
               order_delivered_carrier_date, order_delivered_customer_date,
               order_estimated_delivery_date, created_at, updated_at
        FROM orders
        WHERE order_id = %s
        FOR UPDATE
        """,
        (order.order_id,),
    ).fetchone()
    if existing is None:
        connection.execute(
            """
            INSERT INTO orders (
                order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
                order_delivered_carrier_date, order_delivered_customer_date,
                order_estimated_delivery_date, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            desired,
        )
        return 1, 0
    if tuple(existing) != desired:
        raise ValueError("Existing order differs from the deterministic expected value")
    return 0, 1


def _persist_items(
    connection: psycopg.Connection, items: Iterable[OrderItemRecord]
) -> tuple[int, int]:
    """신규 Item Insert와 동일 Item Skip 건수를 반환한다."""
    inserted = 0
    skipped = 0
    for item in items:
        desired = _item_parameters(item)
        existing = connection.execute(
            """
            SELECT order_id, order_item_id, product_id, seller_id, price, freight_value, created_at
            FROM order_items
            WHERE order_id = %s AND order_item_id = %s
            FOR UPDATE
            """,
            (item.order_id, item.order_item_id),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO order_items (
                    order_id, order_item_id, product_id, seller_id, price, freight_value, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                desired,
            )
            inserted += 1
        elif tuple(existing) == desired:
            skipped += 1
        else:
            raise ValueError("Existing order item differs from the deterministic expected value")
    return inserted, skipped


def _persist_payments(
    connection: psycopg.Connection, payments: Iterable[PaymentRecord]
) -> tuple[int, int]:
    """신규 Payment Insert와 동일 Payment Skip 건수를 반환한다."""
    inserted = 0
    skipped = 0
    for payment in payments:
        desired = _payment_parameters(payment)
        existing = connection.execute(
            """
            SELECT order_id, payment_sequential, payment_type, payment_installments,
                   payment_value, payment_status, created_at, updated_at
            FROM order_payments
            WHERE order_id = %s AND payment_sequential = %s
            FOR UPDATE
            """,
            (payment.order_id, payment.payment_sequential),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO order_payments (
                    order_id, payment_sequential, payment_type, payment_installments,
                    payment_value, payment_status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                desired,
            )
            inserted += 1
        elif tuple(existing) == desired:
            skipped += 1
        else:
            raise ValueError("Existing payment differs from the deterministic expected value")
    return inserted, skipped


def _order_parameters(order: OrderRecord) -> tuple[object, ...]:
    """`orders` INSERT와 비교에 사용할 Source Column 값을 반환한다."""
    return (
        order.order_id,
        order.customer_id,
        order.order_status,
        order.order_purchase_timestamp,
        order.order_approved_at,
        order.order_delivered_carrier_date,
        order.order_delivered_customer_date,
        order.order_estimated_delivery_date,
        order.created_at,
        order.updated_at,
    )


def _item_parameters(item: OrderItemRecord) -> tuple[object, ...]:
    """`order_items` INSERT와 비교에 사용할 Source Column 값을 반환한다."""
    return (
        item.order_id,
        item.order_item_id,
        item.product_id,
        item.seller_id,
        item.price,
        item.freight_value,
        item.created_at,
    )


def _payment_parameters(payment: PaymentRecord) -> tuple[object, ...]:
    """`order_payments` INSERT와 비교에 사용할 Source Column 값을 반환한다."""
    return (
        payment.order_id,
        payment.payment_sequential,
        payment.payment_type,
        payment.payment_installments,
        payment.payment_value,
        payment.payment_status,
        payment.created_at,
        payment.updated_at,
    )


def _selector(config: GeneratorConfig, entity_name: str, *components: object) -> int:
    """선택 순번을 만드는 결정적 정수 Hash를 반환한다."""
    value = logical_hash(
        {
            "generator_inputs": config.deterministic_inputs(),
            "entity_name": entity_name,
            "components": components,
        }
    )
    return int(value, 16)


def _amount(
    config: GeneratorConfig,
    entity_name: str,
    *components: object,
    minimum_cents: int,
    span: int,
) -> Decimal:
    """최소값과 범위 안에서 결정적으로 계산한 금액을 반환한다."""
    cents = minimum_cents + _selector(config, entity_name, *components) % span
    return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))


def _deterministic_id(config: GeneratorConfig, entity_name: str, *components: object) -> str:
    """Source VARCHAR 식별자에 맞는 32자리 UUIDv5 Hex 문자열을 반환한다."""
    return deterministic_uuid(entity_name, config.deterministic_inputs(), *components).hex


def _assert_positive_ordinal(order_ordinal: int) -> None:
    """주문 순번이 1부터 시작하는 정수인지 확인한다."""
    if isinstance(order_ordinal, bool) or not isinstance(order_ordinal, int) or order_ordinal < 1:
        raise ValueError("order_ordinal must be an integer greater than or equal to 1")
