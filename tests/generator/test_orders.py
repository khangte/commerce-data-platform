"""결정적 Order·Item·Payment Bundle 생성 계약을 검증한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import new_customer_record
from src.generator.ids import logical_hash
from src.generator.orders import (
    OrderBundle,
    OrderCatalog,
    ProductReference,
    SellerReference,
    new_order_bundle,
)


def _config() -> GeneratorConfig:
    """Order Bundle 생성에 사용할 고정 Generator Config를 반환한다."""
    return GeneratorConfig(
        source_snapshot_id="seed:abc123",
        random_seed=42,
        logical_date=datetime(2026, 9, 4, tzinfo=UTC),
        order_count=10,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )


def _catalog() -> OrderCatalog:
    """결정성 테스트에 사용할 정렬된 Product·Seller 후보를 반환한다."""
    return OrderCatalog(
        products=(ProductReference("product-1"), ProductReference("product-2")),
        sellers=(SellerReference("seller-1"), SellerReference("seller-2")),
    )


def test_new_order_bundle_is_stable_and_preserves_source_grains() -> None:
    """같은 Config·Customer·Catalog 입력은 같은 Order Bundle을 만든다."""
    customer = new_customer_record(_config(), 1)

    first = new_order_bundle(_config(), customer, _catalog(), 1)
    second = new_order_bundle(_config(), customer, _catalog(), 1)

    assert first == second
    assert first.order.customer_id == customer.customer_id
    assert first.order.order_status == "created"
    assert [item.order_item_id for item in first.items] == list(range(1, len(first.items) + 1))
    assert [payment.payment_sequential for payment in first.payments] == [1]
    assert first.payments[0].payment_status == "pending"
    assert sum((item.price + item.freight_value for item in first.items), Decimal("0.00")) == sum(
        (payment.payment_value for payment in first.payments), Decimal("0.00")
    )


def test_same_snapshot_input_reproduces_bundle_keys_counts_statuses_and_hash() -> None:
    """동일 Snapshot과 입력은 Bundle Key·Count·상태·Logical Hash를 재현한다."""
    first = _bundle_evidence(_config())
    second = _bundle_evidence(_config())

    assert first == second


def _bundle_evidence(config: GeneratorConfig) -> dict[str, object]:
    """동일 Snapshot 재현 검증에 필요한 Bundle의 논리 증적을 반환한다."""
    bundles = tuple(
        new_order_bundle(config, new_customer_record(config, ordinal), _catalog(), ordinal)
        for ordinal in range(1, config.order_count + 1)
    )
    rows = [
        {
            "customer_id": bundle.customer.customer_id,
            "order_id": bundle.order.order_id,
            "order_status": bundle.order.order_status,
            "item_ids": [item.order_item_id for item in bundle.items],
            "payment_statuses": [payment.payment_status for payment in bundle.payments],
        }
        for bundle in bundles
    ]
    return {
        "key_set": {
            "customers": [bundle.customer.customer_id for bundle in bundles],
            "orders": [bundle.order.order_id for bundle in bundles],
        },
        "counts": {
            "customers": len(bundles),
            "orders": len(bundles),
            "items": sum(len(bundle.items) for bundle in bundles),
            "payments": sum(len(bundle.payments) for bundle in bundles),
        },
        "rows": rows,
        "logical_hash": logical_hash(rows),
    }


def test_order_bundle_rejects_payment_total_that_differs_from_item_total() -> None:
    """Payment 합계가 Item 가격과 배송비 합계와 다르면 Bundle 생성을 거부한다."""
    customer = new_customer_record(_config(), 1)
    bundle = new_order_bundle(_config(), customer, _catalog(), 1)
    payment = replace(bundle.payments[0], payment_value=bundle.payments[0].payment_value + Decimal("1.00"))

    with pytest.raises(ValueError, match="Payment total"):
        OrderBundle(customer=bundle.customer, order=bundle.order, items=bundle.items, payments=(payment,))


def test_order_catalog_requires_seeded_product_and_seller_candidates() -> None:
    """신규 Order는 기존 Seed Product와 Seller 후보가 있어야 한다."""
    with pytest.raises(ValueError, match="product"):
        OrderCatalog(products=(), sellers=(SellerReference("seller-1"),))
    with pytest.raises(ValueError, match="seller"):
        OrderCatalog(products=(ProductReference("product-1"),), sellers=())
