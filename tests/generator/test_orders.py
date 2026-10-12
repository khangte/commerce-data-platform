"""결정적 Order·Item·Payment Bundle 생성 계약을 검증한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
    estimated_delivery_date,
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
    assert first.payments[0].payment_initiated_at == _config().logical_date
    assert first.payments[0].payment_completed_at is None
    assert first.payments[0].payment_failed_at is None
    assert first.payments[0].payment_refunded_at is None
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


def test_scale_one_keeps_seven_day_estimate() -> None:
    """배율 1의 추정 배송일은 구매 + 7일이다."""
    purchase_at = datetime(2026, 9, 4, tzinfo=UTC)
    assert estimated_delivery_date(purchase_at, 1) == purchase_at + timedelta(days=7)


def test_scaled_bundle_shrinks_estimate_and_shipping_limit() -> None:
    """배율 144는 추정 배송일 7일과 아이템 출고 기한 4~9일 지연을 144로 나눈다."""
    scaled_config = replace(_config(), progress_time_scale=144)
    customer = new_customer_record(scaled_config, 1)
    scaled = new_order_bundle(scaled_config, customer, _catalog(), 1)
    logical_date = scaled_config.logical_date
    assert scaled.order.order_estimated_delivery_date == logical_date + timedelta(days=7) / 144
    allowed_limits = {timedelta(days=days) / 144 for days in range(4, 10)}
    for item in scaled.items:
        assert item.shipping_limit_date - logical_date in allowed_limits
