"""신규 고객·주문 Bundle을 결정적으로 만들어 Source에 저장한다."""

from __future__ import annotations

from datetime import timedelta

import psycopg

from src.common.database import PostgresSettings
from src.generator.core.config import GeneratorConfig
from src.generator.core.lease import SourceMutationLease, assert_source_mutation_lease
from src.generator.customers.customers import new_customer_record
from src.generator.orders.orders import new_order_bundle, persist_order_bundle
from src.generator.orders.scenarios import late_order_bundle


def create_new_orders(
    connection: psycopg.Connection,
    settings: PostgresSettings,
    config: GeneratorConfig,
    lease: SourceMutationLease,
    catalog,
    result_counts: dict[str, int],
    logical_rows: list[dict],
) -> None:
    """order_count만큼 신규 주문 Bundle을 순서대로 만들어 저장하고 Hash 입력 Row를 남긴다."""
    for order_ordinal in range(1, config.order_count + 1):
        assert_source_mutation_lease(settings, lease)
        customer = new_customer_record(config, order_ordinal)
        bundle = _bundle_for_profile(config, customer, catalog, order_ordinal)
        mutation_result = persist_order_bundle(connection, bundle)
        _add_mutation_counts(result_counts, mutation_result)
        logical_rows.append(
            {
                "customer_id": bundle.customer.customer_id,
                "order_id": bundle.order.order_id,
                "item_ids": [item.order_item_id for item in bundle.items],
                "payment_sequences": [payment.payment_sequential for payment in bundle.payments],
            }
        )


def _bundle_for_profile(config: GeneratorConfig, customer, catalog, order_ordinal: int):
    """실행 가능한 Anomaly Profile에 맞는 결정적 Order Bundle을 만든다."""
    if config.anomaly_profile == "late-arrival":
        business_event_time = config.logical_date - timedelta(days=3 + order_ordinal % 3)
        return late_order_bundle(config, customer, catalog, order_ordinal, business_event_time)
    return new_order_bundle(config, customer, catalog, order_ordinal)


def _add_mutation_counts(result_counts: dict[str, int], mutation_result) -> None:
    """Order Bundle 저장 결과를 Generator Metadata의 Entity별 Count에 누적한다."""
    result_counts["customers_inserted"] += mutation_result.customer.inserted
    result_counts["customers_updated"] += mutation_result.customer.updated
    result_counts["customers_skipped"] += mutation_result.customer.skipped
    result_counts["subscriptions_inserted"] += mutation_result.subscription.inserted
    result_counts["subscriptions_updated"] += mutation_result.subscription.updated
    result_counts["subscriptions_skipped"] += mutation_result.subscription.skipped
    result_counts["membership_tiers_inserted"] += mutation_result.membership_tier.inserted
    result_counts["membership_tiers_updated"] += mutation_result.membership_tier.updated
    result_counts["membership_tiers_skipped"] += mutation_result.membership_tier.skipped
    result_counts["orders_inserted"] += mutation_result.orders_inserted
    result_counts["orders_skipped"] += mutation_result.orders_skipped
    result_counts["order_items_inserted"] += mutation_result.items_inserted
    result_counts["order_items_skipped"] += mutation_result.items_skipped
    result_counts["payments_inserted"] += mutation_result.payments_inserted
    result_counts["payments_skipped"] += mutation_result.payments_skipped
