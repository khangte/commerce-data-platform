"""새 Mutation Cursor로 도착한 Late Order와 연결 주문의 지연 결제가 과거 Mart를 정확히 갱신하는지 검증한다."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.generator.customers import (
    ensure_subscription_records,
    new_customer_record,
    new_membership_tier_record,
    new_subscription_record,
    persist_membership_tier_records,
    persist_subscription_records,
)
from src.generator.orders import new_order_bundle, persist_order_bundle
from src.generator.scenarios import (
    delayed_payment_transition,
    late_order_bundle,
    membership_change_scenario,
    subscription_transition_scenario,
)
from src.generator.subscription_payments import (
    persist_subscription_payments,
    plan_subscription_payment,
)
from src.generator.transitions import apply_payment_transition, fetch_payment_state
from src.ingestion.metadata import CursorPosition
from src.ingestion.service import TableIngestionRequest, TableIngestionResult, ingest_table
from src.ingestion.storage import SeaweedFSSettings
from src.warehouse.mart_hash import mart_logical_hashes, mart_row_counts
from tests.integration.test_order_e2e_and_late_order_mart_integration import (
    FIXTURE_START as ORDER_FIXTURE_START,
)
from tests.integration.test_order_e2e_and_late_order_mart_integration import (
    _assert_relationship_tests_passed,
    _fetch_catalog,
    _seed_watermarks,
)
from tests.integration.test_order_e2e_and_late_order_mart_integration import (
    _cleanup as _cleanup_order_fixture,
)
from tests.integration.test_order_e2e_and_late_order_mart_integration import (
    _generator_config as _order_generator_config,
)
from tests.integration.test_subscription_payment_temporal_join_integration import (
    _append_fixture_catalog,
    _combined_output,
    _create_fixture_catalog,
    _ingest,
    _run_dbt_build,
    _set_watermark,
)
from tests.reliability.harness import write_evidence

ORDER_FIXTURE_TABLES = (
    "customers",
    "customer_subscriptions",
    "customer_membership_tiers",
    "products",
    "sellers",
    "orders",
    "order_items",
    "order_payments",
)

R10_FIXTURE_TABLES = ORDER_FIXTURE_TABLES + ("subscription_payments",)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
        or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
        reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
    ),
]


def _assert_prefixed_tests_passed(target_path: Path, prefix: str) -> None:
    """지정한 접두사가 unique_id에 포함된 dbt Singular Test가 모두 통과했는지 확인한다."""
    payload = json.loads((target_path / "run_results.json").read_text(encoding="utf-8"))
    matching_nodes = [
        result
        for result in payload["results"]
        if result["unique_id"].startswith("test.") and prefix in result["unique_id"]
    ]
    assert matching_nodes, f"No dbt tests matched prefix {prefix!r}"
    failed_nodes = [
        (result["unique_id"], result.get("failures"))
        for result in matching_nodes
        if result["status"] != "pass" or result.get("failures") != 0
    ]
    assert failed_nodes == [], f"dbt tests failed for prefix {prefix!r}: {failed_nodes}"


def test_r08_a_late_order_updates_the_past_business_date_mart_once(tmp_path: Path) -> None:
    """3일 전 Business Time의 Late Order가 두 번째 Build에서 정확히 1회 수집되어 과거 날짜 Mart를 갱신한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_r08_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    mutation_time = ORDER_FIXTURE_START
    late_mutation_time = mutation_time + timedelta(hours=1)
    business_event_time = mutation_time - timedelta(days=3)
    baseline_config = _order_generator_config(mutation_time)
    late_config = _order_generator_config(late_mutation_time)
    catalog = _fetch_catalog(postgres)
    customer = new_customer_record(baseline_config, 1)
    subscription = new_subscription_record(customer)
    baseline_bundle = new_order_bundle(baseline_config, customer, catalog, order_ordinal=1)
    late_bundle = late_order_bundle(
        late_config, customer, catalog, order_ordinal=2, business_event_time=business_event_time
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection, connection.transaction():
            mutation = persist_order_bundle(connection, baseline_bundle)
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
        assert mutation.orders_inserted == 1

        _seed_watermarks(postgres, pipeline_name, baseline_bundle, ingested_at)
        for source_table in ORDER_FIXTURE_TABLES:
            results.append(
                _ingest(postgres, storage, pipeline_name, source_table, mutation_time, 1, tmp_path, ingested_at)
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        pre_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert pre_build.returncode == 0, _combined_output(pre_build)
        _assert_relationship_tests_passed(tmp_path / "dbt-target")

        pre_hashes = mart_logical_hashes(warehouse_path)
        pre_counts = mart_row_counts(warehouse_path)

        with postgres.source_connection() as connection, connection.transaction():
            late_mutation_result = persist_order_bundle(connection, late_bundle)
        assert late_mutation_result.orders_inserted == 1

        late_results = [
            _ingest(postgres, storage, pipeline_name, "orders", late_mutation_time, 2, tmp_path, ingested_at),
            _ingest(postgres, storage, pipeline_name, "order_items", late_mutation_time, 2, tmp_path, ingested_at),
            _ingest(postgres, storage, pipeline_name, "order_payments", late_mutation_time, 2, tmp_path, ingested_at),
        ]
        results.extend(late_results)
        _append_fixture_catalog(postgres, warehouse_path, late_results)
        post_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert post_build.returncode == 0, _combined_output(post_build)
        _assert_relationship_tests_passed(tmp_path / "dbt-target")

        post_hashes = mart_logical_hashes(warehouse_path)
        post_counts = mart_row_counts(warehouse_path)

        expected_date_key = int(business_event_time.strftime("%Y%m%d"))
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            late_rows = connection.execute(
                "SELECT purchase_date_key FROM facts.fct_order WHERE order_id = ?",
                [late_bundle.order.order_id],
            ).fetchall()
            baseline_row = connection.execute(
                "SELECT purchase_date_key FROM facts.fct_order WHERE order_id = ?",
                [baseline_bundle.order.order_id],
            ).fetchone()

        assert late_rows == [(expected_date_key,)]
        assert baseline_row == (int(mutation_time.strftime("%Y%m%d")),)
        assert post_counts["facts.fct_order"] == pre_counts["facts.fct_order"] + 1
        assert post_hashes["facts.fct_order"] != pre_hashes["facts.fct_order"]

        idle_request = TableIngestionRequest.for_dag_run(
            source_table="orders",
            dag_id=f"{pipeline_name}_3",
            logical_date=late_mutation_time + timedelta(hours=1),
            pipeline_name=pipeline_name,
        )
        idle_result = ingest_table(
            postgres, storage, idle_request, local_directory=tmp_path / "bronze", now=ingested_at
        )
        results.append(idle_result)
        assert idle_result.status == "SUCCESS_NO_DATA"
        assert idle_result.row_count == 0

        write_evidence(
            "r08",
            {
                "pipeline_name": pipeline_name,
                "late_order_id": late_bundle.order.order_id,
                "business_event_time": business_event_time.isoformat(),
                "pre_build_fct_order_row_count": pre_counts["facts.fct_order"],
                "post_build_fct_order_row_count": post_counts["facts.fct_order"],
                "pre_build_fct_order_hash": pre_hashes["facts.fct_order"],
                "post_build_fct_order_hash": post_hashes["facts.fct_order"],
                "late_order_purchase_date_key": expected_date_key,
                "late_order_fact_row_count": len(late_rows),
                "idle_reingest_row_count": idle_result.row_count,
            },
        )
    finally:
        _cleanup_order_fixture(postgres, storage, pipeline_name, results, customer.customer_unique_id)


def test_r09_a_late_payment_pulls_the_linked_order_purchase_date_into_the_affected_range(
    tmp_path: Path,
) -> None:
    """오래된 구매일을 가진 주문의 지연 결제 완료가 그 구매일을 영향 범위에 다시 포함시킨다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_r09_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    purchase_time = ORDER_FIXTURE_START - timedelta(days=10)
    late_mutation_time = ORDER_FIXTURE_START
    payment_completed_at = purchase_time + timedelta(hours=2)
    config = _order_generator_config(purchase_time)
    catalog = _fetch_catalog(postgres)
    customer = new_customer_record(config, 1)
    subscription = new_subscription_record(customer)
    bundle = new_order_bundle(config, customer, catalog, order_ordinal=1)
    order_id = bundle.order.order_id
    expected_date_key = int(purchase_time.strftime("%Y%m%d"))
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection, connection.transaction():
            mutation = persist_order_bundle(connection, bundle)
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
        assert mutation.orders_inserted == 1

        _seed_watermarks(postgres, pipeline_name, bundle, ingested_at)
        for source_table in ORDER_FIXTURE_TABLES:
            results.append(
                _ingest(postgres, storage, pipeline_name, source_table, purchase_time, 1, tmp_path, ingested_at)
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)
        _assert_relationship_tests_passed(tmp_path / "dbt-target")

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            pre_purchase_date_key = connection.execute(
                "SELECT purchase_date_key FROM facts.fct_order WHERE order_id = ?", [order_id]
            ).fetchone()
            pre_payment_status = connection.execute(
                "SELECT payment_status FROM facts.fct_order_payment "
                "WHERE order_id = ? AND payment_sequence = 1",
                [order_id],
            ).fetchone()
            pre_affected_count = connection.execute(
                "SELECT count(*) FROM control.affected_keys "
                "WHERE affected_domain = 'order' AND entity_key = ?",
                [order_id],
            ).fetchone()[0]
            pre_fct_order_payment_total = connection.execute(
                "SELECT payment_total FROM facts.fct_order WHERE order_id = ?", [order_id]
            ).fetchone()[0]
            pre_payment_value_sum = connection.execute(
                "SELECT sum(payment_value) FROM facts.fct_order_payment WHERE order_id = ?",
                [order_id],
            ).fetchone()[0]

        pre_hashes = mart_logical_hashes(warehouse_path)
        pre_counts = mart_row_counts(warehouse_path)

        assert pre_purchase_date_key == (expected_date_key,)
        assert pre_payment_status == ("PENDING",)
        assert pre_fct_order_payment_total == pre_payment_value_sum

        with postgres.source_connection() as connection:
            current_payment = fetch_payment_state(connection, order_id, 1)
        transition = delayed_payment_transition(current_payment, payment_completed_at, late_mutation_time)
        apply_payment_transition(postgres, transition)

        late_results = [
            _ingest(
                postgres, storage, pipeline_name, "order_payments", late_mutation_time, 2, tmp_path, ingested_at
            )
        ]
        assert late_results[0].row_count == 1
        results.extend(late_results)
        _append_fixture_catalog(postgres, warehouse_path, late_results)
        second_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)
        _assert_relationship_tests_passed(tmp_path / "dbt-target")

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            post_purchase_date_key = connection.execute(
                "SELECT purchase_date_key FROM facts.fct_order WHERE order_id = ?", [order_id]
            ).fetchone()
            post_payment_status = connection.execute(
                "SELECT payment_status FROM facts.fct_order_payment "
                "WHERE order_id = ? AND payment_sequence = 1",
                [order_id],
            ).fetchone()
            post_affected_rows = connection.execute(
                "SELECT business_date_key FROM control.affected_keys "
                "WHERE affected_domain = 'order' AND entity_key = ? ORDER BY recorded_at",
                [order_id],
            ).fetchall()
            post_fct_order_payment_total = connection.execute(
                "SELECT payment_total FROM facts.fct_order WHERE order_id = ?", [order_id]
            ).fetchone()[0]
            post_payment_value_sum = connection.execute(
                "SELECT sum(payment_value) FROM facts.fct_order_payment WHERE order_id = ?",
                [order_id],
            ).fetchone()[0]

        post_hashes = mart_logical_hashes(warehouse_path)
        post_counts = mart_row_counts(warehouse_path)

        assert post_purchase_date_key == (expected_date_key,)
        assert post_payment_status == ("COMPLETED",)
        assert len(post_affected_rows) == pre_affected_count + 1
        assert post_affected_rows[-1] == (expected_date_key,)
        assert post_fct_order_payment_total == post_payment_value_sum
        assert post_counts["facts.fct_order"] == pre_counts["facts.fct_order"]
        assert post_counts["facts.fct_order_payment"] == pre_counts["facts.fct_order_payment"]
        assert post_hashes["facts.fct_order_payment"] != pre_hashes["facts.fct_order_payment"]

        write_evidence(
            "r09",
            {
                "pipeline_name": pipeline_name,
                "order_id": order_id,
                "purchase_date_key": expected_date_key,
                "payment_status_before_late_payment": pre_payment_status[0],
                "payment_status_after_late_payment": post_payment_status[0],
                "affected_key_records_before": pre_affected_count,
                "affected_key_records_after": len(post_affected_rows),
                "fct_order_payment_total_before": float(pre_fct_order_payment_total),
                "fct_order_payment_total_after": float(post_fct_order_payment_total),
                "pre_build_fct_order_row_count": pre_counts["facts.fct_order"],
                "post_build_fct_order_row_count": post_counts["facts.fct_order"],
                "pre_build_fct_order_payment_row_count": pre_counts["facts.fct_order_payment"],
                "post_build_fct_order_payment_row_count": post_counts["facts.fct_order_payment"],
                "pre_build_fct_order_payment_hash": pre_hashes["facts.fct_order_payment"],
                "post_build_fct_order_payment_hash": post_hashes["facts.fct_order_payment"],
            },
        )
    finally:
        _cleanup_order_fixture(postgres, storage, pipeline_name, results, customer.customer_unique_id)


def test_r10_a_subscription_or_tier_change_opens_a_new_version_and_rebinds_events(
    tmp_path: Path,
) -> None:
    """등급 변경과 구독 상태 전이가 각각 새 SCD2 Version을 열고 그 이후 사건이 새 Version에 결합한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_r10_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    before_time = ORDER_FIXTURE_START
    change_time = before_time + timedelta(days=5)
    after_time = change_time + timedelta(days=1)
    before_config = _order_generator_config(before_time)
    change_config = _order_generator_config(change_time)
    after_config = _order_generator_config(after_time)
    catalog = _fetch_catalog(postgres)
    customer = new_customer_record(before_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    before_bundle = new_order_bundle(before_config, customer, catalog, order_ordinal=1)
    after_bundle = new_order_bundle(after_config, customer, catalog, order_ordinal=2)
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection, connection.transaction():
            mutation = persist_order_bundle(connection, before_bundle)
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
        assert mutation.orders_inserted == 1
        assert mutation.membership_tier.inserted == 1

        before_payment = plan_subscription_payment(
            before_config, subscription.subscription_id, 1, 1, subscription.current_period_started_at
        )
        with postgres.source_connection() as connection, connection.transaction():
            assert persist_subscription_payments(connection, (before_payment,)) == 1

        _seed_watermarks(postgres, pipeline_name, before_bundle, ingested_at)
        _set_watermark(
            postgres,
            pipeline_name,
            "subscription_payments",
            CursorPosition(
                before_payment.updated_at - timedelta(microseconds=1),
                (str(before_payment.payment_id),),
            ),
            now=ingested_at,
        )
        for source_table in R10_FIXTURE_TABLES:
            results.append(
                _ingest(postgres, storage, pipeline_name, source_table, before_time, 1, tmp_path, ingested_at)
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)
        _assert_relationship_tests_passed(tmp_path / "dbt-target")
        _assert_prefixed_tests_passed(tmp_path / "dbt-target", "dim_customer_")
        _assert_prefixed_tests_passed(tmp_path / "dbt-target", "dim_subscription_")

        changed_tier = membership_change_scenario(change_config, (tier,), delivered_order_count=5)
        changed_subscription = subscription_transition_scenario(
            change_config, (subscription,), "PAYMENT_FAILED"
        )
        with postgres.source_connection() as connection, connection.transaction():
            assert persist_membership_tier_records(connection, changed_tier).updated == 1
            assert persist_subscription_records(connection, changed_subscription).updated == 1

        after_payment = plan_subscription_payment(
            after_config, subscription.subscription_id, 2, 1, subscription.current_period_started_at
        )
        with postgres.source_connection() as connection, connection.transaction():
            after_mutation = persist_order_bundle(connection, after_bundle)
            assert persist_subscription_payments(connection, (after_payment,)) == 1
        assert after_mutation.orders_inserted == 1

        second_results = [
            _ingest(
                postgres, storage, pipeline_name, "customer_membership_tiers", change_time, 2, tmp_path, ingested_at
            ),
            _ingest(
                postgres, storage, pipeline_name, "customer_subscriptions", change_time, 2, tmp_path, ingested_at
            ),
            _ingest(postgres, storage, pipeline_name, "orders", after_time, 2, tmp_path, ingested_at),
            _ingest(postgres, storage, pipeline_name, "order_items", after_time, 2, tmp_path, ingested_at),
            _ingest(postgres, storage, pipeline_name, "order_payments", after_time, 2, tmp_path, ingested_at),
            _ingest(postgres, storage, pipeline_name, "subscription_payments", after_time, 2, tmp_path, ingested_at),
        ]
        results.extend(second_results)
        _append_fixture_catalog(postgres, warehouse_path, second_results)
        second_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)
        _assert_relationship_tests_passed(tmp_path / "dbt-target")
        _assert_prefixed_tests_passed(tmp_path / "dbt-target", "dim_customer_")
        _assert_prefixed_tests_passed(tmp_path / "dbt-target", "dim_subscription_")

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            customer_versions = connection.execute(
                "SELECT customer_key, membership_tier, valid_from, valid_to, is_current, "
                "effective_from = timestamptz '-infinity' AS effective_from_is_negative_infinity "
                "FROM dimensions.dim_customer WHERE customer_id = ? ORDER BY valid_from",
                [customer.customer_unique_id],
            ).fetchall()
            subscription_versions = connection.execute(
                "SELECT subscription_key, subscription_status, valid_from, valid_to, is_current, "
                "effective_from = timestamptz '-infinity' AS effective_from_is_negative_infinity "
                "FROM dimensions.dim_subscription WHERE subscription_id = ? ORDER BY valid_from",
                [str(subscription.subscription_id)],
            ).fetchall()
            before_order_customer_key = connection.execute(
                "SELECT customer_key FROM facts.fct_order WHERE order_id = ?",
                [before_bundle.order.order_id],
            ).fetchone()[0]
            after_order_customer_key = connection.execute(
                "SELECT customer_key FROM facts.fct_order WHERE order_id = ?",
                [after_bundle.order.order_id],
            ).fetchone()[0]
            before_payment_subscription_key = connection.execute(
                "SELECT subscription_key FROM facts.fct_subscription_payment WHERE payment_id = ?",
                [str(before_payment.payment_id)],
            ).fetchone()[0]
            after_payment_subscription_key = connection.execute(
                "SELECT subscription_key FROM facts.fct_subscription_payment WHERE payment_id = ?",
                [str(after_payment.payment_id)],
            ).fetchone()[0]

        assert len(customer_versions) == 2
        assert len(subscription_versions) == 2

        first_customer_version, second_customer_version = customer_versions
        first_subscription_version, second_subscription_version = subscription_versions

        assert first_customer_version[1] == "BRONZE"
        assert second_customer_version[1] == "SILVER"
        assert first_customer_version[4] is False
        assert second_customer_version[4] is True
        assert first_customer_version[5] is True
        assert second_customer_version[5] is False
        assert first_customer_version[3] == second_customer_version[2]

        assert first_subscription_version[1] == "ACTIVE"
        assert second_subscription_version[1] == "PAYMENT_FAILED"
        assert first_subscription_version[4] is False
        assert second_subscription_version[4] is True
        assert first_subscription_version[5] is True
        assert second_subscription_version[5] is False
        assert first_subscription_version[3] == second_subscription_version[2]

        assert before_order_customer_key == first_customer_version[0]
        assert after_order_customer_key == second_customer_version[0]
        assert before_payment_subscription_key == first_subscription_version[0]
        assert after_payment_subscription_key == second_subscription_version[0]

        write_evidence(
            "r10",
            {
                "pipeline_name": pipeline_name,
                "customer_unique_id": customer.customer_unique_id,
                "subscription_id": str(subscription.subscription_id),
                "customer_version_count": len(customer_versions),
                "subscription_version_count": len(subscription_versions),
                "membership_tier_before": first_customer_version[1],
                "membership_tier_after": second_customer_version[1],
                "subscription_status_before": first_subscription_version[1],
                "subscription_status_after": second_subscription_version[1],
                "before_order_customer_key": before_order_customer_key,
                "after_order_customer_key": after_order_customer_key,
                "before_payment_subscription_key": before_payment_subscription_key,
                "after_payment_subscription_key": after_payment_subscription_key,
            },
        )
    finally:
        with postgres.source_connection() as connection:
            connection.execute(
                "DELETE FROM subscription_payments WHERE subscription_id = %s",
                (subscription.subscription_id,),
            )
            connection.commit()
        _cleanup_order_fixture(postgres, storage, pipeline_name, results, customer.customer_unique_id)
