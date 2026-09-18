"""결정적 구독 결제가 Bronze와 dbt Temporal Join까지 이어지는지 검증한다."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.customers import (
    ensure_membership_tier_records,
    ensure_subscription_records,
    new_customer_record,
    new_membership_tier_record,
    new_subscription_record,
    persist_customer_records,
    persist_subscription_records,
    subscription_transition_records,
)
from src.generator.subscription_payments import (
    persist_subscription_payments,
    plan_subscription_payment,
)
from src.ingestion.metadata import CursorPosition, get_or_create_watermark
from src.ingestion.service import TableIngestionRequest, TableIngestionResult, ingest_table
from src.ingestion.storage import SeaweedFSSettings, seaweedfs_s3_client

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_SNAPSHOT_ID = "fixture:subscription-payment-temporal-join"
FIXTURE_RANDOM_SEED = 20260911
FIXTURE_START = datetime(2100, 1, 1, tzinfo=UTC)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_subscription_payment_uses_the_active_customer_version_at_billing_time(tmp_path) -> None:
    """결정적 구독 전이·결제가 Bronze 이력과 dbt 시점 결합에서 한 행으로 이어진다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_subscription_temporal_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    payment_at = FIXTURE_START + timedelta(days=1)
    payment_config = _generator_config(payment_at, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    initial_subscription = new_subscription_record(customer)
    initial_tier = new_membership_tier_record(customer)
    payment = plan_subscription_payment(
        payment_config,
        initial_subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    results: list[TableIngestionResult] = []

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (initial_subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (initial_tier,)).inserted == 1
            connection.commit()

        _set_watermark(
            postgres,
            pipeline_name,
            "customer_subscriptions",
            CursorPosition(
                initial_subscription.updated_at - timedelta(microseconds=1),
                (str(initial_subscription.subscription_id),),
            ),
            now=ingested_at,
        )
        _set_watermark(
            postgres,
            pipeline_name,
            "customer_membership_tiers",
            CursorPosition(
                initial_tier.updated_at - timedelta(microseconds=1),
                (customer.customer_unique_id,),
            ),
            now=ingested_at,
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_subscriptions",
                initial_subscription.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_membership_tiers",
                initial_tier.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )

        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (payment,)) == 1
            connection.commit()

        _set_watermark(
            postgres,
            pipeline_name,
            "subscription_payments",
            CursorPosition(
                payment.updated_at - timedelta(microseconds=1),
                (str(payment.payment_id),),
            ),
            now=ingested_at,
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "subscription_payments",
                payment.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )

        assert [result.row_count for result in results] == [1, 1, 1]
        warehouse_path = tmp_path / "warehouse.duckdb"
        _create_fixture_catalog(postgres, warehouse_path, results)
        dbt_result = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert dbt_result.returncode == 0, _combined_output(dbt_result)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            fact_rows = connection.execute(
                """
                SELECT
                    fact.customer_key,
                    fact.payment_status,
                    fact.payment_value,
                    subscription.subscription_status,
                    customer.membership_tier,
                    subscription.valid_from,
                    subscription.valid_to
                FROM facts.fact_subscription_payments AS fact
                LEFT JOIN dimensions.dim_subscription AS subscription USING (subscription_key)
                LEFT JOIN dimensions.dim_customer AS customer USING (customer_key)
                WHERE fact.payment_id = ?
                """,
                [str(payment.payment_id)],
            ).fetchall()

        assert len(fact_rows) == 1
        fact_row = fact_rows[0]
        assert fact_row[0] is not None
        assert fact_row == (
            fact_row[0],
            payment.payment_status,
            payment.payment_value,
            "ACTIVE",
            "BRONZE",
            FIXTURE_START,
            None,
        )
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_subscription_payment_before_subscription_first_observation_still_resolves_keys(
    tmp_path,
) -> None:
    """구독 결제가 구독·고객 첫 관측보다 앞서도(Early-Arriving Fact) subscription_key·customer_key는 NULL이 되지 않는다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_subscription_early_arriving_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    early_payment_at = FIXTURE_START - timedelta(days=3)
    payment_config = _generator_config(early_payment_at, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    initial_subscription = new_subscription_record(customer)
    initial_tier = new_membership_tier_record(customer)
    payment = plan_subscription_payment(
        payment_config,
        initial_subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=early_payment_at,
    )
    results: list[TableIngestionResult] = []

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (initial_subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (initial_tier,)).inserted == 1
            connection.commit()

        _set_watermark(
            postgres,
            pipeline_name,
            "customer_subscriptions",
            CursorPosition(
                initial_subscription.updated_at - timedelta(microseconds=1),
                (str(initial_subscription.subscription_id),),
            ),
            now=ingested_at,
        )
        _set_watermark(
            postgres,
            pipeline_name,
            "customer_membership_tiers",
            CursorPosition(
                initial_tier.updated_at - timedelta(microseconds=1),
                (customer.customer_unique_id,),
            ),
            now=ingested_at,
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_subscriptions",
                initial_subscription.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_membership_tiers",
                initial_tier.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )

        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (payment,)) == 1
            connection.commit()

        _set_watermark(
            postgres,
            pipeline_name,
            "subscription_payments",
            CursorPosition(
                payment.updated_at - timedelta(microseconds=1),
                (str(payment.payment_id),),
            ),
            now=ingested_at,
        )
        results.append(
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "subscription_payments",
                payment.updated_at,
                1,
                tmp_path,
                ingested_at,
            )
        )

        assert [result.row_count for result in results] == [1, 1, 1]
        warehouse_path = tmp_path / "warehouse.duckdb"
        _create_fixture_catalog(postgres, warehouse_path, results)
        dbt_result = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert dbt_result.returncode == 0, _combined_output(dbt_result)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            fact_rows = connection.execute(
                """
                SELECT fact.subscription_key, fact.customer_key
                FROM facts.fact_subscription_payments AS fact
                WHERE fact.payment_id = ?
                """,
                [str(payment.payment_id)],
            ).fetchall()

        assert len(fact_rows) == 1
        subscription_key, customer_key = fact_rows[0]
        assert subscription_key is not None
        assert customer_key is not None
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_late_subscription_payment_updates_the_past_payment_date_fact(tmp_path) -> None:
    """과거 결제 시각을 가진 지연 결제가 두 번째 Build에서 과거 날짜 Fact로 들어온다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_late_subscription_payment_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    first_payment_at = FIXTURE_START + timedelta(days=1)
    first_payment = plan_subscription_payment(
        _generator_config(first_payment_at, anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    late_payment_at = FIXTURE_START + timedelta(days=2)
    late_arrival_at = FIXTURE_START + timedelta(days=5)
    late_payment = replace(
        plan_subscription_payment(
            _generator_config(late_payment_at, anomaly_profile="default"),
            subscription.subscription_id,
            billing_cycle_sequence=2,
            attempt_sequence=1,
            billing_period_start_at=late_payment_at,
        ),
        updated_at=late_arrival_at,
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (first_payment,)) == 1
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
            ("subscription_payments", first_payment.updated_at, str(first_payment.payment_id)),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)

        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (late_payment,)) == 1
            connection.commit()

        late_results = [
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "subscription_payments",
                late_arrival_at,
                1,
                tmp_path,
                ingested_at,
            )
        ]
        results.extend(late_results)
        _append_fixture_catalog(postgres, warehouse_path, late_results)
        second_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            rows = connection.execute(
                """
                SELECT payment_id, payment_date_key
                FROM facts.fact_subscription_payments
                ORDER BY payment_date_key
                """
            ).fetchall()

        assert rows == [
            (str(first_payment.payment_id), 21000102),
            (str(late_payment.payment_id), 21000103),
        ]
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_late_contract_observation_rebinds_following_payment_versions(tmp_path) -> None:
    """지연 도착한 계약 상태 관측이 그 이후 결제의 계약 Version을 다시 묶는다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_late_contract_observation_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    early_payment_at = FIXTURE_START + timedelta(days=1)
    early_payment = plan_subscription_payment(
        _generator_config(early_payment_at, anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    later_payment_at = FIXTURE_START + timedelta(days=40)
    later_payment = plan_subscription_payment(
        _generator_config(later_payment_at, anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=2,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START + timedelta(days=31),
    )
    transition_at = FIXTURE_START + timedelta(days=20)
    transition_arrival_at = FIXTURE_START + timedelta(days=50)
    (transitioned_subscription,) = subscription_transition_records(
        _generator_config(transition_at, anomaly_profile="default"),
        (subscription,),
        "PAYMENT_FAILED",
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (early_payment, later_payment)) == 2
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
            ("subscription_payments", early_payment.updated_at, str(early_payment.payment_id)),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            before = dict(
                connection.execute(
                    "SELECT payment_id, subscription_key FROM facts.fact_subscription_payments"
                ).fetchall()
            )

        with postgres.source_connection() as connection:
            assert (
                persist_subscription_records(connection, (transitioned_subscription,)).updated == 1
            )
            connection.commit()

        transition_results = [
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "customer_subscriptions",
                transition_arrival_at,
                1,
                tmp_path,
                ingested_at,
            )
        ]
        results.extend(transition_results)
        _append_fixture_catalog(postgres, warehouse_path, transition_results)
        second_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert second_build.returncode == 0, _combined_output(second_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            after = dict(
                connection.execute(
                    """
                    SELECT fact.payment_id, subscription.subscription_status
                    FROM facts.fact_subscription_payments AS fact
                    JOIN dimensions.dim_subscription AS subscription USING (subscription_key)
                    """
                ).fetchall()
            )
            keys_after = dict(
                connection.execute(
                    "SELECT payment_id, subscription_key FROM facts.fact_subscription_payments"
                ).fetchall()
            )

        assert after[str(early_payment.payment_id)] == "ACTIVE"
        assert after[str(later_payment.payment_id)] == "PAYMENT_FAILED"
        assert keys_after[str(early_payment.payment_id)] == before[str(early_payment.payment_id)]
        assert keys_after[str(later_payment.payment_id)] != before[str(later_payment.payment_id)]
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_two_batches_before_one_build_are_both_recomputed(tmp_path) -> None:
    """dbt Build 없이 쌓인 두 Batch의 변경이 한 번의 Build에서 모두 반영된다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_two_batches_one_build_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    first_payment = plan_subscription_payment(
        _generator_config(FIXTURE_START + timedelta(days=1), anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    second_payment = plan_subscription_payment(
        _generator_config(FIXTURE_START + timedelta(days=32), anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=2,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START + timedelta(days=31),
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        baseline_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert baseline_build.returncode == 0, _combined_output(baseline_build)

        _set_watermark(
            postgres,
            pipeline_name,
            "subscription_payments",
            CursorPosition(
                first_payment.updated_at - timedelta(microseconds=1),
                (str(first_payment.payment_id),),
            ),
            now=ingested_at,
        )
        staged_results: list[TableIngestionResult] = []
        for payment in (first_payment, second_payment):
            with postgres.source_connection() as connection:
                assert persist_subscription_payments(connection, (payment,)) == 1
                connection.commit()
            staged_results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    "subscription_payments",
                    payment.updated_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        results.extend(staged_results)
        _append_fixture_catalog(postgres, warehouse_path, staged_results)
        single_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert single_build.returncode == 0, _combined_output(single_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            payment_ids = {
                row[0]
                for row in connection.execute(
                    "SELECT payment_id FROM facts.fact_subscription_payments"
                ).fetchall()
            }

        assert payment_ids == {str(first_payment.payment_id), str(second_payment.payment_id)}
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
    or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
    reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
)
def test_watermark_does_not_advance_when_a_model_fails(tmp_path) -> None:
    """실패 Build는 Watermark를 보존하고 복구 Build가 누락 Batch를 다시 처리한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_watermark_failed_build_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _generator_config(FIXTURE_START, anomaly_profile="default")
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    first_payment_at = FIXTURE_START + timedelta(days=1)
    first_payment = plan_subscription_payment(
        _generator_config(first_payment_at, anomaly_profile="default"),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    late_payment_at = FIXTURE_START + timedelta(days=2)
    late_arrival_at = FIXTURE_START + timedelta(days=5)
    late_payment = replace(
        plan_subscription_payment(
            _generator_config(late_payment_at, anomaly_profile="default"),
            subscription.subscription_id,
            billing_cycle_sequence=2,
            attempt_sequence=1,
            billing_period_start_at=late_payment_at,
        ),
        updated_at=late_arrival_at,
    )
    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (first_payment,)) == 1
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
            ("subscription_payments", first_payment.updated_at, str(first_payment.payment_id)),
        ):
            _set_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres,
                    storage,
                    pipeline_name,
                    source_table,
                    cursor_at,
                    1,
                    tmp_path,
                    ingested_at,
                )
            )

        _create_fixture_catalog(postgres, warehouse_path, results)
        first_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert first_build.returncode == 0, _combined_output(first_build)
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            watermark_before_failure = connection.execute(
                "SELECT max(processed_batch_id) FROM control.dbt_processed_batch"
            ).fetchone()[0]

        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (late_payment,)) == 1
            connection.commit()

        late_results = [
            _ingest(
                postgres,
                storage,
                pipeline_name,
                "subscription_payments",
                late_arrival_at,
                1,
                tmp_path,
                ingested_at,
            )
        ]
        results.extend(late_results)
        _append_fixture_catalog(postgres, warehouse_path, late_results)
        original_object_key = late_results[0].object_key
        assert original_object_key is not None
        missing_object_key = f"missing/{uuid.uuid4().hex}.parquet"
        with duckdb.connect(str(warehouse_path)) as connection:
            connection.execute(
                "UPDATE control.bronze_files SET object_key = ? WHERE object_key = ?",
                [missing_object_key, original_object_key],
            )

        failed_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert failed_build.returncode != 0
        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            watermark_after_failure = connection.execute(
                "SELECT max(processed_batch_id) FROM control.dbt_processed_batch"
            ).fetchone()[0]
        assert watermark_after_failure == watermark_before_failure

        with duckdb.connect(str(warehouse_path)) as connection:
            connection.execute(
                "UPDATE control.bronze_files SET object_key = ? WHERE object_key = ?",
                [original_object_key, missing_object_key],
            )
        recovered_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert recovered_build.returncode == 0, _combined_output(recovered_build)

        with duckdb.connect(str(warehouse_path), read_only=True) as connection:
            watermark_after_recovery = connection.execute(
                "SELECT max(processed_batch_id) FROM control.dbt_processed_batch"
            ).fetchone()[0]
            payment_ids = {
                row[0]
                for row in connection.execute(
                    "SELECT payment_id FROM facts.fact_subscription_payments"
                ).fetchall()
            }

        assert watermark_after_recovery == late_results[0].run.batch_id
        assert payment_ids == {str(first_payment.payment_id), str(late_payment.payment_id)}
    finally:
        _cleanup(postgres, storage, pipeline_name, results, customer.customer_unique_id)


def _generator_config(logical_date: datetime, *, anomaly_profile: str) -> GeneratorConfig:
    """고정된 Snapshot·Seed로 Fixture의 각 업무 시각 Generator 입력을 만든다."""
    return GeneratorConfig(
        source_snapshot_id=FIXTURE_SNAPSHOT_ID,
        random_seed=FIXTURE_RANDOM_SEED,
        logical_date=logical_date,
        order_count=1,
        anomaly_profile=anomaly_profile,
        generator_version=GENERATOR_VERSION,
    )


def _set_watermark(
    postgres: PostgresSettings,
    pipeline_name: str,
    source_table: str,
    cursor: CursorPosition,
    *,
    now: datetime,
) -> None:
    """Fixture 행 직전 Cursor를 넣어 기존 Source 기준선이 Bronze에 섞이지 않게 한다."""
    get_or_create_watermark(postgres, pipeline_name, source_table, now=now)
    with postgres.pipeline_connection() as connection:
        connection.execute(
            """
            UPDATE watermarks
            SET watermark_timestamp = %s, watermark_keys = %s, updated_at = %s
            WHERE pipeline_name = %s AND source_table = %s
            """,
            (cursor.timestamp, Jsonb(cursor.as_json()), now, pipeline_name, source_table),
        )
        connection.commit()


def _ingest(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    source_table: str,
    logical_date: datetime,
    sequence: int,
    tmp_path: Path,
    ingested_at: datetime,
) -> TableIngestionResult:
    """Fixture 전용 Batch로 Source 한 Table을 Bronze까지 수집한다."""
    request = TableIngestionRequest.for_dag_run(
        source_table=source_table,
        dag_id=f"{pipeline_name}_{sequence}",
        logical_date=logical_date,
        pipeline_name=pipeline_name,
    )
    result = ingest_table(
        postgres,
        storage,
        request,
        local_directory=tmp_path / "bronze",
        now=ingested_at,
    )
    assert result.status == "SUCCESS"
    return result


def _create_fixture_catalog(
    postgres: PostgresSettings,
    warehouse_path: Path,
    results: list[TableIngestionResult],
) -> None:
    """Fixture가 Commit한 Object만 임시 Warehouse의 Bronze Catalog에 등록한다."""
    table_batch_ids = [f"{result.run.batch_id}__{result.run.source_table}" for result in results]
    rows: list[tuple[object, ...]] = []
    with postgres.pipeline_connection() as connection:
        for table_batch_id in table_batch_ids:
            row = connection.execute(
                """
                SELECT source_table, object_key, schema_version, batch_id,
                       committed_at, row_count, logical_hash
                FROM bronze_objects
                WHERE table_batch_id = %s AND status = 'COMMITTED'
                """,
                (table_batch_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Fixture Bronze object is missing: {table_batch_id}")
            rows.append(tuple(row))

    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute("CREATE SCHEMA control")
        connection.execute(
            """
            CREATE TABLE control.bronze_files (
                source_table VARCHAR NOT NULL,
                object_key VARCHAR PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                batch_id VARCHAR NOT NULL,
                committed_at TIMESTAMPTZ NOT NULL,
                row_count BIGINT NOT NULL,
                logical_hash VARCHAR NOT NULL
            )
            """
        )
        connection.executemany(
            "INSERT INTO control.bronze_files VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )


def _append_fixture_catalog(
    postgres: PostgresSettings,
    warehouse_path: Path,
    results: list[TableIngestionResult],
) -> None:
    """이미 만들어진 Bronze Catalog에 추가 Batch의 Object만 등록한다."""
    table_batch_ids = [f"{result.run.batch_id}__{result.run.source_table}" for result in results]
    rows: list[tuple[object, ...]] = []
    with postgres.pipeline_connection() as connection:
        for table_batch_id in table_batch_ids:
            row = connection.execute(
                """
                SELECT source_table, object_key, schema_version, batch_id,
                       committed_at, row_count, logical_hash
                FROM bronze_objects
                WHERE table_batch_id = %s AND status = 'COMMITTED'
                """,
                (table_batch_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Fixture Bronze object is missing: {table_batch_id}")
            rows.append(tuple(row))

    with duckdb.connect(str(warehouse_path)) as connection:
        connection.executemany(
            "INSERT INTO control.bronze_files VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )


def _run_dbt_build(
    warehouse_path: Path, storage: SeaweedFSSettings, tmp_path: Path
) -> subprocess.CompletedProcess[str]:
    """격리된 Warehouse와 Fixture Bronze 목록만 사용해 전체 dbt Build를 실행한다."""
    environment = {
        **os.environ,
        "WAREHOUSE_PATH": str(warehouse_path),
        "SEAWEEDFS_HOST": storage.host,
        "SEAWEEDFS_S3_PORT": str(storage.port),
        "SEAWEEDFS_BUCKET": storage.bucket,
        "SEAWEEDFS_ACCESS_KEY": storage.access_key,
        "SEAWEEDFS_SECRET_KEY": storage.secret_key,
    }
    return subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target-path",
            str(tmp_path / "dbt-target"),
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def _cleanup(
    postgres: PostgresSettings,
    storage: SeaweedFSSettings,
    pipeline_name: str,
    results: list[TableIngestionResult],
    customer_unique_id: str,
) -> None:
    """Fixture가 만든 Source 행·Bronze Object·Pipeline Metadata만 역순으로 제거한다."""
    client = seaweedfs_s3_client(storage)
    for result in results:
        for object_key in (result.manifest_key, result.object_key):
            if object_key is not None:
                client.delete_object(Bucket=storage.bucket, Key=object_key)

    with postgres.pipeline_connection() as connection:
        for result in results:
            connection.execute(
                "DELETE FROM bronze_objects WHERE table_batch_id = %s",
                (f"{result.run.batch_id}__{result.run.source_table}",),
            )
        connection.execute("DELETE FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,))
        connection.execute("DELETE FROM watermarks WHERE pipeline_name = %s", (pipeline_name,))
        connection.commit()

    with postgres.source_connection() as connection:
        connection.execute(
            """
            DELETE FROM subscription_payments
            WHERE subscription_id IN (
                SELECT subscription_id
                FROM customer_subscriptions
                WHERE customer_unique_id = %s
            )
            """,
            (customer_unique_id,),
        )
        connection.execute(
            "DELETE FROM customer_subscriptions WHERE customer_unique_id = %s",
            (customer_unique_id,),
        )
        connection.execute(
            "DELETE FROM customer_membership_tiers WHERE customer_unique_id = %s",
            (customer_unique_id,),
        )
        connection.execute(
            "DELETE FROM customers WHERE customer_unique_id = %s",
            (customer_unique_id,),
        )
        connection.commit()


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    """dbt 실패 시 버전별 출력 위치 차이와 무관하게 전체 진단을 반환한다."""
    return f"{result.stdout}\n{result.stderr}"
