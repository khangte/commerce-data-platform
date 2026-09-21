"""누락된 Schedule이 Gap을 만들지 않는다는 성질과 Source 연결 실패의 안전한 실패를 검증한다."""

from __future__ import annotations

import os
import shutil
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from psycopg import OperationalError

from src.common.database import PostgresSettings
from src.generator.customers import (
    ensure_membership_tier_records,
    ensure_subscription_records,
    new_customer_record,
    new_membership_tier_record,
    new_subscription_record,
    persist_customer_records,
)
from src.generator.subscription_payments import (
    persist_subscription_payments,
    plan_subscription_payment,
)
from src.ingestion.errors import SOURCE_CONNECTION_ERROR, classify_error, is_retryable
from src.ingestion.metadata import CursorPosition
from src.ingestion.reprocess import rewind_tables
from src.ingestion.service import TableIngestionResult, ingest_orders, orders_object_keys
from src.ingestion.storage import SeaweedFSSettings
from src.warehouse.mart_hash import describe_mart_difference, mart_logical_hashes, target_for
from tests.integration.test_incremental_full_refresh_hash_integration import (
    _run_full_refresh_build,
)
from tests.integration.test_replay_boundary_hash_integration import (
    _ingest_first_batch,
    _ingest_second_batch,
    _max_committed_at,
)
from tests.integration.test_replay_boundary_hash_integration import (
    _run_build as _run_boundary_build,
)
from tests.integration.test_subscription_payment_temporal_join_integration import (
    FIXTURE_START,
    _append_fixture_catalog,
    _combined_output,
    _create_fixture_catalog,
    _generator_config,
    _ingest,
    _run_dbt_build,
)
from tests.integration.test_subscription_payment_temporal_join_integration import (
    _cleanup as _cleanup_subscription_fixture,
)
from tests.integration.test_subscription_payment_temporal_join_integration import (
    _set_watermark as _set_fixture_watermark,
)
from tests.reliability.faults import fail_source_connection
from tests.reliability.harness import write_evidence
from tests.reliability.test_r01_r07_ingestion_commit import (
    _cleanup as _cleanup_orders_scenario,
)
from tests.reliability.test_r01_r07_ingestion_commit import (
    _lower_bound_before_five_rows,
    _orphan_request,
)
from tests.reliability.test_r01_r07_ingestion_commit import (
    _set_watermark as _set_orders_watermark,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
    pytest.mark.skipif(
        os.environ.get("RUN_POSTGRES_INTEGRATION") != "1"
        or os.environ.get("RUN_SEAWEEDFS_INTEGRATION") != "1",
        reason="Set PostgreSQL and SeaweedFS integration environment flags after starting containers.",
    ),
]


def test_r15_source_connection_failure_creates_no_object_and_holds_the_watermark(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Source 연결 실패가 Object·Watermark 전진 없이 재시도 가능한 상태로 끝나는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    now = datetime(2026, 9, 20, tzinfo=UTC)
    request = _orphan_request("r15", now, page_size=2)
    _set_orders_watermark(postgres, request.pipeline_name, _lower_bound_before_five_rows(postgres), now)
    with postgres.pipeline_connection() as connection:
        watermark_before = connection.execute(
            "SELECT watermark_timestamp, watermark_keys FROM watermarks"
            " WHERE pipeline_name = %s AND source_table = 'orders'",
            (request.pipeline_name,),
        ).fetchone()

    try:
        with fail_source_connection(monkeypatch), pytest.raises(OperationalError) as failure:
            ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)

        classified = classify_error(failure.value)
        retryable = is_retryable(failure.value)
        assert classified == SOURCE_CONNECTION_ERROR
        assert retryable

        object_key, _ = orders_object_keys(request.batch_id, now)
        with postgres.pipeline_connection() as connection:
            object_count = connection.execute(
                "SELECT count(*) FROM bronze_objects WHERE table_batch_id = %s",
                (f"{request.batch_id}__orders",),
            ).fetchone()[0]
            run_count = connection.execute(
                "SELECT count(*) FROM pipeline_runs WHERE pipeline_name = %s",
                (request.pipeline_name,),
            ).fetchone()[0]
            failed_run = connection.execute(
                "SELECT status, error_type FROM pipeline_runs WHERE pipeline_name = %s",
                (request.pipeline_name,),
            ).fetchone()
            watermark_after = connection.execute(
                "SELECT watermark_timestamp, watermark_keys FROM watermarks"
                " WHERE pipeline_name = %s AND source_table = 'orders'",
                (request.pipeline_name,),
            ).fetchone()
        assert object_count == 0
        assert run_count == 1
        assert failed_run == ("FAILED", SOURCE_CONNECTION_ERROR)
        assert watermark_after == watermark_before

        retry = ingest_orders(postgres, storage, request, local_directory=tmp_path, now=now)
        assert retry.status == "SUCCESS"
        assert retry.object_key == object_key

        write_evidence(
            "r15",
            {
                "batch_id": request.batch_id,
                "classified_error": classified,
                "is_retryable": retryable,
                "object_count_after_failure": object_count,
                "pipeline_run_count_after_failure": run_count,
                "pipeline_run_status_after_failure": failed_run[0],
                "pipeline_run_error_type_after_failure": failed_run[1],
                "retry_object_key": retry.object_key,
                "retry_row_count": retry.row_count,
                "retry_status": retry.status,
                "watermark_after_failure": watermark_after[0].isoformat() if watermark_after else None,
                "watermark_before_failure": watermark_before[0].isoformat() if watermark_before else None,
            },
        )
    finally:
        _cleanup_orders_scenario(postgres, storage, request.pipeline_name, request)


def test_r11_a_skipped_window_self_heals_and_rewind_only_restores_attribution(
    tmp_path: Path,
) -> None:
    """건너뛴 창이 Gap을 만들지 않고 Bronze Partition 귀속만 옮긴다는 것과, Rewind 회복이 귀속만 되돌리는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_r11_{uuid.uuid4().hex}"
    fixture_snapshot_id = f"test:r11-missing-schedule:{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _fixture_config(FIXTURE_START, fixture_snapshot_id)
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)

    window_a_at = FIXTURE_START + timedelta(days=1)
    window_b_at = FIXTURE_START + timedelta(days=2)
    window_c_at = FIXTURE_START + timedelta(days=3)
    payment_a = plan_subscription_payment(
        _fixture_config(window_a_at, fixture_snapshot_id),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )
    payment_b = plan_subscription_payment(
        _fixture_config(window_b_at, fixture_snapshot_id),
        subscription.subscription_id,
        billing_cycle_sequence=2,
        attempt_sequence=1,
        billing_period_start_at=window_b_at,
    )
    payment_c = plan_subscription_payment(
        _fixture_config(window_c_at, fixture_snapshot_id),
        subscription.subscription_id,
        billing_cycle_sequence=3,
        attempt_sequence=1,
        billing_period_start_at=window_c_at,
    )

    results: list[TableIngestionResult] = []
    warehouse_path = tmp_path / "warehouse.duckdb"
    control_path = tmp_path / "warehouse-control.duckdb"

    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (payment_a,)) == 1
            connection.commit()

        for source_table, cursor_at, cursor_key in (
            ("customer_subscriptions", subscription.updated_at, str(subscription.subscription_id)),
            ("customer_membership_tiers", tier.updated_at, customer.customer_unique_id),
            ("subscription_payments", payment_a.updated_at, str(payment_a.payment_id)),
        ):
            _set_fixture_watermark(
                postgres,
                pipeline_name,
                source_table,
                CursorPosition(cursor_at - timedelta(microseconds=1), (cursor_key,)),
                now=ingested_at,
            )
            results.append(
                _ingest(
                    postgres, storage, pipeline_name, source_table, cursor_at, 1, tmp_path, ingested_at
                )
            )

        run_a = results[-1]
        assert run_a.row_count == 1

        # 윈도 B: 결제 데이터는 발생했지만 Schedule이 건너뛰어 Ingest를 생략한다.
        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (payment_b,)) == 1
            connection.commit()

        # 윈도 C: 새 결제가 발생하고 이번에는 Schedule이 정상 실행된다.
        with postgres.source_connection() as connection:
            assert persist_subscription_payments(connection, (payment_c,)) == 1
            connection.commit()

        run_c = _ingest(
            postgres, storage, pipeline_name, "subscription_payments", window_c_at, 2, tmp_path, ingested_at
        )
        results.append(run_c)

        assert run_c.row_count == 2  # Watermark가 연속이라 C 실행이 B까지 함께 쓸어 담아 Gap이 없다

        with postgres.pipeline_connection() as connection:
            watermark_after_c = connection.execute(
                "SELECT watermark_timestamp, watermark_keys FROM watermarks"
                " WHERE pipeline_name = %s AND source_table = 'subscription_payments'",
                (pipeline_name,),
            ).fetchone()
            run_c_object = connection.execute(
                "SELECT object_key FROM bronze_objects WHERE table_batch_id = %s",
                (f"{run_c.run.batch_id}__subscription_payments",),
            ).fetchone()

        expected_after_c = CursorPosition(payment_c.updated_at, (str(payment_c.payment_id),))
        assert watermark_after_c == (expected_after_c.timestamp, expected_after_c.as_json())
        assert f"ingestion_date={window_c_at.date().isoformat()}" in run_c_object[0]
        assert f"batch_id={run_c.run.batch_id}" in run_c_object[0]

        _create_fixture_catalog(postgres, warehouse_path, results)
        pre_recovery_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert pre_recovery_build.returncode == 0, _combined_output(pre_recovery_build)

        shutil.copy2(warehouse_path, control_path)
        control_build = _run_full_refresh_build(control_path, storage, tmp_path)
        assert control_build.returncode == 0, _combined_output(control_build)

        control_hashes = mart_logical_hashes(control_path)
        pre_recovery_hashes = mart_logical_hashes(warehouse_path)
        # 회복 조치 전에도 Mart는 이미 건너뛴 적 없는 Full Refresh Control과 같다.
        assert pre_recovery_hashes == control_hashes

        rewind_tables(
            postgres,
            source_tables=("subscription_payments",),
            boundary=window_b_at,
            pipeline_name=pipeline_name,
        )
        recovery_run = _ingest(
            postgres, storage, pipeline_name, "subscription_payments", window_b_at, 3, tmp_path, ingested_at
        )
        results.append(recovery_run)
        # B를 위한 명시 Batch가 C까지 다시 쓸어 담는 것은 설계상 정상이며 막지 않는다.
        assert recovery_run.row_count == 2

        with postgres.pipeline_connection() as connection:
            recovery_object = connection.execute(
                "SELECT object_key FROM bronze_objects WHERE table_batch_id = %s",
                (f"{recovery_run.run.batch_id}__subscription_payments",),
            ).fetchone()
        # 회복은 귀속만 교정한다 — B 구간 Row가 이제 B의 ingestion_date Partition에 있다.
        assert f"ingestion_date={window_b_at.date().isoformat()}" in recovery_object[0]

        _append_fixture_catalog(postgres, warehouse_path, [recovery_run])
        post_recovery_build = _run_dbt_build(warehouse_path, storage, tmp_path)
        assert post_recovery_build.returncode == 0, _combined_output(post_recovery_build)

        post_recovery_hashes = mart_logical_hashes(warehouse_path)
        # 귀속 교정은 멱등이다 — Mart는 회복 전후로 바뀌지 않는다.
        assert post_recovery_hashes == control_hashes

        write_evidence(
            "r11",
            {
                "pipeline_name": pipeline_name,
                "recovery_object_key": recovery_object[0],
                "recovery_row_count": recovery_run.row_count,
                "run_a_row_count": run_a.row_count,
                "run_c_object_key": run_c_object[0],
                "run_c_row_count": run_c.row_count,
                "mart_hashes_equal_control_after_recovery": post_recovery_hashes == control_hashes,
                "mart_hashes_equal_control_before_recovery": pre_recovery_hashes == control_hashes,
            },
        )
    finally:
        _cleanup_subscription_fixture(
            postgres, storage, pipeline_name, results, customer.customer_unique_id
        )


def test_r12_replay_matches_a_full_refresh_of_the_same_range(tmp_path: Path) -> None:
    """Bronze as-of Replay가 Source를 다시 읽지 않고 같은 범위의 Full Refresh와 Hash까지 같은지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_r12_{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    results: list[TableIngestionResult] = []
    customer_unique_id = ""

    try:
        first_results, customer_unique_id = _ingest_first_batch(
            postgres, storage, pipeline_name, tmp_path, ingested_at
        )
        results.extend(first_results)

        control_path = tmp_path / "r12-control.duckdb"
        _create_fixture_catalog(postgres, control_path, first_results)
        control_build = _run_boundary_build(control_path, storage, tmp_path, "r12-control")
        assert control_build.returncode == 0, _combined_output(control_build)
        control_hashes = mart_logical_hashes(control_path)
        boundary = _max_committed_at(control_path)

        second_results = _ingest_second_batch(
            postgres, storage, pipeline_name, customer_unique_id, tmp_path, ingested_at
        )
        results.extend(second_results)

        with postgres.pipeline_connection() as connection:
            run_count_before_replay_build = connection.execute(
                "SELECT count(*) FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,)
            ).fetchone()[0]

        replay_path = tmp_path / "r12-replay.duckdb"
        shutil.copyfile(control_path, replay_path)
        _append_fixture_catalog(postgres, replay_path, second_results)
        replay_build = _run_boundary_build(
            replay_path, storage, tmp_path, "r12-replay", bronze_as_of=boundary
        )
        assert replay_build.returncode == 0, _combined_output(replay_build)
        replay_hashes = mart_logical_hashes(replay_path)

        with postgres.pipeline_connection() as connection:
            run_count_after_replay_build = connection.execute(
                "SELECT count(*) FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,)
            ).fetchone()[0]

        mismatched = [name for name, value in control_hashes.items() if replay_hashes[name] != value]
        report = "\n".join(
            describe_mart_difference(control_path, replay_path, target_for(name)) for name in mismatched
        )
        assert mismatched == [], report
        # Replay Build는 dbt build만 실행하고 ingest_table을 호출하지 않는다 — Source Read가 0건이다.
        assert run_count_after_replay_build == run_count_before_replay_build

        write_evidence(
            "r12",
            {
                "pipeline_name": pipeline_name,
                "bronze_as_of": boundary,
                "mart_hashes_match_control": mismatched == [],
                "pipeline_run_count_before_replay_build": run_count_before_replay_build,
                "pipeline_run_count_after_replay_build": run_count_after_replay_build,
            },
        )
    finally:
        _cleanup_subscription_fixture(postgres, storage, pipeline_name, results, customer_unique_id)


def test_r13_re_extract_records_a_new_batch_id_and_keeps_prior_objects(tmp_path: Path) -> None:
    """되감기 뒤 재수집이 새 batch_id로 기록되고 기존 Commit Object는 그대로 남는지 검증한다."""
    postgres = PostgresSettings.from_environment()
    storage = SeaweedFSSettings.from_environment()
    pipeline_name = f"test_r13_{uuid.uuid4().hex}"
    fixture_snapshot_id = f"test:r13-re-extract:{uuid.uuid4().hex}"
    ingested_at = datetime.now(UTC)
    initial_config = _fixture_config(FIXTURE_START, fixture_snapshot_id)
    customer = new_customer_record(initial_config, 1)
    subscription = new_subscription_record(customer)
    tier = new_membership_tier_record(customer)
    window_at = FIXTURE_START + timedelta(days=1)
    payment = plan_subscription_payment(
        _fixture_config(window_at, fixture_snapshot_id),
        subscription.subscription_id,
        billing_cycle_sequence=1,
        attempt_sequence=1,
        billing_period_start_at=FIXTURE_START,
    )

    results: list[TableIngestionResult] = []
    try:
        with postgres.source_connection() as connection:
            assert persist_customer_records(connection, (customer,)).inserted == 1
            assert ensure_subscription_records(connection, (subscription,)).inserted == 1
            assert ensure_membership_tier_records(connection, (tier,)).inserted == 1
            assert persist_subscription_payments(connection, (payment,)) == 1
            connection.commit()

        _set_fixture_watermark(
            postgres,
            pipeline_name,
            "subscription_payments",
            CursorPosition(payment.updated_at - timedelta(microseconds=1), (str(payment.payment_id),)),
            now=ingested_at,
        )
        original_run = _ingest(
            postgres,
            storage,
            pipeline_name,
            "subscription_payments",
            payment.updated_at,
            1,
            tmp_path,
            ingested_at,
        )
        results.append(original_run)
        assert original_run.row_count == 1

        with postgres.pipeline_connection() as connection:
            original_object_before = connection.execute(
                "SELECT object_key, row_count, logical_hash, status"
                " FROM bronze_objects WHERE table_batch_id = %s",
                (f"{original_run.run.batch_id}__subscription_payments",),
            ).fetchone()
        assert original_object_before[3] == "COMMITTED"

        rewind_tables(
            postgres,
            source_tables=("subscription_payments",),
            boundary=payment.updated_at,
            pipeline_name=pipeline_name,
        )
        with postgres.pipeline_connection() as connection:
            watermark_after_rewind = connection.execute(
                "SELECT watermark_timestamp, watermark_keys FROM watermarks"
                " WHERE pipeline_name = %s AND source_table = 'subscription_payments'",
                (pipeline_name,),
            ).fetchone()
        # 우리 결제보다 앞선 실재 Row가 없으니 되감기는 최초 상태(빈 Cursor)로 돌아간다.
        assert watermark_after_rewind == (None, [])

        re_extract_run = _ingest(
            postgres,
            storage,
            pipeline_name,
            "subscription_payments",
            payment.updated_at,
            2,
            tmp_path,
            ingested_at + timedelta(seconds=1),
        )
        results.append(re_extract_run)
        assert re_extract_run.row_count == 1
        assert re_extract_run.run.batch_id != original_run.run.batch_id

        with postgres.pipeline_connection() as connection:
            run_count = connection.execute(
                "SELECT count(*) FROM pipeline_runs WHERE pipeline_name = %s", (pipeline_name,)
            ).fetchone()[0]
            original_object_after = connection.execute(
                "SELECT object_key, row_count, logical_hash, status"
                " FROM bronze_objects WHERE table_batch_id = %s",
                (f"{original_run.run.batch_id}__subscription_payments",),
            ).fetchone()
            re_extract_object = connection.execute(
                "SELECT object_key, row_count, watermark_before"
                " FROM bronze_objects WHERE table_batch_id = %s",
                (f"{re_extract_run.run.batch_id}__subscription_payments",),
            ).fetchone()

        assert run_count == 2
        # 재수집은 새 batch_id로 기록되고, 기존 Commit Object는 손대지 않는다.
        assert original_object_after == original_object_before
        assert re_extract_object[0] != original_object_before[0]
        assert re_extract_object[2]["timestamp"] is None

        write_evidence(
            "r13",
            {
                "pipeline_name": pipeline_name,
                "original_batch_id": original_run.run.batch_id,
                "original_object_key": original_object_before[0],
                "original_object_unchanged_after_re_extract": original_object_after == original_object_before,
                "pipeline_run_count": run_count,
                "re_extract_batch_id": re_extract_run.run.batch_id,
                "re_extract_object_key": re_extract_object[0],
                "re_extract_row_count": re_extract_run.row_count,
            },
        )
    finally:
        _cleanup_subscription_fixture(
            postgres, storage, pipeline_name, results, customer.customer_unique_id
        )


def _fixture_config(logical_date: datetime, snapshot_id: str):
    """현재 테스트 실행에만 쓰는 고유한 Generator 입력을 만든다."""
    from dataclasses import replace

    return replace(
        _generator_config(logical_date, anomaly_profile="default"),
        source_snapshot_id=snapshot_id,
    )
