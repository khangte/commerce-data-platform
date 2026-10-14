"""결정적 Generator 구성 요소를 실제 Source Mutation 실행으로 조합한다."""

from __future__ import annotations

import uuid
from contextlib import ExitStack, suppress
from dataclasses import dataclass
from datetime import datetime

import psycopg

from src.common.database import PostgresSettings
from src.generator.commits import (
    committed_result,
    ensure_generator_commits,
    record_source_commit,
)
from src.generator.config import GeneratorConfig
from src.generator.errors import SourceCursorRegressionError
from src.generator.existing_orders import run_existing_order_transitions
from src.generator.ids import logical_hash
from src.generator.lease import (
    GENERATOR_OWNER_TYPE,
    SourceMutationLease,
    acquire_source_mutation_lease,
    ensure_source_mutation_lease_metadata,
    fenced_source_commit,
    release_source_mutation_lease,
)
from src.generator.metadata import (
    ensure_generator_metadata,
    record_finished_run,
    record_started_run,
)
from src.generator.new_orders import create_new_orders
from src.generator.orders import fetch_order_catalog
from src.generator.profile_mutations import (
    apply_membership_tier_change,
    apply_subscription_transition,
)
from src.generator.subscription_scan import (
    forced_failure_row,
    run_subscription_expiry_scan,
    select_due_subscription_for_failed_profile,
)
from src.ingestion.tables import TABLE_CONFIGS

MUTABLE_SOURCE_TABLES = (
    "customers",
    "customer_subscriptions",
    "customer_membership_tiers",
    "subscription_payments",
    "orders",
    "order_items",
    "order_payments",
)


@dataclass(frozen=True)
class GeneratorResult:
    """Generator 실행 또는 기존 성공 결과 재사용의 결정성 증적이다."""

    generator_run_id: uuid.UUID
    result_counts: dict[str, int]
    logical_content_hash: str
    reused_successful_run: bool


def resolve_source_snapshot_id(settings: PostgresSettings) -> str:
    """가장 최근 성공한 Seed의 Checksum과 기준 시각으로 Snapshot ID를 만든다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT raw_checksum, seeded_at
            FROM seed_runs
            WHERE status = 'SUCCESS'
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise ValueError("A successful seed run is required before running the generator")
    return f"seed:{row[0]}:{row[1].isoformat()}"


def run_generator(config: GeneratorConfig, settings: PostgresSettings) -> GeneratorResult:
    """Lease 보호 아래서 결정적 Bundle을 적재하고 Generator 실행 증적을 기록한다."""
    expected_snapshot_id = resolve_source_snapshot_id(settings)
    if config.source_snapshot_id != expected_snapshot_id:
        raise ValueError(
            "source_snapshot_id differs from the current successful seed snapshot: "
            f"{expected_snapshot_id}"
        )
    ensure_generator_metadata(settings)
    ensure_source_mutation_lease_metadata(settings)
    existing = _successful_result(settings, config)
    if existing is not None:
        return existing

    generator_run_id = uuid.uuid4()
    lease: SourceMutationLease | None = None
    started = False
    failed = False
    try:
        lease = acquire_source_mutation_lease(
            settings,
            owner_type=GENERATOR_OWNER_TYPE,
            owner_id=generator_run_id,
        )
        ensure_generator_commits(settings)
        committed = committed_result(settings, config)
        if committed is not None:
            record_finished_run(
                settings,
                committed.generator_run_id,
                status="SUCCESS",
                result_counts=committed.result_counts,
                logical_content_hash=committed.logical_content_hash,
            )
            return GeneratorResult(
                generator_run_id=committed.generator_run_id,
                result_counts=committed.result_counts,
                logical_content_hash=committed.logical_content_hash,
                reused_successful_run=True,
            )
        with settings.pipeline_connection() as connection:
            record_started_run(connection, generator_run_id, config)
        started = True

        with (
            settings.source_connection() as connection,
            ExitStack() as fence,
            connection.transaction(),
        ):
            _assert_source_cursor_forward(connection, config.logical_date)
            catalog = fetch_order_catalog(connection)
            result_counts = _empty_result_counts()
            logical_rows: list[dict] = []
            run_existing_order_transitions(
                connection, settings, config, lease, result_counts, logical_rows
            )
            create_new_orders(
                connection, settings, config, lease, catalog, result_counts, logical_rows
            )
            forced_failure_id = None
            if config.anomaly_profile == "subscription-payment-failed":
                forced_failure_id = select_due_subscription_for_failed_profile(connection, config)
            run_subscription_expiry_scan(
                connection, config, result_counts, logical_rows, forced_failure_id
            )
            if forced_failure_id is not None:
                logical_rows.append(forced_failure_row(connection, forced_failure_id))
            if config.anomaly_profile == "membership-change":
                tier_result, tier_row = apply_membership_tier_change(connection, config)
                result_counts["membership_tiers_inserted"] += tier_result.inserted
                result_counts["membership_tiers_updated"] += tier_result.updated
                result_counts["membership_tiers_skipped"] += tier_result.skipped
                logical_rows.append(tier_row)
            elif config.anomaly_profile.startswith("subscription-") and forced_failure_id is None:
                subscription_result, subscription_row = apply_subscription_transition(
                    connection, config, result_counts, logical_rows
                )
                result_counts["subscriptions_inserted"] += subscription_result.inserted
                result_counts["subscriptions_updated"] += subscription_result.updated
                result_counts["subscriptions_skipped"] += subscription_result.skipped
                logical_rows.append(subscription_row)
            logical_content_hash = logical_hash(
                {
                    "generator_inputs": config.deterministic_inputs(),
                    "result_counts": result_counts,
                    "rows": logical_rows,
                }
            )
            record_source_commit(
                connection, generator_run_id, config, result_counts, logical_content_hash
            )
            fence.enter_context(fenced_source_commit(settings, lease))
        record_finished_run(
            settings,
            generator_run_id,
            status="SUCCESS",
            result_counts=result_counts,
            logical_content_hash=logical_content_hash,
        )
        return GeneratorResult(
            generator_run_id=generator_run_id,
            result_counts=result_counts,
            logical_content_hash=logical_content_hash,
            reused_successful_run=False,
        )
    except BaseException as error:
        failed = True
        if started and isinstance(error, Exception):
            record_finished_run(
                settings,
                generator_run_id,
                status="FAILED",
                error_message=str(error),
            )
        raise
    finally:
        if lease is not None:
            if failed:
                with suppress(Exception):
                    release_source_mutation_lease(settings, lease)
            else:
                release_source_mutation_lease(settings, lease)


def _assert_source_cursor_forward(connection: psycopg.Connection, logical_date: datetime) -> None:
    """새 원천 행의 실행 시각이 대상 테이블별 커서 최대값보다 큰지 확인한다."""
    for table_name in MUTABLE_SOURCE_TABLES:
        cursor_column = TABLE_CONFIGS[table_name].cursor_timestamp_column
        maximum = connection.execute(
            f'SELECT max("{cursor_column}") FROM "{table_name}"'
        ).fetchone()[0]
        if maximum is not None and logical_date <= maximum:
            raise SourceCursorRegressionError(
                f"logical_date={logical_date.isoformat()} must be later than "
                f"{table_name}.{cursor_column} max={maximum.isoformat()}"
            )


def _successful_result(
    settings: PostgresSettings, config: GeneratorConfig
) -> GeneratorResult | None:
    """동일 결정성 입력으로 이미 성공한 실행 결과가 있으면 재사용 증적을 반환한다."""
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT generator_run_id, result_counts, logical_hash
            FROM generator_runs
            WHERE source_snapshot_id = %s
              AND random_seed = %s
              AND logical_date = %s
              AND order_count = %s
              AND anomaly_profile = %s
              AND generator_version = %s
              AND progress_time_scale = %s
              AND status = 'SUCCESS'
            """,
            (
                config.source_snapshot_id,
                config.random_seed,
                config.logical_date,
                config.order_count,
                config.anomaly_profile,
                config.generator_version,
                config.progress_time_scale,
            ),
        ).fetchone()
    if row is None:
        return None
    return GeneratorResult(
        generator_run_id=row[0],
        result_counts=dict(row[1]),
        logical_content_hash=row[2],
        reused_successful_run=True,
    )


def _empty_result_counts() -> dict[str, int]:
    """Generator Metadata에 저장할 Entity별 초기 Mutation Count를 반환한다."""
    return {
        "customers_inserted": 0,
        "customers_updated": 0,
        "customers_skipped": 0,
        "subscriptions_inserted": 0,
        "subscriptions_updated": 0,
        "subscriptions_skipped": 0,
        "membership_tiers_inserted": 0,
        "membership_tiers_updated": 0,
        "membership_tiers_skipped": 0,
        "subscription_payments_inserted": 0,
        "orders_inserted": 0,
        "orders_updated": 0,
        "orders_skipped": 0,
        "order_items_inserted": 0,
        "order_items_skipped": 0,
        "payments_inserted": 0,
        "payments_updated": 0,
        "payments_skipped": 0,
    }
