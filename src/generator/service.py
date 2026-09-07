"""결정적 Generator 구성 요소를 실제 Source Mutation 실행으로 조합한다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from src.common.database import PostgresSettings
from src.generator.config import EXECUTABLE_ANOMALY_PROFILES, GeneratorConfig
from src.generator.customers import new_customer_record
from src.generator.ids import logical_hash
from src.generator.lease import (
    GENERATOR_OWNER_TYPE,
    SourceMutationLease,
    acquire_source_mutation_lease,
    assert_source_mutation_lease,
    ensure_source_mutation_lease_metadata,
    release_source_mutation_lease,
)
from src.generator.metadata import (
    ensure_generator_metadata,
    record_finished_run,
    record_started_run,
)
from src.generator.orders import fetch_order_catalog, new_order_bundle, persist_order_bundle
from src.generator.scenarios import late_order_bundle


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
    if config.anomaly_profile not in EXECUTABLE_ANOMALY_PROFILES:
        executable_profiles = ", ".join(sorted(EXECUTABLE_ANOMALY_PROFILES))
        raise ValueError(
            f"The executable generator currently supports {executable_profiles} profiles; "
            "delayed-payment and membership-change are reusable scenario fixtures."
        )

    ensure_generator_metadata(settings)
    ensure_source_mutation_lease_metadata(settings)
    existing = _successful_result(settings, config)
    if existing is not None:
        return existing

    generator_run_id = uuid.uuid4()
    lease: SourceMutationLease | None = None
    started = False
    try:
        lease = acquire_source_mutation_lease(
            settings,
            owner_type=GENERATOR_OWNER_TYPE,
            owner_id=generator_run_id,
        )
        with settings.pipeline_connection() as connection:
            record_started_run(connection, generator_run_id, config)
        started = True

        with settings.source_connection() as connection, connection.transaction():
            catalog = fetch_order_catalog(connection)
            result_counts = _empty_result_counts()
            logical_rows = []
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

        logical_content_hash = logical_hash(
            {
                "generator_inputs": config.deterministic_inputs(),
                "result_counts": result_counts,
                "rows": logical_rows,
            }
        )
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
    except Exception as error:
        if started:
            record_finished_run(
                settings,
                generator_run_id,
                status="FAILED",
                error_message=str(error),
            )
        raise
    finally:
        if lease is not None:
            release_source_mutation_lease(settings, lease)


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
              AND status = 'SUCCESS'
            """,
            (
                config.source_snapshot_id,
                config.random_seed,
                config.logical_date,
                config.order_count,
                config.anomaly_profile,
                config.generator_version,
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


def _bundle_for_profile(
    config: GeneratorConfig, customer, catalog, order_ordinal: int
):
    """실행 가능한 Anomaly Profile에 맞는 결정적 Order Bundle을 만든다."""
    if config.anomaly_profile == "late-arrival":
        business_event_time = config.logical_date - timedelta(days=3 + order_ordinal % 3)
        return late_order_bundle(config, customer, catalog, order_ordinal, business_event_time)
    return new_order_bundle(config, customer, catalog, order_ordinal)


def _empty_result_counts() -> dict[str, int]:
    """Generator Metadata에 저장할 Entity별 초기 Mutation Count를 반환한다."""
    return {
        "customers_inserted": 0,
        "customers_updated": 0,
        "customers_skipped": 0,
        "orders_inserted": 0,
        "orders_skipped": 0,
        "order_items_inserted": 0,
        "order_items_skipped": 0,
        "payments_inserted": 0,
        "payments_skipped": 0,
    }


def _add_mutation_counts(result_counts: dict[str, int], mutation_result) -> None:
    """Order Bundle 저장 결과를 Generator Metadata의 Entity별 Count에 누적한다."""
    result_counts["customers_inserted"] += mutation_result.customer.inserted
    result_counts["customers_updated"] += mutation_result.customer.updated
    result_counts["customers_skipped"] += mutation_result.customer.skipped
    result_counts["orders_inserted"] += mutation_result.orders_inserted
    result_counts["orders_skipped"] += mutation_result.orders_skipped
    result_counts["order_items_inserted"] += mutation_result.items_inserted
    result_counts["order_items_skipped"] += mutation_result.items_skipped
    result_counts["payments_inserted"] += mutation_result.payments_inserted
    result_counts["payments_skipped"] += mutation_result.payments_skipped
