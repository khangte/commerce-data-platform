"""실제 PostgreSQL Generator Metadata 계약을 검증한다."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest

from src.common.database import PostgresSettings
from src.generator.config import GENERATOR_VERSION, GeneratorConfig
from src.generator.ids import logical_hash
from src.generator.metadata import (
    ensure_generator_metadata,
    record_finished_run,
    record_started_run,
)

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_generator_run_persists_deterministic_inputs_and_success_evidence() -> None:
    """Generator 실행 이력은 Snapshot·입력·Count·Hash를 모두 보관한다."""
    settings = PostgresSettings.from_environment()
    generator_run_id = uuid.uuid4()
    source_snapshot_id = f"test:{generator_run_id}"
    config = GeneratorConfig(
        source_snapshot_id=source_snapshot_id,
        random_seed=42,
        logical_date=datetime(2026, 9, 4, tzinfo=UTC),
        order_count=1,
        anomaly_profile="default",
        generator_version=GENERATOR_VERSION,
    )
    result_counts = {"orders_created": 1}

    ensure_generator_metadata(settings)
    try:
        with settings.pipeline_connection() as connection:
            record_started_run(connection, generator_run_id, config)
        record_finished_run(
            settings,
            generator_run_id,
            status="SUCCESS",
            result_counts=result_counts,
            logical_content_hash=logical_hash(result_counts),
        )

        with settings.pipeline_connection() as connection:
            actual = connection.execute(
                """
                SELECT source_snapshot_id, random_seed, logical_date, order_count,
                       anomaly_profile, generator_version, result_counts, logical_hash, status
                FROM generator_runs
                WHERE generator_run_id = %s
                """,
                (generator_run_id,),
            ).fetchone()

        assert actual == (
            source_snapshot_id,
            42,
            datetime(2026, 9, 4, tzinfo=UTC),
            1,
            "default",
            GENERATOR_VERSION,
            result_counts,
            logical_hash(result_counts),
            "SUCCESS",
        )
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute("DELETE FROM generator_runs WHERE generator_run_id = %s", (generator_run_id,))
            connection.commit()
