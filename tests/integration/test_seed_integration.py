from __future__ import annotations

import os

import pytest

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.seed.loader import TARGET_COLUMNS, parse_seeded_at, run_seed

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_same_raw_input_and_seeded_at_are_idempotent() -> None:
    settings = PostgresSettings.from_environment()
    _remove_successful_generator_runs(settings)
    input_dir = PROJECT_ROOT / "data" / "raw" / "olist"
    seeded_at = parse_seeded_at("2026-09-03T00:00:00Z")

    first = run_seed(input_dir, seeded_at, settings)
    second = run_seed(input_dir, seeded_at, settings)

    assert second.raw_checksum == first.raw_checksum
    assert second.table_row_counts == first.table_row_counts
    assert second.table_content_hashes == first.table_content_hashes

    with settings.source_connection() as connection:
        for table_name, expected_count in second.table_row_counts.items():
            assert (
                connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
                == expected_count
            )


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_source_schema_keeps_only_the_selected_raw_columns_and_extensions() -> None:
    settings = PostgresSettings.from_environment()

    with settings.source_connection() as connection:
        for table_name, expected_columns in TARGET_COLUMNS.items():
            actual_columns = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    """,
                    (table_name,),
                )
            }
            assert actual_columns == set(expected_columns)


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_seed_guard_rejects_a_changed_baseline_after_success() -> None:
    settings = PostgresSettings.from_environment()
    _remove_successful_generator_runs(settings)

    with pytest.raises(ValueError, match="baseline input differs"):
        run_seed(
            PROJECT_ROOT / "data" / "raw" / "olist",
            parse_seeded_at("2026-09-04T00:00:00Z"),
            settings,
        )


def _remove_successful_generator_runs(settings: PostgresSettings) -> None:
    """Seed Guard가 검증할 기준선을 위해 이전 Generator 성공 이력을 제거한다."""
    with settings.pipeline_connection() as connection:
        connection.execute("DELETE FROM generator_runs WHERE status = 'SUCCESS'")
        connection.commit()
