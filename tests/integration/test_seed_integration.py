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
    input_dir = PROJECT_ROOT / "data" / "raw" / "olist"
    seeded_at = parse_seeded_at("2026-09-03T00:00:00Z")

    with settings.pipeline_connection() as connection:
        previous = connection.execute(
            """
            SELECT raw_checksum, table_row_counts, table_content_hashes
            FROM seed_runs
            WHERE status = 'SUCCESS'
            ORDER BY finished_at DESC
            LIMIT 1
            """
        ).fetchone()

    current = run_seed(input_dir, seeded_at, settings)
    if previous is None:
        previous = run_seed(input_dir, seeded_at, settings)
        previous_checksum = previous.raw_checksum
        previous_counts = previous.table_row_counts
        previous_hashes = previous.table_content_hashes
    else:
        previous_checksum, previous_counts, previous_hashes = previous

    assert current.raw_checksum == previous_checksum
    assert current.table_row_counts == previous_counts
    assert current.table_content_hashes == previous_hashes

    with settings.source_connection() as connection:
        for table_name, expected_count in current.table_row_counts.items():
            assert connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0] == expected_count


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

    with pytest.raises(ValueError, match="baseline input differs"):
        run_seed(
            PROJECT_ROOT / "data" / "raw" / "olist",
            parse_seeded_at("2026-09-04T00:00:00Z"),
            settings,
        )
