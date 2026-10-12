from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import replace

import psycopg
import pytest
from psycopg import sql

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.seed.loader import TARGET_COLUMNS, parse_seeded_at, run_seed

pytestmark = pytest.mark.integration


@pytest.fixture
def isolated_seed_settings() -> Iterator[PostgresSettings]:
    """각 Seed 테스트에 별도 Source·Metadata DB를 만들고 종료 후 제거한다."""
    settings = PostgresSettings.from_environment()
    suffix = uuid.uuid4().hex
    source_database = f"test_seed_source_{suffix}"
    pipeline_database = f"test_seed_metadata_{suffix}"
    isolated = replace(
        settings,
        source_database=source_database,
        source_user=settings.admin_user,
        source_password=settings.admin_password,
        pipeline_database=pipeline_database,
        pipeline_user=settings.admin_user,
        pipeline_password=settings.admin_password,
    )
    with psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname="postgres",
        user=settings.admin_user,
        password=settings.admin_password,
        autocommit=True,
    ) as admin_connection:
        created_databases = []
        try:
            for database_name in (source_database, pipeline_database):
                admin_connection.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
                )
                created_databases.append(database_name)
            yield isolated
        finally:
            for database_name in reversed(created_databases):
                admin_connection.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database_name))
                )


@pytest.mark.skipif(
    os.environ.get("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Set RUN_POSTGRES_INTEGRATION=1 after starting the Phase 1 PostgreSQL container.",
)
def test_same_raw_input_and_seeded_at_are_idempotent(
    isolated_seed_settings: PostgresSettings,
) -> None:
    """격리된 기준선에서 같은 입력의 두 Seed 결과가 동일한지 검증한다."""
    settings = isolated_seed_settings
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
    """운영 Source의 열 구성이 허용 목록과 같은지 읽기 전용으로 확인한다."""
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
def test_seed_guard_rejects_a_changed_baseline_after_success(
    isolated_seed_settings: PostgresSettings,
) -> None:
    """격리된 기준선에서 변경된 Seed 입력을 Guard가 거부하는지 검증한다."""
    settings = isolated_seed_settings
    run_seed(
        PROJECT_ROOT / "data" / "raw" / "olist",
        parse_seeded_at("2026-09-03T00:00:00Z"),
        settings,
    )

    with pytest.raises(ValueError, match="baseline input differs"):
        run_seed(
            PROJECT_ROOT / "data" / "raw" / "olist",
            parse_seeded_at("2026-09-04T00:00:00Z"),
            settings,
        )
