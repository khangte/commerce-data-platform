"""실제 dbt Test 실패가 Published Warehouse를 바꾸지 못하는지 검증한다 (AC-09)."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from functools import partial
from pathlib import Path

import pytest

from src.common.database import PostgresSettings
from src.ingestion.errors import DBT_TEST_ERROR
from src.warehouse.dbt_runner import run_dbt_build
from src.warehouse.errors import PublishInProgressError, WarehouseBuildError
from src.warehouse.mart_hash import mart_logical_hashes, mart_row_counts
from src.warehouse.publish import WarehousePaths, publish_warehouse
from src.warehouse.publish_metadata import (
    FAILED,
    PublishRun,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        any(
            os.environ.get(flag) != "1"
            for flag in (
                "RUN_POSTGRES_INTEGRATION",
                "RUN_SEAWEEDFS_INTEGRATION",
                "RUN_DBT_PUBLISH_INTEGRATION",
            )
        ),
        reason="Set PostgreSQL, SeaweedFS and RUN_DBT_PUBLISH_INTEGRATION flags to run real dbt.",
    ),
]

CANARY_RUNNER = partial(run_dbt_build, extra_args=("--vars", "{publish_gate_canary: true}"))


@pytest.fixture
def settings() -> Iterator[PostgresSettings]:
    """활성 Publish가 없을 때만 실행하고 테스트 Run Row를 정리한다."""
    settings = PostgresSettings.from_environment()
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")
    yield settings
    with settings.pipeline_connection() as connection:
        connection.execute(
            "DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'"
        )


def _run() -> PublishRun:
    """테스트 전용 Pipeline 이름을 가진 Publish Run을 만든다."""
    return PublishRun(uuid.uuid4(), f"test_publish_{uuid.uuid4().hex[:8]}")


def test_dbt_test_failure_keeps_the_published_warehouse(
    settings: PostgresSettings, tmp_path: Path
) -> None:
    """Publish 성공 → Canary 실패 → 재Publish에서 실패 Run은 Published Hash를 바꾸지 않는다."""
    paths = WarehousePaths.under(tmp_path)

    first = publish_warehouse(settings, paths, _run(), dbt_runner=run_dbt_build)
    assert mart_logical_hashes(paths.published) == first.mart_hashes
    assert mart_row_counts(paths.published) == first.mart_row_counts

    failing = _run()
    with pytest.raises(WarehouseBuildError) as raised:
        publish_warehouse(settings, paths, failing, dbt_runner=CANARY_RUNNER)
    assert raised.value.error_type == DBT_TEST_ERROR
    assert mart_logical_hashes(paths.published) == first.mart_hashes
    assert mart_row_counts(paths.published) == first.mart_row_counts
    failed_record = get_publish_run(settings, failing.publish_run_id)
    assert failed_record is not None
    assert (failed_record.status, failed_record.error_type) == (FAILED, DBT_TEST_ERROR)
    assert paths.failed_file(failing.publish_run_id).is_file()

    third = publish_warehouse(settings, paths, _run(), dbt_runner=run_dbt_build)
    assert third.previous_publish_run_id == first.publish_run_id
    assert third.mart_hashes == first.mart_hashes
    assert third.changed_relations == ()
