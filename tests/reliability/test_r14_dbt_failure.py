"""R-14 dbt Failure가 Bronze/Watermark와 마지막 성공 Mart를 유지하는지 검증한다."""

from __future__ import annotations

import hashlib
import os
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
    PUBLISHED,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
)
from tests.integration.test_publish_gate_dbt_integration import CANARY_RUNNER
from tests.integration.test_publish_gate_dbt_integration import _run as _new_publish_run
from tests.reliability.harness import write_evidence

pytestmark = [
    pytest.mark.integration,
    pytest.mark.reliability,
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


def _bronze_state_hash(settings: PostgresSettings) -> str:
    """bronze_objects 전체 행을 정렬해 MD5 하나로 요약한다."""
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            "SELECT table_batch_id, object_key, status, row_count, logical_hash"
            " FROM bronze_objects ORDER BY table_batch_id"
        ).fetchall()
    return hashlib.md5(repr(rows).encode("utf-8")).hexdigest()


def _watermark_state_hash(settings: PostgresSettings) -> str:
    """watermarks 전체 행을 정렬해 MD5 하나로 요약한다."""
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            "SELECT pipeline_name, source_table, watermark_timestamp, watermark_keys, version"
            " FROM watermarks ORDER BY pipeline_name, source_table"
        ).fetchall()
    return hashlib.md5(repr(rows).encode("utf-8")).hexdigest()


def test_r14_a_failed_build_holds_bronze_watermark_and_the_published_mart(
    tmp_path: Path,
) -> None:
    """Canary dbt Test 실패가 Bronze/Watermark/Published Mart를 바꾸지 않고 격리되는지 확인한다.

    test_dbt_test_failure_keeps_the_published_warehouse
    (tests/integration/test_publish_gate_dbt_integration.py)의 Reliability 확장이다.
    차이는 Bronze/Watermark 불변 확인과 증적 저장뿐이므로 한쪽을 고치면 다른 쪽도 함께 본다.
    """
    settings = PostgresSettings.from_environment()
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")

    try:
        paths = WarehousePaths.under(tmp_path)

        first = publish_warehouse(settings, paths, _new_publish_run(), dbt_runner=run_dbt_build)
        assert mart_logical_hashes(paths.published) == first.mart_hashes
        assert mart_row_counts(paths.published) == first.mart_row_counts

        bronze_hash_before = _bronze_state_hash(settings)
        watermark_hash_before = _watermark_state_hash(settings)

        failing = _new_publish_run()
        with pytest.raises(WarehouseBuildError) as raised:
            publish_warehouse(settings, paths, failing, dbt_runner=CANARY_RUNNER)
        assert raised.value.error_type == DBT_TEST_ERROR

        bronze_hash_after = _bronze_state_hash(settings)
        watermark_hash_after = _watermark_state_hash(settings)
        assert bronze_hash_after == bronze_hash_before
        assert watermark_hash_after == watermark_hash_before
        assert mart_logical_hashes(paths.published) == first.mart_hashes
        assert mart_row_counts(paths.published) == first.mart_row_counts

        failed_record = get_publish_run(settings, failing.publish_run_id)
        assert failed_record is not None
        assert (failed_record.status, failed_record.error_type) == (FAILED, DBT_TEST_ERROR)
        assert paths.failed_file(failing.publish_run_id).is_file()

        second = publish_warehouse(settings, paths, _new_publish_run(), dbt_runner=run_dbt_build)
        assert second.previous_publish_run_id == first.publish_run_id
        assert second.mart_hashes == first.mart_hashes
        assert second.changed_relations == ()

        second_record = get_publish_run(settings, second.publish_run_id)
        assert second_record is not None
        assert second_record.status == PUBLISHED
        published_mart_hash_after_recovery = mart_logical_hashes(paths.published)
        assert published_mart_hash_after_recovery == first.mart_hashes

        write_evidence(
            "r14",
            {
                "first_publish_run_id": str(first.publish_run_id),
                "failing_publish_run_id": str(failing.publish_run_id),
                "second_publish_run_id": str(second.publish_run_id),
                "failed_error_type": failed_record.error_type,
                "bronze_hash_unchanged": bronze_hash_after == bronze_hash_before,
                "watermark_hash_unchanged": watermark_hash_after == watermark_hash_before,
                "published_mart_hash_unchanged": mart_logical_hashes(paths.published)
                == first.mart_hashes,
                "second_publish_chains_to_first": second.previous_publish_run_id
                == first.publish_run_id,
                "second_publish_status": second_record.status,
                "published_mart_hash_after_recovery": published_mart_hash_after_recovery,
            },
        )
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute(
                "DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'"
            )
