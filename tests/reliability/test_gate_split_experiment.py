"""지표 Test 실패가 v1 전체 Gate와 v2 심각도 분리에서 주문 Fact를 얼마나 묶는지 측정한다."""

from __future__ import annotations

import os
from datetime import UTC
from functools import partial
from pathlib import Path

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.observability.run_report import judge_run_report
from src.warehouse.dbt_runner import DbtRunResult, run_dbt_build
from src.warehouse.errors import DBT_TEST_ERROR, PublishInProgressError, WarehouseBuildError
from src.warehouse.mart_hash import mart_logical_hashes, mart_row_counts
from src.warehouse.publish import WarehousePaths, publish_warehouse
from src.warehouse.publish_metadata import (
    FAILED,
    PUBLISHED,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_publish_run,
    get_tests_warned,
)
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

CANARY_NODE = "test.commerce_data_platform.rpt_publish_gate_canary"


def _boundary(settings: PostgresSettings) -> str:
    """최신 주문 Batch 하나를 뺀 직전 Batch의 마지막 Commit 시각을 UTC 문자열로 고른다.

    Batch 중간 시각을 고르면 주문과 하위 Table이 어긋나 Fact Test가 실패하므로 Batch 끝을 쓴다.
    """
    with settings.pipeline_connection() as connection:
        row = connection.execute(
            """
            SELECT max(committed_at) AS batch_end
            FROM bronze_objects
            WHERE status = 'COMMITTED'
              AND batch_id IN (
                  SELECT batch_id FROM bronze_objects
                  WHERE status = 'COMMITTED' AND source_table = 'orders' AND row_count > 0
              )
            GROUP BY batch_id
            ORDER BY batch_end DESC
            OFFSET 1 LIMIT 1
            """
        ).fetchone()
    assert row is not None, "Experiment needs at least two committed order batches"
    return row[0].astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")


def _order_fact(path: Path) -> tuple[int, str, float]:
    """fct_order의 행 수, 최대 주문 시각 문자열, 같은 시각의 Epoch 초를 반환한다."""
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(
            "SELECT count(*), max(purchase_at)::VARCHAR, epoch(max(purchase_at))"
            " FROM facts.fct_order"
        ).fetchone()


def _full_refresh_runner(dbt_vars: dict[str, object], results: list[DbtRunResult]):
    """Full Refresh dbt Build를 실행하고 결과를 results에 남기는 Runner를 만든다."""
    runner = partial(run_dbt_build, extra_args=("--full-refresh",), dbt_vars=dbt_vars)

    def recording(build_path: Path, target_path: Path) -> DbtRunResult:
        """dbt 실행 결과를 기록하고 그대로 돌려준다."""
        result = runner(build_path, target_path)
        results.append(result)
        return result

    return recording


def test_metric_test_failure_blocks_orders_in_v1_and_not_in_v2(tmp_path: Path) -> None:
    """같은 Bronze·같은 Canary 실패에서 v1은 새 주문을 묶고 v2는 경고와 함께 게시한다."""
    settings = PostgresSettings.from_environment()
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")

    try:
        paths = WarehousePaths.under(tmp_path)
        boundary = _boundary(settings)

        baseline = publish_warehouse(
            settings, paths, _new_publish_run(),
            dbt_runner=_full_refresh_runner({"bronze_as_of": boundary}, []),
        )
        published_rows, published_max, published_epoch = _order_fact(paths.published)

        v1_run = _new_publish_run()
        v1_results: list[DbtRunResult] = []
        with pytest.raises(WarehouseBuildError) as raised:
            publish_warehouse(
                settings, paths, v1_run,
                dbt_runner=_full_refresh_runner(
                    {"rpt_publish_gate_canary": True, "metrics_test_severity": "error"}, v1_results
                ),
            )
        assert raised.value.error_type == DBT_TEST_ERROR
        # Fact가 멀쩡한데 지표 Test 하나만으로 막혔다는 것이 이 실험의 전제다.
        v1_failed_nodes = v1_results[0].failed_nodes
        assert v1_failed_nodes == (CANARY_NODE,)
        v1_record = get_publish_run(settings, v1_run.publish_run_id)
        assert (v1_record.status, v1_record.error_type) == (FAILED, DBT_TEST_ERROR)
        assert mart_logical_hashes(paths.published) == baseline.mart_hashes
        assert mart_row_counts(paths.published) == baseline.mart_row_counts

        blocked_rows, blocked_max, blocked_epoch = _order_fact(paths.failed_file(v1_run.publish_run_id))
        withheld_rows = blocked_rows - published_rows
        assert withheld_rows > 0, "Boundary must withhold orders; a zero result proves nothing"

        v2_run = _new_publish_run()
        publish_warehouse(
            settings, paths, v2_run,
            dbt_runner=_full_refresh_runner({"rpt_publish_gate_canary": True}, []),
        )
        v2_record = get_publish_run(settings, v2_run.publish_run_id)
        v2_tests_warned = get_tests_warned(settings, v2_run.publish_run_id)
        assert v2_record.status == PUBLISHED
        assert v2_tests_warned == 1
        v2_rows, v2_max, _ = _order_fact(paths.published)
        assert (v2_rows, v2_max) == (blocked_rows, blocked_max)
        v2_failures = judge_run_report(
            {
                "publish_status": v2_record.status,
                "serving_export_status": "SUCCESS",
                "metabase_repoint_status": "SUCCESS",
                "tests_warned": v2_tests_warned,
            }
        )
        assert v2_failures == ["DBT_TEST_WARN"]

        write_evidence(
            "gate-split",
            {
                "boundary": boundary,
                "published_order_rows": published_rows,
                "published_max_purchase_at": published_max,
                "blocked_build_order_rows": blocked_rows,
                "blocked_build_max_purchase_at": blocked_max,
                "withheld_order_rows": withheld_rows,
                "withheld_purchase_lag_s": blocked_epoch - published_epoch,
                "v1_status": v1_record.status,
                "v1_error_type": v1_record.error_type,
                "v1_failed_nodes": list(v1_failed_nodes),
                "v2_status": v2_record.status,
                "v2_tests_warned": v2_tests_warned,
                "v2_published_order_rows": v2_rows,
                "v2_failures": v2_failures,
            },
        )
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute(
                "DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'"
            )
