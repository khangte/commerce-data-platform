"""도메인 Canary 실패에서 전체 차단과 도메인 부분 게시가 각 도메인을 얼마나 묶는지 측정한다."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from time import perf_counter

import duckdb
import pytest

from src.common.database import PostgresSettings
from src.common.row_hash import hash_cursor_rows
from src.ingestion.bronze.storage import SeaweedFSSettings
from src.observability.run_report import judge_run_report
from src.warehouse.dbt_runner import DbtRunResult, run_dbt_build
from src.warehouse.domain_gate import domain_table
from src.warehouse.errors import DBT_TEST_ERROR, PublishInProgressError, WarehouseBuildError
from src.warehouse.mart_hash import mart_logical_hashes
from src.warehouse.publish import (
    WarehousePaths,
    build_warehouse,
    prepare_warehouse_build,
    publish_build,
    publish_warehouse,
)
from src.warehouse.publish_metadata import (
    FAILED,
    PUBLISHED,
    PublishRun,
    assert_no_active_publish,
    ensure_publish_metadata,
    get_held_domains,
    get_publish_run,
)
from tests.integration.test_publish_gate_dbt_integration import _run as _new_publish_run
from tests.reliability.faults import backdated_tier_observation
from tests.reliability.harness import write_evidence
from tests.reliability.test_gate_split_experiment import (
    _boundary,
    _full_refresh_runner,
    _order_fact,
)
from tests.test_warehouse_domain_boundary import assert_groups_match_lineage
from tests.test_warehouse_domain_gate import DOMAINS, SHARED

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

SCHEMAS = {"dim": "dimensions", "fct": "facts", "rpt": "metrics"}
ALL_MODELS = (*SHARED, *DOMAINS["orders"], *DOMAINS["subscriptions"])
SUBSCRIPTION_CANARY = "subscriptions_publish_gate_canary"
ORDER_CANARY = "orders_publish_gate_canary"
DIM_SUBSCRIPTION_CANARY = "dim_subscription_publish_gate_canary"
UNATTRIBUTED_CANARY = "publish_gate_canary"


@dataclass(frozen=True)
class Attempt:
    """스위치 하나로 실행한 Publish 시도의 결과와 dbt 산출물 요약을 담는다."""

    run: PublishRun
    status: str
    error_type: str | None
    held_domains: tuple[str, ...]
    failed_nodes: tuple[str, ...]
    skipped_nodes: int
    build_seconds: float
    manifest: dict
    error_message: str | None = None


def _attempt(
    settings: PostgresSettings,
    paths: WarehousePaths,
    canaries: tuple[str, ...],
    *,
    partial_publish: bool,
    before_build: Callable[[Path], object] | None = None,
) -> Attempt:
    """운영과 같은 증분 dbt Build로 한 번 게시를 시도하고 결과를 모은다.

    before_build는 Build 파일과 Catalog가 준비된 뒤 dbt 실행 전에 Bronze 조건을 바꿀 때 쓴다.
    """
    run = _new_publish_run()
    observed: dict[str, object] = {}
    runner = partial(run_dbt_build, dbt_vars=dict.fromkeys(canaries, True) or None)

    def recording(build_path: Path, target_path: Path) -> DbtRunResult:
        """Publish가 Target Directory를 지우기 전에 산출물을 읽어 둔다."""
        result = runner(build_path, target_path)
        observed["result"] = result
        observed["manifest"] = json.loads((target_path / "manifest.json").read_text())
        results = json.loads((target_path / "run_results.json").read_text())["results"]
        observed["skipped"] = sum(item["status"] == "skipped" for item in results)
        return result

    prepare_warehouse_build(settings, paths, run)
    if before_build is not None:
        before_build(paths.build_file(run.publish_run_id))
    started = perf_counter()
    error_message = None
    try:
        build_warehouse(
            settings, paths, run.publish_run_id, dbt_runner=recording, partial_publish=partial_publish
        )
    except WarehouseBuildError as error:
        build_seconds = perf_counter() - started
        error_message = str(error)
    else:
        build_seconds = perf_counter() - started
        publish_build(settings, paths, run.publish_run_id)
    record = get_publish_run(settings, run.publish_run_id)
    return Attempt(
        run=run,
        status=record.status,
        error_type=record.error_type,
        held_domains=get_held_domains(settings, run.publish_run_id),
        failed_nodes=observed["result"].failed_nodes,
        skipped_nodes=observed["skipped"],
        build_seconds=build_seconds,
        manifest=observed["manifest"],
        error_message=error_message,
    )


def _relation(model: str) -> str:
    return f"{SCHEMAS[model.split('_')[0]]}.{model}"


def _hashes(path: Path, models: tuple[str, ...]) -> dict[str, str]:
    """rpt까지 포함한 Relation별 내용 Hash를 계산한다. MART_HASH_TARGETS에는 rpt가 없다."""
    with duckdb.connect(str(path), read_only=True) as connection:
        return {
            model: hash_cursor_rows(
                connection.execute(f"SELECT * FROM {_relation(model)} ORDER BY ALL")
            )[0]
            for model in models
        }


def _rows(path: Path, relation: str) -> int:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]


def _withheld_orders(published: Path, built: Path) -> tuple[int, float]:
    """이번 Build에 있지만 게시본에 없는 주문 행 수와 주문 시각 지연을 반환한다."""
    published_rows, _, published_epoch = _order_fact(published)
    built_rows, _, built_epoch = _order_fact(built)
    return built_rows - published_rows, built_epoch - published_epoch


def _withheld_payments(published: Path, built: Path) -> int:
    relation = "facts.fct_subscription_payment"
    return _rows(built, relation) - _rows(published, relation)


def _assert_manifest_matches_domain_table(manifest: dict) -> None:
    """dbt Group이 계보 기준과 같고, 이 실험이 Hash를 비교하는 Model 표와도 같은지 단언한다."""
    nodes = manifest["nodes"]
    assert_groups_match_lineage(nodes)
    table = domain_table(nodes)
    assert table.shared == set(SHARED)
    assert table.domains == {domain: set(names) for domain, names in DOMAINS.items()}


def _judge(attempt: Attempt) -> list[str]:
    """Serving 단계가 정상이라고 두고 Publish 결과만의 Run report 판정을 계산한다."""
    return judge_run_report(
        {
            "publish_status": attempt.status,
            "serving_export_status": "SUCCESS",
            "metabase_repoint_status": "SUCCESS",
            "held_domains": list(attempt.held_domains),
        }
    )


def _node(canary: str) -> str:
    return f"test.commerce_data_platform.{canary}"


def _key_count(path: Path, customer_key: str) -> tuple[int, int]:
    """dim_customer에 그 Key가 있는 행 수와 구독 결제가 그 Key를 참조하는 행 수를 반환한다."""
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(
            "SELECT (SELECT count(*) FROM dimensions.dim_customer WHERE customer_key = $key),"
            " (SELECT count(*) FROM facts.fct_subscription_payment WHERE customer_key = $key)",
            {"key": customer_key},
        ).fetchone()


def _boundary_node(error_message: str) -> str:
    """전체 차단 사유 첫 줄에서 실패한 경계 Test Node ID를 꺼낸다."""
    return error_message.splitlines()[0].split("boundary tests failed: ", 1)[1]


def test_domain_failure_only_holds_its_own_domain(tmp_path: Path) -> None:
    """같은 Bronze·같은 Canary에서 스위치만 바꿔 묶이는 범위와 복구 후 Hash를 비교한다."""
    settings = PostgresSettings.from_environment()
    ensure_publish_metadata(settings)
    try:
        assert_no_active_publish(settings)
    except PublishInProgressError:
        pytest.skip("A real publish run is active; rerun after it finishes.")

    try:
        base = WarehousePaths.under(tmp_path / "baseline")
        base.published.parent.mkdir(parents=True)
        boundary = _boundary(settings)
        publish_warehouse(
            settings, base, _new_publish_run(),
            dbt_runner=_full_refresh_runner({"bronze_as_of": boundary}, []),
        )
        baseline_all = _hashes(base.published, ALL_MODELS)

        def fresh(name: str) -> WarehousePaths:
            """Baseline 게시본에서 시작하는 독립 Warehouse 경로를 만든다."""
            paths = WarehousePaths.under(tmp_path / name)
            paths.published.parent.mkdir(parents=True)
            shutil.copyfile(base.published, paths.published)
            return paths

        def compare(
            name: str, *canaries: str
        ) -> tuple[WarehousePaths, Attempt, WarehousePaths, Attempt]:
            """스위치 끔·켬을 각각 Baseline에서 실행하고 끔의 차단을 단언한다."""
            off_paths = fresh(f"{name}-off")
            off = _attempt(settings, off_paths, canaries, partial_publish=False)
            assert (off.status, off.error_type) == (FAILED, DBT_TEST_ERROR)
            assert off.held_domains == ()
            assert _hashes(off_paths.published, ALL_MODELS) == baseline_all
            on_paths = fresh(f"{name}-on")
            return off_paths, off, on_paths, _attempt(settings, on_paths, canaries, partial_publish=True)

        reference_paths = fresh("reference")
        reference = _attempt(settings, reference_paths, (), partial_publish=True)
        assert (reference.status, reference.held_domains) == (PUBLISHED, ())
        _assert_manifest_matches_domain_table(reference.manifest)
        reference_all = _hashes(reference_paths.published, ALL_MODELS)
        reference_mart = mart_logical_hashes(reference_paths.published)

        # A: 구독 Canary가 실패해도 주문은 게시된다.
        a_off_paths, a_off, a_on_paths, a_on = compare("a", SUBSCRIPTION_CANARY)
        assert a_off.failed_nodes == a_on.failed_nodes == (_node(SUBSCRIPTION_CANARY),)
        assert (a_on.status, a_on.held_domains) == (PUBLISHED, ("subscriptions",))
        a_copy = a_on_paths.failed_file(a_on.run.publish_run_id)
        assert _hashes(a_on_paths.published, DOMAINS["orders"]) == _hashes(a_copy, DOMAINS["orders"])
        assert _hashes(a_on_paths.published, DOMAINS["subscriptions"]) == {
            model: baseline_all[model] for model in DOMAINS["subscriptions"]
        }
        assert "PUBLISH_PARTIAL:subscriptions" in _judge(a_on)
        a_off_rows, a_off_lag = _withheld_orders(
            a_off_paths.published, a_off_paths.failed_file(a_off.run.publish_run_id)
        )
        a_on_rows, a_on_lag = _withheld_orders(a_on_paths.published, a_copy)
        assert a_off_rows > 0, "Boundary must withhold orders; a zero result proves nothing"
        assert a_on_rows == 0
        assert a_on_lag == 0

        # B: 주문 Canary가 실패하면 주문만 Baseline에 남고 구독은 게시된다.
        b_off_paths, b_off, b_on_paths, b_on = compare("b", ORDER_CANARY)
        assert (b_on.status, b_on.held_domains) == (PUBLISHED, ("orders",))
        b_copy = b_on_paths.failed_file(b_on.run.publish_run_id)
        assert _hashes(b_on_paths.published, DOMAINS["orders"]) == {
            model: baseline_all[model] for model in DOMAINS["orders"]
        }
        assert "PUBLISH_PARTIAL:orders" in _judge(b_on)
        b_off_payments = _withheld_payments(
            b_off_paths.published, b_off_paths.failed_file(b_off.run.publish_run_id)
        )
        b_on_payments = _withheld_payments(b_on_paths.published, b_copy)
        b_on_orders, b_on_lag = _withheld_orders(b_on_paths.published, b_copy)

        # C: 어느 도메인에도 귀속되지 않는 Canary는 스위치와 무관하게 전체 차단이다.
        _, c_off, c_on_paths, c_on = compare("c", UNATTRIBUTED_CANARY)
        assert (c_on.status, c_on.error_type) == (FAILED, DBT_TEST_ERROR)
        assert c_on.held_domains == ()
        assert _hashes(c_on_paths.published, ALL_MODELS) == baseline_all
        assert "PUBLISH_FAILED" in _judge(c_on)

        # E: 상위 Dimension 실패로 intermediate·경계 Test가 skipped여도 구독만 보류한다.
        e_off_paths, e_off, e_on_paths, e_on = compare("e", DIM_SUBSCRIPTION_CANARY)
        assert (e_on.status, e_on.held_domains) == (PUBLISHED, ("subscriptions",))
        assert e_on.skipped_nodes > 0
        e_off_rows, _ = _withheld_orders(
            e_off_paths.published, e_off_paths.failed_file(e_off.run.publish_run_id)
        )
        e_on_rows, _ = _withheld_orders(
            e_on_paths.published, e_on_paths.failed_file(e_on.run.publish_run_id)
        )
        assert e_on_rows == 0
        assert "PUBLISH_PARTIAL:subscriptions" in _judge(e_on)

        # F: 두 도메인 Canary가 함께 실패하면 어느 한쪽에 귀속되지 않아 스위치를 켜도 전체 차단이다.
        _, f_off, f_on_paths, f_on = compare("f", ORDER_CANARY, SUBSCRIPTION_CANARY)
        both = tuple(sorted((_node(ORDER_CANARY), _node(SUBSCRIPTION_CANARY))))
        assert tuple(sorted(f_off.failed_nodes)) == tuple(sorted(f_on.failed_nodes)) == both
        assert (f_on.status, f_on.error_type, f_on.held_domains) == (FAILED, DBT_TEST_ERROR, ())
        assert "partial publish fallback" not in (f_on.error_message or "")
        assert _hashes(f_on_paths.published, ALL_MODELS) == baseline_all
        assert "PUBLISH_FAILED" in _judge(f_on)

        # G: 가입 시각을 앞당긴 등급 관측이 늦게 도착해 첫 고객 Version Key가 바뀐 Bronze에서 구독을 보류하면,
        # 되돌린 구독 결제가 사라진 Key를 참조해 경계 Test가 실패하고 전체 차단으로 물러나야 한다.
        with backdated_tier_observation(SeaweedFSSettings.from_environment()) as inject:
            injected = []
            g_control_paths = fresh("g-control")
            g_control = _attempt(
                settings, g_control_paths, (), partial_publish=True,
                before_build=lambda path: injected.append(inject(path)),
            )
            g_paths = fresh("g-on")
            g_on = _attempt(
                settings, g_paths, (SUBSCRIPTION_CANARY,), partial_publish=True,
                before_build=lambda path: injected.append(inject(path)),
            )
        fault = injected[0]
        assert injected[1].replaced_customer_key == fault.replaced_customer_key
        # 대조: 보류가 없으면 영향 Key 재계산이 결제를 새 Key로 옮겨 그대로 게시된다.
        assert (g_control.status, g_control.held_domains) == (PUBLISHED, ())
        assert _key_count(g_control_paths.published, fault.replaced_customer_key) == (0, 0)
        assert (g_on.status, g_on.error_type, g_on.held_domains) == (FAILED, DBT_TEST_ERROR, ())
        assert "partial publish fallback: subscriptions: boundary tests failed" in g_on.error_message
        boundary_node = _boundary_node(g_on.error_message)
        assert "fct_subscription_payment_customer_key" in boundary_node
        assert _hashes(g_paths.published, ALL_MODELS) == baseline_all
        g_copy = g_paths.failed_file(g_on.run.publish_run_id)
        assert g_copy.exists()
        assert not g_paths.build_file(g_on.run.publish_run_id).exists()
        # failed/ 사본은 되돌리기 전 Build다. 새 dim_customer에 옛 Key가 없고 결제도 옮겨져 있다.
        assert _key_count(g_copy, fault.replaced_customer_key) == (0, 0)
        g_baseline_refs = _key_count(base.published, fault.replaced_customer_key)
        assert g_baseline_refs[0] == 1 and g_baseline_refs[1] > 0
        assert "PUBLISH_FAILED" in _judge(g_on)

        # D: A의 부분 게시본에서 Canary 없이 다시 돌리면 한 번에 게시한 결과와 같아야 한다.
        d_on = _attempt(settings, a_on_paths, (), partial_publish=True)
        assert (d_on.status, d_on.held_domains) == (PUBLISHED, ())
        d_all = _hashes(a_on_paths.published, ALL_MODELS)
        d_mismatched = sorted(model for model in ALL_MODELS if d_all[model] != reference_all[model])
        assert d_mismatched == []
        assert mart_logical_hashes(a_on_paths.published) == reference_mart

        # D-orders: 구독은 Baseline 뒤 변화가 없을 수 있어 A의 복구만으로는 되돌린 증분이 검증되지
        # 않는다. 변화가 있는 orders를 보류한 B에서도 복구해 Hash가 같은지 본다.
        changed = sorted(model for model in ALL_MODELS if baseline_all[model] != reference_all[model])
        assert "fct_order" in changed, "Orders recovery proves nothing without a delta"
        d_orders = _attempt(settings, b_on_paths, (), partial_publish=True)
        assert (d_orders.status, d_orders.held_domains) == (PUBLISHED, ())
        d_orders_all = _hashes(b_on_paths.published, ALL_MODELS)
        d_orders_mismatched = sorted(
            model for model in ALL_MODELS if d_orders_all[model] != reference_all[model]
        )
        assert d_orders_mismatched == []
        assert mart_logical_hashes(b_on_paths.published) == reference_mart

        write_evidence(
            "gate-domain-split",
            {
                "boundary": boundary,
                "baseline_order_rows": _order_fact(base.published)[0],
                "baseline_subscription_payment_rows": _rows(
                    base.published, "facts.fct_subscription_payment"
                ),
                "reference_dbt_build_s": reference.build_seconds,
                "a": {
                    "off_status": a_off.status,
                    "off_error_type": a_off.error_type,
                    "off_withheld_order_rows": a_off_rows,
                    "off_withheld_purchase_lag_s": a_off_lag,
                    "on_status": a_on.status,
                    "on_held_domains": list(a_on.held_domains),
                    "on_withheld_order_rows": a_on_rows,
                    "on_withheld_purchase_lag_s": a_on_lag,
                    "on_failures": _judge(a_on),
                    "on_skipped_nodes": a_on.skipped_nodes,
                    "on_dbt_build_s": a_on.build_seconds,
                },
                "b": {
                    "off_status": b_off.status,
                    "off_withheld_subscription_payment_rows": b_off_payments,
                    "on_status": b_on.status,
                    "on_held_domains": list(b_on.held_domains),
                    "on_withheld_subscription_payment_rows": b_on_payments,
                    "on_withheld_order_rows": b_on_orders,
                    "on_withheld_purchase_lag_s": b_on_lag,
                    "on_failures": _judge(b_on),
                },
                "c": {
                    "off_status": c_off.status,
                    "on_status": c_on.status,
                    "on_error_type": c_on.error_type,
                    "on_held_domains": list(c_on.held_domains),
                    "on_failures": _judge(c_on),
                },
                "e": {
                    "off_status": e_off.status,
                    "off_withheld_order_rows": e_off_rows,
                    "on_status": e_on.status,
                    "on_held_domains": list(e_on.held_domains),
                    "on_withheld_order_rows": e_on_rows,
                    "on_skipped_nodes": e_on.skipped_nodes,
                    "on_failures": _judge(e_on),
                },
                "f": {
                    "failed_nodes": list(both),
                    "off_status": f_off.status,
                    "off_error_type": f_off.error_type,
                    "on_status": f_on.status,
                    "on_error_type": f_on.error_type,
                    "on_held_domains": list(f_on.held_domains),
                    "on_published_hash_unchanged": True,
                    "on_failures": _judge(f_on),
                },
                "g": {
                    "injected_object_key": fault.object_key,
                    "customer_id": fault.customer_id,
                    "membership_tier": fault.membership_tier,
                    "replaced_customer_key": fault.replaced_customer_key,
                    "baseline_payment_rows_on_replaced_key": g_baseline_refs[1],
                    "control_status": g_control.status,
                    "control_held_domains": list(g_control.held_domains),
                    "on_status": g_on.status,
                    "on_error_type": g_on.error_type,
                    "on_held_domains": list(g_on.held_domains),
                    "on_failed_nodes": list(g_on.failed_nodes),
                    "on_boundary_node": boundary_node,
                    "on_error_head": g_on.error_message.splitlines()[0],
                    "on_failed_copy": g_copy.name,
                    "on_published_hash_unchanged": True,
                    "on_failures": _judge(g_on),
                },
                "d": {
                    "status": d_on.status,
                    "held_domains": list(d_on.held_domains),
                    "mismatched_models": d_mismatched,
                    "checked_models": len(ALL_MODELS),
                    "dbt_build_s": d_on.build_seconds,
                },
                "d_orders": {
                    "status": d_orders.status,
                    "held_domains": list(d_orders.held_domains),
                    "mismatched_models": d_orders_mismatched,
                    "dbt_build_s": d_orders.build_seconds,
                },
                "models_changed_since_baseline": changed,
            },
        )
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute(
                "DELETE FROM mart_publish_runs WHERE pipeline_name LIKE 'test_publish_%'"
            )
