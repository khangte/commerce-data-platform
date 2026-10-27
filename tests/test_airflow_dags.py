"""DAG Import/Parse Smoke Test(P4-04)를 검증한다.

Airflow Image를 빌드하고 Compose로 실행하므로 무겁다. `airflow` Marker로 분리해
기본 `pytest` 실행에서는 제외하고, `pytest -m airflow`로만 돈다.
"""

from __future__ import annotations

import ast
import os
import re
import subprocess
import uuid
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_SMOKE_SKIP = pytest.mark.skipif(
    os.environ.get("RUN_AIRFLOW_SMOKE_TEST") != "1",
    reason="Set RUN_AIRFLOW_SMOKE_TEST=1 to build and run the airflow Compose profile.",
)


def test_source_dag_schedule_uses_common_default() -> None:
    """원천 DAG 주기는 공통 주기 함수를 호출한다."""
    dag_source = (PROJECT_ROOT / "airflow/dags/source_simulation_dag.py").read_text(
        encoding="utf-8"
    )
    module = ast.parse(dag_source)
    dag_call = next(
        node.items[0].context_expr
        for node in module.body
        if isinstance(node, ast.With)
        and isinstance(node.items[0].context_expr, ast.Call)
        and isinstance(node.items[0].context_expr.func, ast.Name)
        and node.items[0].context_expr.func.id == "DAG"
    )
    schedule = next(keyword.value for keyword in dag_call.keywords if keyword.arg == "schedule")
    assert isinstance(schedule, ast.Call)
    assert isinstance(schedule.func, ast.Name)
    assert schedule.func.id == "source_dag_schedule"
    assert not schedule.args and not schedule.keywords


def _source_dag_param_default(name: str) -> ast.expr:
    """원천 DAG params dict에서 지정 Param의 첫 번째 인자 식을 찾는다."""
    dag_source = (PROJECT_ROOT / "airflow/dags/source_simulation_dag.py").read_text(encoding="utf-8")
    module = ast.parse(dag_source)
    dag_call = next(
        node.items[0].context_expr
        for node in module.body
        if isinstance(node, ast.With)
        and isinstance(node.items[0].context_expr, ast.Call)
        and isinstance(node.items[0].context_expr.func, ast.Name)
        and node.items[0].context_expr.func.id == "DAG"
    )
    params = next(keyword.value for keyword in dag_call.keywords if keyword.arg == "params")
    param_call = next(
        value for key, value in zip(params.keys, params.values, strict=True) if key.value == name
    )
    return param_call.args[0]


@pytest.mark.parametrize(
    ("param", "variable", "configured", "expected"),
    [
        ("orders", "SOURCE_DAG_ORDERS", None, 1),
        ("orders", "SOURCE_DAG_ORDERS", "", 1),
        ("orders", "SOURCE_DAG_ORDERS", "5", 5),
        ("progress_time_scale", "SOURCE_PROGRESS_TIME_SCALE", None, 1),
        ("progress_time_scale", "SOURCE_PROGRESS_TIME_SCALE", "144", 144),
    ],
)
def test_source_dag_param_defaults_from_environment(
    monkeypatch: pytest.MonkeyPatch, param: str, variable: str, configured: str | None, expected: int
) -> None:
    """원천 DAG 주문 수·배율 Param 기본값은 환경 변수 또는 1이다."""
    from src.generator.core.config import int_from_environment

    if configured is None:
        monkeypatch.delenv(variable, raising=False)
    else:
        monkeypatch.setenv(variable, configured)
    default = _source_dag_param_default(param)
    actual = eval(
        compile(ast.Expression(default), "source_simulation_dag.py", "eval"),
        {"int_from_environment": int_from_environment},
    )
    assert actual == expected


@pytest.mark.parametrize(
    ("variable", "configured"),
    [("SOURCE_DAG_ORDERS", "-1"), ("SOURCE_DAG_ORDERS", "abc"), ("SOURCE_PROGRESS_TIME_SCALE", "0")],
)
def test_source_dag_param_rejects_invalid_environment(
    monkeypatch: pytest.MonkeyPatch, variable: str, configured: str
) -> None:
    """잘못된 env 값은 DAG 파싱 단계에서 실패한다."""
    from src.generator.core.config import int_from_environment

    monkeypatch.setenv(variable, configured)
    param = "orders" if variable == "SOURCE_DAG_ORDERS" else "progress_time_scale"
    default = _source_dag_param_default(param)
    with pytest.raises(ValueError, match=variable):
        eval(
            compile(ast.Expression(default), "source_simulation_dag.py", "eval"),
            {"int_from_environment": int_from_environment},
        )


def _run_compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--profile", "airflow", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.airflow
@_SMOKE_SKIP
def test_dags_import_without_errors() -> None:
    """두 DAG 모두 Import Error 없이 Parse된다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        result = _run_compose(
            "exec", "-T", "airflow-scheduler", "airflow", "dags", "list-import-errors"
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert (
            "No data found" in result.stdout or result.stdout.strip() == ""
        ), f"DAG Import Error 발견:\n{result.stdout}"

        listed = _run_compose("exec", "-T", "airflow-scheduler", "airflow", "dags", "list")
        assert listed.returncode == 0, listed.stderr
        assert "source_simulation_dag" in listed.stdout
        assert "warehouse_pipeline_dag" in listed.stdout
    finally:
        _run_compose("down")


@pytest.mark.airflow
@_SMOKE_SKIP
def test_generator_success_triggers_warehouse_with_same_logical_date() -> None:
    """Generator DagRun 성공 뒤 같은 logical_date로 Warehouse DagRun이 생성되는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        # Airflow는 새로 Parse된 DAG를 기본적으로 paused 상태로 만든다(dags_are_paused_at_creation).
        # 이 테스트는 정상 트리거 경로를 검증하는 것이므로 대상 DAG를 먼저 unpause한다.
        unpause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", "warehouse_pipeline_dag",
        )
        assert unpause.returncode == 0, unpause.stdout + unpause.stderr

        logical_date = "2026-09-11T00:00:00+00:00"
        trigger = _run_compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "test",
            "source_simulation_dag",
            logical_date,
            "--conf",
            '{"logical_date": "2026-09-11T00:00:00+00:00", "orders": 1}',
        )
        assert trigger.returncode == 0, trigger.stdout + trigger.stderr

        listed = _run_compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "list-runs",
            "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        assert logical_date in listed.stdout, (
            f"Warehouse DagRun with logical_date={logical_date} not found:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")


@pytest.mark.airflow
@_SMOKE_SKIP
def test_duplicate_generator_run_skips_second_warehouse_trigger() -> None:
    """같은 logical_date로 Generator를 두 번 실행하면 두 번째 Trigger Task는 skip되고 Warehouse DagRun은 1개로 유지되는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        # Airflow는 새로 Parse된 DAG를 기본적으로 paused 상태로 만든다(dags_are_paused_at_creation).
        # 이 테스트는 중복 트리거 경로를 검증하는 것이므로 대상 DAG를 먼저 unpause한다.
        unpause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", "warehouse_pipeline_dag",
        )
        assert unpause.returncode == 0, unpause.stdout + unpause.stderr

        logical_date = "2026-09-11T03:00:00+00:00"
        conf = '{"logical_date": "2026-09-11T03:00:00+00:00", "orders": 1}'

        first = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert first.returncode == 0, first.stdout + first.stderr

        second = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert second.returncode == 0, second.stdout + second.stderr
        assert "trigger_warehouse_pipeline" in second.stdout

        listed = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        run_count = listed.stdout.count(logical_date)
        assert run_count == 1, (
            f"Expected exactly 1 warehouse_pipeline_dag run for {logical_date}, found {run_count}:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")


@pytest.mark.airflow
@_SMOKE_SKIP
def test_paused_warehouse_dag_fails_trigger_task() -> None:
    """warehouse_pipeline_dag가 paused 상태면 Trigger Task가 명시적으로 실패하는지 검증한다."""
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        pause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "pause", "warehouse_pipeline_dag",
        )
        assert pause.returncode == 0, pause.stdout + pause.stderr

        logical_date = "2026-09-11T06:00:00+00:00"
        conf = '{"logical_date": "2026-09-11T06:00:00+00:00", "orders": 1}'
        result = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", conf,
        )
        assert result.returncode != 0, (
            "warehouse_pipeline_dag가 paused일 때 trigger_warehouse_pipeline Task는 실패해야 한다"
        )
        assert "trigger_warehouse_pipeline" in result.stdout + result.stderr
    finally:
        unpause = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", "warehouse_pipeline_dag",
        )
        assert unpause.returncode == 0, unpause.stdout + unpause.stderr
        _run_compose("down")


@pytest.mark.airflow
@_SMOKE_SKIP
def test_generator_failure_does_not_trigger_warehouse() -> None:
    """Generator 실행이 실제로 실패(AirflowFailException)하면 DagRun이 실패로 종료되어
    trigger_warehouse_pipeline Task가 실행되지 못하고(스케줄러 환경에서는 upstream_failed로
    건너뜀) Warehouse DagRun이 생성되지 않는지 검증한다.

    `airflow dags test`는 단일 프로세스로 Task를 순차 실행하는 로컬 테스트 러너라 첫 Task가
    실패하면 DagRun을 즉시 failed로 종료하고 이후 Task를 아예 실행하지 않으므로, stdout에
    "upstream_failed" 문자열 자체는 나타나지 않는다. 따라서 이 테스트는 (1) 실패 원인이
    Param 검증이 아닌 run_source_simulation Task 실행 중 AirflowFailException이라는 점과
    (2) 그 결과 DagRun이 failed로 종료되고 warehouse_pipeline_dag DagRun이 생성되지 않았다는
    점을 관측한다.
    """
    build = _run_compose("build", "airflow-scheduler")
    assert build.returncode == 0, build.stderr

    up = _run_compose("up", "-d")
    assert up.returncode == 0, up.stderr

    try:
        # orders=-1은 Param JSON-Schema 검증(minimum=0) 단계에서 Task 실행 전에 막혀
        # upstream_failed 전이를 관측할 수 없다. 대신 source_snapshot_id에 존재할 수
        # 없는 값을 줘서 run_generator() 내부에서 ValueError가 발생하게 하고,
        # 이를 source_simulation_dag.py가 AirflowFailException으로 재분류해
        # Task 실행 도중 실패하도록 유도한다.
        logical_date = "2026-09-11T12:00:00+00:00"
        nonexistent_snapshot_id = (
            "seed:nonexistent-00000000-0000-0000-0000-000000000000:2020-01-01T00:00:00+00:00"
        )
        failing_conf = (
            '{"logical_date": "2026-09-11T12:00:00+00:00", "orders": 1, '
            f'"source_snapshot_id": "{nonexistent_snapshot_id}"}}'
        )

        result = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "test", "source_simulation_dag", logical_date, "--conf", failing_conf,
        )
        assert result.returncode != 0, (
            "존재하지 않는 source_snapshot_id는 run_generator() 실행 중 "
            "AirflowFailException으로 실패해야 한다"
        )
        combined_output = result.stdout + result.stderr
        # Param 검증이 아니라 Task 실행 도중(run_generator 호출 이후) 발생한 실패인지 확인한다.
        assert "run_source_simulation" in combined_output, (
            f"run_source_simulation Task 실행 흔적을 찾지 못함(Param 검증 단계에서 막혔을 수 있음):\n{combined_output}"
        )
        assert "AirflowFailException" in combined_output, (
            f"AirflowFailException으로 실패한 흔적을 찾지 못함:\n{combined_output}"
        )
        # `airflow dags test`는 첫 Task 실패 시 DagRun을 즉시 failed로 종료하고
        # 스케줄러 없이 downstream Task를 실행하지 않으므로, "DagRun이 failed로
        # 종료되어 trigger_warehouse_pipeline이 실행되지 못했다"가 이 실행 모드에서
        # 관측 가능한 upstream_failed 전이의 대리 신호다.
        assert "DagRun failed" in combined_output, (
            f"DagRun이 failed로 종료된 흔적을 찾지 못함:\n{combined_output}"
        )

        listed = _run_compose(
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "warehouse_pipeline_dag",
        )
        assert listed.returncode == 0, listed.stderr
        assert logical_date not in listed.stdout, (
            f"Generator 실패에도 warehouse_pipeline_dag DagRun이 생성됨:\n{listed.stdout}"
        )
    finally:
        _run_compose("down")


def test_warehouse_pipeline_publishes_then_exports_the_serving_mart() -> None:
    """Warehouse DAG는 Publish 성공 뒤에만 독립된 Serving Mart를 내보낸다."""
    dag_source = (PROJECT_ROOT / "airflow/dags/warehouse_pipeline_dag.py").read_text(
        encoding="utf-8"
    )
    compile(dag_source, "warehouse_pipeline_dag.py", "exec")

    for task_id in ("prepare_warehouse_build", "dbt_build", "publish_mart", "export_serving_mart", "repoint_metabase_serving"):
        pattern = rf'@task\(\s*task_id="{task_id}",\s*trigger_rule="all_success"'
        assert re.search(pattern, dag_source)
    for task_id in ("dbt_build", "publish_mart", "export_serving_mart"):
        pattern = rf'task_id="{task_id}",\s*trigger_rule="all_success",\s*retries=0'
        assert re.search(pattern, dag_source)
    assert "extract_results >> verification >> build >> dbt_result >> publish >> serving_export >> metabase_repoint" in dag_source
    assert "publish_run_summary(run_info, verification, build, publish, serving_export, metabase_repoint)" in dag_source
    assert 'serving_export_status = "SUCCESS" if serving_export else "SERVING_EXPORT_FAILED"' in dag_source
    assert re.search(r'metabase_repoint_summary_status\(\s*serving_export, metabase_repoint\s*\)', dag_source)
    assert 'raise AirflowException("METABASE_REPOINT_FAILED: ' in dag_source
    assert '"retries": 2' in dag_source
    assert 'repoint_and_prune_serving(' in dag_source
    assert not re.search(r'task_id="repoint_metabase_serving"[^\n]*retries=0', dag_source)


def test_repoint_task_fails_when_repoint_result_is_failed(tmp_path: Path) -> None:
    """재지정 결과가 FAILED이면 Airflow Task가 실패 예외를 던진다."""
    source = (PROJECT_ROOT / "airflow/dags/warehouse_pipeline_dag.py").read_text(encoding="utf-8")
    dag_block = next(node for node in ast.parse(source).body if isinstance(node, ast.With))
    task_node = next(
        node for node in dag_block.body
        if isinstance(node, ast.FunctionDef) and node.name == "repoint_metabase_serving_task"
    )
    task_node.decorator_list = []

    class StubAirflowException(Exception):
        """테스트에서 재시도 가능한 Airflow 예외를 대신한다."""

    class StubAirflowFailException(StubAirflowException):
        """재시도하지 않는 하위 예외를 구분한다."""

    def failed_repoint(*args):
        """실패한 재지정 결과를 반환한다."""
        return "FAILED"

    namespace = {
        "AirflowFailException": StubAirflowFailException,
        "AirflowException": StubAirflowException,
        "SERVING_PATHS": SimpleNamespace(serving=tmp_path / "mart.duckdb"),
        "os": os,
        "repoint_and_prune_serving": failed_repoint,
    }
    module = ast.fix_missing_locations(ast.Module(body=[task_node], type_ignores=[]))
    exec(compile(module, "warehouse_pipeline_dag.py", "exec"), namespace)  # noqa: S102
    with pytest.raises(StubAirflowException, match="METABASE_REPOINT_FAILED") as error:
        namespace["repoint_metabase_serving_task"]({"export_id": "x"})
    assert type(error.value) is StubAirflowException


def _warehouse_dbt_runner(full_refresh: bool):
    """Airflow 없이 실제 dbt 작업 본문을 실행하고 선택된 실행기를 반환한다."""
    source = (PROJECT_ROOT / "airflow/dags/warehouse_pipeline_dag.py").read_text()
    block = next(node for node in ast.parse(source).body if isinstance(node, ast.With))
    node = next(
        node for node in block.body
        if isinstance(node, ast.FunctionDef) and node.name == "dbt_build_task"
    )
    node.decorator_list = []
    captured = {}

    def runner(*args, **kwargs):
        """선택 여부를 비교할 dbt 실행기 대역이다."""
        raise AssertionError("dbt must not run in this boundary test")

    def build(*args, **kwargs):
        """실제 작업이 전달한 실행기를 기록한다."""
        captured["runner"] = kwargs.get("dbt_runner")
        return SimpleNamespace(tests_passed=1, tests_failed=0)

    namespace = {
        "get_current_context": lambda: {"params": {"full_refresh": full_refresh}},
        "PostgresSettings": SimpleNamespace(from_environment=lambda: object()),
        "WAREHOUSE_PATHS": object(), "uuid": uuid, "partial": partial,
        "run_dbt_build": runner, "build_warehouse": build,
        "_reraise_classified": lambda error: None,
    }
    module = ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[]))
    exec(compile(module, "warehouse_pipeline_dag.py", "exec"), namespace)  # noqa: S102
    assert namespace["dbt_build_task"]({"publish_run_id": str(uuid.uuid4())}) == {
        "tests_passed": 1, "tests_failed": 0,
    }
    return captured["runner"], runner


def test_dbt_build_passes_full_refresh_when_param_is_true() -> None:
    """명시적 전체 새로고침 매개변수만 dbt 추가 인자로 전달한다."""
    selected, plain = _warehouse_dbt_runner(True)
    assert isinstance(selected, partial)
    assert selected.func is plain
    assert selected.keywords == {"extra_args": ("--full-refresh",)}


def test_dbt_build_uses_plain_runner_by_default() -> None:
    """기본값에서는 부분 실행기나 전체 새로고침 인자를 사용하지 않는다."""
    selected, plain = _warehouse_dbt_runner(False)
    assert selected is plain


def test_warehouse_full_refresh_param_defaults_to_false() -> None:
    """conf가 없는 원천 DAG 트리거에서도 전체 새로고침 기본값은 거짓이다."""
    source = (PROJECT_ROOT / "airflow/dags/warehouse_pipeline_dag.py").read_text()
    assert '\"full_refresh\": Param(False, type=\"boolean\"' in source
