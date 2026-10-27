"""운영 기록의 판정·수집 격리·저장 실패 처리를 검증한다."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.observability import run_report as module


@pytest.fixture
def healthy():
    """모든 운영 조건을 만족하는 기록을 만든다."""
    return {
        "batch_id": "batch", "dag_run_id": "run", "logical_date": None,
        "recorded_at": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "tasks": [{"task_id": "extract", "state": "success", "map_index": 0}],
        "publish_status": "PUBLISHED", "serving_export_status": "SUCCESS",
        "metabase_repoint_status": "SUCCESS", "fresh": True,
        "exports_count": 3, "failed_builds_count": 3,
        "warehouse_build_empty": True, "serving_build_empty": True,
        "held_table_leases": 0, "source_lease_owner": None,
        "dag_elapsed_s": 599.9, "probe_errors": {},
    }


def test_healthy_and_nonterminal_tasks_pass(healthy):
    """종료 전 작업은 판정에서 빼고 정상 기록을 통과시킨다."""
    healthy["tasks"].append({"task_id": "summary", "state": "not_terminal"})
    assert module.judge_run_report(healthy) == []
    healthy["fresh"] = None
    assert module.judge_run_report(healthy) == []


@pytest.mark.parametrize(("changes", "expected"), [
    ({"tasks": [{"task_id": "extract", "state": "failed"}]}, "TASK_NOT_SUCCESS:extract"),
    ({"tasks": [{"task_id": "extract", "state": "skipped"}]}, "TASK_NOT_SUCCESS:extract"),
    ({"publish_status": "FAILED"}, "PUBLISH_FAILED"),
    ({"serving_export_status": "NOT_REQUESTED"}, "SERVING_EXPORT_FAILED"),
    ({"metabase_repoint_status": "SKIPPED"}, "REPOINT_SKIPPED"),
    ({"fresh": False}, "NOT_FRESH"),
    ({"data_lag_s": 7200.1, "stale_after_s": 7200.0}, "DATA_STALE"),
    ({"exports_count": 2}, "EXPORTS_COUNT_2"),
    ({"exports_count": 4}, "EXPORTS_COUNT_4"),
    ({"failed_builds_count": 4}, "FAILED_BUILDS_OVER_RETENTION"),
    ({"warehouse_build_empty": False}, "BUILD_DIR_NOT_EMPTY"),
    ({"serving_build_empty": False}, "BUILD_DIR_NOT_EMPTY"),
    ({"held_table_leases": 1}, "LEASE_HELD"),
    ({"source_lease_owner": "owner"}, "LEASE_HELD"),
    ({"dag_elapsed_s": 600}, "SLOW_RUN"),
    ({"probe_errors": {"bronze": "offline"}}, "PROBE_ERROR:bronze"),
])
def test_failure_codes(healthy, changes, expected):
    """경계값과 운영 실패를 정해진 코드로 판정한다."""
    healthy.update(changes)
    assert module.judge_run_report(healthy) == [expected]


@pytest.mark.parametrize("failed_probe", ["dag", "tasks", "ingestion", "leases", "bronze", "publish", "freshness", "staleness", "row_counts", "files"])
def test_each_probe_failure_preserves_other_probes(monkeypatch, failed_probe):
    """각 수집 실패 뒤에도 나머지 수집과 판정을 끝낸다."""
    probes = {}
    for name in ("dag", "tasks", "ingestion", "leases", "bronze", "publish", "freshness", "staleness", "row_counts", "files"):
        probe = Mock(return_value={f"collected_{name}": True})
        if name == failed_probe:
            probe.side_effect = RuntimeError("offline")
        monkeypatch.setattr(module, f"_probe_{name}", probe)
        probes[name] = probe
    report = module.collect_run_report(None, {"batch_id": "batch"}, {}, None, None)
    assert report["probe_errors"] == {failed_probe: "offline"}
    assert f"PROBE_ERROR:{failed_probe}" in report["failures"]
    assert report["ok"] is False
    for name, probe in probes.items():
        probe.assert_called_once()
        if name != failed_probe:
            assert report[f"collected_{name}"] is True


def test_task_breadcrumbs_include_mapping_and_missing_tasks():
    """매핑별 종료 결과와 아직 종료하지 않은 작업을 구분한다."""
    ti = SimpleNamespace(task_id="summary", get_task_breadcrumbs=Mock(return_value=[
        {"task_id": "extract", "map_index": 0, "state": "success", "duration": 1.2},
        {"task_id": "extract", "map_index": 1, "state": "failed", "duration": 2.3},
    ]))
    context = {"ti": ti, "dag_run": SimpleNamespace(dag_id="dag", run_id="run"),
               "dag": SimpleNamespace(task_ids=["extract", "release", "summary"])}
    tasks = module._probe_tasks(None, {}, context, None, None, None)["tasks"]
    assert tasks[:2] == ti.get_task_breadcrumbs.return_value
    assert tasks[2:] == [
        {"task_id": "release", "map_index": -1, "state": "not_terminal", "duration": None},
        {"task_id": "summary", "map_index": -1, "state": "not_terminal", "duration": None},
    ]
    ti.get_task_breadcrumbs.assert_called_once_with("dag", "run")


def test_record_failure_does_not_raise(healthy, capsys):
    """DB 장애가 기록 호출 밖으로 전파되지 않는다."""
    settings = Mock()
    settings.pipeline_connection.side_effect = RuntimeError("offline")
    module.record_run_report(settings, healthy)
    assert "RUN_REPORT_WRITE_FAILED: offline" in capsys.readouterr().out


def test_summary_keeps_xcom_and_emits_json(healthy, capsys):
    """기존 요약 반환값을 보존하며 구조화 기록을 로그와 DB에 전달한다."""
    import ast
    import json
    from pathlib import Path

    source = Path('airflow/dags/warehouse_pipeline_dag.py').read_text()
    tree = ast.parse(source)
    function = next(node for node in ast.walk(tree)
                    if isinstance(node, ast.FunctionDef) and node.name == 'publish_run_summary')
    function.decorator_list = []
    collector = Mock(return_value=healthy)
    recorder = Mock()
    namespace = {
        'PostgresSettings': SimpleNamespace(from_environment=Mock(return_value='settings')),
        'collect_run_report': collector, 'record_run_report': recorder,
        'get_current_context': Mock(return_value={'context': True}),
        'WAREHOUSE_PATHS': 'warehouse', 'SERVING_PATHS': 'serving', 'json': json,
        'metabase_repoint_summary_status': lambda export, status: status or 'NOT_REQUESTED',
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), '<summary>', 'exec'), namespace)  # noqa: S102 - 지정된 DAG 함수만 격리 실행한다.
    result = namespace['publish_run_summary']({'batch_id': 'batch'}, None, None, None, None, None)
    assert result == {
        'batch_id': 'batch', 'verified_table_count': 0, 'publish_run_id': None,
        'publish_status': 'SKIPPED', 'publish_error_type': None, 'changed_relations': [],
        'serving_export_status': 'NOT_REQUESTED', 'serving_export_id': None,
        'serving_row_counts': {}, 'metabase_repoint_status': 'NOT_REQUESTED',
    }
    collector.assert_called_once_with('settings', result, {'context': True}, 'warehouse', 'serving')
    recorder.assert_called_once_with('settings', healthy)
    line = capsys.readouterr().out.strip()
    assert json.loads(line.removeprefix('RUN_REPORT ')) == healthy
    assert 'release_task >> summary' in source


def test_dag_timing_uses_run_after():
    """논리 시각 대신 실행 가능 시각을 기준으로 시작 지연을 계산한다."""
    from datetime import timedelta

    logical = datetime(2026, 1, 1, tzinfo=UTC)
    run = SimpleNamespace(run_id='run', logical_date=logical,
                          run_after=logical + timedelta(minutes=10),
                          start_date=logical + timedelta(minutes=10, seconds=5))
    report = module._probe_dag(None, {}, {'dag_run': run}, None, None,
                               logical + timedelta(minutes=12))
    assert report['start_lag_s'] == 5
    assert report['dag_elapsed_s'] == 115
    assert report['logical_date'] == logical.isoformat()


def test_freshness_reuses_existing_check(monkeypatch):
    """설정이 없으면 생략하고 설정이 있으면 기존 최신 여부 검사를 사용한다."""
    names = ('METABASE_URL', 'METABASE_API_KEY', 'METABASE_SERVING_DATABASE_ID')
    for name in names:
        monkeypatch.delenv(name, raising=False)
    check = Mock(return_value=SimpleNamespace(fresh=False, live_export_id='old'))
    monkeypatch.setattr(module, 'check_serving_freshness', check)
    assert module._probe_freshness('settings', {}, {}, None, None, None) == {
        'fresh': None, 'live_export_id': None,
    }
    check.assert_not_called()
    for name, value in zip(names, ('http://metabase', 'key', '2'), strict=True):
        monkeypatch.setenv(name, value)
    assert module._probe_freshness('settings', {}, {}, None, None, None) == {
        'fresh': False, 'live_export_id': 'old',
    }
    check.assert_called_once_with('settings', 'http://metabase', 'key', 2)


def test_files_probe_counts_sizes_and_build_residue(tmp_path):
    """버전 파일 수와 크기를 계산하고 임시 파일 잔류를 감지한다."""
    from src.serving.export import ServingPaths
    from src.warehouse.publish import WarehousePaths

    warehouse = WarehousePaths.under(tmp_path / 'warehouse')
    serving = ServingPaths.under(tmp_path / 'serving')
    exports = serving.serving.parent / 'exports'
    for path in (warehouse.build_dir, warehouse.failed_dir, serving.build_dir, exports):
        path.mkdir(parents=True)
    warehouse.published.write_bytes(b'warehouse')
    serving.serving.write_bytes(b'serving')
    (exports / '1.duckdb').write_bytes(b'export')
    (warehouse.failed_dir / '1.duckdb').write_bytes(b'failed')
    (serving.build_dir / '.residue').write_bytes(b'x')
    report = module._probe_files(None, {}, {}, warehouse, serving, None)
    assert report['exports_count'] == report['failed_builds_count'] == 1
    assert report['warehouse_bytes'] == 9
    assert report['serving_bytes'] == 7
    assert report['exports_bytes'] == report['failed_bytes'] == 6
    assert report['warehouse_build_empty'] is True
    assert report['serving_build_empty'] is False
    assert report['disk_free_bytes'] > 0


@pytest.mark.parametrize('changes', [
    {'data_lag_s': 7200.0, 'stale_after_s': 7200.0},
    {'data_lag_s': None, 'stale_after_s': 7200.0},
    {'data_lag_s': 7201.0, 'stale_after_s': None},
])
def test_data_stale_not_raised(healthy, changes):
    """경계값과 측정 불가는 데이터 지연으로 판정하지 않는다."""
    healthy.update(changes)
    assert module.judge_run_report(healthy) == []


@pytest.mark.parametrize('column_type', ['TIMESTAMPTZ', 'TIMESTAMP'])
def test_staleness_measures_published_fact(tmp_path, monkeypatch, column_type):
    """게시 주문의 최신 시각과 수집 시각 차이를 UTC로 수집한다."""
    import duckdb

    from src.warehouse.publish import WarehousePaths

    monkeypatch.setenv('SOURCE_DAG_SCHEDULE', '*/10 * * * *')
    paths = WarehousePaths.under(tmp_path)
    with duckdb.connect(str(paths.published)) as connection:
        connection.execute('CREATE SCHEMA facts')
        connection.execute(f'CREATE TABLE facts.fct_order (purchase_at {column_type})')
        connection.execute("INSERT INTO facts.fct_order VALUES ('2026-01-01 00:00:00+00'), ('2026-01-01 01:00:00+00')")
    result = module._probe_staleness(None, {}, {}, paths, None,
                                     datetime(2026, 1, 1, 1, 30, tzinfo=UTC))
    assert result == {'max_purchase_at': '2026-01-01T01:00:00+00:00',
                      'data_lag_s': 1800.0, 'stale_after_s': 1200.0}


@pytest.mark.parametrize('empty_fact', [False, True])
def test_staleness_without_data(tmp_path, monkeypatch, empty_fact):
    """게시 파일이 없거나 주문이 비어 있으면 지연을 측정하지 않는다."""
    import duckdb

    from src.warehouse.publish import WarehousePaths

    monkeypatch.delenv('SOURCE_DAG_SCHEDULE', raising=False)
    paths = WarehousePaths.under(tmp_path)
    if empty_fact:
        with duckdb.connect(str(paths.published)) as connection:
            connection.execute('CREATE SCHEMA facts')
            connection.execute('CREATE TABLE facts.fct_order (purchase_at TIMESTAMPTZ)')
    assert module._probe_staleness(None, {}, {}, paths, None, datetime.now(UTC)) == {
        'max_purchase_at': None, 'data_lag_s': None, 'stale_after_s': 7200.0,
    }
    assert paths.published.exists() is empty_fact


def test_staleness_unsupported_schedule_is_probe_error(tmp_path, monkeypatch):
    """지원하지 않는 주기는 게시 파일이 없어도 수집 오류로 기록한다."""
    from src.warehouse.publish import WarehousePaths

    monkeypatch.setenv('SOURCE_DAG_SCHEDULE', '0 9 * * 1-5')
    for name in ('dag', 'tasks', 'ingestion', 'leases', 'bronze', 'publish',
                 'freshness', 'row_counts', 'files'):
        monkeypatch.setattr(module, f'_probe_{name}', Mock(return_value={}), raising=False)
    report = module.collect_run_report(None, {}, {}, WarehousePaths.under(tmp_path), None)
    assert 'PROBE_ERROR:staleness' in report['failures']


P = 'facts.fct_order_item'


@pytest.mark.parametrize(('previous', 'current', 'full_refresh', 'expected'), [
    ({P: 100}, {P: 98}, False, [f'ROW_COUNT_DROP:{P}']),
    ({P: 100}, {P: 99}, False, []),
    ({P: 100}, {P: 0}, False, [f'ROW_COUNT_DROP:{P}']),
    ({P: 0}, {P: 0}, False, []),
    ({P: 100}, {P: 50}, True, []),
    (None, {P: 50}, False, []),
    ({P: 100}, None, False, []),
    ({}, {P: 50}, False, []),
    ({P: 100, 'x.gone': 5}, {P: 100}, False, []),
    ({'b.t': 100, 'a.t': 100}, {'b.t': 1, 'a.t': 1}, False,
     ['ROW_COUNT_DROP:a.t', 'ROW_COUNT_DROP:b.t']),
])
def test_row_count_drop(healthy, previous, current, full_refresh, expected):
    """직전 게시 대비 1% 넘는 감소만 판정하고 전체 재구축은 제외한다."""
    healthy.update(previous_mart_row_counts=previous, mart_row_counts=current,
                   full_refresh=full_refresh)
    assert module.judge_run_report(healthy) == expected


@pytest.mark.parametrize(('context', 'expected'), [
    ({}, False), ({'params': {'full_refresh': False}}, False),
    ({'params': {'full_refresh': True}}, True),
])
def test_row_counts_probe_skips_without_publish_run(context, expected):
    """게시 실행이 없으면 DB를 열지 않고 전체 재구축 여부만 반환한다."""
    settings = Mock()
    assert module._probe_row_counts(settings, {'publish_run_id': None}, context,
                                    None, None, None) == {
        'mart_row_counts': None, 'previous_mart_row_counts': None, 'full_refresh': expected,
    }
    settings.pipeline_connection.assert_not_called()
