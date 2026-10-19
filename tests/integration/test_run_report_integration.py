"""실제 PostgreSQL에서 운영 기록 갱신과 시간 창 판정을 검증한다."""

import os
import uuid
from datetime import UTC, datetime

import pytest
from psycopg.types.json import Jsonb

from src.common.database import PROJECT_ROOT, PostgresSettings
from src.observability.run_report import record_run_report

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get('RUN_POSTGRES_INTEGRATION') != '1',
    reason='RUN_POSTGRES_INTEGRATION=1이 필요하다.',
)]


def test_record_upserts_same_dag_run():
    """같은 실행의 재기록은 한 행만 남기고 JSON과 판정을 갱신한다."""
    settings = PostgresSettings.from_environment()
    run_id = f'test_report_{uuid.uuid4().hex}'
    report = {
        'dag_run_id': run_id, 'batch_id': run_id, 'logical_date': None,
        'recorded_at': datetime.now(UTC).isoformat(), 'ok': True, 'failures': [],
    }
    try:
        record_run_report(settings, report)
        with settings.pipeline_connection() as connection:
            assert connection.execute(
                'SELECT ok, failures, report FROM warehouse_run_reports WHERE dag_run_id = %s',
                (run_id,),
            ).fetchall() == [(True, [], report)]
        report.update(ok=False, failures=['NOT_FRESH'])
        record_run_report(settings, report)
        with settings.pipeline_connection() as connection:
            assert connection.execute(
                'SELECT ok, failures, report FROM warehouse_run_reports WHERE dag_run_id = %s',
                (run_id,),
            ).fetchall() == [(False, ['NOT_FRESH'], report)]
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute('DELETE FROM warehouse_run_reports WHERE dag_run_id = %s', (run_id,))


@pytest.mark.parametrize(('scenario', 'verdict', 'missing', 'not_ok'), [
    ('healthy', 'PASS', 0, 0), ('missing', 'FAIL', 1, 0),
    ('failed', 'FAIL', 0, 1), ('slow', 'FAIL', 0, 0), ('empty', 'FAIL', 144, 0),
])
def test_window_query(scenario, verdict, missing, not_ok):
    """임시 테이블로 운영 데이터를 보호하며 누락·실패·지연과 원인 상세를 검증한다."""
    settings = PostgresSettings.from_environment()
    sql = (PROJECT_ROOT / 'sql/validation/warehouse_run_report_window.sql').read_text()
    sql = sql.replace('CURRENT_TIMESTAMP', "timestamptz '2000-01-02 00:00:00+00'")
    with settings.pipeline_connection() as connection:
        connection.execute('CREATE TEMP TABLE warehouse_run_reports (dag_run_id text, logical_date timestamptz, recorded_at timestamptz, ok boolean, failures jsonb, report jsonb)')
        connection.execute('CREATE TEMP TABLE generator_runs (logical_date timestamptz, status text, error_message text, started_at timestamptz)')
        if scenario != 'empty':
            connection.execute("""
                INSERT INTO warehouse_run_reports
                SELECT slot::text, slot, slot, true, '[]'::jsonb, %s
                FROM generate_series(timestamptz '2000-01-01', timestamptz '2000-01-01 23:50:00', interval '10 minutes') slot
            """, (Jsonb({'dag_elapsed_s': 301 if scenario == 'slow' else 300,
                         'start_lag_s': 1, 'exports_count': 3, 'failed_builds_count': 0,
                         'disk_free_bytes': 1000, 'probe_errors': {}}),))
        if scenario == 'missing':
            connection.execute("DELETE FROM warehouse_run_reports WHERE logical_date = '2000-01-01'")
            connection.execute("INSERT INTO generator_runs VALUES ('2000-01-01', 'FAILED', 'fixture failure', '2000-01-01')")
        if scenario == 'failed':
            connection.execute("UPDATE warehouse_run_reports SET ok = false, failures = '[\"NOT_FRESH\"]' WHERE logical_date = '2000-01-01'")
        cursor = connection.execute(sql)
        results = []
        while True:
            if cursor.description:
                results.append(cursor.fetchall())
            if not cursor.nextset():
                break
        aggregate = results[0][0]
        assert aggregate[:5] == (144, 0 if scenario == 'empty' else 144 - missing,
                                 0 if scenario == 'empty' else 144 - missing - not_ok, not_ok, missing)
        assert aggregate[-1] == verdict
        assert aggregate[8] == ({'NOT_FRESH': 1} if scenario == 'failed' else {})
        assert len(results[1]) == not_ok
        assert len(results[2]) == missing
        if scenario == 'missing':
            assert results[2][0][1:] == ('FAILED', 'fixture failure')
        connection.rollback()


@pytest.mark.parametrize('logical_date', [None, datetime(2000, 1, 1, tzinfo=UTC)])
def test_dag_probe_failure_is_recorded(monkeypatch, logical_date):
    """DAG 시각 수집이 실패해도 실행 식별자·논리 시각·실패 판정을 저장한다."""
    from types import SimpleNamespace
    from unittest.mock import Mock

    from src.observability import run_report as module

    settings = PostgresSettings.from_environment()
    run_id = f'test_report_{uuid.uuid4().hex}'
    # run_after가 없어서 실제 DAG 수집 단계가 실패하는 상황을 재현한다.
    run = SimpleNamespace(run_id=run_id, logical_date=logical_date)
    for probe in ('tasks', 'ingestion', 'leases', 'bronze', 'publish', 'freshness', 'files'):
        monkeypatch.setattr(module, f'_probe_{probe}', Mock(return_value={}))
    summary = {'batch_id': run_id, 'publish_status': 'PUBLISHED',
               'serving_export_status': 'SUCCESS', 'metabase_repoint_status': 'SUCCESS'}
    report = module.collect_run_report(
        settings, summary, {'dag_run': run, 'logical_date': logical_date}, None, None,
    )
    try:
        module.record_run_report(settings, report)
        with settings.pipeline_connection() as connection:
            rows = connection.execute(
                'SELECT logical_date, ok, failures, report FROM warehouse_run_reports WHERE dag_run_id = %s',
                (run_id,),
            ).fetchall()
        assert len(rows) == 1
        saved_date, ok, failures, saved_report = rows[0]
        assert saved_date == logical_date
        assert ok is False
        assert failures == ['PROBE_ERROR:dag']
        assert saved_report['dag_run_id'] == run_id
        assert saved_report['logical_date'] == (logical_date.isoformat() if logical_date else None)
        assert 'run_after' in saved_report['probe_errors']['dag']
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute('DELETE FROM warehouse_run_reports WHERE dag_run_id = %s', (run_id,))
