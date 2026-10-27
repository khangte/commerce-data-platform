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
    ('failed', 'FAIL', 0, 1), ('slow', 'FAIL', 0, 0), ('empty', 'FAIL', 18, 0),
])
def test_window_query(scenario, verdict, missing, not_ok):
    """임시 테이블로 운영 데이터를 보호하며 3시간 창의 18개 슬롯과 원인 상세를 검증한다."""
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
                FROM generate_series(timestamptz '2000-01-01 21:00:00', timestamptz '2000-01-01 23:50:00', interval '10 minutes') slot
            """, (Jsonb({'dag_elapsed_s': 301 if scenario == 'slow' else 300,
                         'start_lag_s': 1, 'exports_count': 3, 'failed_builds_count': 0,
                         'disk_free_bytes': 1000, 'probe_errors': {}}),))
        if scenario == 'missing':
            connection.execute("DELETE FROM warehouse_run_reports WHERE logical_date = '2000-01-01 21:00:00+00'")
            connection.execute("INSERT INTO generator_runs VALUES ('2000-01-01 21:00:00+00', 'FAILED', 'fixture failure', '2000-01-01 21:00:00+00')")
        if scenario == 'failed':
            connection.execute("UPDATE warehouse_run_reports SET ok = false, failures = '[\"NOT_FRESH\"]' WHERE logical_date = '2000-01-01 21:00:00+00'")
        cursor = connection.execute(sql)
        results = []
        while True:
            if cursor.description:
                results.append(cursor.fetchall())
            if not cursor.nextset():
                break
        aggregate = results[0][0]
        assert aggregate[:5] == (18, 0 if scenario == 'empty' else 18 - missing,
                                 0 if scenario == 'empty' else 18 - missing - not_ok, not_ok, missing)
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
    for probe in ('tasks', 'ingestion', 'leases', 'bronze', 'publish', 'freshness', 'staleness', 'row_counts', 'files'):
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


@pytest.mark.parametrize(('status', 'has_previous'), [
    ('PUBLISHED', True), ('FAILED', True), ('PUBLISHED', False),
])
def test_row_counts_probe_reads_publish_history(status, has_previous):
    """실제 게시 이력에서 직전 행 수를 읽고 미게시 실행과 첫 실행을 구분한다."""
    from src.observability.run_report import _probe_row_counts

    settings = PostgresSettings.from_environment()
    previous, current = uuid.uuid4(), uuid.uuid4()
    pipeline = f'test_row_counts_{current.hex}'
    relation = 'facts.fct_order_item'
    try:
        with settings.pipeline_connection() as connection:
            for run_id, state, counts, prior in [
                (previous, 'PUBLISHED', {relation: 100}, None),
                (current, status, {relation: 90}, previous if has_previous else None),
            ]:
                connection.execute(
                    """INSERT INTO mart_publish_runs
                       (publish_run_id, pipeline_name, status, mart_hashes, mart_row_counts,
                        previous_publish_run_id, error_type, started_at, finished_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now())""",
                    (run_id, pipeline, state, Jsonb({}) if state == 'PUBLISHED' else None,
                     Jsonb(counts) if state == 'PUBLISHED' else None, prior,
                     'DBT_TEST_FAILED' if state == 'FAILED' else None),
                )
        result = _probe_row_counts(settings, {'publish_run_id': str(current)},
                                   {'params': {'full_refresh': False}}, None, None, None)
        assert result == {
            'mart_row_counts': {relation: 90} if status == 'PUBLISHED' else None,
            'previous_mart_row_counts': {relation: 100}
                if status == 'PUBLISHED' and has_previous else None,
            'full_refresh': False,
        }
    finally:
        with settings.pipeline_connection() as connection:
            connection.execute('DELETE FROM mart_publish_runs WHERE pipeline_name = %s', (pipeline,))
