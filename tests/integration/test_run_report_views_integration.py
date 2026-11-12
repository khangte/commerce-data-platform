"""실제 PostgreSQL에서 운영 Dashboard View의 판정과 metabase_ops_ro 권한 경계를 검증한다."""

import os
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg.types.json import Jsonb

from src.common.database import PostgresSettings, apply_sql_file, environment_values

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get('RUN_POSTGRES_INTEGRATION') != '1',
    reason='RUN_POSTGRES_INTEGRATION=1이 필요하다.',
)]

VIEW_SQL = 'sql/metadata/007_create_warehouse_run_report_views.sql'
BASE = {'publish_status': 'PUBLISHED', 'serving_export_status': 'SUCCESS',
        'metabase_repoint_status': 'SUCCESS', 'stale_after_s': 1200}


@pytest.fixture
def reports():
    """고유 접두사로 기록을 넣고 끝나면 지운다. (식별자 → (기록 시각, 판정 코드, report))"""
    settings = PostgresSettings.from_environment()
    prefix = f'test_views_{uuid.uuid4().hex}'
    inserted = []

    def insert(rows):
        """기록을 넣고 View를 적용한 뒤 실행 식별자 목록을 돌려준다."""
        with settings.pipeline_connection() as connection:
            apply_sql_file(connection, 'sql/metadata/006_create_warehouse_run_reports.sql')
            for name, (recorded_at, failures, report) in rows.items():
                run_id = f'{prefix}_{name}'
                inserted.append(run_id)
                connection.execute(
                    """INSERT INTO warehouse_run_reports
                       (dag_run_id, batch_id, logical_date, recorded_at, ok, failures, report)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (run_id, run_id, recorded_at, recorded_at, not failures,
                     Jsonb(failures), Jsonb({**BASE, **report, 'failures': failures})),
                )
            connection.commit()
            apply_sql_file(connection, VIEW_SQL)
        return prefix

    yield settings, insert
    with settings.pipeline_connection() as connection:
        connection.execute('DELETE FROM warehouse_run_reports WHERE dag_run_id = ANY(%s)', (inserted,))


def test_status_and_alert_codes(reports):
    """판정이 다른 네 기록과 오래된 기록의 alert_codes·status가 계획 §3.2 표와 같다."""
    settings, insert = reports
    now = datetime.now(UTC)
    prefix = insert({
        'healthy': (now, [], {'fresh': True, 'data_lag_s': 10.5}),
        'failed': (now, ['PUBLISH_FAILED', 'SERVING_EXPORT_FAILED'], {'publish_status': 'FAILED'}),
        'drop': (now, ['ROW_COUNT_DROP:facts.fct_order'], {}),
        'slow': (now, ['SLOW_RUN'], {'dag_elapsed_s': 700}),
        'old': (now - timedelta(seconds=1300), [], {}),
        'no_threshold': (now, [], {'stale_after_s': None}),
    })
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            """SELECT dag_run_id, failure_codes, alert_codes, status, fresh, data_lag_s,
                      dag_elapsed_s, held_domains
               FROM warehouse_run_report_status WHERE dag_run_id LIKE %s""",
            (f'{prefix}%',),
        ).fetchall()
    result = {row[0].removeprefix(f'{prefix}_'): row[1:] for row in rows}
    assert result == {
        'healthy': (None, None, '정상', True, 10.5, None, None),
        'failed': ('PUBLISH_FAILED, SERVING_EXPORT_FAILED', 'PUBLISH_FAILED', '알림 대상',
                   None, None, None, None),
        'drop': ('ROW_COUNT_DROP:facts.fct_order', 'ROW_COUNT_DROP:facts.fct_order', '알림 대상',
                 None, None, None, None),
        'slow': ('SLOW_RUN', None, '판정 있음', None, None, 700.0, None),
        'old': (None, None, '확인 불가', None, None, None, None),
        'no_threshold': (None, None, '확인 불가', None, None, None, None),
    }


def test_domain_view_rows(reports):
    """도메인 값이 있는 기록만 도메인별 행을 만들고 없거나 null인 기록은 행 없이 넘어간다."""
    settings, insert = reports
    now = datetime.now(UTC)
    prefix = insert({
        'domains': (now, ['PUBLISH_PARTIAL:subscriptions'], {
            'held_domains': ['subscriptions'],
            'domain_lag_s': {'orders': 30.0, 'subscriptions': None},
            'domain_refreshed_at': {'orders': '2026-11-12T00:00:00+00:00', 'subscriptions': None},
            'domain_as_of': {'orders': '2026-11-11T23:00:00+00:00'},
            'consecutive_holds': {'orders': 0, 'subscriptions': 3},
        }),
        'missing': (now, ['PUBLISH_SKIPPED'], {}),
        'null': (now, [], {'domain_lag_s': None, 'held_domains': None}),
    })
    with settings.pipeline_connection() as connection:
        rows = connection.execute(
            """SELECT dag_run_id, domain, lag_s, refreshed_at, as_of, consecutive_holds, held,
                      stale_after_s
               FROM warehouse_run_report_domain_status WHERE dag_run_id LIKE %s
               ORDER BY domain""",
            (f'{prefix}%',),
        ).fetchall()
        held = connection.execute(
            'SELECT held_domains FROM warehouse_run_report_status WHERE dag_run_id = %s',
            (f'{prefix}_domains',),
        ).fetchone()[0]
    run_id = f'{prefix}_domains'
    assert rows == [
        (run_id, 'orders', 30.0, datetime(2026, 11, 12, tzinfo=UTC),
         datetime(2026, 11, 11, 23, tzinfo=UTC), 0, False, 1200.0),
        (run_id, 'subscriptions', None, None, None, 3, True, 1200.0),
    ]
    assert held == 'subscriptions'


def test_ops_role_reads_only_views(reports):
    """metabase_ops_ro는 View 2개만 읽고 원본 Table은 권한 오류가 난다."""
    settings, insert = reports
    insert({})
    values = environment_values()
    connect = {'host': settings.host, 'port': settings.port, 'dbname': settings.pipeline_database,
               'user': values['METABASE_OPS_DB_USER'], 'password': values['METABASE_OPS_DB_PASSWORD']}
    with psycopg.connect(**connect) as connection:
        for view in ('warehouse_run_report_status', 'warehouse_run_report_domain_status'):
            connection.execute(f'SELECT * FROM {view} LIMIT 1').fetchall()
    for table in ('warehouse_run_reports', 'mart_publish_runs'):
        with psycopg.connect(**connect) as connection, pytest.raises(
            psycopg.errors.InsufficientPrivilege,
        ):
            connection.execute(f'SELECT 1 FROM {table} LIMIT 1')
