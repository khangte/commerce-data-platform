# partial_publish=false + orders Canary 강제 실패 (Check 3)

- 수집 시각: 2026-11-11T07:41Z (UTC)
- 환경: branch `feat/publish-gate-split` (HEAD 1d687ac, 브랜치 코드 수정 없이 실행), compose profile `airflow`, Airflow 3.3.1 LocalExecutor, dbt-core 1.12.3, dbt-duckdb 1.11.0
- 목적: 스위치를 끄면 v1처럼 전체 게시가 차단되는지 확인

## 판정: PASS

- `status=FAILED`, `error_type=DBT_TEST_ERROR`, `held_domains=[]`. `publish_mart`, `export_serving_mart`, `repoint_metabase_serving`는 `upstream_failed`.
- Published 파일 `warehouse.duckdb`의 inode/mtime(834322 / 1794378586)은 직전 `verify_partial_on` 게시 기록(`published_inode=834322`, `published_mtime_ns=1794378586078455393`)과 같다. 게시본이 바뀌지 않았다.

## 재현 절차

`docker compose exec -T airflow-api-server python /tmp/af.py POST /api/v2/dags/warehouse_pipeline_dag/dagRuns '{"dag_run_id":"verify_partial_off","logical_date":"<now>","conf":{"partial_publish":false}}'` (Canary는 02와 같은 강제 실패 상태)

## mart_publish_runs

명령: `docker compose exec -T postgres bash -c 'psql -U "$POSTGRES_USER" -d "$PIPELINE_METADATA_DB" -x -c "select publish_run_id, pipeline_name, batch_id, dag_run_id, status, error_type, left(error_message,120) error_message_head, tests_passed, tests_failed, tests_warned, held_domains, failed_path, previous_publish_run_id, started_at, finished_at, mart_row_counts from mart_publish_runs where dag_run_id = ..."'`

> `failure_code`는 테이블 컬럼명이 `error_type`이다. `error_message_head`는 ANSI 색 코드를 제거해 앞 120자만 남겼다.

```
publish_run_id          | 6bcbc091-fef6-4757-bc5c-5d8efec9eb79
pipeline_name           | warehouse_pipeline_dag
batch_id                | warehouse_pipeline_dag__20261111T063225Z
dag_run_id              | verify_partial_off
status                  | FAILED
error_type              | DBT_TEST_ERROR
error_message_head      | DBT_TEST_ERROR: failed nodes: test.commerce_data_platform.orders_publish_gate_canary                                                                                                                                                                                                                +
                        | .............. [PASS in 0.
tests_passed            | 150
tests_failed            | 1
tests_warned            | 0
held_domains            | []
failed_path             | /opt/airflow/project/data/warehouse/failed/6bcbc091-fef6-4757-bc5c-5d8efec9eb79.duckdb
previous_publish_run_id | 960e6ef9-d8b4-4f0d-843d-f149a5416982
started_at              | 2026-11-11 06:33:23.044415+00
finished_at             | 2026-11-11 06:48:15.616467+00
mart_row_counts         |
```

## Run report 판정

명령: `docker compose exec -T postgres bash -c 'psql ... -x -c "select dag_run_id, ok, failures, report->>publish_status, report->>publish_error_type, report->held_domains, report->>tests_warned, report->>serving_export_status, report->>metabase_repoint_status, recorded_at from warehouse_run_reports where dag_run_id = ..."'`

```
dag_run_id              | verify_partial_off
ok                      | f
failures                | ["TASK_NOT_SUCCESS:dbt_build", "TASK_NOT_SUCCESS:export_serving_mart", "TASK_NOT_SUCCESS:publish_mart", "TASK_NOT_SUCCESS:repoint_metabase_serving", "PUBLISH_FAILED", "SERVING_EXPORT_FAILED", "REPOINT_NOT_REQUESTED", "DATA_STALE", "SLOW_RUN", "PROBE_ERROR:freshness"]
publish_status          | FAILED
publish_error_type      | DBT_TEST_ERROR
held_domains            | []
tests_warned            | 0
serving_export_status   | NOT_REQUESTED
metabase_repoint_status | NOT_REQUESTED
recorded_at             | 2026-11-11 06:48:20.539536+00
```

## Airflow task 로그 발췌

명령: `python3 logx.py airflow/logs/dag_id=warehouse_pipeline_dag/run_id=<run>/task_id={dbt_build,publish_mart,publish_run_summary}/attempt=*.log  # ANSI 제거, 판정 이벤트만`

```
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_off/task_id=dbt_build/attempt=1.log
2026-11-11T06:34:52.630 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:35:35.914 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:36:21.495 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:37:04.652 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:37:46.476 warning Publish 생존 신호 갱신 실패: OperationalError
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_off/task_id=dbt_build/attempt=2.log
2026-11-11T06:39:37.196 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:40:22.703 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:41:08.316 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:41:50.622 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:42:35.741 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:43:23.405 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:44:09.280 warning Publish 생존 신호 갱신 실패: OperationalError
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_off/task_id=dbt_build/attempt=3.log
2026-11-11T06:45:44.551 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:46:27.769 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:47:13.753 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:48:15.648 error   Task failed with exception | AirflowFailException: DBT_TEST_ERROR: DBT_TEST_ERROR: failed nodes: test.commerce_data_platform.orders_publish_gate_canary
.............. [PASS in 0.05s]
06:48:12  173 of 184 START test accepted_values_rpt_subscription_payment_outcomes_daily_payment_status__COMPLETED__FAILED  [RUN]
06:48:12  173 of 184 PASS accepted_values_rpt_subscription_payment_outcomes_daily_payment_status__COMPLETED__FAILED  
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_off/task_id=publish_mart/attempt=*.log
```

## Task 상태

```
initialize_run                   map=-1 state=success try=1 start=2026-11-11T06:32:28.799055Z duration=1.271443
acquire_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T06:32:30.278740Z duration=1.408976
extract_validate_load            map= 2 state=success try=1 start=2026-11-11T06:32:32.171804Z duration=3.395176
extract_validate_load            map= 1 state=success try=1 start=2026-11-11T06:32:32.175721Z duration=3.402453
extract_validate_load            map= 0 state=success try=1 start=2026-11-11T06:32:32.176649Z duration=3.407405
extract_validate_load            map= 5 state=success try=1 start=2026-11-11T06:32:35.830612Z duration=3.087646
extract_validate_load            map= 3 state=success try=1 start=2026-11-11T06:32:35.832318Z duration=3.053746
extract_validate_load            map= 4 state=success try=1 start=2026-11-11T06:32:35.832595Z duration=3.090587
extract_validate_load            map= 8 state=success try=1 start=2026-11-11T06:32:39.224905Z duration=20.273519
extract_validate_load            map= 6 state=success try=1 start=2026-11-11T06:32:39.227115Z duration=20.368363
extract_validate_load            map= 7 state=success try=1 start=2026-11-11T06:32:39.233101Z duration=20.526119
verify_bronze_commit_task        map=-1 state=success try=1 start=2026-11-11T06:33:01.039730Z duration=20.354672
release_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T06:33:01.040570Z duration=19.448713
prepare_warehouse_build          map=-1 state=success try=1 start=2026-11-11T06:33:21.525289Z duration=5.298286
dbt_build                        map=-1 state=failed try=3 start=2026-11-11T06:44:26.810461Z duration=228.881305
publish_mart                     map=-1 state=upstream_failed try=0 start=2026-11-11T06:48:16.080035Z duration=0.0
export_serving_mart              map=-1 state=upstream_failed try=0 start=2026-11-11T06:48:16.249646Z duration=0.0
repoint_metabase_serving         map=-1 state=upstream_failed try=0 start=2026-11-11T06:48:17.315005Z duration=0.0
publish_run_summary              map=-1 state=success try=1 start=2026-11-11T06:48:18.468270Z duration=6.45263
```

## 실행 환경 관찰 (판정과 무관)

- WSL 메모리 5.9GB(swap 2GB 소진)에 Metabase 1.3GB가 겹쳐 `dbt_build` 프로세스가 예외 없이 두 번 종료됐다(scheduler: `executor_state=failed, state=None`). Airflow가 `retries=0`이어도 이를 재큐잉해 attempt 3에서 정상 판정(FAILED/DBT_TEST_ERROR)으로 끝났다. 같은 `publish_run_id`로 재진입해도 결과 행은 1개만 남았다.
- 같은 구간의 `Publish 생존 신호 갱신 실패: OperationalError` 경고도 메모리 고갈 때만 나왔다. 메모리가 정상인 run(02, 04 attempt 3)에서는 0건이었고, 같은 함수를 직접 호출해도 정상 반환했다.
- DagRun state가 `success`인 것은 leaf task `publish_run_summary`(`trigger_rule=all_done`)가 성공했기 때문이다. 기존 DAG 구조 그대로이고 이 브랜치의 변경이 아니다. 실패 판정은 run report(`ok=false`, `PUBLISH_FAILED`)로 한다. (2026-11-12 이후에는 마지막 Task `domain_publish_alert`가 이 판정에서 실패해 DagRun이 `failed`가 된다.)
