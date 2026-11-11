# Canary 복원 후 보류 도메인 복구

- 수집 시각: 2026-11-11T07:41Z (UTC)
- 환경: branch `feat/publish-gate-split` (HEAD 1d687ac, 브랜치 코드 수정 없이 실행), compose profile `airflow`, Airflow 3.3.1 LocalExecutor, dbt-core 1.12.3, dbt-duckdb 1.11.0
- 목적: Canary를 되돌린 다음 run이 보류됐던 orders 변경분을 게시하는지 확인 (추가 검증)

## 판정: PASS

- `status=PUBLISHED`, `held_domains=[]`, `tests_passed=151, tests_failed=0`.
- `changed_relations=['facts.fct_order', 'facts.fct_order_item', 'facts.fct_order_payment']`: 보류 동안 쌓인 orders 변경분이 게시됐다. `fct_order` 99829 → 99849(+20, source run 2회 × 10 orders), `fct_order_item` 113412 → 113445, `fct_order_payment` 104274 → 104294.
- run report의 `PUBLISH_PARTIAL`이 사라졌다. 남은 `REPOINT_FAILED`, `TASK_NOT_SUCCESS:repoint_metabase_serving`, `EXPORTS_COUNT_4`는 메모리 확보를 위해 Metabase를 내렸기 때문이다. `SLOW_RUN`은 아래 Docker 재시작 지연 때문이다.

## 재현 절차

1. `git checkout -- dbt/tests/orders_publish_gate_canary.sql`로 Canary를 복원했다(`var('orders_publish_gate_canary', false)`).
2. `docker compose exec -T airflow-api-server python /tmp/af.py POST /api/v2/dags/warehouse_pipeline_dag/dagRuns '{"dag_run_id":"verify_partial_recover","logical_date":"<now>","conf":{}}'`

## mart_publish_runs

명령: `docker compose exec -T postgres bash -c 'psql -U "$POSTGRES_USER" -d "$PIPELINE_METADATA_DB" -x -c "select publish_run_id, pipeline_name, batch_id, dag_run_id, status, error_type, left(error_message,120) error_message_head, tests_passed, tests_failed, tests_warned, held_domains, failed_path, previous_publish_run_id, started_at, finished_at, mart_row_counts from mart_publish_runs where dag_run_id = ..."'`

> `failure_code`는 테이블 컬럼명이 `error_type`이다. `error_message_head`는 ANSI 색 코드를 제거해 앞 120자만 남겼다.

```
publish_run_id          | bd9d0f24-5364-41ab-aa4f-14ffbc14b6d5
pipeline_name           | warehouse_pipeline_dag
batch_id                | warehouse_pipeline_dag__20261111T064846Z
dag_run_id              | verify_partial_recover
status                  | PUBLISHED
error_type              | 
error_message_head      | 
tests_passed            | 151
tests_failed            | 0
tests_warned            | 0
held_domains            | []
failed_path             | 
previous_publish_run_id | 960e6ef9-d8b4-4f0d-843d-f149a5416982
started_at              | 2026-11-11 06:49:00.21894+00
finished_at             | 2026-11-11 07:30:57.108454+00
mart_row_counts         | {"facts.fct_order": 99849, "dimensions.dim_date": 3721, "facts.fct_order_item": 113445, "dimensions.dim_seller": 3095, "dimensions.dim_product": 32951, "dimensions.dim_customer": 96504, "facts.fct_order_payment": 104294, "dimensions.dim_subscription": 11, "facts.fct_subscription_payment": 9}
```

## Run report 판정

명령: `docker compose exec -T postgres bash -c 'psql ... -x -c "select dag_run_id, ok, failures, report->>publish_status, report->>publish_error_type, report->held_domains, report->>tests_warned, report->>serving_export_status, report->>metabase_repoint_status, recorded_at from warehouse_run_reports where dag_run_id = ..."'`

```
dag_run_id              | verify_partial_recover
ok                      | f
failures                | ["TASK_NOT_SUCCESS:repoint_metabase_serving", "REPOINT_FAILED", "DATA_STALE", "EXPORTS_COUNT_4", "SLOW_RUN", "PROBE_ERROR:freshness"]
publish_status          | PUBLISHED
publish_error_type      | 
held_domains            | []
tests_warned            | 0
serving_export_status   | SUCCESS
metabase_repoint_status | FAILED
recorded_at             | 2026-11-11 07:35:04.060402+00
```

## Airflow task 로그 발췌

명령: `python3 logx.py airflow/logs/dag_id=warehouse_pipeline_dag/run_id=<run>/task_id={dbt_build,publish_mart,publish_run_summary}/attempt=*.log  # ANSI 제거, 판정 이벤트만`

```
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_recover/task_id=dbt_build/attempt=1.log
2026-11-11T06:50:17.706 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:51:08.371 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:51:52.928 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:53:05.833 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:53:50.511 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:54:36.225 warning Publish 생존 신호 갱신 실패: OperationalError
2026-11-11T06:55:18.135 warning Publish 생존 신호 갱신 실패: OperationalError
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_recover/task_id=dbt_build/attempt=2.log
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_recover/task_id=dbt_build/attempt=3.log
2026-11-11T07:30:48.710 info    Done. Returned value was: {'tests_passed': 151, 'tests_failed': 0}
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_recover/task_id=publish_mart/attempt=1.log
2026-11-11T07:30:57.180 info    Done. Returned value was: {'publish_run_id': 'bd9d0f24-5364-41ab-aa4f-14ffbc14b6d5', 'changed_relations': ['facts.fct_order', 'facts.fct_order_item', 'facts.fct_order_payment'], 'mart_hashes': {'dimensions.dim_customer': 'fe83440734c49de097d85d2ba1882b4d000d1fc150e1f8893aa3650d0875d07c', 'dimensions.dim_date': 'd3c10463613576fdf557ead27ef6bce6371a0193e364b3e238c9f7ad62154bbb', 'dimensions.dim_product': '163e5432a786cfab5ac557e0aaa2544bbe36ce1fe03c1508442443b1cbc23875', 'dimensions.dim_seller': 'c132702c2a033f8a6b6dffb44bd5372752095fb1036ee8107cb6cc5a5ec8cc31', 'dimensions.dim_subscription': 'd
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_recover/task_id=publish_run_summary/attempt=1.log
2026-11-11T07:35:08.025 info    RUN_REPORT {"batch_id": "warehouse_pipeline_dag__20261111T064846Z", "verified_table_count": 9, "publish_run_id": "bd9d0f24-5364-41ab-aa4f-14ffbc14b6d5", "publish_status": "PUBLISHED", "publish_error_type": null, "changed_relations": ["facts.fct_order", "facts.fct_order_item", "facts.fct_order_payment"], "serving_export_status": "SUCCESS", "serving_export_id": "dcbfe49f-2a47-4c15-bf82-78f3ca277675", "serving_row_counts": {"facts.fct_order": 99849, "dimensions.dim_date": 3721, "facts.fct_order_item": 113445, "dimensions.dim_seller": 3095, "dimensions.dim_product": 32951, "dimensions.dim_customer
2026-11-11T07:35:08.046 info    Done. Returned value was: {'batch_id': 'warehouse_pipeline_dag__20261111T064846Z', 'verified_table_count': 9, 'publish_run_id': 'bd9d0f24-5364-41ab-aa4f-14ffbc14b6d5', 'publish_status': 'PUBLISHED', 'publish_error_type': None, 'changed_relations': ['facts.fct_order', 'facts.fct_order_item', 'facts.fct_order_payment'], 'serving_export_status': 'SUCCESS', 'serving_export_id': 'dcbfe49f-2a47-4c15-bf82-78f3ca277675', 'serving_row_counts': {'facts.fct_order': 99849, 'dimensions.dim_date': 3721, 'facts.fct_order_item': 113445, 'dimensions.dim_seller': 3095, 'dimensions.dim_product': 32951, 'dimensio
```

## Task 상태

```
initialize_run                   map=-1 state=success try=1 start=2026-11-11T06:48:48.602138Z duration=0.876316
acquire_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T06:48:49.750968Z duration=1.050433
extract_validate_load            map= 0 state=success try=1 start=2026-11-11T06:48:50.936200Z duration=2.83504
extract_validate_load            map= 1 state=success try=1 start=2026-11-11T06:48:50.938926Z duration=2.835834
extract_validate_load            map= 2 state=success try=1 start=2026-11-11T06:48:50.953177Z duration=2.825019
extract_validate_load            map= 4 state=success try=1 start=2026-11-11T06:48:54.263240Z duration=2.173129
extract_validate_load            map= 5 state=success try=1 start=2026-11-11T06:48:54.267609Z duration=2.318158
extract_validate_load            map= 3 state=success try=1 start=2026-11-11T06:48:54.270806Z duration=2.196071
extract_validate_load            map= 7 state=success try=1 start=2026-11-11T06:48:56.750754Z duration=1.219362
extract_validate_load            map= 8 state=success try=1 start=2026-11-11T06:48:56.750756Z duration=1.183339
extract_validate_load            map= 6 state=success try=1 start=2026-11-11T06:48:56.754439Z duration=1.150918
verify_bronze_commit_task        map=-1 state=success try=1 start=2026-11-11T06:48:58.535363Z duration=0.881297
release_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T06:48:58.539764Z duration=0.676936
prepare_warehouse_build          map=-1 state=success try=1 start=2026-11-11T06:48:59.698787Z duration=2.053641
dbt_build                        map=-1 state=success try=3 start=2026-11-11T07:29:57.704425Z duration=51.056907
publish_mart                     map=-1 state=success try=1 start=2026-11-11T07:30:49.849384Z duration=7.408036
export_serving_mart              map=-1 state=success try=1 start=2026-11-11T07:30:58.275333Z duration=3.932745
repoint_metabase_serving         map=-1 state=failed try=3 start=2026-11-11T07:34:58.158483Z duration=4.280447
publish_run_summary              map=-1 state=success try=1 start=2026-11-11T07:35:03.535941Z duration=4.696664
```

## 실행 환경 관찰 (판정과 무관)

attempt 1 도중 Docker Desktop 데몬이 재시작됐다(메모리 고갈). 재시작 뒤 bind mount(`/opt/airflow/dags` 등)가 비어 DAG가 `is_stale=True`가 됐고 TI가 `state=None`으로 멈췄다. 컨테이너를 `--force-recreate`로 다시 만들고 `dbt_build`부터 clear하자 attempt 3이 같은 `publish_run_id`로 정상 완료됐다.
