# H. Airflow에서 `subscriptions`만 보류

- 수집 시각: 2026-11-11T09:58Z (UTC)
- 환경:
  - `docker compose --profile airflow up -d`로 띄웠다. 메모리 때문에 Metabase는 띄우지 않았다.
  - Airflow 3.3.1 LocalExecutor, `partial_publish` 기본값 `true`
- 대표 run: `manual__2026-11-11T09:40:00+00:00` (publish_run_id `13e57fda-f644-43a9-9217-7c42dc92c251`)
- 같은 조건의 두 번째 run: `manual__2026-11-11T09:42:02+00:00` (`e34cbcfc-6ea1-4aee-9eed-97bb927af43b`)

## 판정: PASS

- `dbt_build`는 success다. 실패 Node는 `subscriptions_publish_gate_canary` 하나이고 `held_domains=['subscriptions']`다.
- `mart_publish_runs`는 `PUBLISHED`이고 `held_domains=["subscriptions"]`다.
- Run report: `ok=false`, failures에 `PUBLISH_PARTIAL:subscriptions`
- 주문 Fact는 최신으로 게시됐다.
  - `changed_relations`에 `facts.fct_order`·`fct_order_item`·`fct_order_payment`·`dim_customer`가 들어 있다.
  - `fct_order`는 99849 → 99869가 됐다.
- 구독 결제 `fct_subscription_payment`는 9 그대로다.

## 재현 절차

1. `dbt/tests/subscriptions_publish_gate_canary.sql`의 var 기본값을 `false`에서 `true`로 잠시 바꿨다. DAG에 dbt var Param이 없어서 쓴 수동 절차다.
2. `source_simulation_dag`를 unpause했다. 이어서 run `verify_subs_sim`을 trigger했다.
   - unpause하면서 `scheduled__2026-11-11T09:40:00+00:00`도 같이 생겼다.
   - 두 시뮬레이션 run이 끝나자 `warehouse_pipeline_dag` run 2개가 자동 trigger됐다.
3. 실행이 끝나고 `source_simulation_dag`를 다시 pause했다. Canary SQL은 `git checkout dbt/tests/subscriptions_publish_gate_canary.sql`로 복원했다.

```bash
docker compose exec -T airflow-api-server python /tmp/af.py PATCH /api/v2/dags/source_simulation_dag '{"is_paused": false}'
docker compose exec -T airflow-api-server python /tmp/af.py POST /api/v2/dags/source_simulation_dag/dagRuns "{\"dag_run_id\":\"verify_subs_sim\",\"logical_date\":\"$NOW\",\"conf\":{}}"
docker compose exec -T airflow-api-server python /tmp/af.py PATCH /api/v2/dags/source_simulation_dag '{"is_paused": true}'
```

## mart_publish_runs

```text
-[ RECORD 1 ]------------+-------------------------------------
publish_run_id           | 13e57fda-f644-43a9-9217-7c42dc92c251
batch_id                 | warehouse_pipeline_dag__20261111T094000Z
status                   | PUBLISHED
tests_passed / failed / warned | 150 / 1 / 0
held_domains             | ["subscriptions"]
failed_path              | .../failed/13e57fda-f644-43a9-9217-7c42dc92c251.duckdb
previous_publish_run_id  | bd9d0f24-5364-41ab-aa4f-14ffbc14b6d5
started_at → finished_at | 09:42:20 → 09:43:07
row counts               | fct_order 99869, fct_order_item 113491, fct_order_payment 104314,
                         | dim_customer 96524, fct_subscription_payment 9
-[ RECORD 2 ]------------+-------------------------------------
publish_run_id           | e34cbcfc-6ea1-4aee-9eed-97bb927af43b
status                   | PUBLISHED
held_domains             | ["subscriptions"]
previous_publish_run_id  | 13e57fda-f644-43a9-9217-7c42dc92c251
row counts               | RECORD 1과 같음 (새 원천 변경 없음, changed_relations [])
```

직전 게시본(`bd9d0f24…`, `verify_partial_recover`, 07:30:57Z)은 보류 없이 게시됐다.

```text
status PUBLISHED | held_domains [] | fct_order 99849 | fct_subscription_payment 9
```

`failed_path`에는 경계 재검사 전 Build 복사본이 남는다. 보류가 있으면 항상 남기는 것이며, 이번처럼 경계를 통과해도 남는다.

## Run report 판정 (warehouse_run_reports)

```text
manual__2026-11-11T09:40:00+00:00
  ok              | f
  publish_status  | PUBLISHED
  held_domains    | ["subscriptions"]
  serving_export  | SUCCESS
  repoint         | FAILED
  data_lag_s      | 405.56
  failures        | ["TASK_NOT_SUCCESS:repoint_metabase_serving", "REPOINT_FAILED",
                  |  "PUBLISH_PARTIAL:subscriptions", "EXPORTS_COUNT_5", "PROBE_ERROR:freshness"]
manual__2026-11-11T09:42:02+00:00
  ok              | f
  failures        | 위와 같고 EXPORTS_COUNT_6
  data_lag_s      | 760.40
```

`PUBLISH_PARTIAL:subscriptions` 외의 failure는 이 판정과 무관하다. 원인은 다음과 같다.

- `REPOINT_FAILED`·`TASK_NOT_SUCCESS:repoint_metabase_serving`·`PROBE_ERROR:freshness`: Metabase를 띄우지 않았다.
- `EXPORTS_COUNT_*`: 그동안 Serving export가 누적됐다.

## Airflow task 로그 발췌

```text
--- run_id=manual__2026-11-11T09:40:00+00:00/task_id=dbt_build/attempt=1.log
2026-11-11T09:42:59.133 warning dbt warned_nodes=[] failed_nodes=['test.commerce_data_platform.subscriptions_publish_gate_canary'] held_domains=['subscriptions']
2026-11-11T09:42:59.134 info    Done. Returned value was: {'tests_passed': 150, 'tests_failed': 1}
--- task_id=publish_mart/attempt=1.log
2026-11-11T09:43:07.043 info    Done. Returned value was: {'publish_run_id': '13e57fda-f644-43a9-9217-7c42dc92c251', 'changed_relations': ['dimensions.dim_customer', 'facts.fct_order', 'facts.fct_order_item', 'facts.fct_order_payment'], ...}
--- task_id=publish_run_summary/attempt=1.log
2026-11-11T09:48:50.642 info    RUN_REPORT {"batch_id": "warehouse_pipeline_dag__20261111T094000Z", "verified_table_count": 9, "publish_run_id": "13e57fda-f644-43a9-9217-7c42dc92c251", "publish_status": "PUBLISHED", "publish_error_type": null, "changed_relations": ["dimensions.dim_customer", "facts.fct_order", "facts.fct_order_item", "facts.fct_order_payment"], "serving_export_status": "SUCCESS", "serving_row_counts": {"facts.fct_order": 99869, "facts.fct_order_item": 113491, ...}, ...}

--- run_id=manual__2026-11-11T09:42:02+00:00/task_id=dbt_build/attempt=1.log
2026-11-11T09:49:32.971 warning dbt warned_nodes=[] failed_nodes=['test.commerce_data_platform.subscriptions_publish_gate_canary'] held_domains=['subscriptions']
2026-11-11T09:49:32.972 info    Done. Returned value was: {'tests_passed': 150, 'tests_failed': 1}
--- task_id=publish_mart/attempt=1.log
2026-11-11T09:49:42.382 info    Done. Returned value was: {'publish_run_id': 'e34cbcfc-6ea1-4aee-9eed-97bb927af43b', 'changed_relations': [], ...}
```

## Task 상태 (`manual__2026-11-11T09:40:00+00:00`)

```text
initialize_run                   map=-1 state=success try=1 start=2026-11-11T09:42:10Z
acquire_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T09:42:11Z
extract_validate_load            map=0..8 state=success try=1 (9개)
verify_bronze_commit_task        map=-1 state=success try=1 start=2026-11-11T09:42:18Z
release_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T09:42:18Z
prepare_warehouse_build          map=-1 state=success try=1 start=2026-11-11T09:42:19Z
dbt_build                        map=-1 state=success try=1 start=2026-11-11T09:42:22Z duration=37.0s
publish_mart                     map=-1 state=success try=1 start=2026-11-11T09:42:59Z duration=7.2s
export_serving_mart              map=-1 state=success try=1 start=2026-11-11T09:43:07Z
repoint_metabase_serving         map=-1 state=failed  try=3 start=2026-11-11T09:48:42Z   (Metabase 미기동)
publish_run_summary              map=-1 state=success try=1 start=2026-11-11T09:48:47Z
```
