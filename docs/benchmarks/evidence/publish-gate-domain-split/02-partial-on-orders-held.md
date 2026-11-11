# partial_publish=true + orders Canary 강제 실패 (Check 2)

- 수집 시각: 2026-11-11T07:41Z (UTC)
- 환경: branch `feat/publish-gate-split` (HEAD 1d687ac, 브랜치 코드 수정 없이 실행), compose profile `airflow`, Airflow 3.3.1 LocalExecutor, dbt-core 1.12.3, dbt-duckdb 1.11.0
- 목적: orders 도메인 Canary 실패 시 orders만 보류(held)하고 다른 도메인은 게시되는지 확인

## 판정: PASS

- `status=PUBLISHED`, `held_domains=["orders"]`, `tests_failed=1`, run report failures에 `PUBLISH_PARTIAL:orders`.
- 06:10 run에서 Bronze에 orders 70행이 들어왔지만 `changed_relations`에는 orders 관계가 없고 `dim_customer, dim_date, dim_subscription, fct_subscription_payment`만 바뀌었다. orders 3개 fact의 hash와 row는 직전 게시(`ceddd951`)와 같다.

## 재현 절차

1. `dbt/tests/orders_publish_gate_canary.sql`의 `var('orders_publish_gate_canary', false)`를 `true`로 임시 변경했다(DAG에는 dbt vars 경로가 없어 마운트된 SQL을 바꿨다). 검증 후 `git checkout -- dbt/tests/orders_publish_gate_canary.sql`로 복원했다.
2. `source_simulation_dag`를 1회 실행해 orders 변경분을 만들었다(seed 20261111). 이 DAG가 끝나면 `warehouse_pipeline_dag`를 자동 트리거해 `manual__2026-11-11T06:10:00+00:00`, `manual__2026-11-11T06:16:14+00:00` run이 생겼고, 둘 다 강제된 Canary 상태로 돌았다.
3. `docker compose exec -T airflow-api-server python /tmp/af.py POST /api/v2/dags/warehouse_pipeline_dag/dagRuns '{"dag_run_id":"verify_partial_on","logical_date":"<now>","conf":{}}'`로 기본값(partial_publish=true) run을 실행했다.

## mart_publish_runs

명령: `docker compose exec -T postgres bash -c 'psql -U "$POSTGRES_USER" -d "$PIPELINE_METADATA_DB" -x -c "select publish_run_id, pipeline_name, batch_id, dag_run_id, status, error_type, left(error_message,120) error_message_head, tests_passed, tests_failed, tests_warned, held_domains, failed_path, previous_publish_run_id, started_at, finished_at, mart_row_counts from mart_publish_runs where dag_run_id = ..."'`

> `failure_code`는 테이블 컬럼명이 `error_type`이다. `error_message_head`는 ANSI 색 코드를 제거해 앞 120자만 남겼다.

```
publish_run_id          | d6bb6daf-899d-4919-87c7-daa6bc296e19
pipeline_name           | warehouse_pipeline_dag
batch_id                | warehouse_pipeline_dag__20261111T061000Z
dag_run_id              | manual__2026-11-11T06:10:00+00:00
status                  | PUBLISHED
error_type              | 
error_message_head      | 
tests_passed            | 150
tests_failed            | 1
tests_warned            | 0
held_domains            | ["orders"]
failed_path             | /opt/airflow/project/data/warehouse/failed/d6bb6daf-899d-4919-87c7-daa6bc296e19.duckdb
previous_publish_run_id | ceddd951-b9e2-4aee-b0d5-dad6e2ceaf6a
started_at              | 2026-11-11 06:16:38.347844+00
finished_at             | 2026-11-11 06:17:26.153761+00
mart_row_counts         | {"facts.fct_order": 99829, "dimensions.dim_date": 3721, "facts.fct_order_item": 113412, "dimensions.dim_seller": 3095, "dimensions.dim_product": 32951, "dimensions.dim_customer": 96504, "facts.fct_order_payment": 104274, "dimensions.dim_subscription": 11, "facts.fct_subscription_payment": 9}

publish_run_id          | 960e6ef9-d8b4-4f0d-843d-f149a5416982
pipeline_name           | warehouse_pipeline_dag
batch_id                | warehouse_pipeline_dag__20261111T061639Z
dag_run_id              | verify_partial_on
status                  | PUBLISHED
error_type              | 
error_message_head      | 
tests_passed            | 150
tests_failed            | 1
tests_warned            | 0
held_domains            | ["orders"]
failed_path             | /opt/airflow/project/data/warehouse/failed/960e6ef9-d8b4-4f0d-843d-f149a5416982.duckdb
previous_publish_run_id | 1e0a1933-9ae2-4eee-ba17-797550d054cd
started_at              | 2026-11-11 06:28:35.692782+00
finished_at             | 2026-11-11 06:29:56.293373+00
mart_row_counts         | {"facts.fct_order": 99829, "dimensions.dim_date": 3721, "facts.fct_order_item": 113412, "dimensions.dim_seller": 3095, "dimensions.dim_product": 32951, "dimensions.dim_customer": 96504, "facts.fct_order_payment": 104274, "dimensions.dim_subscription": 11, "facts.fct_subscription_payment": 9}
```

## Run report 판정 (warehouse_run_reports)

명령: `docker compose exec -T postgres bash -c 'psql ... -x -c "select dag_run_id, ok, failures, report->>publish_status, report->>publish_error_type, report->held_domains, report->>tests_warned, report->>serving_export_status, report->>metabase_repoint_status, recorded_at from warehouse_run_reports where dag_run_id = ..."'`

```
dag_run_id              | manual__2026-11-11T06:10:00+00:00
ok                      | f
failures                | ["TASK_NOT_SUCCESS:repoint_metabase_serving", "REPOINT_FAILED", "PUBLISH_PARTIAL:orders", "DATA_STALE", "EXPORTS_COUNT_4", "PROBE_ERROR:freshness"]
publish_status          | PUBLISHED
publish_error_type      | 
held_domains            | ["orders"]
tests_warned            | 0
serving_export_status   | SUCCESS
metabase_repoint_status | FAILED
recorded_at             | 2026-11-11 06:22:04.161539+00

dag_run_id              | verify_partial_on
ok                      | f
failures                | ["PUBLISH_PARTIAL:orders", "DATA_STALE"]
publish_status          | PUBLISHED
publish_error_type      | 
held_domains            | ["orders"]
tests_warned            | 0
serving_export_status   | SUCCESS
metabase_repoint_status | SUCCESS
recorded_at             | 2026-11-11 06:31:24.056605+00
```

> 06:10 run의 `REPOINT_FAILED`와 `TASK_NOT_SUCCESS:repoint_metabase_serving`는 그때 Metabase(bi profile)를 띄우지 않아 생겼다. Metabase를 띄운 뒤의 `verify_partial_on`은 repoint까지 SUCCESS다. `DATA_STALE`은 freshness probe 판정이다.

## Airflow task 로그 발췌 (dbt_build / publish_mart / publish_run_summary)

명령: `python3 logx.py airflow/logs/dag_id=warehouse_pipeline_dag/run_id=<run>/task_id={dbt_build,publish_mart,publish_run_summary}/attempt=*.log  # ANSI 제거, 판정 이벤트만`

```
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=manual__2026-11-11T06:10:00+00:00/task_id=dbt_build/attempt=1.log
2026-11-11T06:17:18.090 warning dbt warned_nodes=[] failed_nodes=['test.commerce_data_platform.orders_publish_gate_canary'] held_domains=['orders']
2026-11-11T06:17:18.091 info    Done. Returned value was: {'tests_passed': 150, 'tests_failed': 1}
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=manual__2026-11-11T06:10:00+00:00/task_id=publish_mart/attempt=1.log
2026-11-11T06:17:26.193 info    Done. Returned value was: {'publish_run_id': 'd6bb6daf-899d-4919-87c7-daa6bc296e19', 'changed_relations': ['dimensions.dim_customer', 'dimensions.dim_date', 'dimensions.dim_subscription', 'facts.fct_subscription_payment'], 'mart_hashes': {'dimensions.dim_customer': 'fe83440734c49de097d85d2ba1882b4d000d1fc150e1f8893aa3650d0875d07c', 'dimensions.dim_date': 'd3c10463613576fdf557ead27ef6bce6371a0193e364b3e238c9f7ad62154bbb', 'dimensions.dim_product': '163e5432a786cfab5ac557e0aaa2544bbe36ce1fe03c1508442443b1cbc23875', 'dimensions.dim_seller': 'c132702c2a033f8a6b6dffb44bd5372752095fb1036ee8107cb6cc5
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=manual__2026-11-11T06:10:00+00:00/task_id=publish_run_summary/attempt=1.log
2026-11-11T06:22:07.526 info    RUN_REPORT {"batch_id": "warehouse_pipeline_dag__20261111T061000Z", "verified_table_count": 9, "publish_run_id": "d6bb6daf-899d-4919-87c7-daa6bc296e19", "publish_status": "PUBLISHED", "publish_error_type": null, "changed_relations": ["dimensions.dim_customer", "dimensions.dim_date", "dimensions.dim_subscription", "facts.fct_subscription_payment"], "serving_export_status": "SUCCESS", "serving_export_id": "4e3b7e87-b96d-43fc-ad8e-2dc61c484a7e", "serving_row_counts": {"facts.fct_order": 99829, "dimensions.dim_date": 3721, "facts.fct_order_item": 113412, "dimensions.dim_seller": 3095, "dimensions.
2026-11-11T06:22:07.564 info    Done. Returned value was: {'batch_id': 'warehouse_pipeline_dag__20261111T061000Z', 'verified_table_count': 9, 'publish_run_id': 'd6bb6daf-899d-4919-87c7-daa6bc296e19', 'publish_status': 'PUBLISHED', 'publish_error_type': None, 'changed_relations': ['dimensions.dim_customer', 'dimensions.dim_date', 'dimensions.dim_subscription', 'facts.fct_subscription_payment'], 'serving_export_status': 'SUCCESS', 'serving_export_id': '4e3b7e87-b96d-43fc-ad8e-2dc61c484a7e', 'serving_row_counts': {'facts.fct_order': 99829, 'dimensions.dim_date': 3721, 'facts.fct_order_item': 113412, 'dimensions.dim_seller': 309
```

```
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_on/task_id=dbt_build/attempt=1.log
2026-11-11T06:29:46.284 warning dbt warned_nodes=[] failed_nodes=['test.commerce_data_platform.orders_publish_gate_canary'] held_domains=['orders']
2026-11-11T06:29:46.285 info    Done. Returned value was: {'tests_passed': 150, 'tests_failed': 1}
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_on/task_id=publish_mart/attempt=1.log
2026-11-11T06:29:56.332 info    Done. Returned value was: {'publish_run_id': '960e6ef9-d8b4-4f0d-843d-f149a5416982', 'changed_relations': [], 'mart_hashes': {'dimensions.dim_customer': 'fe83440734c49de097d85d2ba1882b4d000d1fc150e1f8893aa3650d0875d07c', 'dimensions.dim_date': 'd3c10463613576fdf557ead27ef6bce6371a0193e364b3e238c9f7ad62154bbb', 'dimensions.dim_product': '163e5432a786cfab5ac557e0aaa2544bbe36ce1fe03c1508442443b1cbc23875', 'dimensions.dim_seller': 'c132702c2a033f8a6b6dffb44bd5372752095fb1036ee8107cb6cc5a5ec8cc31', 'dimensions.dim_subscription': 'd9cb5aec21f3d6d420727d1b9dd65dbf5be7725f575d3f1611f73d0b6a1f7ff2', 'f
--- airflow/logs/dag_id=warehouse_pipeline_dag/run_id=verify_partial_on/task_id=publish_run_summary/attempt=1.log
2026-11-11T06:31:28.431 info    RUN_REPORT {"batch_id": "warehouse_pipeline_dag__20261111T061639Z", "verified_table_count": 9, "publish_run_id": "960e6ef9-d8b4-4f0d-843d-f149a5416982", "publish_status": "PUBLISHED", "publish_error_type": null, "changed_relations": [], "serving_export_status": "SUCCESS", "serving_export_id": "d693af6b-2af5-494d-8db6-8c703c476130", "serving_row_counts": {"facts.fct_order": 99829, "dimensions.dim_date": 3721, "facts.fct_order_item": 113412, "dimensions.dim_seller": 3095, "dimensions.dim_product": 32951, "dimensions.dim_customer": 96504, "facts.fct_order_payment": 104274, "dimensions.dim_subscri
2026-11-11T06:31:28.407 info    Done. Returned value was: {'batch_id': 'warehouse_pipeline_dag__20261111T061639Z', 'verified_table_count': 9, 'publish_run_id': '960e6ef9-d8b4-4f0d-843d-f149a5416982', 'publish_status': 'PUBLISHED', 'publish_error_type': None, 'changed_relations': [], 'serving_export_status': 'SUCCESS', 'serving_export_id': 'd693af6b-2af5-494d-8db6-8c703c476130', 'serving_row_counts': {'facts.fct_order': 99829, 'dimensions.dim_date': 3721, 'facts.fct_order_item': 113412, 'dimensions.dim_seller': 3095, 'dimensions.dim_product': 32951, 'dimensions.dim_customer': 96504, 'facts.fct_order_payment': 104274, 'dimensi
```

## Task 상태

```
initialize_run                   map=-1 state=success try=1 start=2026-11-11T06:28:23.875393Z duration=0.65923
acquire_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T06:28:25.190683Z duration=0.997723
extract_validate_load            map= 0 state=success try=1 start=2026-11-11T06:28:26.707290Z duration=2.585934
extract_validate_load            map= 2 state=success try=1 start=2026-11-11T06:28:26.739228Z duration=2.536833
extract_validate_load            map= 1 state=success try=1 start=2026-11-11T06:28:26.770616Z duration=2.502152
extract_validate_load            map= 3 state=success try=1 start=2026-11-11T06:28:29.619240Z duration=1.798575
extract_validate_load            map= 4 state=success try=1 start=2026-11-11T06:28:29.638642Z duration=1.77854
extract_validate_load            map= 5 state=success try=1 start=2026-11-11T06:28:29.646694Z duration=1.78893
extract_validate_load            map= 6 state=success try=1 start=2026-11-11T06:28:31.623561Z duration=1.284199
extract_validate_load            map= 7 state=success try=1 start=2026-11-11T06:28:31.641015Z duration=1.376209
extract_validate_load            map= 8 state=success try=1 start=2026-11-11T06:28:31.657822Z duration=1.357827
verify_bronze_commit_task        map=-1 state=success try=1 start=2026-11-11T06:28:33.942522Z duration=0.854073
release_source_snapshot_lease    map=-1 state=success try=1 start=2026-11-11T06:28:33.945276Z duration=0.644007
prepare_warehouse_build          map=-1 state=success try=1 start=2026-11-11T06:28:35.147980Z duration=1.586518
dbt_build                        map=-1 state=success try=1 start=2026-11-11T06:28:37.364162Z duration=68.963085
publish_mart                     map=-1 state=success try=1 start=2026-11-11T06:29:46.453375Z duration=9.967723
export_serving_mart              map=-1 state=success try=1 start=2026-11-11T06:29:56.792624Z duration=2.644203
repoint_metabase_serving         map=-1 state=success try=2 start=2026-11-11T06:31:16.888658Z duration=2.210264
publish_run_summary              map=-1 state=success try=1 start=2026-11-11T06:31:19.879605Z duration=9.276618
```

## 도메인별 hash / row 비교 (게시본 기준)

hash는 앞 8자리만 적었다. orders 도메인(`fct_order`, `fct_order_item`, `fct_order_payment`)은 보류된 run에서 직전 게시와 같고, subscriptions 도메인과 공유 dim은 갱신됐다.

| run | held_domains | dim_customer | dim_date | dim_product | dim_seller | dim_subscription | fct_order | fct_order_item | fct_order_payment | fct_subscription_payment |
|---|---|---|---|---|---|---|---|---|---|---|
| (cli) ceddd951 | [] | `98b479df` / 96484 | `3db89044` / 3698 | `163e5432` / 32951 | `c132702c` / 3095 | `3f71c30c` / 9 | `8d184352` / 99829 | `42844523` / 113412 | `4513f347` / 104274 | `8bfdf320` / 7 |
| manual__2026-11-11T06:10:00+00:00 | ["orders"] | `fe834407` / 96504 | `d3c10463` / 3721 | `163e5432` / 32951 | `c132702c` / 3095 | `d9cb5aec` / 11 | `8d184352` / 99829 | `42844523` / 113412 | `4513f347` / 104274 | `403250ce` / 9 |
| manual__2026-11-11T06:16:14+00:00 | ["orders"] | `fe834407` / 96504 | `d3c10463` / 3721 | `163e5432` / 32951 | `c132702c` / 3095 | `d9cb5aec` / 11 | `8d184352` / 99829 | `42844523` / 113412 | `4513f347` / 104274 | `403250ce` / 9 |
| verify_partial_on | ["orders"] | `fe834407` / 96504 | `d3c10463` / 3721 | `163e5432` / 32951 | `c132702c` / 3095 | `d9cb5aec` / 11 | `8d184352` / 99829 | `42844523` / 113412 | `4513f347` / 104274 | `403250ce` / 9 |
| verify_partial_recover | [] | `fe834407` / 96504 | `d3c10463` / 3721 | `163e5432` / 32951 | `c132702c` / 3095 | `d9cb5aec` / 11 | `45a2cbcf` / 99849 | `bc423276` / 113445 | `9b203db2` / 104294 | `403250ce` / 9 |
