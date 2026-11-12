# R-14 dbt Failure

## 문제

Warehouse Build 중 dbt Test가 실패했을 때, 실패한 Build 파일이 `failed/`로 격리되고 Bronze
Metadata(`bronze_objects`)와 Watermark(`watermarks`)가 전혀 바뀌지 않으며, 마지막으로 Publish된
Mart Hash·Row Count도 그대로 유지되는지 확인한다. 격리 대신 Published 파일이 오염되면 다음
소비자가 검증되지 않은 데이터를 읽게 된다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -k r14 -v`를
실행한다. 정상 `dbt build`로 첫 Publish(`first`)를 성공시킨 뒤, `publish_gate_canary` Var로
Singular Test 하나를 고의로 실패시키는 `CANARY_RUNNER`(`tests/integration/test_publish_gate_dbt_integration.py`)로
두 번째 Publish(`failing`)를 실행하고, 그 실패가 격리·정리된 뒤 다시 정상 `dbt build`로 세 번째
Publish(`second`)를 실행한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| Canary 실패 직후 | `WarehouseBuildError.error_type == "DBT_TEST_ERROR"` | 일치 |
| Canary 실패 직후 | `bronze_objects`/`watermarks` 전체 Row의 MD5 요약이 실패 전과 동일 | 일치 |
| Canary 실패 직후 | Published Mart Logical Hash/Row Count가 `first`와 동일 | 일치 |
| Canary 실패 직후 | `mart_publish_runs`에서 `failing` Run이 `(status, error_type) == ("FAILED", "DBT_TEST_ERROR")` | 일치 |
| Canary 실패 직후 | 실패한 Build 파일이 `paths.failed_file(failing.publish_run_id)`에 격리 | 일치 |
| 재Publish(`second`) 직후 | `second.previous_publish_run_id == first.publish_run_id`, `second.mart_hashes == first.mart_hashes`, `second.changed_relations == ()` | 일치 |
| 재Publish(`second`) 직후 | `mart_publish_runs`에서 `second` Run이 `PUBLISHED` 상태이고, Published 파일의 Mart Logical Hash가 `first`와 동일(Swap 안착 확인) | 일치 |

## 원인과 불변 조건

`publish_warehouse`(`src/warehouse/publish.py`)는 Build-then-swap 구조다.
`prepare_warehouse_build`는 Published 파일을 Build 경로로 복사하고 `sync_bronze_catalog`로
PostgreSQL `bronze_objects`를 DuckDB Build 파일에 **읽기 전용**으로 반영할 뿐 PostgreSQL에는 아무
것도 쓰지 않는다. `build_warehouse`가 `dbt_runner`를 실행해 `DbtRunResult`를 얻고,
`run_results.json`의 Node 상태를 `classify_dbt_failure`(`src/warehouse/dbt_runner.py`)로
분류한다 — Model/Seed/Snapshot Error가 있으면 `DBT_BUILD_ERROR`, 없고 Test 실패만 있으면
`DBT_TEST_ERROR`다. 실패하면 `_fail()`이 Build 파일(과 WAL)을 `os.replace`로 `failed/`에 옮기고
`mark_failed`로 Run을 `FAILED`로 종료한 뒤 `WarehouseBuildError`를 던진다. Published 파일 교체는
`publish_build`의 마지막 `os.replace` 한 번뿐이라, Build/Test 단계에서 실패하면 그 `os.replace`에
도달하지 않으므로 Published 파일과 그 Hash는 물리적으로 손댈 일이 없다. Watermark는 Publish
경로 어디에서도 갱신되지 않는다(Ingestion 쪽 별도 관심사).

## 복구 절차

이 시나리오는 정상 동작이라 복구 대상이 아니다. 실패가 Published Mart를 오염시켰다면 원인은
보통 (1) `_fail()` 호출 전에 이미 `publish_build`의 `os.replace`가 실행됐거나(코드 순서 회귀),
(2) `failed_path`로 격리되지 않고 Build 파일이 그대로 남아 다음 Run이 재사용한 경우다. Error
Type별 원인·조치는 [Warehouse Publish Troubleshooting](../troubleshooting/warehouse-publish.md)을
참고한다. 원인 제거 후 `dbt build`를 다시 실행하면 이전 성공 Run에 이어 새 Publish가 만들어진다.

### 구독 결제 상태 대문자 전환 후 첫 게시

구독 결제 상태 표준화 코드를 병합한 뒤 첫 게시에는 반드시 다음 명령을 사용한다.

```bash
airflow dags trigger warehouse_pipeline_dag --conf '{"full_refresh": true}'
```

호스트 CLI로 게시했다면(`uv run python -m src.warehouse.publish --full-refresh`) 바로 `uv run python -m src.serving export`를 실행한다. 실행하지 않으면 Metabase가 이전 Export를 계속 읽는다(059).

증분 게시를 실행하면 복사한 기존 Warehouse의 소문자 `completed`·`failed` 행이 남아
`accepted_values_fct_subscription_payment_payment_status__COMPLETED__FAILED` 검증에 실패한다.
품질 Gate가 게시를 차단하고 마지막 성공 Warehouse는 보존한다. 위 전체 새로고침 명령으로
모든 결제 행을 `COMPLETED`·`FAILED`로 재계산한 뒤, 이후 게시부터 증분 실행을 사용한다.
전환 검증 결과는 [Phase 7 검증 기록](../phases/phase-07-data-quality-publish.md)에 정리했다.

## 지표 계층 Test 경고 (`DBT_TEST_WARN`)

지표 계층(`marts/metrics`, `rpt_*`) Test는 `severity: warn`이라 실패해도 게시를 막지 않는다
([ADR 020](../adr/020-split-publish-gate-by-severity.md)). 위 격리 절차는 `error` 등급 Test에만
해당한다. 경고가 있는 게시는 정상 `PUBLISHED`이고, Fact·Dimension은 검증된 상태다. 어긋났을 수
있는 것은 `rpt_*`뿐이다.

확인:

```sql
SELECT dag_run_id, recorded_at FROM warehouse_run_reports
WHERE failures ? 'DBT_TEST_WARN' ORDER BY recorded_at DESC;

SELECT publish_run_id, tests_warned, finished_at FROM mart_publish_runs
WHERE tests_warned > 0 ORDER BY started_at DESC;
```

어떤 Test인지는 `dbt_build` Task 로그의 `dbt warned_nodes=[...]` 줄에 남는다(DB에는 건수만 남는다).
로그가 없으면 Published 파일 **복사본**에서 지표 Test만 다시 돌려 식별한다. 복사본에서는 Test 결과가 출력된 뒤 종료 Hook이 Catalog 오류로 끝날 수 있다. Test
결과 줄만 본다.

```bash
cp data/warehouse/warehouse.duckdb /tmp/w.duckdb
WAREHOUSE_PATH=/tmp/w.duckdb .venv/bin/dbt test --project-dir dbt --profiles-dir dbt \
  --select config.severity:warn --vars "$(.venv/bin/python -m src.warehouse.dbt_vars)"
```

조치: 원인이 지표 Model이면 Model을 고친 뒤 다음 게시에서 경고가 사라지는지 확인한다. Fact가
원인으로 보이면 Fact Test가 그 경우를 잡지 못한 것이므로 `error` 등급 Test를 보강한다.

전체 Gate(v1)로 되돌리기: 경고 상태의 지표를 게시하면 안 되는 기간에는 `metrics_test_severity`를
`error`로 준다.

```bash
uv run python -m src.warehouse.publish --dbt-vars "{metrics_test_severity: error}"
```

DAG에는 dbt var Param이 없다. DAG 게시까지 되돌리려면 `dbt/dbt_project.yml`의
`metrics_test_severity` 기본값을 `error`로 바꿔 배포한다.

## 도메인 부분 게시 (`PUBLISH_PARTIAL:<domain>`)

`error` 등급 Test 실패가 한 도메인(`orders` 또는 `subscriptions`)에만 귀속되면 그 도메인만 직전
게시 값으로 되돌리고 나머지를 게시한다([ADR 021](../adr/021-split-publish-gate-by-domain.md)).
이때 `dbt_build` Task는 **성공**으로 끝나고 상태는 `PUBLISHED`다. 대신 마지막 Task
`domain_publish_alert`가 실패하고 DagRun이 `failed`가 된다. 실패 메시지
`DOMAIN_PUBLISH_ALERT: <판정 코드>`에 보류된 도메인이 있다. 이 실패는 게시가 안 됐다는 뜻이 아니다.
나머지 도메인은 게시됐고 Serving 내보내기와 Metabase 재지정도 끝났다. 자세한 내용은 Run report로 확인한다. 같은 Task는 `DATA_STALE:<domain>`, `PUBLISH_FAILED`,
`ROW_COUNT_DROP:<relation>`에서도 실패하므로 메시지의 판정 코드를 먼저 본다. 보류 도메인의 Table은 직전 게시 시점, 나머지는 최신 시점이다.

확인:

```sql
SELECT dag_run_id, failures, recorded_at FROM warehouse_run_reports
WHERE failures::text LIKE '%PUBLISH_PARTIAL%' ORDER BY recorded_at DESC;

SELECT publish_run_id, held_domains, tests_failed, failed_path, finished_at
FROM mart_publish_runs WHERE held_domains <> '[]' ORDER BY started_at DESC;
```

- 실패한 Test 이름: `dbt_build` Task 로그의 `dbt warned_nodes=... failed_nodes=[...] held_domains=[...]` 줄.
- 실패 당시 데이터: `failed_path`의 파일. 되돌리기 **전** Build 복사본이다. `failed/`는 최근 3개만
  남으므로 부분 게시가 이어지면 오래된 증거부터 지워진다.

보류가 이어지는지 확인: Run report의 `consecutive_holds`가 도메인별 연속 보류 횟수다. 마지막으로
보류 없이 게시된 시각(`domain_refreshed_at`)부터 원천 주기의 2배가 지나면 `DATA_STALE:<domain>`이
함께 뜬다([게시 SLO](../reference/publish-slo.md)). 이 판정은 결제·주문 시각이 아니라 게시 시각 기준이다.

```sql
SELECT dag_run_id, report->'consecutive_holds' AS holds,
       report->'domain_refreshed_at' AS refreshed_at, failures
FROM warehouse_run_reports ORDER BY recorded_at DESC LIMIT 5;
```

조치: `failed_path` 파일에서 실패 Test의 원인을 찾아 고친다. 다음 실행이 성공하면 보류 도메인이
밀린 Key까지 반영하고 `held_domains`가 `[]`로 돌아온다. 부분 게시 동안 재계산 경계는 전진하지
않으므로 별도 Backfill은 필요 없다.

전체 차단으로 끝나는 경우(`FAILED`, 위 격리 절차 적용): 공유 Dimension(`dim_customer`,
`dim_date`)·staging·intermediate Test 실패, Model 오류, 두 도메인 동시 실패, 첫 게시, 되돌린 뒤
경계 Test 실패. 마지막 두 경우는 `error_message` 앞부분에 물러난 사유(`boundary tests failed`,
`revert failed`)가 남는다.

전체 Gate로 되돌리기: 시점이 어긋난 게시본을 내보내면 안 되는 기간에는 부분 게시를 끈다.

```bash
airflow dags trigger warehouse_pipeline_dag --conf '{"partial_publish": false}'
uv run python -m src.warehouse.publish --no-partial-publish
```

정기 실행까지 끄려면 DAG Param `partial_publish`의 기본값을 `False`로 바꿔 배포한다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r14", ...)` 증적에서
`bronze_hash_unchanged == true`, `watermark_hash_unchanged == true`,
`published_mart_hash_unchanged == true`, `failed_error_type == "DBT_TEST_ERROR"`,
`second_publish_chains_to_first == true`, `second_publish_status == "PUBLISHED"`,
`published_mart_hash_after_recovery == first.mart_hashes`를 확인한다.
