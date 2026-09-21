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

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r14", ...)` 증적에서
`bronze_hash_unchanged == true`, `watermark_hash_unchanged == true`,
`published_mart_hash_unchanged == true`, `failed_error_type == "DBT_TEST_ERROR"`,
`second_publish_chains_to_first == true`, `second_publish_status == "PUBLISHED"`,
`published_mart_hash_after_recovery == first.mart_hashes`를 확인한다.
