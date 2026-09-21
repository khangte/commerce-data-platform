# Warehouse Publish 오류 유형

`src/warehouse/publish.py`의 Build-then-swap Pipeline이 낼 수 있는 Error Type과 예외를
원인·조치와 함께 정리한다. 관련 시나리오는 [R-14 dbt Failure](../runbooks/r14-dbt-failure.md).

| Error Type/예외 | 발생 지점 | 원인 | 조치 |
| --- | --- | --- | --- |
| `DBT_BUILD_ERROR` | `build_warehouse` → `classify_dbt_failure` | `run_results.json`에서 Model/Seed/Snapshot Node가 `error` 상태이거나, 분류 근거가 없을 때 기본값으로 반환됨 | Build 실패 Node의 SQL/의존성 오류를 수정한다. Build 파일은 `failed/`에 격리돼 있으므로 원인 파악에 활용하고, 원인 제거 후 `dbt build`를 재실행한다 |
| `DBT_TEST_ERROR` | `build_warehouse` → `classify_dbt_failure` | Build Node는 모두 성공했지만 Singular/Generic Test(`test.`/`unit_test.`)가 `error` 또는 `fail` | 실패한 Test가 지키려는 불변 조건과 실제 데이터를 비교해 Upstream Model 또는 Source 데이터를 고친다. Published Mart는 변경되지 않았으므로 즉시 조치할 필요는 없다 |
| `UNKNOWN_ERROR` (Abandoned Run) | `recover_incomplete_publishes` | `BUILDING`/`PUBLISHING` 상태로 `PUBLISH_STALE_AFTER`(1시간)를 넘긴 Run — Process가 죽거나 강제 종료됨 | 해당 Run은 자동으로 `FAILED`(`error_message == "abandoned"`)로 정리되고 Build 파일도 격리된다. 남은 Published 파일이 최신 성공 Run과 일치하는지만 확인하고 재실행한다 |
| `PublishInProgressError` | `start_publish_run`, `assert_no_active_publish` | `mart_publish_runs`에 이미 `BUILDING`/`PUBLISHING` 상태인 다른 Run이 있음(`mart_publish_runs_single_active_idx` 위반) | 동시 실행 중인 Publish가 끝날 때까지 기다린다. 오래됐다면 `PUBLISH_STALE_AFTER` 경과 후 다음 실행이 자동으로 Abandoned 처리하므로 강제로 상태를 바꾸지 않는다 |
| `PublishedWalError` | `prepare_warehouse_build` | Published 파일 옆에 DuckDB WAL(`.wal`)이 남아 있어 마지막 CHECKPOINT가 불완전할 가능성이 있음 | Published 파일이 정상 CHECKPOINT됐는지 확인 후 WAL을 제거한다. 원인 불명이면 마지막 성공 `publish_run_id`의 Mart Hash와 현재 Published 파일 Hash를 대조해 데이터 손상 여부부터 확인한다 |

## 상태 전이 요약

`mart_publish_runs.status`는 `BUILDING → PUBLISHING → PUBLISHED` 또는 `BUILDING/PUBLISHING → FAILED`로만
전이한다(`src/warehouse/publish_metadata.py`의 `_transition`이 기대 상태가 아니면
`PublishStateError`로 막는다). `FAILED`로 끝난 Run은 `error_type`·`error_message`·`failed_path`를
남기고, Published 파일 교체(`publish_build`의 `os.replace`)는 `PUBLISHING` 상태에서만 일어나므로
`BUILDING` 단계 실패는 Published 파일에 영향을 줄 수 없다.
