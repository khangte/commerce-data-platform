# ADR 016. Warehouse File 교체로 Mart를 Publish한다

## Status

Accepted (2026-09-20, Phase 7)

## Context

Published Warehouse에 직접 dbt build를 실행하면 Test 실패 후에도 마지막 성공 Mart를 보존할 수 없다. Airflow Task가 별도 Process로 실행되므로 파일 Lock만으로 동시 Publish를 안전하게 제어할 수도 없다.

## Decision

- Published 파일을 `build/{publish_run_id}.duckdb`로 복사하고, 그 파일에서 Bronze Catalog 동기화와 dbt build·test를 수행한다.
- 성공한 Build만 CHECKPOINT, Hash·행 수 기록, `os.replace`, Directory fsync 순서로 Published 파일로 교체한다.
- 실패 Build는 `failed/`에 격리하고 최신 3개만 보존한다.
- PostgreSQL `mart_publish_runs`와 부분 Unique Index `mart_publish_runs_single_active_idx`로 실행 상태와 활성 Publish Mutex를 관리한다.
- 1시간 이상 활성 상태인 Run은 다음 실행에서 Hash 비교로 확정하거나 `UNKNOWN_ERROR`의 abandoned 실패로 종료한다.

## Alternatives

| 대안 | 기각 사유 |
| ---- | --------- |
| 같은 파일의 Schema 교체 | Table별 교체는 부분 반영 위험이 있다. |
| Published 파일에 ATTACH 후 복사 | Catalog·Control·Mart 복사 범위가 넓고 중간 실패를 원자화하기 어렵다. |
| 파일 Lock Mutex | Airflow Process 경계를 넘지 못한다. |

## Consequences

매 Publish마다 Warehouse 파일 복사가 발생하며, Rebaseline은 활성 Publish가 있으면 거부된다. 실패 원인 분석을 위해 실패 Build는 최대 3개 보관한다.

## Validation

| 근거 | 결과 |
| ---- | ---- |
| 가짜 Runner Publish 통합 테스트 | `10 passed in 3.42s` |
| 오류 분류·dbt Runner·경로 단위 테스트 | 통과 |
| 실제 dbt Canary·새 Clone 전체 재현 | 실행 환경 준비 후 `scripts/verify_clean_clone.sh`로 수행 필요 |
