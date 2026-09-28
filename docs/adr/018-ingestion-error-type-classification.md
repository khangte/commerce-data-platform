# ADR 018. Ingestion 실패 기록을 classify_error로 통일한다

## Status

Accepted (2026-09-21, Phase 8 Task 13A)

## Context

`src/ingestion/service.py`의 실패 기록 경로 두 곳이 `classify_error`(`src/ingestion/errors.py`)를
거치지 않고 있었다.

- `_record_failure_without_masking`은 `type(error).__name__[:64]`를 그대로 저장해, `RuntimeError`·
  `FileExistsError`처럼 Python 예외 클래스 이름이 `pipeline_runs.error_type`에 남았다.
- `_record_lease_failure`는 Lease 계열 예외 4종(`LeaseUnavailableError`, `LeaseOwnershipLostError`,
  `TableLeaseUnavailableError`, `TableLeaseOwnershipLostError`)만 별도 tuple 검사로 리터럴
  `"SOURCE_MUTATION_CONFLICT"`를 저장하고 나머지는 역시 클래스 이름을 저장했다.

두 경로가 서로 다른 방식으로 분류해, 같은 예외라도 어느 경로로 실패했는지에 따라 저장값이 갈렸다.
판정 [014](../architect-review/014_ingestion-error-type-vocabulary.md)가 이 '혼합' 상태를 지적했다.

또한 [019](../architect-review/019_task9-r15-r11-verification.md)는 별개 결함을 지적했다: R-15
Source 연결 실패 시나리오에서 `open_table_snapshot`의 연결 열기가 `ingest_table`
(`src/ingestion/service.py`)의 `record_started_run`보다 먼저 실행되는 `with`문 안에 있는데, 이
`with`문 전체를 감싸는 `except`가 전혀 없어 연결이 실패하면 `pipeline_runs`에 행이 아예 생기지
않았다 — 오분류가 아니라 관측 자체의 부재였다.

Task 13A 착수 전 두 가지 설계 질의를 architect에 올렸다(판정 [024](../architect-review/024_task13a-error-vocabulary-rulings.md)):

1. Lease 예외 4종을 `SOURCE_MUTATION_CONFLICT` 하나로 합칠지, 아니면 잠금 종류로 가를지.
2. 019의 관측 부재를 어떻게 메울지.

## Decision

### Lease 예외의 Error Type — 잠금 종류가 아니라 사건 종류로 가른다

`classify_error`의 기존 Lease 분기는 그대로 둔다. `_record_lease_failure` 한 곳만 고친다 —
`SOURCE_MUTATION_CONFLICT` 리터럴을 지우고 `classify_error(error)`를 그대로 쓴다.

| 예외 | Error Type | 재시도 |
| ---- | ---------- | ------ |
| `LeaseUnavailableError`(원천 동시성 잠금) | `LEASE_UNAVAILABLE` | 가능 |
| `TableLeaseUnavailableError`(테이블별 수집 잠금) | `LEASE_UNAVAILABLE` | 가능 |
| `PublishInProgressError`(Publish 잠금) | `LEASE_UNAVAILABLE` | 가능 |
| `LeaseOwnershipLostError` | `LEASE_OWNERSHIP_LOST` | 불가 |
| `TableLeaseOwnershipLostError` | `LEASE_OWNERSHIP_LOST` | 불가 |
| `SourceCursorRegressionError`(원천 갱신 시각 역행) | `SOURCE_CONTRACT_ERROR` | 불가 |

가르는 축은 "어느 잠금이냐"가 아니라 "무슨 일이 일어났느냐"다. 대기 후 재시도 가능한 Unavailable과
소유권을 잃어 재시도 불가능한 OwnershipLost는 잠금 종류와 무관하게 같은 재시도 답을 낸다.
`pipeline_runs`에 잠금 종류를 담는 컬럼이 없고, 두 종류 잠금의 실패 조치(대기 후 재실행)도 같다.
어느 잠금이었는지는 `error_message`가 그대로 담는다.

`PRD_v1.14.md` §18(1704-1719행)은 `LEASE_UNAVAILABLE`/`LEASE_OWNERSHIP_LOST`를 Error Type으로
등재하고 `SOURCE_MUTATION_CONFLICT`를 Deprecated(Superseded by `LEASE_UNAVAILABLE`/
`LEASE_OWNERSHIP_LOST`)로 표시한다. **PRD 개정 요청 반영됨**: 두 코드를 §18에 추가하고
`SOURCE_MUTATION_CONFLICT`는 대체됨으로 표시해달라는 요청을 사용자 승인을 받아 반영했다. 이전
Version(`PRD_v1.13.md`)은 `PRD.bak/`로 옮겼다.

### `_record_failure_without_masking`도 `classify_error`를 거친다

`type(error).__name__[:64]` 저장을 제거하고 `classify_error(error)`로 바꾼다. `pipeline_runs.error_type`에
Python 예외 클래스 이름이 남는 경우가 없어진다. `classify_error`에 분류 규칙이 없는 예외는 여전히
`UNKNOWN_ERROR`로 떨어진다 — 예를 들어 `src/ingestion/storage.py`가 412 응답을 바꿔 던지는
`FileExistsError`(Object 중복)는 계속 `UNKNOWN_ERROR`다. Object 중복은 Lease·연결 장애와 다른
관심사라 이 ADR에서 전용 코드를 만들지 않는다.

### 019 관측 부재 — Run 시작 전 실패를 공유 경로로 기록한다

`ingest_table`의 두 번째 `with`문(`LeaseHeartbeat` + `open_table_snapshot`) 전체를 감싸는 `except`를
추가한다. 다만 그 `with` 본문 안에는 이미 `record_started_run`과 `_record_failure_without_masking`을
쓰는 안쪽 `try`가 있어, 바깥 `except`가 조건 없이 기록하면 같은 `run_id`로 `record_started_run`과
`FAILED` 기록이 두 번 일어난다(둘 다 `suppress(Exception)` 안이라 조용히 어긋난다).

- `record_started_run` 직후 `run_started = True`를 세운다.
- 바깥 `except`는 `run_started`가 `False`일 때만 기록한다.
- 바깥 기록도 `classify_error`를 거친다. `SOURCE_CONNECTION_ERROR`를 하드코딩하지 않는다 — 이
  지점에서는 Heartbeat 진입 실패나 설정 오류도 날 수 있고, 각자 자기 코드로 남아야 한다.
- `_record_lease_failure`는 더 이상 Lease 전용이 아니므로 `_record_prestart_failure`로 이름을
  바꾸고, Lease 획득 실패(첫 번째 `except`)와 연결 실패(새 `except`) 두 호출처가 이 함수 하나를
  공유한다. 두 곳 모두 `extract_upper_bound=None`인 `PipelineRun`을 만들고 `record_started_run` →
  `record_failed_run(classify_error(error))` 순서로 기록한다.

### Object Storage 실패 — 코드 이름은 `OBJECT_STORAGE_ERROR`, 재시도는 HTTP 상태로 가른다

`PRD_v1.14.md` §18에 이미 `OBJECT_STORAGE_ERROR`가 있다. 새 이름을 짓지 않는다.
`src/ingestion/storage.py`는 `ClientError`를 감싸지 않고 그대로 올린다(412만 `FileExistsError`로
바꾼다). `classify_error`는 `botocore.exceptions.ClientError`/`BotoCoreError`에 직접 분기해 항상
`OBJECT_STORAGE_ERROR`를 반환한다.

Error Type은 하나로 두고 재시도 가부만 HTTP 상태로 가른다. `is_retryable`은 `OBJECT_STORAGE_ERROR`일
때 `error.response["ResponseMetadata"]["HTTPStatusCode"]`(`.get` 연쇄로 방어)가 5xx 또는 429면
`True`, 그 외(4xx, 상태 없음)는 `False`를 반환한다 — `RETRYABLE_ERROR_TYPES` 집합 소속 여부만으로는
결정되지 않는 유일한 예외다. 503(`ServiceUnavailable`)은 기다리면 낫고 `AccessDenied`는 몇 번을
다시 해도 같다. 이 차이를 코드 이름으로 나누면 PRD에 없는 코드가 늘고 AC-13 집계 축만 흐려진다.

## Alternatives

| 대안 | 기각 사유 |
| ---- | --------- |
| Lease 예외를 잠금 종류(원천/테이블)로 갈라 `SOURCE_MUTATION_CONFLICT` vs `LEASE_*` | 대기 후 재시도 가능한 Unavailable과 재시도 불가능한 OwnershipLost가 한 코드 안에 섞여 둘 중 하나는 반드시 틀린 재시도 답을 낸다. `TableLeaseOwnershipLostError`는 ADR-017 Accepted가 이미 `LEASE_OWNERSHIP_LOST`로 확정했다. |
| Lease 예외 4종 전부를 `SOURCE_MUTATION_CONFLICT`로 통일 | `LeaseUnavailableError`는 지금 재시도 가능(`LEASE_UNAVAILABLE`)인데 여기 합치면 재시도 불가로 바뀐다. 014가 고치라던 재시도 판정 불능을 다른 모양으로 되살린다. |
| `open_table_snapshot` 실패 시 바깥 `except`가 조건 없이 기록 | 본문 안에서 실패한 정상 경로 Run과 겹쳐 같은 `run_id`로 이중 기록(`record_started_run` 2회, `FAILED` 2회)이 조용히 발생한다. |
| Object Storage 실패를 HTTP 상태별로 별도 Error Type(예: `OBJECT_STORAGE_TRANSIENT_ERROR`/`OBJECT_STORAGE_PERMANENT_ERROR`)으로 분리 | PRD §18에 없는 코드가 늘어 AC-13 집계 축이 흐려진다. 재시도 가부는 원래 `is_retryable`이 답할 몫이지 분류 코드의 몫이 아니다. |

## Consequences

- `pipeline_runs.error_type`에 Python 예외 클래스 이름이 남는 경로가 없어진다.
- `tests/integration/test_orders_ingestion_service_integration.py`의 두 테스트를 갱신했다(계획 파일
  목록 밖이지만 이 결정의 직접 결과):
  - `test_orders_service_does_not_read_or_publish_when_generator_holds_the_global_lease`: 기대값
    `("FAILED", "SOURCE_MUTATION_CONFLICT")` → `("FAILED", "LEASE_UNAVAILABLE")`.
  - `test_orders_service_keeps_watermark_when_final_object_already_exists`: 기대값
    `("FAILED", "FileExistsError")` → `("FAILED", "UNKNOWN_ERROR")`.
- R-15 시나리오는 이제 `pipeline_runs`에 `("FAILED", "SOURCE_CONNECTION_ERROR")` 1행을 남긴다.
  `run_count == 0` 단언을 `run_count == 1`로 바꾸고, `docs/runbooks/r15-source-connection-failure.md`의
  '알려진 한계'를 수정 완료로 갱신했다.
- R-02 시나리오는 `("FAILED", "OBJECT_STORAGE_ERROR")`를 남긴다. R-02 Test는 이제 `http_status`와
  `is_retryable`을 증적에 남긴다. 기존 R-02 증적 JSON(`data/reliability/r02/*.json`)은 고치지 않는다 —
  수정 전 동작(클래스 이름 저장) 그대로 보존한다.
- `PRD_v1.13.md` §18에 `LEASE_UNAVAILABLE`/`LEASE_OWNERSHIP_LOST`가 없다는 어긋남은 사용자 승인을
  받아 `PRD_v1.14.md`에서 해소했다. `SOURCE_MUTATION_CONFLICT`는 §18에 대체됨으로 표시했다.
- `src/ingestion/storage.py`의 412 → `FileExistsError` → `UNKNOWN_ERROR` 경로는 이 ADR의 범위 밖으로
  남는다.

## Validation

| 근거 | 결과 |
| ---- | ---- |
| `uv run ruff check .` | 통과 |
| `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py tests/reliability/test_r11_r15_reprocess_and_source.py tests/integration/test_orders_ingestion_service_integration.py -v` | `23 passed`(기대값 갱신 전 1건 실패) |
| `grep -rn 'error_type=' --include=*.py src/ \| grep -v classify_error` | 4곳(`publish.py:258`, `publish_metadata.py:213`, `dbt_runner.py:90`, `service.py:593` — `BATCH_IDENTITY_CONFLICT` 상수 참조) 모두 `classify_error` 밖에서 상수를 직접 쓰는 정당한 경로 |
| `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest -q`(전체) | `278 passed, 0 failed, 5 skipped` |
