# 024. Task 13A 착수 전 설계 질의 2건 — 판정

> 판정: architect, 2026-09-21 (Task 13A 착수 전 developer 질의)
> 관련: [014](014_ingestion-error-type-vocabulary.md), [019](019_task9-r15-r11-verification.md), ADR-017, PRD v1.13 §18

## 질의 1 — Lease 예외의 Error Type

**developer 제안(잠금 종류로 가르기: Generator 잠금 → `SOURCE_MUTATION_CONFLICT`, Table/Publish 잠금 → `LEASE_*`)을 기각한다. 계획 지시문의 "4종 전부 `SOURCE_MUTATION_CONFLICT`"도 함께 기각한다.**

### 판정

`classify_error`의 Lease 분기는 **지금 그대로 둔다**. 바꾸는 것은 `_record_lease_failure` 한 곳뿐이다. `SOURCE_MUTATION_CONFLICT` 리터럴을 지우고 `classify_error`를 거치게 한다.

| 예외 | Error Type | 재시도 |
| ---- | ---------- | ------ |
| `LeaseUnavailableError` (원천 동시성 잠금) | `LEASE_UNAVAILABLE` | 가능 |
| `TableLeaseUnavailableError` (테이블별 수집 잠금) | `LEASE_UNAVAILABLE` | 가능 |
| `PublishInProgressError` (Publish 잠금) | `LEASE_UNAVAILABLE` | 가능 |
| `LeaseOwnershipLostError` | `LEASE_OWNERSHIP_LOST` | 불가 |
| `TableLeaseOwnershipLostError` | `LEASE_OWNERSHIP_LOST` | 불가 |

즉 **어느 잠금인지가 아니라 무슨 일이 일어났는지로 가른다.**

### 근거

1. **재시도 판정이 무너진다.** `LeaseUnavailableError`는 지금 `LEASE_UNAVAILABLE`이고 `RETRYABLE_ERROR_TYPES`에 들어 있다. 이를 `SOURCE_MUTATION_CONFLICT`로 옮기면 재시도 가능하던 실패가 재시도 불가로 바뀐다. 잠금 종류로 가르는 어떤 배치도 "대기 후 재시도"와 "소유권 상실"을 한 코드 안에 같이 담게 되어 둘 중 하나는 반드시 틀린 재시도 답을 낸다. 014가 고치라고 한 결함이 바로 재시도 판정 불능이다. 그 결함을 다른 모양으로 되살릴 수는 없다.

2. **ADR-017이 이미 확정했다.** `classify_error(TableLeaseOwnershipLostError) == LEASE_OWNERSHIP_LOST`는 Accepted ADR의 결정이다. 뒤집으려면 ADR을 Supersede해야 하고, Task 13A에 그럴 근거가 없다.

3. **잠금 종류는 `error_type`이 옮길 정보가 아니다.** `pipeline_runs`에 잠금 종류 컬럼은 없고(`sql/metadata/004_create_ingestion_metadata.sql`), 두 경우의 조치는 같다 — 기다렸다 다시 돌린다. 어느 잠금이었는지는 `error_message`가 그대로 담는다. 저카디널리티 분류 컬럼은 기계가 읽는 구분(재시도 가부)을 담고, 사람이 읽는 맥락은 메시지에 둔다.

### 파급

- `tests/integration/test_orders_ingestion_service_integration.py:435`의 `("FAILED", "SOURCE_MUTATION_CONFLICT")` 기대를 `("FAILED", "LEASE_UNAVAILABLE")`로 갱신한다. 계획의 파일 목록 밖이지만 이 판정의 직접 결과이므로 범위에 포함한다.
- `SOURCE_MUTATION_CONFLICT`는 PRD v1.13 §18 목록에 있고 `LEASE_UNAVAILABLE`/`LEASE_OWNERSHIP_LOST`는 없다. 이 판정으로 구현이 PRD 어휘와 어긋난다. **ADR에 PRD §18 개정 요청을 명시한다** — 두 코드를 추가하고 `SOURCE_MUTATION_CONFLICT`를 대체됨으로 표시. PRD 개정 자체는 Task 13A에서 하지 않는다. 버전 업과 `PRD.bak/` 이동이 필요한 사용자 승인 사안이라 lead에 올린다.

## 질의 2 — 019 관측 부재 수정

**방향 승인. 단 이중 기록 방지 장치를 반드시 넣는다.**

### 판정

두 번째 `with`문 전체를 `try`로 감싸는 것은 맞다. 그러나 그 `with` 본문 안에는 이미 `record_started_run`(`service.py:279`)과 `_record_failure_without_masking`을 쓰는 안쪽 `try`가 있다. 바깥 `except`가 조건 없이 기록하면, 본문 안에서 실패한 Run은 같은 `run_id`로 `record_started_run`이 두 번 호출되고 FAILED 행도 두 번 기록된다. 둘 다 `suppress(Exception)` 안이라 **조용히** 어긋난다.

- `record_started_run` 직후에 `run_started = True`를 세우고, 바깥 `except`는 `run_started`가 False일 때만 기록한다.
- 바깥 기록도 `classify_error`를 거친다. `SOURCE_CONNECTION_ERROR`를 하드코딩하지 않는다. 이 지점에서는 Heartbeat 진입 실패나 설정 오류도 날 수 있고, 각자 자기 코드로 남아야 한다.
- `_record_lease_failure`는 이제 Lease 전용이 아니다. 두 호출처가 공유하는 `_record_prestart_failure`로 이름을 바꾸고 본문을 복사하지 않는다. 두 곳 모두 `extract_upper_bound=None`으로 `PipelineRun`을 만들고 `record_started_run` → `record_failed_run(classify_error(error))` 순서를 쓴다.

R-15 단언은 `run_count == 1`, `error_type == SOURCE_CONNECTION_ERROR`, `is_retryable == True`로 갱신하고, `docs/runbooks/r15-source-connection-failure.md`의 '알려진 한계' 줄을 수정 완료 상태로 바꾼다. 실측 표는 그대로 둔다.

## 질의하지 않았지만 Task 13A에서 정해야 하는 것

### Object Storage 분기의 코드 이름

**`OBJECT_STORAGE_ERROR`를 쓴다.** PRD v1.13 §18 Error Type 목록에 이미 있다. 014는 이 이름을 열어 뒀지만 PRD가 정해 놓고 있었다. 새로 짓지 않는다.

`src/ingestion/storage.py`는 `ClientError`를 감싸지 않고 그대로 올린다(412만 `FileExistsError`로 바꾼다). 따라서 `classify_error`는 `botocore.exceptions.ClientError`/`BotoCoreError`에 직접 분기한다.

### Object Storage 장애의 재시도 가부

014가 남긴 질문에 답한다. **Error Type은 하나로 두고, 재시도 가부만 HTTP 상태로 가른다.**

- `classify_error` → 항상 `OBJECT_STORAGE_ERROR`. PRD 어휘를 쪼개지 않는다.
- `is_retryable` → `OBJECT_STORAGE_ERROR`일 때 `error.response["ResponseMetadata"]["HTTPStatusCode"]`가 5xx 또는 429면 True, 그 외(4xx, 상태 없음)는 False. `.get` 연쇄로 방어한다.

이유: 503은 기다리면 낫고 `AccessDenied`는 몇 번을 다시 해도 같다. 이 차이를 코드 이름으로 나누면 PRD에 없는 코드가 또 늘고 AC-13 집계 축만 흐려진다. 재시도 가부는 원래 재시도 판정 함수가 답할 몫이다. `RETRYABLE_ERROR_TYPES` 집합만으로 재시도가 결정되지 않게 되므로 `is_retryable` Docstring과 ADR에 그 예외를 적는다.

R-02 증적(`data/reliability/r02/*.json`)에는 HTTP 상태가 없다. 이 규칙이 증적에 근거하도록 R-02 Test가 `http_status`를 기록하게 고친다. **기존 증적 JSON은 고치지 않는다.**

### 범위 밖으로 두는 것

`src/ingestion/storage.py`가 412에 올리는 `FileExistsError`는 지금 `UNKNOWN_ERROR`로 떨어진다. Object 중복은 Lease나 연결 장애와 다른 관심사다. Task 13A에서 건드리지 않고 여기 기록만 남긴다.
