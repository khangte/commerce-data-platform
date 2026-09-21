# 014. Ingestion 실패 경로가 Error Type 계약을 쓰지 않는다

> 판정: architect, 2026-09-20 (Phase 8 Task 5 검수 중 발견)
> 관련: PRD v1.13 §18 Observability와 오류, AC-13, [Phase 8 계획](../superpowers/plans/2026-09-20-phase8-reliability-scenarios.md)

## 발견

Phase 8 R-02(Upload Failure) 증적에서 `pipeline_runs.error_type`이 `ClientError`로 기록됐다. 코드를 확인한 결과 Ingestion 실패 경로는 `classify_error`를 호출하지 않는다.

| 경로 | 기록 값 | 근거 |
| ---- | ------- | ---- |
| `src/ingestion/service.py:631` `_record_failure_without_masking` | `type(error).__name__[:64]` | Python 예외 클래스 이름 |
| `src/ingestion/service.py:663` `_record_lease_failure` | Lease 계열은 `SOURCE_MUTATION_CONFLICT`, 그 외는 `type(error).__name__` | 혼합 |
| `src/ingestion/service.py:587` | `"BATCH_IDENTITY_CONFLICT"` | 계약 코드 |
| `src/warehouse/publish.py:141,164,199` | `classify_error(error)` | 계약 코드 |

`classify_error`의 호출자는 `src/warehouse/publish.py`와 두 Airflow DAG뿐이다. Ingestion Service는 호출하지 않는다.

## 영향

1. **AC-13 위반 위험.** `sql/validation/observability_run_status.sql`은 `error_type`을 그대로 집계한다. 한 컬럼에 계약 코드(`BATCH_IDENTITY_CONFLICT`)와 예외 클래스 이름(`ClientError`, `RuntimeError`, `OperationalError`)이 섞인다. 단일 SQL로 실행 상태를 분류한다는 계약이 어휘 수준에서 깨진다.
2. **재시도 판정 불능.** `is_retryable`은 `RETRYABLE_ERROR_TYPES = {SOURCE_CONNECTION_ERROR, LEASE_UNAVAILABLE}` 와 비교한다. 저장된 값이 `OperationalError`면 실제로는 재시도 가능한 연결 장애인데 재시도 대상으로 인식되지 않는다.
3. **S3 장애 전체가 미분류.** `classify_error`에 botocore `ClientError` 분기가 없어, 고치더라도 Object Storage 장애는 `UNKNOWN_ERROR`로 떨어진다. 503은 재시도 가능한데 비재시도로 보고된다.

## 판정

**Phase 8 범위 안에서 고친다. 단 지금은 아니다.**

- 지금 고치면 R-03(`RuntimeError`), R-15(`OperationalError`)의 관측 증거가 사라진 채로 수정하게 된다. Phase 8은 관측이 먼저다.
- R-02, R-03, R-15 증적이 모두 쌓인 뒤 한 번에 수정한다. 세 시나리오가 같은 결함의 세 얼굴이므로 한 건으로 묶는 편이 ADR 근거가 명확하다.
- 수정 범위는 두 곳으로 한정한다. `_record_failure_without_masking`과 `_record_lease_failure`가 `classify_error`를 쓰도록 바꾸고, `classify_error`에 Object Storage 장애 분기를 추가한다. 분기 추가 시 재시도 가능 여부를 HTTP 상태로 판단할지 예외 타입만으로 판단할지는 증적을 본 뒤 정한다.
- 기존 Test 영향은 작다. `error_type` 문자열을 단언하는 테스트는 모두 Warehouse Publish 경로(`DBT_TEST_ERROR`, `CONFIGURATION_ERROR`, `UNKNOWN_ERROR`)이고, 이 경로는 이미 `classify_error`를 쓰므로 바뀌지 않는다.

## 조치

| 대상 | 조치 |
| ---- | ---- |
| Phase 8 계획 | Task 5·6·9는 **현재 동작을 그대로 단언**한다. 계약 코드를 기대하지 않는다. |
| Phase 8 계획 | 수정 Task를 Task 13 뒤에 신설한다. R-02/R-03/R-15 증적을 근거로 분류를 교정하고 ADR을 남긴다. |
| 증적 | 주장 문자열을 증적에 넣지 않는다. 저장된 `error_type`과 `classify_error(error)` 결과를 각각 실제 값으로 기록한다. |

## Phase 8이 증명한 것

이 결함은 Phase 3~7의 어떤 테스트도 잡지 못했다. 정상 경로만 검증했기 때문이다. 의도적 실패 주입이 관측 계약의 구멍을 드러낸 첫 사례다.
