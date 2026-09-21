# 025. Task 13A 검수 — 승인, 보강 3건

> 판정: architect, 2026-09-21 (Phase 8 Task 13A developer 구현 검수)
> 관련: [014](014_ingestion-error-type-vocabulary.md), [019](019_task9-r15-r11-verification.md), [024](024_task13a-error-vocabulary-rulings.md), ADR-018

## 판정

**Task 13A를 승인한다. 보강 3건 뒤 닫는다.**

### 판정 024 대조

| 지시 | 구현 | 확인 |
| ---- | ---- | ---- |
| `classify_error` Lease 분기 유지, `_record_lease_failure`만 수정 | 분기 5종 그대로, 리터럴 제거 | 통과 |
| `_record_prestart_failure`로 이름 변경, 두 호출처 공유 | `service.py:250`, `:340` | 통과 |
| `run_started` 이중 기록 방지 | `:259` 초기화, `:278` 설정, `:339` 검사 | 통과 |
| 바깥 기록도 `classify_error` 경유(하드코딩 금지) | `:671` | 통과 |
| `test_orders_ingestion_service_integration.py:435` 갱신 | `LEASE_UNAVAILABLE` | 통과 |
| Object Storage 코드명 `OBJECT_STORAGE_ERROR` | `errors.py:34`, 분기 추가 | 통과 |
| 재시도는 HTTP 상태로(5xx/429), `.get` 연쇄 방어 | `_object_storage_http_status`, `is_retryable` | 통과 |
| `is_retryable` Docstring·ADR에 예외 명시 | 양쪽 모두 | 통과 |
| R-02 Test가 `http_status` 기록, 기존 증적 불변 | `test_r01_r07:321,331` | 통과 |
| R-15 `run_count == 1`, `SOURCE_CONNECTION_ERROR` | `test_r11_r15:128,129` | 통과 |
| PRD 본문 미수정, ADR에 개정 요청만 | 확인 | 통과 |

`run_started` 위치가 정확하다. `record_started_run` **직후**에 세워야 그 호출 자체가 실패했을 때 바깥 경로가 기록을 맡는다. 한 줄 아래였다면 그 경우가 새는 구멍이 됐다.

`FileExistsError` 기대값을 `UNKNOWN_ERROR`로 함께 갱신한 판단도 맞다. 024가 범위 밖으로 둔 것은 **전용 코드를 만들지 말라**는 뜻이지, 라우팅 변경의 결과를 덮어두라는 뜻이 아니었다. 기존 기대값이 예외 클래스 이름이었으므로 갱신이 불가피하다.

`ingestion-errors.md`가 각 Error Type에 발생 지점과 기록 함수까지 적은 점이 좋다. 장애 대응자가 `error_type` 하나로 코드 경로를 역추적할 수 있다.

## 보강 1 — ADR Validation 행의 숫자가 어긋난다

해당 행이 같은 선택에 대해 `22 passed`와 `23 passed` 둘을 적었다. 같은 파일 3개를 고른 실행의 총계가 두 번 달라질 수는 없다. 둘 중 하나가 오기이거나 선택이 달랐다.

ADR Validation은 나중에 이 결정을 검증할 사람이 그대로 재현하는 줄이다. 숫자가 맞지 않으면 재현이 실패했을 때 무엇이 틀렸는지 판별할 수 없다. 실제 최종 실행의 총계 하나로 고치고, 최초 실패 1건은 "기대값 갱신 전 1건 실패"로 총계 없이 쓴다.

## 보강 2 — `select distinct error_type` 확인은 아무것도 증명하지 못한다

계획 Task 13A의 마지막 Verify 항목이 `psql`로 `pipeline_runs`의 `error_type` 값을 모두 확인하라는 것이었다. 직접 조회했다.

```
select coalesce(error_type,'(null)'), count(*) from pipeline_runs group by 1;
→ 0 rows
```

Reliability·Integration Test가 끝에 자기 행을 지우므로 테이블이 비어 있다. "잔여 행 없음 확인"은 사실이지만 **계약 위반 값이 없다는 증거가 아니다.** 빈 테이블은 어떤 어휘 주장도 지지하지 않는다.

실제로 증명할 수 있는 것은 정적 불변 조건이다. 내가 확인했다.

```
grep -rn "error_type=" --include=*.py src/ | grep -v classify_error
→ src/warehouse/publish.py:258 (상위에서 classify_error 결과를 전달)
  src/warehouse/publish_metadata.py:213 (행 읽기)
  src/warehouse/dbt_runner.py:90 (classify_dbt_failure)
  src/ingestion/service.py:593 (계약 코드 리터럴, 보강 3)
```

`src/` 어디에도 `classify_error`를 우회해 임의 문자열을 쓰는 경로가 없다. **조치**: ADR Validation의 해당 행을 이 정적 확인으로 바꾸고, 빈 테이블 조회 결과를 근거로 제시하지 않는다. 계획 Verify 항목도 같은 내용으로 고친다(내가 갱신했다).

## 보강 3 — 어휘 정의 지점을 하나로 둔다

`src/ingestion/service.py:593`이 `error_type="BATCH_IDENTITY_CONFLICT"` 리터럴을 쓴다. 값은 맞지만 같은 코드가 `errors.py:31`에 상수로도 있다. 두 곳에 같은 문자열이 있으면 한쪽 오타를 아무도 잡지 못한다 — `pipeline_runs`에 조용히 잘못된 값이 들어가고 AC-13 집계에서만 드러난다.

**조치**: `src.ingestion.errors`에서 `BATCH_IDENTITY_CONFLICT`를 Import해 쓴다. Error Type 문자열의 정의 지점은 `errors.py` 하나뿐이어야 한다.

## 재검수 (2026-09-21)

보강 3건 반영 확인. Task 13A를 닫는다.

| 항목 | 확인 |
| ---- | ---- |
| ADR Validation 숫자 정리 | `23 passed`(기대값 갱신 전 1건 실패) 한 줄 |
| 정적 검증 행으로 교체 | `grep -rn 'error_type=' … | grep -v classify_error` 4곳 |
| 어휘 정의 지점 단일화 | `src/ingestion/service.py:34` Import, `:593` 상수 사용 |

판정 014와 019가 지적한 두 결함이 모두 닫혔다. Phase 8이 드러낸 계약 결함 셋(014 오류 분류, 016 Lease Fencing, 019 관측 부재)이 전부 수정·ADR화됐다 — 016은 ADR-017, 014·019는 ADR-018이다.

남은 것은 Task 14(Troubleshooting 색인 완성)와 Task 15(전체 스위트 마감), 그리고 사용자 승인 대기 중인 PRD §18 개정이다.
