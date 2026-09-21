# 023. Task 13 (R-14) 검수 — 승인, 보강 3건

> 판정: architect, 2026-09-21 (Phase 8 Task 13 developer 구현 검수)
> 관련: AC-19, R-14, `tests/integration/test_publish_gate_dbt_integration.py`

## 판정

**R-14를 승인한다. 보강 3건 뒤 Task 13을 닫는다.**

### 계획 요구 대조

| 계획 요구 | 구현 | 확인 |
| --------- | ---- | ---- |
| 기존 dbt Canary 실패 경로를 재사용 | `CANARY_RUNNER`를 Integration Test 모듈에서 Import | 통과 |
| Published Hash·Row Count가 실패 전후 동일 | `mart_logical_hashes`/`mart_row_counts` 대조 | 통과 |
| 실패 Build가 `failed/`에 격리 | `paths.failed_file(failing.publish_run_id).is_file()` | 통과 |
| Bronze·Watermark 불변 | 두 Table 전체 Row의 MD5 요약 전후 대조 | 통과 |
| Run Row가 `FAILED` + `DBT_TEST_ERROR` | `(failed_record.status, failed_record.error_type)` | 통과 |
| 재실행이 직전 성공 Run에 연결 | `second.previous_publish_run_id == first.publish_run_id` | 부분 — 아래 보강 1 |
| `warehouse-publish.md`가 5개 코드/예외를 매핑 | 표 5행 + 상태 전이 요약 | 통과 |

Bronze와 Watermark를 개별 Row가 아니라 Table 전체의 정렬된 MD5 요약으로 잡은 선택이 맞다. 특정 Row만 비교하면 Publish 경로가 **다른** Row를 건드린 경우를 놓친다. 요약 Hash는 "아무것도 안 건드렸다"를 통째로 증명한다.

Runbook의 '원인과 불변 조건' 절이 `publish_build`의 `os.replace` 한 번만이 Published 파일을 바꾼다는 구조적 근거까지 적은 점이 좋다. 관측 결과가 아니라 왜 그럴 수밖에 없는지를 남겨야 회귀를 알아본다.

## 보강 1 — 회복 Publish를 결과 객체가 아니라 실제 상태로 확인한다

현재 `second`에 대한 단언은 셋 다 `publish_warehouse`가 돌려준 결과 객체만 본다. `PUBLISHED` 상태는 이 Test 어디에서도 단언하지 않고, 회복 이후의 Published 파일도 열어보지 않는다.

첫 Publish에는 `mart_logical_hashes(paths.published) == first.mart_hashes`로 디스크를 확인해 놓고, 정작 **실패 뒤 회복된** Publish에는 같은 확인이 없다. R-14가 증명해야 할 마지막 한 걸음이 바로 그 Swap이 실제로 안착했다는 것이다.

**조치**: `second` 뒤에 두 줄을 추가한다.

- `mart_logical_hashes(paths.published) == first.mart_hashes` (Swap 안착 확인)
- `get_publish_run(settings, second.publish_run_id).status == PUBLISHED`

증적에도 `second_publish_status`와 `published_mart_hash_after_recovery`를 넣는다. Runbook 기대/실제 표에도 해당 행을 추가한다.

## 보강 2 — Sibling Test와의 관계를 Test 안에 남긴다

`tests/integration/test_publish_gate_dbt_integration.py::test_dbt_test_failure_keeps_the_published_warehouse`가 이 Test와 거의 같다. 첫 Publish, Canary 실패, `error_type`, Published Hash 불변, `FAILED` Record, `failed_file`, 재Publish 연결까지 단언이 겹친다.

겹침 자체는 기각하지 않는다. R-14 Test에만 있는 것이 Bronze·Watermark 불변과 증적이고, 그 둘을 얻으려면 어차피 같은 3회 Build가 필요하다. 단언을 지워도 Build 시간은 줄지 않는다.

문제는 **Drift**다. 한쪽 계약이 바뀔 때 다른 쪽이 따라가지 않으면, 둘 중 어느 쪽이 현재 계약인지 알 수 없게 된다.

**조치**: `test_r14_a_failed_build_holds_bronze_watermark_and_the_published_mart`의 Docstring에 한 줄을 더한다 — 이 Test가 위 Integration Test의 Reliability 확장이고, 차이는 Bronze/Watermark 불변과 증적뿐이며, 한쪽을 고치면 다른 쪽도 같이 본다는 사실. `_run`을 Private Name으로 Import한 결합도 이 한 줄이 설명해 준다.

## 보강 3 — 색인의 시나리오 열은 실측한 것만 R-14로 적는다

`docs/troubleshooting/README.md`가 다섯 행을 모두 시나리오 `R-14`로 적었다. 그중 R-14 Test가 실제로 관측한 것은 `DBT_TEST_ERROR` 하나뿐이다. `DBT_BUILD_ERROR`, `UNKNOWN_ERROR`(Abandoned Run), `PublishInProgressError`, `PublishedWalError`는 코드를 읽고 쓴 문서 근거이지 증적이 아니다.

Phase 8 내내 지켜온 선이 "증적으로 보인 것과 문서로 설명한 것을 섞지 않는다"였다. 색인은 나중에 장애 대응자가 제일 먼저 보는 표라 이 구분이 흐려지면 곤란하다.

**조치**: 실측하지 않은 네 행의 시나리오 열을 `R-14`에서 `—(문서 근거)`로 바꾼다. 원인·조치·문서 열은 그대로 둔다. `warehouse-publish.md` 본문은 손대지 않는다 — 그쪽은 애초에 문서 매핑이 목적이다.

## 재검수 (2026-09-21)

보강 3건 반영 확인. Task 13을 닫는다.

| 항목 | 확인 |
| ---- | ---- |
| 회복 Publish 상태·디스크 확인 | `tests/reliability/test_r14_dbt_failure.py:115-117`, 증적 `second_publish_status`/`published_mart_hash_after_recovery`, Runbook 표 28행 |
| Sibling Test 관계 Docstring | 같은 파일 `:69-71` |
| 색인 시나리오 열 정정 | `docs/troubleshooting/README.md:14-17` 네 행 `—(문서 근거)` |

R-14 종결. Phase 8 남은 항목은 Task 13A(판정 014·019 결함 수정), Task 14(Troubleshooting 색인 완성), Task 15(전체 스위트 마감)다. Task 13A는 R-02·R-03·R-15 증적이 모두 확보된 지금 착수 조건을 만족한다.
