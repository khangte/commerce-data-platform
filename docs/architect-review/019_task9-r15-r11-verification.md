# 019. Task 9 (R-15 / R-11) 검수 — 승인, 그리고 AC-13 사각 하나

> 판정: architect, 2026-09-21 (Phase 8 Task 9 developer 구현 검수)
> 관련: [018 판정](018_r11-missing-schedule-premise.md), [014 판정](014_ingestion-error-type-vocabulary.md), AC-13

## 판정

**Task 9를 승인한다. 재작업 없다.** R-11에서 부수 발견 1건이 나왔고, 이미 있는 Task 13A에 흡수한다.

### R-11 — 018 반영 확인

| 018 요구 | 구현 | 확인 |
| -------- | ---- | ---- |
| `_set_watermark`로 Gap 위조 금지 | B/C 사이·회복 구간에 없음. `_set_fixture_watermark`는 부트스트랩 1회(`:198`) | 통과 |
| Gap 없음 단언 | `run_c.row_count == 2` — C가 B까지 쓸어 담는다 | 통과 |
| `watermark_after`가 Source 최대 Cursor | `watermark_after_c == (expected_after_c.timestamp, ...)` | 통과 |
| **회복 전** Mart Hash == Control | `pre_recovery_hashes == control_hashes` | 통과 |
| 귀속 이동 관측 | B Row가 `ingestion_date={C의 date}` / `batch_id={C의 batch_id}` 아래 | 통과 |
| 회복 재수집 범위를 좁히지 않음 | `recovery_run.row_count == 2`를 정상으로 단언 | 통과 |
| 회복 후 멱등 + Partition 이동 | `post_recovery_hashes == control_hashes`, `ingestion_date={B의 date}` | 통과 |

Runbook도 "누락 Schedule은 데이터 손실을 만들지 않는다"로 시작하고 근거(연속 Watermark, Source 최대 Upper Bound)를 먼저 쓴 뒤 귀속 이동을 설명한다. 요구한 순서다.

주석이 단언 옆에 붙어 있어 나중에 읽는 사람이 `row_count == 2`가 왜 Gap 부재의 증거인지 다시 추론하지 않아도 된다. 이 Test는 수치가 아니라 성질을 지키는 Test이므로 그 주석이 본문이다.

### R-15

Object 0건, Watermark 불변, `SOURCE_CONNECTION_ERROR` 분류, `is_retryable` 참, 같은 `batch_id` 즉시 재시도 성공까지 계획대로다. Runbook이 원인을 코드 위치로 짚고 불변 조건("Object가 없으면 Watermark도 움직이지 않는다")을 명시한 점이 좋다.

## 부수 발견 — Source 연결 실패는 Metadata에 흔적을 남기지 않는다

`pipeline_run_count_after_failure == 0`은 **정확한 관측**이고 Test는 옳다. 그러나 그 관측이 드러내는 것이 있다.

`src/ingestion/service.py`에서 `open_table_snapshot`은 `with` 진입 시점에 Source 연결을 열고, 이는 `record_started_run`보다 앞선다. 따라서 연결 자체가 실패하면 `pipeline_runs` 행이 아예 생기지 않는다.

반면 같은 함수의 Lease 획득 실패는 `_record_lease_failure`가 `record_started_run` + `record_failed_run`으로 **FAILED 행을 남긴다.**

| 실패 지점 | `pipeline_runs` 행 | 관측 가능 |
| --------- | ------------------ | --------- |
| Lease 획득 실패 | FAILED 기록됨 | 가능 |
| Source 연결 실패 | 행 없음 | **불가능** |
| Upload·Commit 실패 | FAILED 기록됨 | 가능 |

`sql/validation/observability_run_status.sql`은 `pipeline_runs`를 집계한다. Source 연결 실패는 그 집계에 **나타나지 않는다.** Airflow Task는 실패했는데 Metadata Store는 조용하다. 운영자가 실행 상태를 SQL 하나로 분류한다는 AC-13의 전제가, 이 경로에서는 오분류가 아니라 **부재**로 깨진다. 014가 찾은 것이 잘못된 이름이었다면 이것은 이름 자체가 없는 경우다.

재시도 판정도 근거를 잃는다. `is_retryable`은 저장된 `error_type`을 보는데 저장된 것이 없다. Test가 `classify_error(failure.value)`를 예외 객체에서 직접 계산한 이유가 그것이다 — DB에서 읽어올 수 없었기 때문이다.

### 판정

**Task 9를 되돌리지 않는다. Task 13A 범위에 넣는다.**

- 014와 같은 결함군이다. 별도 Task와 별도 ADR로 쪼개면 같은 판단을 두 번 하게 된다.
- 수정 방향: Lease 획득 성공 직후, `open_table_snapshot` 진입 **전에** `record_started_run`을 옮기는 것이 자연스럽다. 다만 `PipelineRun`이 `snapshot.extract_upper_bound`를 필요로 하므로 그대로는 옮길 수 없다. `extract_upper_bound=None`으로 먼저 기록하고 Snapshot을 연 뒤 갱신할지, 아니면 `_record_lease_failure`처럼 연결 실패 전용 기록 경로를 둘지는 **R-02·R-03·R-15 증적을 함께 보고 Task 13A에서 정한다.** 지금 정하지 않는다.
- R-15 Test의 `run_count == 0` 단언은 Task 13A에서 갱신한다. 지금은 그대로 둔다. 수정 전 동작의 증거다.
- Runbook `docs/runbooks/r15-source-connection-failure.md`에 "알려진 한계: 이 실패는 `pipeline_runs`에 남지 않아 AC-13 집계에서 보이지 않는다. Task 13A에서 교정한다"를 한 줄 추가한다. 실측 표는 건드리지 않는다.

## Phase 8이 증명한 것

세 번째다. 014(오분류), 016(Fencing 구멍), 019(관측 부재). 세 건 모두 정상 경로 Test가 통과하는 상태에서 숨어 있었고, 셋 다 의도적 실패 주입으로만 드러났다. 016은 이미 닫혔고 나머지 둘은 Task 13A로 모인다.
