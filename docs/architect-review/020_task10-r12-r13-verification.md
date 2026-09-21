# 020. Task 10 (R-12 / R-13) 검수 — 승인, 명명 정정 1건

> 판정: architect, 2026-09-21 (Phase 8 Task 10 developer 구현 검수)
> 관련: [018 판정](018_r11-missing-schedule-premise.md), AC-07, R-12, R-13

## 판정

**Task 10을 승인한다.** 정정 1건과 보강 1건만 남았고 둘 다 문서 수준이다.

### R-12

| 계획 요구 | 구현 | 확인 |
| --------- | ---- | ---- |
| Source 재읽기 없는 Replay | `bronze_as_of` 경계 Build만 실행 | 통과 |
| Source Read 0건 | Replay Build 전후 `pipeline_runs` 행 수 불변 | 통과(Proxy, 아래) |
| 같은 범위 Full Refresh와 Hash 일치 (AC-07) | `mismatched == []` | 통과 |
| 비교 Helper 재사용 | `mart_logical_hashes`, `describe_mart_difference` | 통과 |

`mismatched`가 비지 않았을 때 `describe_mart_difference`로 차이를 출력하게 해 둔 점이 좋다. Hash 불일치는 원인을 모르면 디버깅이 불가능한 종류의 실패다.

**Source Read 0건은 직접 증거가 아니라 Proxy다.** `pipeline_runs` 행 수 불변은 "`ingest_table`이 호출되지 않았다"를 뜻하지 "Source Connection이 한 번도 열리지 않았다"를 뜻하지 않는다. dbt build는 Subprocess에서 돌아 Monkeypatch가 닿지 않으므로 이보다 직접적인 증거를 싸게 얻을 방법이 없다. Runbook이 이 구조를 정확히 적었다 — "Replay는 `dbt build`만 실행하고 Source Connection을 열지 않으므로 Source Read는 항상 0건이다 — 이 Test가 `pipeline_runs` 행 수 불변으로 그 사실을 증거로 남긴다." Proxy임을 숨기지 않았으므로 추가 조치는 없다.

### R-13

| 계획 요구 | 구현 | 확인 |
| --------- | ---- | ---- |
| 되감기 뒤 명시 재수집 | `rewind_tables` + 새 `sequence` `ingest_table` | 통과 |
| 새 식별자 기록 | `re_extract_run.run.batch_id != original_run.run.batch_id` | 통과 |
| 기존 Commit Object 무손상 | `original_object_after == original_object_before` (4개 칼럼 전체 비교) | 통과 |
| Replay와 Hash 동일성을 약속하지 않는다는 한계 명시 | Runbook '알려진 한계' 절 | 통과 |

원본 Object를 `object_key`/`row_count`/`logical_hash`/`status` 네 칼럼 Tuple로 통째 비교한 것이 옳다. 개별 칼럼을 하나씩 단언했으면 나중에 칼럼이 늘 때 조용히 검사 범위 밖으로 빠진다.

되감기 결과가 `(None, [])`인 이유를 주석으로 남긴 점, 018의 부수 발견(과거 창 단독 재수집 수단 없음)을 Runbook 본문에 연결한 점도 확인했다.

## 정정 1건 — Test 이름이 없는 칼럼을 가리킨다

`test_r13_re_extract_uses_a_new_reprocess_id`의 `reprocess_id`는 이 시스템에 없는 식별자다. developer가 Runbook에 정확히 적었다 — "`bronze_objects` 스키마에는 `reprocess_id` 칼럼이 없다. 새 식별자는 새 `batch_id`/`dag_id`로 남는다(`docs/architecture/08-bronze-replay-and-reextract-boundary.md` D-5)." Docstring도 `batch_id`라고 쓴다. **이름만 틀렸다.**

원인은 내 계획이다. 계획 Task 10의 Interfaces에 그 이름을 적었고, `docs/phases/phase-08-reliability.md:83`의 R-13 행도 "명시 범위를 새 `reprocess_id`로 추출"이라고 쓴다. Phase 문서의 표현을 확인 없이 Test 이름으로 옮겼다.

`reprocess_id` 칼럼을 새로 만들지 않는다. `batch_id`가 이미 재수집마다 다른 식별자를 주고 Object Key를 가른다. 쓰이지 않는 식별자를 하나 더 만드는 것은 투기적 일반화다.

**조치**: Test 이름을 `test_r13_re_extract_records_a_new_batch_id_and_keeps_prior_objects`로 바꾼다. 계획 Task 10은 내가 갱신했다. `docs/phases/phase-08-reliability.md:83`의 R-13 행도 `batch_id`로 고치고, 같은 문서 25번째 줄의 "필요 시 `reprocess_id`"도 함께 정리한다.

## 보강 1건 — R-11이 이미 증명한 것을 가리켜 둔다

R-13은 같은 Source Row를 서로 다른 `batch_id`로 두 번 Bronze에 올린다. 여기서 당연히 물어야 할 것은 "그래서 Mart가 이중 계상되지 않는가"다. R-13 Test는 그것을 단언하지 않는다.

**추가 Test를 요구하지 않는다.** R-11의 회복 단계가 이미 같은 성질을 증명했다. `recovery_run`이 B·C 구간을 새 `batch_id`로 다시 올린 뒤 `post_recovery_hashes == control_hashes`가 성립했다. 중복 Bronze Object가 Mart를 바꾸지 않는다는 증거가 이미 있고, 같은 것을 다시 증명하려고 dbt Build를 한 번 더 돌리는 것은 비싸다.

**조치**: `docs/runbooks/r13-re-extract.md`에 한 줄 추가한다 — "같은 Row가 서로 다른 `batch_id`로 두 번 Bronze에 올라도 Mart는 바뀌지 않는다. Staging이 `_ingested_at`/`_batch_id` 순서로 중복을 제거하기 때문이고, 그 증거는 R-11 회복 단계의 `post_recovery_hashes == control_hashes`다." 근거가 다른 Runbook에 있다는 사실을 적어 두지 않으면 다음 사람이 같은 질문에서 멈춘다.

## Task 9 지적 반영

`docs/runbooks/r15-source-connection-failure.md:34`에 '알려진 한계' 줄이 들어갔다. 확인했다.
