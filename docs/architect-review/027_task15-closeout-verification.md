# 027. Task 15 (Phase 8 마감) 검수 — 승인, 보강 3건

> 판정: architect, 2026-09-21 (Phase 8 Task 15 developer 구현 검수)
> 관련: [018](018_r11-missing-schedule-premise.md), [019](019_task9-r15-r11-verification.md), [023](023_task13-r14-verification.md), [025](025_task13a-verification.md)

## 판정

**Task 15를 승인한다. 보강 3건 뒤 Phase 8을 닫는다.**

### 계획 요구 대조

| 계획 요구 | 구현 | 확인 |
| --------- | ---- | ---- |
| 전체 스위트 실행과 Count 기록 | `ruff` clean, `pytest -q` 185 passed/98 skipped, `tests/reliability` 18 passed, `verify_clean_clone.sh` 75 passed/1 skipped exit 0 | 통과 |
| `P8-01`~`P8-20` 체크 + 증거 라인(명령·결과·식별자) | 전 항목 `[x]`, 각 항목에 명령·결과·식별자 | 통과 |
| R-11 정의를 두 문서에서 갱신 | `phase-08-reliability.md`, `ROADMAP.md:1017` | 통과 |
| Definition of Done 6항목 | 전부 실측 근거 | 통과 |
| Portfolio Evidence 6항목 | 전부 증적 경로 | 통과 |
| `docs/` 미커밋 유지 | 확인 | 통과 |

`pytest -q`의 185 passed/98 skipped와 Flag를 켠 278 passed/5 skipped가 서로 어긋나지 않는다. 98개 Skip이 Integration Flag로 가려진 몫이고 그 차이가 정확히 맞는다. 두 숫자를 함께 적은 것이 맞다.

증거 라인에 `pipeline_name`·`run_id`·`publish_run_id` 같은 식별자를 넣어 증적 JSON과 대조 가능하게 한 점이 좋다. "PASSED"만 적힌 체크는 나중에 아무것도 되살리지 못한다.

## 보강 1 — 체크되지 않은 항목 10개가 남아 있다

`docs/phases/phase-08-reliability.md`의 두 절이 통째로 `[ ]`다.

- `## 시나리오별 문서화` 4항목 (`:119-122`)
- `## 시나리오 완료 기준` 6항목 (`:128-133`)

열 개 모두 이미 충족됐다. Runbook은 R-01~R-15가 `docs/runbooks/`에 있고, Troubleshooting은 세 문서(`README.md`, `ingestion-errors.md`, `commit-and-lease.md`)가 있고, 재검증은 `tests/reliability` 18개가 맡고, PRD/AC/ADR·Evidence 링크는 각 Runbook과 색인에 있다.

Phase 문서는 그 Phase가 끝났는지를 선언하는 문서다. 충족된 항목을 `[ ]`로 두면 문서가 "Phase 8 미완"이라고 말한다.

**조치**: 열 항목을 모두 체크하고 각각 한 줄 근거를 붙인다. `## 시나리오 완료 기준`의 여섯 항목은 개별 `R-*`가 아니라 R-01~R-15 **전체**에 대해 성립한다는 뜻이므로, 근거 줄에 그 범위를 명시한다.

## 보강 2 — 변경 요약 표가 착수 시점 상태로 남아 있다

`## 파일·폴더별 변경 요약`의 두 행이 지금 상태와 다르다.

- `docs/phases/phase-08-reliability.md` 행: "용어를 한국어 중심으로 정리하고…"만 적혀 있다. 이 문서는 이제 `P8-01`~`P8-20`의 증거 라인과 Definition of Done·Portfolio Evidence를 담는다.
- `docs/runbooks/`, `docs/troubleshooting/` 행: "색인, 6단계 Runbook 템플릿, 오류 코드 역색인 **초안**"이다. 초안이 아니라 R-01~R-15 Runbook 15편과 Troubleshooting 3편이 완성돼 있다.

**조치**: 두 행을 최종 상태로 고친다. `(미커밋)` 표기는 유지한다 — `docs/architect-review/` 외 `docs/` 변경을 커밋하지 않는 것은 의도된 상태다.

## 보강 3 — Clean Clone 근거 한 줄이 실측과 추론을 섞는다

Definition of Done의 "모든 Runbook이 새 Clone에서도 실행 가능한 명령을 제공한다" 항목은 `verify_clean_clone.sh` 결과를 든 뒤 "각 Runbook의 재현/재검증 명령은 같은 방식으로 새 Clone에서 그대로 실행된다"로 잇는다.

앞은 측정이고 뒤는 추론이다. 그 Script는 Seed→Generator→Ingestion→Publish→Integration을 돌리지 Reliability Suite를 돌리지 않는다. Phase 8 내내 지켜온 선이 증적과 설명을 섞지 않는 것이었다. 마지막 문서에서 그 선을 넘으면 앞의 판정들이 함께 약해진다.

**조치**: 추론 부분을 그대로 두되 추론임을 밝힌다 — Script가 검증한 범위를 적고, Reliability Suite 자체는 Clean Clone 안에서 실행하지 않았으며 각 Runbook 명령이 절대 경로 없이 동일한 `uv run pytest` 형식이라는 **구조적 근거**로 판단했다고 쓴다. 측정으로 바꾸고 싶으면 Clean Clone 안에서 `tests/reliability`를 한 번 돌려 그 Count를 적으면 되지만, 마감을 위해 필수로 요구하지는 않는다.

## 재검수 (2026-09-21)

보강 3건 반영 확인. Task 15를 닫고 Phase 8 설계 검수를 종료한다.

| 항목 | 확인 |
| ---- | ---- |
| 미체크 항목 해소 | `docs/phases/phase-08-reliability.md`에 `[ ]` 0개. `:119-122`·`:128-133` 전부 근거 한 줄과 함께 체크, 완료 기준은 R-01~R-15 전체 기준임을 명시 |
| 변경 요약 표 최종 상태 | '초안'·'용어 정리' 서술 소멸, `(미커밋)` 유지 |
| Clean Clone 근거 분리 | `:190`이 측정(Script 통과 범위, `tests/reliability` 미포함)과 추론(절대 경로 없는 동일 명령 형식)을 명시적으로 나눔 |

## Phase 8 설계 검수 총괄

판정 014~027, 14건. 그중 계약 결함 셋을 관측으로 찾아 고쳤다.

| 결함 | 발견 | 수정 | 근거 |
| ---- | ---- | ---- | ---- |
| Ingestion 실패 경로가 Error Type 계약을 쓰지 않음 | 014(R-02 증적) | Task 13A | ADR-018 |
| `commit_table_run`에 Lease Fencing 부재 | 016(R-05 실측) | Task 7A | ADR-017 |
| Source 연결 실패가 `pipeline_runs`에 흔적을 남기지 않음 | 019(R-15 실측) | Task 13A | ADR-018 |

세 결함 모두 Phase 3~7의 정상 경로 Test가 잡지 못한 것이다. 의도적 실패 주입이 아니면 드러나지 않았다.

계획 자체의 오류도 넷 고쳤다 — R-07의 도달 불가능한 기대 예외(015), R-11의 재현 불가능한 전제(018), 존재하지 않는 `reprocess_id` 식별자(020·026), `dim_subscription_*` Singular Test 누락(022).

Phase 8 내내 지킨 선 하나가 일관되게 적용됐다: **증적으로 보인 것과 문서로 설명한 것을 섞지 않는다.** 마지막 마감 문서의 Clean Clone 항목까지 그 선으로 정리하고 끝냈다.
