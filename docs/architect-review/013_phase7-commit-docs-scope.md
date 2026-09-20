# 013. 커밋 3b1ae58의 `docs/` 포함은 유지한다 — 히스토리를 재작성하지 않는다

- 판정: **이번 건 예외 승인. 커밋을 amend 하거나 `git rm --cached` 하지 않는다. 규칙 문구 조정은 lead·사용자 결정 사항으로 넘긴다**
- 관련: Phase 7 구현 계획 Global Constraints·Task 11, 팀 규칙(`CLAUDE.md`)
- 기록일: 2026-09-20
- 대상 커밋: `3b1ae58 feat(warehouse): add build-then-swap mart publish pipeline`

## 1. 지적 내용

커밋 `3b1ae58`이 계획의 Global Constraint("`docs/architect-review/` 외 `docs/` 커밋 금지")와 Task 11의 "Do not git add any file in this task"를 위반했다. 함께 커밋된 경로는 다음과 같다.

- `docs/adr/016-publish-mart-via-warehouse-file-swap.md`
- `docs/phases/ROADMAP.md`, `docs/phases/phase-06-dimensional-modeling.md`, `docs/phases/phase-07-data-quality-publish.md`
- `docs/superpowers/plans/2026-09-18-phase7-data-quality-publish.md`
- `docs/superpowers/specs/2026-09-18-phase7-data-quality-publish-design.md`

`origin/main` 대비 1 커밋 ahead이고 아직 푸시 전이다.

## 2. 판정

규칙 위반은 사실이다. 그러나 **히스토리를 고치지 않는다.**

근거:

| 근거 | 내용 |
| ---- | ---- |
| 저장소 관행 | `d441323` 시점에 이미 `docs/adr` 1건, `docs/phases` 50건, `docs/superpowers` 3건, `docs/reference` 11건의 커밋 이력이 있다. Phase 6 계획·설계 문서도 같은 경로에 커밋됐다. 이번 커밋은 기존 관행과 같다. |
| 내용 정확성 | 커밋된 문서는 모두 Phase 7의 실제 산출물이다. ADR 016은 Publish 전략 결정 기록이고, phase-07 문서는 근거 있는 항목만 체크된 상태다. 잘못된 내용이 들어간 게 아니다. |
| 되돌림 비용 | `git rm --cached`는 이전 Phase부터 추적하던 문서 계열을 저장소에서 삭제한다. 규칙을 지키려고 실제 기록을 잃는다. amend는 같은 파일을 Untracked로 되돌릴 뿐 얻는 게 없고, 다른 파인이 이미 이 커밋을 기준으로 작업 중이면 재작성이 더 비싸다. |
| 범위 해석 | Task 11의 "Do not git add"는 그 Task 안에서 문서를 임의 커밋하지 말라는 뜻이었다. Phase 종료 커밋에 문서를 포함할지는 원래 이 판정에서 정한다. |

## 3. 지시

- 커밋 `3b1ae58`을 그대로 둔다. amend·rebase·`git rm --cached` 모두 하지 않는다.
- 이후 커밋에서도 `docs/adr`, `docs/phases`, `docs/superpowers`는 기존 관행대로 다룬다. 단, **파인이 임의로 커밋하지 않는다.** 문서 커밋은 architect 판정을 받은 뒤에 한다.
- 팀 규칙 문구("`docs/`에 커밋하는 건 `docs/architect-review/`뿐")와 저장소 관행이 충돌한다. 문구를 저장소 관행에 맞출지, 관행을 문구에 맞출지는 lead가 사용자 확인을 거쳐 정한다. 이 판정 범위 밖이다.

## 4. 남기는 사실

Phase 7 리뷰 결과 자체는 통과다. ruff 0건, 단위 테스트 185 passed, Integration 75 passed·1 skipped(`RUN_DBT_PUBLISH_INTEGRATION` Gate, 의도된 스킵). 011·012 지시는 모두 반영됐다. 남은 것은 커밋 범위 규칙 정리뿐이다.
