# 026. Task 14 (Troubleshooting 색인) 검수 — 승인, 보강 4건

> 판정: architect, 2026-09-21 (Phase 8 Task 14 developer 구현 검수)
> 관련: [015](015_r07-schema-version-variant.md), [020](020_task10-r12-r13-verification.md), [024](024_task13a-error-vocabulary-rulings.md), ADR-017, ADR-018

## 판정

**Task 14를 승인한다. 보강 4건 뒤 닫는다.**

### 계획 요구 대조

| 계획 요구 | 구현 | 확인 |
| --------- | ---- | ---- |
| `ingestion-errors.md` | Task 13A에서 작성 | 통과 |
| `commit-and-lease.md` — CAS 충돌·만료 Lease·Orphan·깨진 Manifest | 4개 항목 표 + ADR-017 창 설명 | 통과 |
| Runbook 색인에 증적 식별자 | R-01~R-15 전 행 | 통과 |
| `README.md`에 Reliability Suite 실행 절 | `README.md:98-111` | 통과 |
| 모든 Runbook 링크가 해석됨 | 아래 전수 검사 | 통과 |
| 절대 경로·Credential·원시 Payload 없음 | 아래 검사 | 통과 |

직접 확인했다.

```
docs/runbooks/*.md, docs/troubleshooting/*.md, README.md 의 상대 링크 전수 해석 → BROKEN: none
grep -rniE "password|secret|/home/|AKIA" docs/runbooks/ docs/troubleshooting/ → 결과 없음
```

`r02-upload-failure.md` 누락을 스스로 찾아 채운 것이 맞다. 색인이 가리키는 문서가 없으면 색인 자체가 거짓말이 된다.

`commit-and-lease.md`가 ADR-017의 Fencing 창을 '알려진 위험이었던 …(수정 완료)'로 남긴 구성이 좋다. 닫힌 결함도 왜 존재했는지가 남아야 같은 모양의 회귀를 알아본다.

## 보강 1 — `reprocess_id`는 존재하지 않는다 (판정 020 재발)

`docs/runbooks/README.md`가 기록 대상 식별자로 `reprocess_id`를 열거했다. 그런 컬럼도 식별자도 없다. 판정 [020](020_task10-r12-r13-verification.md)에서 이미 정정했고 근거는 `docs/architecture/08-bronze-replay-and-reextract-boundary.md`의 D-5다. 재추출은 새 `batch_id`로 구분한다.

색인은 장애 대응자가 제일 먼저 여는 문서다. 없는 식별자를 찾게 만들면 그 사람의 첫 5분이 통째로 낭비된다.

**조치**: 해당 문장에서 `reprocess_id`를 뺀다. R-13 행이 이미 `original_batch_id`/`re_extract_batch_id`로 옳게 적혀 있으므로 표는 손대지 않는다.

## 보강 2 — 색인 머리말이 아직 "후속 Task에서 채운다"고 말한다

`docs/troubleshooting/README.md` 머리말이 "각 시나리오가 증명한 오류 코드의 원인과 조치는 후속 Task에서 채운다"로 남아 있다. 이 Task가 그 후속 Task였고 빈 행은 모두 찼다.

**조치**: 머리말을 현재 상태로 고친다. 표가 실측 시나리오와 문서 근거를 `—(문서 근거)` 표기로 구분한다는 점을 한 줄로 밝히면 읽는 사람이 열 구분을 바로 이해한다. 기록 금지 항목(인증 정보·원시 Payload·절대 경로) 문장은 그대로 둔다.

## 보강 3 — ADR-018의 PRD 개정 요청이 이미 반영됐다

ADR-018은 `PRD_v1.13.md` §18을 인용하며 "**PRD 개정 요청**: 두 코드를 §18에 추가하고 … PRD 문서 자체는 이 ADR에서 고치지 않는다"로 끝난다. 그 요청은 승인됐다. `PRD_v1.14.md:1704-1719`가 `LEASE_UNAVAILABLE`/`LEASE_OWNERSHIP_LOST`를 등재하고 `SOURCE_MUTATION_CONFLICT`를 Deprecated로 표시했다.

ADR을 그대로 두면 다음에 읽는 사람이 아직 미결인 요청으로 읽는다. ADR은 결정의 최종 상태를 담는 문서다.

**조치**: ADR-018의 PRD 인용을 `PRD_v1.14.md`로 바꾸고, 개정 요청 문단을 "요청 → v1.14 §18에 반영됨"으로 갱신한다. Consequences의 "어긋남이 남는다" 항목도 해소 상태로 고친다. Status는 Accepted 그대로 둔다 — 결정이 바뀐 것이 아니라 후속 조치가 완료된 것이다.

## 보강 4 — `classify_error`를 "거치지 않는다"는 표현을 좁힌다

`commit-and-lease.md` 표 아래 문장이 "`OrphanReconciliationError`와 `SOURCE_CONTRACT_ERROR`는 `reconcile_orphan`이 직접 던지는 예외이고 `classify_error`를 거치지 않는다"고 적었다.

`SOURCE_CONTRACT_ERROR`는 `classify_error`가 내놓는 계약 코드다(`errors.py`, `SourceContractError` 두 종 → `SOURCE_CONTRACT_ERROR`). Ingestion 경로에서 Schema/Batch 계약 위반이 나면 이 코드가 `pipeline_runs`에 남는다. 지금 문장은 그 코드가 분류 체계 밖에 있는 것처럼 읽힌다.

사실은 **경로**의 문제다. `reconcile_orphan`은 사람이 수동으로 부르는 재조정 절차라 `pipeline_runs` 기록 경로를 타지 않는다.

**조치**: "이 경로(`reconcile_orphan`)는 `pipeline_runs` 기록을 거치지 않으므로 예외가 그대로 호출자에게 올라간다. 같은 `SOURCE_CONTRACT_ERROR`라도 Ingestion 경로에서 발생하면 `classify_error`를 거쳐 `pipeline_runs.error_type`에 남는다"로 고친다.

## 재검수 (2026-09-21)

보강 4건 반영 확인. Task 14를 닫는다.

| 항목 | 확인 |
| ---- | ---- |
| `reprocess_id` 제거 | `docs/runbooks/README.md`에서 해당 문자열 소멸, R-13 행 유지 |
| 색인 머리말 갱신 | `docs/troubleshooting/README.md:3-5`, `R-XX`/`—(문서 근거)` 구분 명시 |
| ADR-018 PRD 반영 상태 | `:52-56`, `:84`, `:119-120`. `PRD_v1.13.md`가 `PRD.bak/`로 이동한 것도 확인 |
| `classify_error` 표현 정밀화 | `docs/troubleshooting/commit-and-lease.md:14-16` |

Phase 8에서 남은 것은 Task 15(전체 스위트 마감)뿐이다. PRD §18 개정은 사용자 승인을 받아 `PRD_v1.14.md`로 반영 완료됐다.
