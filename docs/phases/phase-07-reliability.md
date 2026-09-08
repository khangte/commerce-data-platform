# Phase 7. Reliability Scenarios

> 상태: Planned  
> Milestone: 3 — Portfolio Evidence  
> 선행 Phase: [Phase 6. Data Quality & Publish](phase-06-data-quality-publish.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.4](../../PRD_v1.4.md)

## 목표

완성된 시스템을 의도적으로 실패시키고, 핵심 장애가 재현·탐지·복구·재검증 가능한지 증명한다. 이 Phase의 중심 산출물은 새로운 기능보다 실행 가능한 Test Evidence, Runbook, Troubleshooting 문서다.

## 문서 원칙

각 시나리오는 다음 순서로 기록한다.

```text
문제
→ 재현 조건과 명령
→ 기대/실제 관측
→ 원인과 불변 조건
→ 복구 절차
→ 재검증 명령과 결과
```

문서에는 Credential, Raw Payload, 환경별 Absolute Path를 기록하지 않는다. 실행마다 `batch_id`, `run_id`, `table_batch_id`, 필요 시 `reprocess_id`를 남긴다.

## 선행 조건

- Phase 0~6의 Acceptance Test가 통과한다.
- 성공 기준 Snapshot과 Mart Logical Hash가 저장돼 있다.
- 실패 주입이 정상 Source를 영구 오염시키지 않는 격리 경로를 사용한다.
- Metadata/Manifest/Object 상태를 조회하는 검증 SQL과 CLI가 있다.

## 구현 순서

### 1. Reliability Harness

- [ ] `P7-01` 공통 Fixture, 기준 Snapshot, Result Hash 구성
- [ ] `P7-02` 실패 지점별 Fault Injection Hook 구성
- [ ] `P7-03` Metadata/Object/Mart 상태 수집기 구성
- [ ] `P7-04` Scenario 결과의 Pass/Fail 판정과 Evidence 저장 형식 정의
- [ ] `P7-05` Runbook/Troubleshooting Template 작성

### 2. Ingestion과 Commit 장애

| ID     | 시나리오               | 핵심 검증                                        |
| ------ | ---------------------- | ------------------------------------------------ |
| `R-01` | Duplicate Batch        | 동일 Batch 3회에 Object/Key 중복 0, Hash 동일    |
| `R-02` | Upload Failure         | Watermark 유지, Metadata COMMITTED 없음          |
| `R-03` | Metadata Failure       | VERIFIED Object는 Orphan 후보이며 Catalog Read 0 |
| `R-04` | Watermark CAS Conflict | 충돌 Run 실패, 승자만 Commit                     |
| `R-05` | Expired Lease          | 소유권 상실 Run이 Commit하지 못함                |
| `R-06` | Orphan Object          | 일치 Object만 Reconcile, 불일치는 거부           |
| `R-07` | Broken Manifest        | Checksum/Range/Version 불일치 자동 Commit 금지   |

- [ ] `P7-06` R-01 Duplicate Batch 실행 및 문서화
- [ ] `P7-07` R-02 Upload Failure 실행 및 문서화
- [ ] `P7-08` R-03 Metadata Failure 실행 및 문서화
- [ ] `P7-09` R-04 Watermark CAS Conflict 실행 및 문서화
- [ ] `P7-10` R-05 Expired Lease 실행 및 문서화
- [ ] `P7-11` R-06 Orphan Object 실행 및 문서화
- [ ] `P7-12` R-07 Broken Manifest 실행 및 문서화

### 3. 시간, 이력, 재처리

| ID     | 시나리오             | 핵심 검증                                     |
| ------ | -------------------- | --------------------------------------------- |
| `R-08` | Late Order           | 새 Mutation Cursor로 1회 수집, 과거 Mart 갱신 |
| `R-09` | Late Payment         | 연결 주문의 구매일이 영향 범위에 포함         |
| `R-10` | Customer SCD2 Change | Version 구간과 주문 Temporal Join 갱신        |
| `R-11` | Missing Schedule     | 누락 기간을 Replay/명시 Batch로 회복          |
| `R-12` | Backfill Replay      | Source Read 없이 COMMITTED Bronze 재적용      |
| `R-13` | Re-extract           | 명시 범위를 새 `reprocess_id`로 추출          |

- [ ] `P7-13` R-08 Late Order 실행 및 문서화
- [ ] `P7-14` R-09 Late Payment 실행 및 문서화
- [ ] `P7-15` R-10 Customer SCD2 Change 실행 및 문서화
- [ ] `P7-16` R-11 Missing Schedule 실행 및 문서화
- [ ] `P7-17` R-12 Backfill Replay 실행 및 문서화
- [ ] `P7-18` R-13 Re-extract 실행 및 문서화

Backfill 기본값은 Replay다. Re-extract는 Source 현재 상태가 과거 Snapshot과 같지 않을 수 있다는 한계를 결과에 명시한다.

### 4. Warehouse와 외부 의존 장애

| ID     | 시나리오                  | 핵심 검증                                        |
| ------ | ------------------------- | ------------------------------------------------ |
| `R-14` | dbt Failure               | Bronze/Watermark와 마지막 성공 Mart 유지         |
| `R-15` | Source Connection Failure | Object 생성/Watermark 전진 없이 재시도 가능 상태 |

- [ ] `P7-19` R-14 dbt Failure 실행 및 문서화
- [ ] `P7-20` R-15 Source Connection Failure 실행 및 문서화

### 5. 시나리오별 문서화

- [ ] 각 운영 복구 절차를 `docs/runbooks/`에 작성
- [ ] 원인 분석과 자주 발생하는 오류를 `docs/troubleshooting/`에 작성
- [ ] 자동화 가능한 재검증을 Integration Test로 연결
- [ ] 각 문서에 관련 PRD/AC/ADR와 Evidence 경로 연결

## 시나리오 완료 기준

각 `R-*`는 다음 여섯 항목이 모두 있어야 완료다.

- [ ] 실패를 결정적으로 재현할 수 있다.
- [ ] 기대한 Metadata/Error Type으로 탐지된다.
- [ ] 관련 불변 조건이 깨지지 않는다.
- [ ] 문서화된 절차로 복구할 수 있다.
- [ ] 재검증 후 정상 상태의 Count/Hash가 일치한다.
- [ ] 자동 테스트 또는 보존된 Evidence가 있다.

## Acceptance 연결

| AC       | 담당 시나리오                                                  |
| -------- | -------------------------------------------------------------- |
| AC-03    | R-01 Duplicate Batch                                           |
| AC-04    | R-02 Upload Failure, R-03 Metadata Failure                     |
| AC-06    | R-04 CAS Conflict, R-05 Expired Lease                          |
| AC-07    | R-12 Backfill Replay, R-13 Re-extract 비교                     |
| AC-09/10 | R-10 Customer SCD2 Change                                      |
| AC-11/20 | R-08 Late Order, R-09 Late Payment                             |
| AC-13    | 모든 Scenario의 Metadata 조회                                  |
| AC-21    | Warehouse의 원천 데이터 동시성 잠금 중 Generator 충돌 시나리오 |
| AC-23    | R-03/R-06/R-07 메타데이터 커밋 상태 기준                       |
| AC-24    | Broken/Unsupported Schema Version 변형                         |

AC-07은 동일 범위 Full Refresh와 Key별 값/Logical Hash가 같아야 한다.

## 요구사항 추적

| 구분 | 연결 항목                                 |
| ---- | ----------------------------------------- |
| PRD  | Section 8 증분 추출과 동시성              |
| PRD  | Section 9 Batch Identity와 재실행         |
| PRD  | Section 10 Bronze와 Quarantine            |
| PRD  | Section 16 Late Arrival, Backfill, Re-run |
| PRD  | Section 18 Observability와 오류           |
| FR   | FR-08 Idempotency와 Orphan Recovery       |
| FR   | FR-13 Backfill Replay/Re-extract          |
| FR   | FR-14 Late Arrival 재처리                 |

## 산출물

- Reliability Test Harness와 Fault Injection Fixture
- R-01~R-15 실행 결과
- 장애 유형별 Runbook
- 원인/Error Type별 Troubleshooting 문서
- Backfill/Full Refresh Hash 비교
- 정상 복귀 후 E2E 재검증 기록

## 파일·폴더별 변경 요약

| 경로                                  | 변경 내용                                                                                                       |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `docs/phases/phase-07-reliability.md` | 프로젝트 내부 잠금·커밋 상태 기준 용어를 한국어 중심으로 정리하고, Batch Identity·Logical Hash 표기는 유지했다. |

## Definition of Done

- [ ] 모든 `P7-*` Task가 완료됐다.
- [ ] R-01~R-15가 재현·탐지·복구·재검증 가능하다.
- [ ] 실패 중 Commit/Watermark/Mart 불변 조건이 유지된다.
- [ ] Replay 결과가 같은 범위의 Full Refresh와 일치한다.
- [ ] 모든 Runbook이 새 Clone에서도 실행 가능한 명령을 제공한다.
- [ ] 관측과 구현이 PRD 가정과 다르면 ADR/PRD를 갱신했다.

## Portfolio Evidence

- 실패 지점별 상태 전이 Matrix
- Watermark/Lease/CAS 충돌 Timeline
- Orphan Reconciliation의 성공과 거부 비교
- Late Arrival 전후 Mart Diff
- Replay와 Full Refresh Logical Hash
- 실패한 dbt Build 전후 Published Mart Hash

## 권장 Commit

```text
test: document reliability and recovery scenarios
```

## 다음 Phase 인계

Phase 8은 Reliability Harness의 고정 Dataset, Scenario, Hash, 환경 Metadata 형식을 재사용해 성능 실험의 재현성을 확보한다.
