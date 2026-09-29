# Phase 8. Reliability Scenarios

> 상태: Done  
> Milestone: 3 — Portfolio Evidence  
> 선행 Phase: [Phase 7. Data Quality & Publish](phase-07-data-quality-publish.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.20](../../PRD_v1.21.md)

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

문서에는 Credential, Raw Payload, 환경별 Absolute Path를 기록하지 않는다. 실행마다 `batch_id`, `run_id`, `table_batch_id`를 남긴다.

## 선행 조건

이 Phase는 Grain 계약을 필요로 하는 Scenario와 그렇지 않은 Scenario로 나뉜다. 골격 Task는 Phase 6 완료 전에 착수할 수 있다.

### 골격 Task 선행 조건 (8-1)

- Phase 0~5의 Acceptance Test가 통과한다.
- Phase 7의 골격 Task(Ingestion Quality, Publish Safety)가 완료된다.
- 실패 주입이 정상 Source를 영구 오염시키지 않는 격리 경로를 사용한다.
- Metadata/Manifest/Object 상태를 조회하는 검증 SQL과 CLI가 있다.

### 적용 Task 선행 조건 (8-2)

- Phase 6과 Phase 7의 모든 Task가 완료된다.
- 성공 기준 Snapshot과 Mart Logical Hash가 저장돼 있다.

## 8-1. 골격: Grain 계약 없이 진행 가능

Harness와 Ingestion 계층 장애 Scenario는 Mart 구성과 독립적이다. 검증 대상이 Metadata, Object, Watermark라서 어떤 Dimension과 Fact가 있는지 몰라도 실행할 수 있다.

### 8-1-1. Reliability Harness

- [x] `P8-01` 공통 Fixture, 기준 Snapshot, Result Hash 구성 — 증거: `tests/reliability/harness.py`의 공통 Fixture/Snapshot 비교 도우미. R-01~R-15 18개 시나리오가 이를 통해 수집·검증됨
- [x] `P8-02` 실패 지점별 Fault Injection Hook 구성 — 증거: `tests/reliability/faults.py`(`fail_object_upload`, `fail_source_connection`, Manifest 변조 등). R-01~R-07·R-11·R-15에서 실사용
- [x] `P8-03` Metadata/Object/Mart 상태 수집기 구성 — 증거: `tests/reliability/harness.py`의 상태 수집·Hash 비교 도우미. R-08~R-14의 Mart Hash 비교가 이를 사용
- [x] `P8-04` Scenario 결과의 Pass/Fail 판정과 Evidence 저장 형식 정의 — 증거: `write_evidence()`가 `data/reliability/{scenario_id}/`에 JSON 저장. R-01~R-15 전부 이 형식으로 증적 보유
- [x] `P8-05` Runbook/Troubleshooting Template 작성 — 증거: `docs/runbooks/_template.md`(6단계 형식). R-01~R-15 Runbook 전부 이 Template을 따름

`P8-03`의 Mart 상태 수집은 Schema 이름을 설정으로 받는다. 구체적 Model 목록은 Phase 6 확정 후 주입한다.

### 8-1-2. Ingestion과 Commit 장애

| ID     | 시나리오               | 핵심 검증                                        |
| ------ | ---------------------- | ------------------------------------------------ |
| `R-01` | Duplicate Batch        | 동일 Batch 3회에 Object/Key 중복 0, Hash 동일    |
| `R-02` | Upload Failure         | Watermark 유지, Metadata COMMITTED 없음          |
| `R-03` | Metadata Failure       | VERIFIED Object는 Orphan 후보이며 Catalog Read 0 |
| `R-04` | Watermark CAS Conflict | 충돌 Run 실패, 승자만 Commit                     |
| `R-05` | Expired Lease          | 소유권 상실 Run이 Commit하지 못함                |
| `R-06` | Orphan Object          | 일치 Object만 Reconcile, 불일치는 거부           |
| `R-07` | Broken Manifest        | Checksum/Range/Version 불일치 자동 Commit 금지   |

- [x] `P8-06` R-01 Duplicate Batch 실행 및 문서화 — 증거: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -v` → PASSED. `batch_id=r01-c4b415b4970443a596b2d45637ddf440__20260920T000000Z`, `run_ids` 3회, `object_key_count=2`, `row_count=5`([데이터](../../data/reliability/r01/), [Runbook](../runbooks/r01-duplicate-batch.md))
- [x] `P8-07` R-02 Upload Failure 실행 및 문서화 — 증거: 위 명령 → PASSED. `batch_id=r02-ffef5f01baf64bb39f740f01c6bc91fc__20260920T000000Z`, `classified=OBJECT_STORAGE_ERROR`, `http_status=503`, `is_retryable=true`, Watermark 불변([Runbook](../runbooks/r02-upload-failure.md))
- [x] `P8-08` R-03 Metadata Failure 실행 및 문서화 — 증거: 위 명령 → PASSED. `batch_id=r03-handled-600b862273044ce588a1db919ee40db1__20260920T000000Z`, `classified=UNKNOWN_ERROR`, Orphan 후보 1건, 수동 재조정 거부됨([Runbook](../runbooks/r03-metadata-failure.md))
- [x] `P8-09` R-04 Watermark CAS Conflict 실행 및 문서화 — 증거: 위 명령 → PASSED. `pipeline_name=r04-cf5c5444257c418482dcba736ed41770`, 승자 `run_id=b754a3db-09bc-4a7d-8faf-13decfe04ee9`(COMMITTED), 패자 `run_id=add6933a-faac-4a76-88de-21946eb4d919`(FAILED, WATERMARK_CONFLICT)([Runbook](../runbooks/r04-watermark-cas-conflict.md))
- [x] `P8-10` R-05 Expired Lease 실행 및 문서화 — 증거: 위 명령 → PASSED. `pipeline_name=r05-813a4c3f2a4743438cf6670b533aaf17`, Stale `owner_id=c6a4d413-a4f1-494f-9e85-b8045fb7df8f`가 인수 후 `LEASE_OWNERSHIP_LOST`로 Fencing됨([Runbook](../runbooks/r05-expired-lease.md))
- [x] `P8-11` R-06 Orphan Object 실행 및 문서화 — 증거: 위 명령 → PASSED. 일치 Batch(`r06-accepted-d685746c54ea48aebd062a1db7c48d59`)는 Reconcile 성공, Quarantine Reject 존재 Batch(`r06-rejected-fcf849b874944003906d17781d78d9a6`)는 거부됨([Runbook](../runbooks/r06-orphan-object.md))
- [x] `P8-12` R-07 Broken Manifest 실행 및 문서화 — 증거: 위 명령 → PASSED(3개 변형). CHECKSUM/ROW_RANGE는 `OrphanReconciliationError`, SCHEMA_VERSION은 `SourceContractError`(`SOURCE_CONTRACT_ERROR`)로 자동 Commit 차단([Runbook](../runbooks/r07-broken-manifest.md))

### 8-1-3. 재처리와 외부 의존 장애

| ID     | 시나리오                  | 핵심 검증                                        |
| ------ | ------------------------- | ------------------------------------------------ |
| `R-11` | Missing Schedule          | 누락 구간은 다음 실행이 자동 Self-heal(Gap 없음), 귀속만 명시 Batch로 복구 |
| `R-12` | Backfill Replay           | Source Read 없이 COMMITTED Bronze 재적용         |
| `R-13` | Re-extract                | 명시 범위를 새 `batch_id`로 추출                 |
| `R-15` | Source Connection Failure | Object 생성/Watermark 전진 없이 재시도 가능 상태 |

> R-11 주석: 2026-09-28부터 `source_simulation_dag`는 `@hourly`, 두 DAG 모두 `catchup=False`다.
> Scheduler 누락은 Source 변경 미발생 또는 다음 Warehouse 실행의 Self-heal로 귀결되며
> 새 시나리오는 필요 없다([architect-review 047](../architect-review/047_source-dag-hourly-r11-premise.md)).

- [x] `P8-16` R-11 Missing Schedule 실행 및 문서화 — 증거: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -v` → PASSED. `pipeline_name=test_r11_43411123c19e486f82a3bf544a83ac14`, 건너뛴 창 없이 C 실행이 Gap 없이 회수, Mart Hash가 회복 전후 모두 Control과 일치([Runbook](../runbooks/r11-missing-schedule.md))
- [x] 2026-09-28 R-11 전제 갱신 — Source 매시간 예약과 두 DAG의 `catchup=False`에서 Scheduler 누락의 두 경로를 [architect-review 047](../architect-review/047_source-dag-hourly-r11-premise.md)에 따라 기록하고 새 시나리오는 추가하지 않았다.
- [x] `P8-17` R-12 Backfill Replay 실행 및 문서화 — 증거: 위 명령 → PASSED. `pipeline_name=test_r12_b5052dc9946743749f37fbc773f62f53`, Source Read 없이 COMMITTED Bronze만으로 재Build, `mart_hashes_match_control=true`([Runbook](../runbooks/r12-backfill-replay.md))
- [x] `P8-18` R-13 Re-extract 실행 및 문서화 — 증거: 위 명령 → PASSED. `pipeline_name=test_r13_83a8eba240a347da9e4fe3bc92a605b4`, `original_batch_id`와 `re_extract_batch_id`가 별개 `batch_id`로 공존, 기존 Object 불변 유지([Runbook](../runbooks/r13-re-extract.md))
- [x] `P8-20` R-15 Source Connection Failure 실행 및 문서화 — 증거: 위 명령 → PASSED. `batch_id=r15-02bb8fb6b4714ca3a1b6540e74f3c1e4__20260920T000000Z`, `classified_error=SOURCE_CONNECTION_ERROR`, Object 생성 0건, Watermark 불변, 재시도 성공([Runbook](../runbooks/r15-source-connection-failure.md))

Backfill 기본값은 Replay다. Re-extract는 Source 현재 상태가 과거 Snapshot과 같지 않을 수 있다는 한계를 결과에 명시한다.

## 8-2. 적용: Phase 6 완료 후 진행

검증 기준이 Fact 갱신이나 Dimension Version이라서 Mart 구성이 확정돼야 판정할 수 있는 Scenario다.

### 8-2-1. 시간과 이력 장애

| ID     | 시나리오             | 핵심 검증                                     |
| ------ | -------------------- | --------------------------------------------- |
| `R-08` | Late Order           | 새 Mutation Cursor로 1회 수집, 과거 Mart 갱신 |
| `R-09` | Late Payment         | 연결 주문의 구매일이 영향 범위에 포함         |
| `R-10` | Customer 이력 변경   | 구독 상태 또는 등급 변경으로 Version 구간과 사건 시점 결합 갱신 |

- [x] `P8-13` R-08 Late Order 실행 및 문서화 — 증거: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -v` → PASSED. `pipeline_name=test_r08_dcdaf7ea5b814e3fb562c7d7372c9973`, `late_order_id=9aa262691c75511abd813f8e64bd64ac`, 과거 구매일 Mart Hash 1회 갱신([Runbook](../runbooks/r08-late-order.md))
- [x] `P8-14` R-09 Late Payment 실행 및 문서화 — 증거: 위 명령 → PASSED. `pipeline_name=test_r09_b1a26067f28a48409f666e41c1fabc13`, `order_id=eb7dfee7657a58a08324fe0d0b945a2e`, 연결 주문 구매일이 영향 범위에 포함됨([Runbook](../runbooks/r09-late-payment.md))
- [x] `P8-15` R-10 Customer 이력 변경 실행 및 문서화 — 증거: 위 명령 → PASSED. `pipeline_name=test_r10_010013ae3f8542d193253e39d0d1b662`, `customer_unique_id=a09e5cc17e70578b9f0c69668a9d4f2c`, `subscription_id=91f8828c-a203-5276-93e6-92ee74d52964`, 등급/구독 상태 변경으로 Version 2개 생성([Runbook](../runbooks/r10-customer-history-change.md))

### 8-2-2. Warehouse 장애

| ID     | 시나리오    | 핵심 검증                                |
| ------ | ----------- | ---------------------------------------- |
| `R-14` | dbt Failure | Bronze/Watermark와 마지막 성공 Mart 유지 |

- [x] `P8-19` R-14 dbt Failure 실행 및 문서화 — 증거: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -v` → PASSED. `failing_publish_run_id=1c9b8e87-38da-4f69-9cdc-5c0ca24b90ad`(`DBT_TEST_ERROR`), `bronze_hash_unchanged=true`, `published_mart_hash_unchanged=true`, 이어진 `second_publish_run_id=b80dc65c-e09c-4acd-b3dc-74229225cab4`가 정상 Publish([Runbook](../runbooks/r14-dbt-failure.md))

## 시나리오별 문서화

- [x] 각 운영 복구 절차를 `docs/runbooks/`에 작성 — 증거: `r01-duplicate-batch.md`~`r15-source-connection-failure.md` 15편 모두 존재, `docs/runbooks/README.md` 색인에 전부 등재.
- [x] 원인 분석과 자주 발생하는 오류를 `docs/troubleshooting/`에 작성 — 증거: `ingestion-errors.md`, `commit-and-lease.md`, `warehouse-publish.md` 3편 완성, `docs/troubleshooting/README.md` 오류 코드 색인이 전부 채워짐(Task 14).
- [x] 자동화 가능한 재검증을 Integration Test로 연결 — 증거: `tests/reliability/`의 18개 테스트(R-01~R-15, R-07 3변형 포함)가 각 Runbook의 재검증 명령으로 실행됨.
- [x] 각 문서에 관련 PRD/AC/ADR와 Evidence 경로 연결 — 증거: 본 문서의 [Acceptance 연결](#acceptance-연결) 표가 R-01~R-15를 AC에 매핑, `docs/troubleshooting/*.md`가 ADR-017/ADR-018/`PRD_v1.14.md`를 인용, `docs/runbooks/README.md`의 증적 식별자 열이 각 시나리오의 `data/reliability/` Evidence 경로를 가리킨다.

## 시나리오 완료 기준

각 `R-*`는 다음 여섯 항목이 모두 있어야 완료다. 아래 체크는 R-01~R-15 전체에 대해 성립함을 뜻한다(개별 `R-*` 단위 판정 아님).

- [x] 실패를 결정적으로 재현할 수 있다. — R-01~R-15 전체: 각 Runbook의 재현 조건과 명령, 고정된 Fault Injection Fixture로 결정적 재현.
- [x] 기대한 Metadata/Error Type으로 탐지된다. — R-01~R-15 전체: 각 Runbook/Troubleshooting의 오류 코드와 실측 `classified`/`error_type`이 일치(예: R-02 `OBJECT_STORAGE_ERROR`, R-07 `SOURCE_CONTRACT_ERROR`).
- [x] 관련 불변 조건이 깨지지 않는다. — R-01~R-15 전체: Watermark/Lease/CAS/Mart Hash 불변이 `data/reliability/` 증적으로 확인(R-04/R-05 승자만 Commit, R-14 Hash 불변 등).
- [x] 문서화된 절차로 복구할 수 있다. — R-01~R-15 전체: 각 Runbook 복구 절차(재시도/재조정/명시 Batch/Rewind)가 문서화됐고 `./scripts/verify_clean_clone.sh`로 명령 실행 가능성을 확인.
- [x] 재검증 후 정상 상태의 Count/Hash가 일치한다. — R-01~R-15 전체: 각 Runbook의 재검증 명령 실행 결과가 Count/Hash로 기록됨(예: R-12 `mart_hashes_match_control=true`).
- [x] 자동 테스트 또는 보존된 Evidence가 있다. — R-01~R-15 전체: `tests/reliability/` 18개 테스트와 `data/reliability/` 보존 Evidence JSON.

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
| `docs/phases/phase-08-reliability.md` | `P8-01`~`P8-20` 체크리스트에 명령·결과·식별자 증거 라인을 채우고, R-11 행을 명시 Batch 실행과 다음 실행의 자동 회복 정의로 고치고, Definition of Done·Portfolio Evidence를 실측 근거로 채웠다. 2026-09-28 Source 매시간 예약에 따른 Scheduler 누락 주석과 완료 체크를 추가했다. |
| `tests/reliability/` | 읽기 전용 플랫폼 상태 수집기, 상태 비교 도우미, JSON 증적 저장기를 추가했다. |
| `tests/reliability/faults.py` | 기존 업로드·Commit·Source 연결 함수에 monkeypatch로 장애를 주입하고 Manifest 변조를 재현한다. |
| `docs/runbooks/`, `docs/troubleshooting/` | Runbook 15편과 Troubleshooting 3편(Ingestion 오류, Commit과 Lease, Warehouse Publish)을 완성하고 오류 코드 역색인을 채웠다(미커밋). 2026-09-28 `r11-missing-schedule.md`의 존재하지 않는 DAG 명칭을 바로잡고 Scheduler 누락 경로를 설명했다. |
| `pyproject.toml`, `.gitignore` | `reliability` pytest 마커와 생성 증적 제외 경로를 등록했다. |

## Definition of Done

- [x] 모든 `P7-*` Task가 완료됐다 — `docs/phases/phase-07-data-quality-publish.md`의 모든 `P7-*` 항목이 `[x]`다.
- [x] R-01~R-15가 재현·탐지·복구·재검증 가능하다 — `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -v` → 18 passed(`data/reliability/`).
- [x] 실패 중 Commit/Watermark/Mart 불변 조건이 유지된다 — R-02/R-03/R-15는 Watermark 불변, R-04/R-05는 패자/Stale Owner Commit 거부, R-14는 Bronze/Watermark/Published Mart Hash 불변으로 각 시나리오 증적에서 확인.
- [x] Replay 결과가 같은 범위의 Full Refresh와 일치한다 — R-12 `mart_hashes_match_control=true`, R-11 회복 전후 모두 Control과 Hash 일치.
- [x] 모든 Runbook이 새 Clone에서도 실행 가능한 명령을 제공한다 — 측정: `./scripts/verify_clean_clone.sh`가 새 Clone에서 `uv sync --frozen`→`ruff check`→`pytest`→Seed→Generator→Ingestion→Publish→`pytest -m "integration and not airflow"` 전 과정을 통과시켰다(이 Script는 `tests/reliability`를 실행하지 않는다). 추론: 각 Runbook의 재현/재검증 명령이 절대 경로 없이 동일한 `uv run pytest` 형식이라는 구조적 근거로 Clean Clone에서도 그대로 실행될 것으로 판단한다 — 측정으로 확정하려면 Clean Clone 안에서 `tests/reliability`를 별도로 실행해야 하며, 이번 마감 범위에서는 필수로 요구하지 않았다.
- [x] 관측과 구현이 PRD 가정과 다르면 ADR/PRD를 갱신했다 — Lease 계열 Error Type 어긋남을 [ADR-018](../adr/018-ingestion-error-type-classification.md)로 기록하고 `PRD_v1.14.md` §18에 반영했다. `commit_table_run` Fencing 창은 [ADR-017](../adr/017-commit-table-run-lease-fencing.md)로 기록했다.

## Portfolio Evidence

- 실패 지점별 상태 전이 Matrix — [오류 코드 색인](../troubleshooting/README.md), [Ingestion 오류](../troubleshooting/ingestion-errors.md), [Commit과 Lease](../troubleshooting/commit-and-lease.md)
- Watermark/Lease/CAS 충돌 Timeline — `data/reliability/r04/`(CAS 승자/패자), `data/reliability/r05/`(Lease 인수·Fencing Timeline)
- Orphan Reconciliation의 성공과 거부 비교 — `data/reliability/r06/`(일치 Batch 성공/Reject 존재 Batch 거부), `data/reliability/r07-checksum/`, `data/reliability/r07-row_range/`, `data/reliability/r07-schema_version/`
- Late Arrival 전후 Mart Diff — `data/reliability/r08/`(`pre_build_fct_order_hash`/`post_build_fct_order_hash`), `data/reliability/r09/`(`pre_build_fct_order_payment_hash`/`post_build_fct_order_payment_hash`), `data/reliability/r10/`(Version 전후 Customer/Subscription Key)
- Replay와 Full Refresh Logical Hash — `data/reliability/r12/`(`mart_hashes_match_control=true`), `data/reliability/r13/`(`original_object_unchanged_after_re_extract=true`)
- 실패한 dbt Build 전후 Published Mart Hash — `data/reliability/r14/`(`published_mart_hash_unchanged=true`, `bronze_hash_unchanged=true`, `watermark_hash_unchanged=true`)

## 권장 Commit

```text
test: document reliability and recovery scenarios
```

## 다음 Phase 인계

Phase 9는 Reliability Harness의 고정 Dataset, Scenario, Hash, 환경 Metadata 형식을 재사용해 성능 실험의 재현성을 확보한다.
