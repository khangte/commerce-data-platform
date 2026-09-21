# Phase 7. Data Quality & Publish

> 상태: In Progress  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 6. Dimensional Modeling](phase-06-dimensional-modeling.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.14](../../PRD_v1.14.md), [Mart Grain 계약](../reference/mart-grain.md)

## 목표

Ingestion과 Warehouse의 품질 책임을 명확히 분리하고, 품질 검증을 통과한 Mart만 소비 대상으로 Publish한다. 실패한 Build는 이미 Commit된 Bronze나 마지막 성공 Mart를 훼손하지 않아야 한다.

## 품질 책임 경계

| 계층      | 책임                                                                                                  | 실패 처리                      |
| --------- | ----------------------------------------------------------------------------------------------------- | ------------------------------ |
| Ingestion | Schema, Type, NULL Key, Batch Duplicate, Source Domain, Numeric Range, Broken Reference, Cursor Range | Row Quarantine 또는 Batch 실패 |
| Warehouse | Business Key, Relationship, Canonical 값, 시간 순서, SCD2, Fact Grain/Measure                         | Build/Test 실패                |
| Publish   | 검증된 Build만 소비 경로로 승격                                                                       | 이전 성공 Mart 유지            |

Phase 3과 6에서 각 계층의 기본 테스트를 구현하고, 이 Phase에서는 이를 실행 가능한 통합 Gate와 Publish 경계로 완성한다.

## 구독·등급 전환 Step 7 반영 완료

전환 계획 7단계의 Warehouse 품질 검증은 Phase 6 dbt 프로젝트에 먼저 반영했다. 이 작업은
Phase 7의 Publish Workflow와 E2E 품질 Gate를 완료했다는 뜻은 아니다.

| 경로 | 변경 내용 |
| ---- | --------- |
| `dbt/models/marts/dimensions/schema.yml` | 고객 SCD2 Key·상태 도메인·등급 도메인·필수값 테스트를 추가했다. |
| `dbt/tests/dim_customer_*.sql` | SCD2 구간 비중복, 고객별 Current Version 1건, 허용 상태 전이, 재가입 측정값, 등급 하락 금지를 검증한다. |
| `dbt/tests/fct_subscription_payment_missing_customer_key.sql` | 구독 결제 Fact의 고객 SCD2 Version 누락을 검증한다. |

- [x] 구독 상태 전이·SCD2 구간 비중복·재가입·등급 규칙을 dbt Test로 검증한다.
- [x] Publish 경계, 실패 Build 격리와 마지막 성공 Mart 보존은 `P7-13` 이후 작업으로 닫았다.

## 선행 조건

이 Phase는 Grain 계약을 필요로 하는 Task와 그렇지 않은 Task로 나뉜다. 골격 Task는 Phase 6 완료 전에 착수할 수 있다.

### 골격 Task 선행 조건 (7-1)

- Phase 3의 Quarantine과 Batch Failure 정책이 자동 테스트된다.
- Warehouse Build를 격리할 Schema/File 경계가 결정됐다.

### 적용 Task 선행 조건 (7-2)

- Phase 6의 Mart Model과 Model-level Test가 통과한다.
- [Mart Grain 계약](../reference/mart-grain.md)이 확정됐다.

## 7-1. 골격: Grain 계약 없이 진행 가능

Ingestion 계층 품질과 Publish 전환 메커니즘은 Mart 구성과 독립적이다. 어떤 Dimension과 Fact가 있는지 몰라도 "검증 통과한 Build만 승격한다"는 경계는 설계할 수 있다.

### 7-1-1. Ingestion Quality 통합

- [x] `P7-01` Ingestion Validation Rule Registry 정리
- [x] `P7-02` Row Error와 Batch Error 분류 검증
- [x] `P7-03` Freeze된 Parent Key 기준 Broken Reference 검증
  - 근거: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_child_parent_references_integration.py::test_parent_committed_after_snapshot_is_not_visible -v` → `1 passed`.
- [x] `P7-04` Reject Rate 0/이하/초과 경계 테스트
- [x] `P7-05` Duplicate/NULL Key/Broken FK/Invalid Status/Negative Value Fixture
  - 근거: `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_corruption_matrix_integration.py -v` → `5 passed`.

검증 순서와 Error Code가 Phase 3 문서 및 PRD Section 11과 일치해야 한다.

### 7-1-2. Publish Safety

- [x] `P7-13` Build 대상과 Published Mart의 물리적 경계 정의
- [x] `P7-14` Build → Test → Publish 전환 구현
- [x] `P7-15` Publish 전환을 단일 Transaction 또는 복구 가능한 단위로 처리
- [x] `P7-16` dbt Build/Test 실패 시 이전 Published Mart 유지
- [x] `P7-17` Publish Run ID, Invocation ID, Hash, 상태 기록

기본 의미:

```text
dbt Build/Test 성공
→ 검증된 Build를 Published Mart로 승격

dbt Build/Test 실패
→ Bronze/Watermark 유지
→ 실패 Build 격리
→ 마지막 성공 Mart 유지
```

DuckDB 제약을 관측한 뒤 Build Schema → Test → Swap 또는 별도 Warehouse File → 검증 → 교체 중 하나를 선택하고 ADR에 근거를 기록한다.

**Publish 대상 Schema 목록은 Phase 6에서 Mart 구성이 확정된 뒤 채운다.** 전환 메커니즘 자체는 목록과 무관하게 먼저 구현하고, 목록은 설정으로 분리해 나중에 주입한다.

### 7-1-3. Metadata 조회

- [x] `P7-19` 성공/빈/실패/재실행 Metadata 조회 SQL
  - 근거: `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_observability_run_status_integration.py::test_observability_query_distinguishes_run_outcomes -v` → `1 passed`.

## 7-2. 적용: Phase 6 완료 후 진행

Mart Model 이름과 Grain이 확정돼야 작성할 수 있는 Test와 검증이다.

### 7-2-1. Warehouse Quality

- [x] `P7-06` dbt Generic Test 구성: `unique`, `not_null`, `relationships`, `accepted_values`
- [x] `P7-07` 금액 Non-negative Custom Test
- [x] `P7-08` 주문 Timestamp 순서 Custom Test
- [x] `P7-09` 이력 Version 구간 Overlap/Current Custom Test
- [x] `P7-10` Fact FK Missing/Business Key Duplicate Test
- [x] `P7-11` 정상 E2E Unknown Key 0 Test
  - 근거: 정상 E2E 2건에서 `run_results.json`의 `relationships_` Test 노드 실행 수·`status=pass`·`failures=0`을 직접 단언했고 `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_order_e2e_and_late_order_mart_integration.py -v` → `2 passed`.
- [x] `P7-12` Fact Measure/Fan-out 회귀 Test

필수 Custom Contract:

```text
payment_value >= 0
price >= 0
freight_value >= 0
delivered_at >= purchase_at
approved_at >= purchase_at
이력 Version 구간 overlap = 0
Business Key별 Current Version = 1
Fact FK Missing = 0
Fact Business Key Duplicate = 0
Normal E2E Unknown Key = 0
```

구체적인 Model 이름과 검증 대상 컬럼은 [Mart Grain 계약](../reference/mart-grain.md)에서 가져온다.

### 7-2-2. E2E 품질 Gate

- [x] `P7-18` Source→Bronze Catalog→Fact Count/Key 추적
- [x] `P7-20` 새 Clone에서 Seed→Generator→Ingestion→dbt Test 재현
  - 근거: Commit `dbf66fb` 기준 `./scripts/verify_clean_clone.sh` → `clean clone verification passed`, 로그 `/tmp/cdp-clean-clone-20260920T092930Z.log`.
- [x] `P7-21` Phase 0~7 통합 검증 명령을 README에 반영

## 범위 밖

- 모든 운영 장애에 대한 Runbook 작성
- 성능 최적화와 Benchmark 수치
- Dashboard 구현

## 테스트와 Gate

### Corruption Matrix

| Corruption     | 기대 계층 | 기대 결과                             |
| -------------- | --------- | ------------------------------------- |
| Duplicate      | Ingestion | Quarantine/정확한 Error Count         |
| NULL Key       | Ingestion | Quarantine                            |
| Broken FK      | Ingestion | Freeze된 Parent Key 기준 격리         |
| Invalid Status | Ingestion | Source Domain Error                   |
| Negative Value | Ingestion | Numeric Range Error                   |
| SCD2 Overlap   | Warehouse | dbt Test 실패, Publish 차단           |
| Fact Fan-out   | Warehouse | Grain/Measure Test 실패, Publish 차단 |

### Acceptance 연결

| AC    | 시나리오              | 합격 증거                                 |
| ----- | --------------------- | ----------------------------------------- |
| AC-01 | E2E                   | 고정 주문의 Source→Bronze→Fact Count 추적 |
| AC-08 | 5종 Corruption        | 기대 Reject 일치, 정상 Source 무오염      |
| AC-12 | Referential Integrity | FK/Unique 통과, 정상 Unknown 0            |
| AC-13 | Observability         | 단일 SQL로 네 실행 상태 조회              |
| AC-16 | 새 Clone              | Version/Health/Seed/E2E/dbt Test 성공     |

### Publish 실패 시나리오

1. 성공 Mart를 Publish하고 Logical Hash를 기록한다.
2. 의도적으로 dbt Test가 실패하는 입력을 Build한다.
3. Publish가 거부되는지 확인한다.
4. Published Mart의 Row Count/Hash가 이전 성공 상태와 같은지 확인한다.
5. 수정 후 다시 Build/Test/Publish하고 새 Run을 기록한다.

## 요구사항 추적

| 구분 | 연결 항목                         |
| ---- | --------------------------------- |
| PRD  | Section 11 Ingestion Validation   |
| PRD  | Section 17 Data Quality와 Publish |
| PRD  | Section 18 Observability와 오류   |
| FR   | FR-12 Ingestion/Warehouse Quality |
| FR   | FR-16 Quarantine                  |

## 산출물

- 계층별 Data Quality Rule Registry
- Corruption Fixture와 통합 테스트
- dbt Generic/Custom Test Suite
- 안전한 Mart Publish Workflow
- Publish Metadata와 이전 성공 Mart 보존 테스트
- E2E 검증 명령과 새 Clone 재현 기록

## Definition of Done

- [x] 모든 `P6-*` Task가 완료됐다.
  - 근거: `5516261`(`docs: close phase6`)와 `d441323`(`fix: stabilize phase6 closure tests`)로 Phase 6 종료 근거를 남겼고, Phase 7 잔여 근거 보강 중 `uv run pytest -q` → `185 passed, 80 skipped`.
- [x] 5종 Corruption을 기대 계층에서 정확히 탐지한다.
  - 근거: Corruption Matrix 5종 통합 테스트 `5 passed`.
- [x] 정상 데이터 False Positive가 0이다.
  - 근거: 정상 E2E 2건에서 relationships Test 노드가 모두 `status=pass`, `failures=0`임을 직접 단언했고 E2E 전체 `2 passed`.
- [x] Warehouse 실패가 Bronze/Watermark를 변경하지 않는다.
- [x] Build/Test 실패 뒤 마지막 성공 Mart의 Count/Hash가 유지된다.
- [x] AC-01, 08, 12, 13, 16이 통과한다.
  - 근거:

    | AC | 근거 |
    | -- | ---- |
    | AC-01 | `tests/integration/test_order_e2e_and_late_order_mart_integration.py::test_fixed_order_is_traceable_from_source_to_fact` 포함 E2E `2 passed` |
    | AC-08 | `tests/integration/test_corruption_matrix_integration.py` → `5 passed` |
    | AC-12 | E2E `run_results.json` relationships Test 노드 실행·통과·`failures=0` 직접 단언 → `2 passed` |
    | AC-13 | `tests/integration/test_observability_run_status_integration.py::test_observability_query_distinguishes_run_outcomes` → `1 passed` |
    | AC-16 | `./scripts/verify_clean_clone.sh` → `clean clone verification passed` |
- [x] Publish 전략과 관측 근거가 ADR에 기록됐다.

## Portfolio Evidence

- 계층별 품질 책임 Matrix
- Corruption별 Quarantine/Error Count
- 실패한 dbt Build 전후 Published Mart Hash
- Source→Bronze→Fact 단일 Record 추적
- 새 Clone 재현 로그

### Corruption Matrix 실행 근거

- `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_child_parent_references_integration.py -v`: `3 passed`.
- `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/integration/test_corruption_matrix_integration.py -v`: `5 passed`.
- 5종 결과: Duplicate `BATCH_DUPLICATE=1`, NULL Key `REQUIRED_NULL=1`·`KEY_NULL=1`, Broken FK `BROKEN_REFERENCE=1`, Invalid Status `STATUS_DOMAIN_INVALID=1`, Negative Value `NUMERIC_RANGE_INVALID=1`.

### Observability SQL 실행 근거

- `RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_observability_run_status_integration.py -v`: `1 passed`.

### Clean Clone 실행 근거

- 실행 일시: `2026-09-20T09:29:30Z`.
- Commit `dbf66fb` 기준 `./scripts/verify_clean_clone.sh` 실행 완료.
- 로그: `/tmp/cdp-clean-clone-20260920T092930Z.log`.
- `uv sync --frozen`: 성공, 76개 Package 설치.
- `uv run ruff check .`: `All checks passed!`.
- `uv run pytest`: `185 passed, 80 skipped`.
- Seed: `seed_run_id=174458da-ddb0-4d19-9f30-c98729f4720a`, Raw Checksum `e290791d496f2443eeb5f78e23f977adec618f82f843a265ed4362b76822ec33`.
- Generator: `generator_run_id=3cfe794a-b2cf-4384-adfc-90ac97f21c92`, 주문 20건·Item 34건·Payment 20건 생성.
- 9개 Table Ingestion: `customers`, `customer_membership_tiers`, `products`, `sellers`, `orders`, `order_items`, `order_payments`는 `SUCCESS`; `customer_subscriptions`, `subscription_payments`는 `SUCCESS_NO_DATA`; 거부 Row 0건.
- Publish CLI: Warehouse Publish Run `03207743-a264-4cb1-a37a-84141d27f101` `PUBLISHED`.
- Integration Gate: `75 passed, 1 skipped, 189 deselected`.
- 마지막 줄: `clean clone verification passed`.

### Publish Gate 실행 근거

- 실패 Run `f7b2b395-0d47-4acb-9a51-357e2ebe31a9`는 구독 결제 상태 Test 대소문자 회귀로 `DBT_TEST_ERROR`가 발생했고 Build를 `failed/`에 격리했다.
- 성공 Run `53f2c77c-77fb-44c0-a24a-f4fcc2e41531`는 E2E Fixture와 전역 PUBLISHED Chain Test 보정 후 `PUBLISHED`로 완료했다. 직전 성공 Run은 `5a15484a-b831-4535-904d-1d9a6709228b`로 연결됐다.

## 파일·폴더별 변경 요약

| 경로 | 구분 | 변경 내용 |
| ---- | ---- | --------- |
| `src/warehouse/errors.py`, `src/ingestion/errors.py` | 생성·수정 | Warehouse 오류 분류와 기본 `UNKNOWN_ERROR`를 추가했다. |
| `sql/metadata/005_create_mart_publish_runs.sql`, `src/warehouse/publish_metadata.py` | 생성 | Publish 상태 전이와 활성 실행 Mutex를 구현했다. |
| `src/warehouse/dbt_runner.py`, `src/warehouse/publish.py` | 생성 | 격리 Build, dbt 결과 분류, 원자 파일 교체, 복구 CLI를 구현했다. |
| `tests/integration/test_corruption_matrix_integration.py`, `tests/integration/test_child_parent_references_integration.py` | 생성·수정 | Corruption Matrix 5종과 Snapshot 이후 Parent Commit 비가시성 검증을 추가했다. |
| `tests/integration/test_observability_run_status_integration.py` | 생성 | Batch별 성공·빈·실패·재실행·Publish 실패 상태를 관측 SQL로 검증한다. |
| `tests/integration/test_publish_gate_dbt_integration.py` | 생성 | 실제 dbt Canary 실패가 Published Warehouse Hash와 Row Count를 바꾸지 못함을 검증한다. |
| `tests/integration/test_order_e2e_and_late_order_mart_integration.py` | 수정 | 정상 E2E 2건에서 dbt `relationships` Test 실행·통과·Failure 0건을 `run_results.json`으로 직접 검증한다. |
| `scripts/verify_clean_clone.sh` | 수정 | Raw Dataset을 새 Clone의 `data/raw/` 아래에 정확히 복사하고 Phase 0~7 전체 검증 로그를 남긴다. |
| `src/warehouse/mart_hash.py` | 수정 | Mart별 행 수 계산을 추가했다. |
| `airflow/dags/warehouse_pipeline_dag.py`, `src/rebaseline.py` | 수정 | DAG Publish Task 체인과 Rebaseline 활성 Publish 방어를 추가했다. |
| `src/ingestion/rules.py`, `src/ingestion/validation.py`, `src/ingestion/corruption.py`, `src/ingestion/service.py` | 생성·수정 | 검증 규칙 등록부와 Page 단위 PK 중복 오염을 추가했다. |
| `dbt/tests/`, `dbt/models/marts/facts/schema.yml` | 생성·수정 | 상태·FK·금액·시간·합계·Canary 품질 Gate를 추가했다. |
| `sql/validation/observability_run_status.sql` | 생성 | Batch별 Ingestion·Publish 상태 단일 조회를 추가했다. |
| `scripts/verify_clean_clone.sh`, `README.md` | 생성·수정 | 새 Clone 재현 및 수동 Publish 절차를 추가했다. |
| `tests/` | 생성·수정 | 오류, 메타데이터, Publish, 규칙, 품질 계약 테스트를 추가했다. |

## 권장 Commit

```text
feat: enforce data quality and safe mart publishing
```

## 다음 Phase 인계

Phase 8은 새 기능 추가보다 Phase 0~7에서 구현한 실패·충돌·재처리 계약을 의도적으로 깨뜨리고, 탐지와 복구 과정을 Runbook으로 증명한다.
