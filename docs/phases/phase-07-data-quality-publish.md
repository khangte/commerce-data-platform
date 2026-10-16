# Phase 7. Data Quality & Publish

> 상태: Done  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 6. Dimensional Modeling](phase-06-dimensional-modeling.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.25](../../PRD_v1.25.md), [Mart Grain 계약](../reference/mart-grain.md)

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
| `dbt/tests/dim_customer_*.sql` | 고객 SCD2 구간 비중복, 고객별 Current Version 1건, 등급 하락 금지를 검증한다. |
| `dbt/tests/dim_subscription_*.sql` | 계약 허용 상태 전이, 재가입 측정값을 검증한다. |
| `dbt/models/marts/facts/schema.yml` | `fct_subscription_payment`의 `customer_key` `not_null`·`relationships` 제약으로 고객 SCD2 Version 누락을 검증한다. |

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
- [x] 056 승인: 주문 결제 구매일의 NULL 금지·날짜 FK·주문 Fact와의 구매일 일치 Test를 추가하고, 선언 및 날짜 불일치 탐지를 격리 DuckDB에서 검증했다.
- [x] 056 운영 반영: 전체 새로고침 Run `3520e201-f49a-4aa6-9d67-377fbcd44e73`와 다음 정기 Run `467bb050-ec75-4f69-a1bb-4ec1f56e962c`의 품질 Test 144건이 각각 통과했다. 전체 새로고침에서 결제 구매일 NULL·주문 날짜 불일치·날짜 FK 누락은 0건이며, 상세 증적은 Phase 6 운영 체크리스트에 기록했다.

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

2026-09-22 발견한 Publish 경로 결함: `metrics` Model이 View로 실체화돼 있었다. ADR-016의 Build 파일 교체 이후 Published Warehouse의 모든 `rpt_*` View가 `Binder Error: Catalog "…" does not exist!`로 실패했다. 수정(`marts.metrics` → `table`)은 Phase 10 Task 1(`docs/superpowers/plans/2026-09-22-phase10-bi.md`)이 맡았고, Published Run `a10d06c3`에서 `rpt_*` 3개가 `BASE TABLE`임을 확인했다. 이 결함을 주장한 `P7-*` 항목은 없으므로 Phase 7 Task는 다시 열지 않는다([042](../architect-review/042_mart-grain-column-drift-ruling.md) §3).

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

## 게시 파일 식별값 전환 (2026-10-16)

3번 작업 검증: 전체 테스트 394건 통과·107건 생략, PostgreSQL 통합 테스트 52건 통과·29건 생략. 통합 실행 플래그에 따라 외부 시스템을 요구하는 일부 테스트는 생략된다.

- [x] 공통 DuckDB 파일 함수와 `(st_ino, st_mtime_ns)` 식별값을 분리하고 기존 import 경로를 유지한다.
- [x] 게시 해시와 행 수를 대상 테이블별 단일 스캔으로 계산한다.
- [x] nullable 열의 반복 적용과 게시 직전 파일 식별값 기록을 실제 PostgreSQL로 검증한다.
- [x] 중단된 게시 복구를 기록된 파일 식별값 비교로 전환한다.
- [x] 서빙 내보내기를 복사 전·교체 전 식별값 검사로 전환하고 검증한다.
- [x] PRD v1.24, 전체 테스트, 실제 게시·서빙·Metabase 카드 검증을 완료한다.


최종 검증: 전체 테스트 396건 통과·109건 생략, `RUN_POSTGRES_INTEGRATION=1` 통합 테스트 54건 통과·29건 생략, `ruff check .`와 공백 검사를 통과했다. 외부 연동별 실행 플래그가 없는 테스트는 생략됐다. 검증 시작 전과 실제 게시 직전 `mart_publish_runs`의 `BUILDING`·`PUBLISHING`은 각각 0건이었다. 정상 명령 `uv run python -m src.warehouse.publish --pipeline-name manual_run`으로 실행 `539746b5-32f5-4df4-a57e-dbf442db18f8`를 게시했고 dbt 테스트 143건 통과·0건 실패, 9개 해시 대상의 변경 없음과 기록 식별값 `(1020866, 1792141253195265434)`의 실제 파일 일치를 확인했다. `uv run python -m src.serving`은 1.67초에 내보내기 `85ecffe0-9401-497d-be6b-11f9c39c1a1b`를 만들었고 Manifest의 게시 실행이 일치했다. 기존 `repoint_and_prune_serving`으로 Metabase DB 2번을 `/serving/exports/85ecffe0-9401-497d-be6b-11f9c39c1a1b.duckdb`로 재연결해 Manifest를 검증했다. 카드 49·50·51을 각각 1회 실행해 오류 없이 74·10·10행을 확인했고 저장된 쿼리를 보존했다. 원시 로그·응답은 `/tmp/publish-identity-dev/`에 남겼다. 5.3초는 architect의 기존 해시 재계산 기준값이며 동일 환경의 전후 전체 시간을 이번에 다시 측정한 값은 아니다.

## 서빙 갱신 절차 통합 (2026-10-16)

- [x] 호스트 주소는 프로세스 설정만 사용하고 키·DB ID는 빈 값을 제외한 프로세스 설정과 루트 `.env` 순서로 읽는다.
- [x] CLI 내보내기를 재지정·조건부 정리까지 연결하고 최신 상태 확인 명령을 추가한다.
- [x] 카드 `--apply`는 변경 0건이어도 모든 GET 뒤 첫 PUT 전에 DB마다 실제 매니페스트를 검사한다.
- [x] DAG 전체 새로고침 기본값은 거짓으로 두고 기존 서빙 작업·재시도·매니페스트 구조를 유지한다.
- [x] 운영 절차와 PRD v1.25를 동기화하고 기존 PRD를 원문 그대로 보관한다.
- [x] 전체 테스트·마커별 결과와 실제 CLI 내보내기·정리·상태·카드 49–51·반영 사전검사를 검증한다.

최종 검증: 전체 테스트 437건 통과·109건 생략·실패 0건, Ruff·공백 검사 통과. 마커별 통과/실패/생략은 미지정 436/0/4, integration 1/0/100, reliability 0/0/18, airflow 0/0/5, benchmark 0/0/0이다. 복수 마커는 중복 집계되며 외부 연동 실행 플래그가 없는 테스트는 생략됐다. 인프라 관련 실패는 없었다. 실제 `uv run python -m src.serving export` 한 번으로 내보내기 `a3b6b240-e08c-4fcf-9c60-3e41c1247747`와 `metabase_repoint=SUCCESS`를 확인했고 새 파일 포함 3개만 남았다. Metabase DB 2의 `details.database_file`이 `/serving/exports/a3b6b240-e08c-4fcf-9c60-3e41c1247747.duckdb`와 일치했다. `status`는 종료 0·`fresh=true`이며 실제 실행과 최신 게시 실행 `539746b5-32f5-4df4-a57e-dbf442db18f8`가 같았다. 카드 49·50·51을 각각 1회 실행해 오류 없이 74·10·10행을 확인했다. `uv run python scripts/metabase_import.py 49 50 51 --apply`는 최신 여부 검사를 통과했고 쿼리 변경 0건으로 종료 0이었다. 기존 PRD 보관본과 매니페스트 기록·인증 전송·재지정·DAG 서빙 두 작업의 원문 또는 AST 보존도 검증했다. 원시 결과는 `/tmp/serving-refresh-dev/`에 보관하며 커밋하지 않았다.

## 게시 실행 생존 신호와 안전한 인수 (061, 2026-10-16)

- [x] `heartbeat_at` nullable 컬럼을 멱등적으로 추가하고 새 실행은 시작 시각으로 초기화한다. 기존 NULL 행은 시작 시각으로 만료를 판단한다.
- [x] 준비·dbt 실행·게시 단계에서 30초 daemon 갱신과 정상 종료 갱신을 적용한다. DB 오류는 예외 종류만 경고하고 재시도하며, 주기 갱신의 소유권 상실 시 갱신을 멈춘다. 061 §2.1.1에 따라 정상 종료 시 마지막 갱신의 False는 경고 없이 무시하고, 마지막 갱신 DB 예외는 경고한다.
- [x] 마지막 생존 신호 뒤 5분이 지난 후보를 상태·시각 조건부 UPDATE로 먼저 종료한다. 갱신 경합 시 파일을 보존하고, 인수 성공 후에만 정리·격리·실패 경로 기록을 수행한다.
- [x] 이전 버전 PRD를 원문 보관하고 v1.27·복구 절차를 동기화한다.
- [x] 전체 테스트 464개 통과·121개 건너뜀·실패 0개(91.00초), 대상 단위·PostgreSQL 통합 31개 통과(12.97초), Ruff·diff 검사를 통과했다. 마커별 통과/실패/건너뜀은 미지정 463/0/4, integration 1/0/112, reliability 0/0/18, airflow 0/0/5, benchmark 0/0/0이며 마커 중복을 포함한다.
- [x] 운영 활성 게시·실행 중인 Warehouse DagRun 0건과 빈 build/를 확인한 뒤, dbt 진입 후 setsid 프로세스 그룹 전체를 SIGKILL했다. 이전 실행 `5d25b265-ef74-4c0c-8274-af1697c609f7`의 마지막 생존 신호는 12:59:57.527017 UTC, 종료 시각은 13:00:01.547073 UTC였다. 즉시 재시도는 PublishInProgressError로 차단됐고 기존 게시 식별값을 보존했다.
- [x] 실제 5분 뒤 13:04:59.302316 UTC에 인수했다(마지막 신호 뒤 301.775299초). 이전 행은 FAILED·UNKNOWN_ERROR·abandoned이고 격리 파일은 `data/warehouse/failed/5d25b265-ef74-4c0c-8274-af1697c609f7.duckdb`다. 새 실행 `f9960f3b-f5ac-40e2-bb8a-14cb0891ce00`은 PUBLISHED·품질 테스트 143개 통과·실패 0개·해시 변경 없음이다. Export `7e0deae2-ce49-41cf-ab65-445dcbdabe8e`의 재지정 SUCCESS·status 종료 0·fresh=true와 build/ 잔여 0개를 확인했다. 실환경 로그에 키·DB 비밀번호는 없으며 증적은 `.team/developer/061-review.md`와 `061-live-result.json`에 남겼다.

- [x] 061 reviewer 후속(§2.1.1): 마지막 갱신 False의 불필요한 경고를 제거하고 주기 갱신 False 경고·중단 및 마지막 DB 예외 경고를 유지했다. 실패 테스트 확인 후 수정했으며 대상 단위·PostgreSQL 통합 32개 통과(12.92초), 전체 465개 통과·121개 건너뜀·실패 0개(91.29초), Ruff·diff 검사를 통과했다. 기존 실환경 검증 기록을 재사용하고 추가 운영 실행은 하지 않았다.

## 파일·폴더별 변경 요약

- `sql/metadata/005_create_mart_publish_runs.sql`, `src/warehouse/publish_metadata.py`, `tests/integration/test_publish_metadata_integration.py`: 생존 신호 컬럼·활성 갱신·COALESCE 만료 판정·조건부 종료·실패 경로 기록을 추가했다. NULL 호환·멱등 적용·종료 후 갱신 거부·조회 후 갱신 경합을 실제 PostgreSQL로 검증했다.
- `src/warehouse/publish_heartbeat.py`, `src/warehouse/publish.py`, `tests/test_publish_heartbeat.py`, `tests/integration/test_warehouse_publish_integration.py`: daemon 갱신·안전한 경고·단계 종료 갱신과 인수 후 파일 처리를 구현했다. reviewer 후속 판정으로 주기 갱신 False만 경고·중단하고 종료 갱신 False는 무시하며, 종료 DB 예외 경고는 유지한다. 두 False 경로를 별도 단위 테스트로 검증했다. 오래된 정상 실행 보존·두 복구 분기의 갱신 경합·인수된 소유자의 전이 거부·실제 SIGKILL 후 만료 전 차단과 만료 뒤 인수를 검증했다.
- `PRD.bak/PRD_v1.26.md`, `PRD_v1.27.md`, `docs/adr/016-publish-mart-via-warehouse-file-swap.md`, `docs/troubleshooting/warehouse-publish.md`, `docs/troubleshooting/README.md`, `docs/phases/phase-07-data-quality-publish.md`: 생존 신호 시각·5분 소유 유지 기간·행 종료 후 파일 처리 계약과 진행 기록을 동기화했다.


- [x] Bronze 스키마 통합: 게시·Airflow 경로와 직접 실행 테스트가 Python 계약 변수를 한 번의 `--vars`로 전달하도록 연결했다. Canary·재처리 변수는 같은 JSON에 병합한다. 리뷰 후 R-12 실행 안내도 같은 경로로 맞췄다.
- `docs/runbooks/r12-backfill-replay.md`: 재현용 Warehouse 경로를 지정하는 게시 CLI의 `--full-refresh --dbt-vars` 명령과 테스트의 `dbt_vars_json` 병합 경로를 설명했다.
- 전후 검증: 운영 Warehouse의 격리된 복사본에서 변경 전·후 전체 새로고침이 모두 통과했고 5개 Dimension·4개 Fact의 행 수·논리 해시, 고객·구독 키 목록과 관측 속성 해시가 완전히 일치했다. 운영 파일 수정·게시·커밋은 수행하지 않았다.
- 최종 검증: `pytest -q tests/test_dbt_*.py tests/test_warehouse_publish_cli.py` 64건이 통과했다. 여기에는 빈 카탈로그의 전체 Build와 모든 Fact 0행 확인이 포함된다. 세 통합 실행 플래그를 활성화한 게시 차단·R-14 테스트 2건도 통과했다. 실제 dbt로 변수 없는 `parse` 성공, 파일이 있는 카탈로그에서도 변수 없는 Source·카탈로그 검증 실행의 명확한 계약 오류를 확인했다. 직접 명령 생성부 8개의 단일 병합 변수 전달, Ruff·`git diff --check`도 통과했다.
- `src/warehouse/dbt_runner.py`: `dbt_vars` 매핑을 생성된 Bronze 계약과 병합하고 `--vars`를 정확히 한 번 전달한다. `extra_args`로 별도 `--vars`를 넘기면 실행 전에 거절한다.
- `src/warehouse/publish.py`: `--dbt-vars`의 YAML 매핑을 해석하고 잘못된 입력·예약 키를 게시 시작 전에 차단한다. 전체 새로고침 인자는 유지한다.
- `tests/test_dbt_runner.py`, `tests/test_warehouse_publish_cli.py`: Canary·재처리·스키마의 동시 전달과 단일 `--vars`, CLI 입력 오류를 검증한다.
- `tests/integration/test_publish_gate_dbt_integration.py`: Canary 설정을 별도 인자 대신 병합할 매핑으로 전달한다. R-14도 이 Runner를 재사용한다.
- `tests/integration/test_replay_boundary_hash_integration.py`, `tests/integration/test_incremental_full_refresh_hash_integration.py`, `tests/integration/test_order_e2e_and_late_order_mart_integration.py`, `tests/integration/test_subscription_payment_temporal_join_integration.py`: 직접 dbt 호출의 변수를 공통 헬퍼로 생성하고 재처리 경계와 병합한다. 이 헬퍼를 공유하는 이력·재수집 신뢰성 테스트도 같은 경로를 사용한다.

- 최종 검증: `pytest -q tests/test_dbt_*.py` 30건, `tests/test_warehouse_quality_contract.py` 4건이 통과했다. `RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1`로 게시 차단 통합 테스트와 구독 결제 시점 결합 통합 테스트를 실행해 2건 통과했다. Ruff·`git diff --check`도 통과했다. 기존 매크로·관측 모델·원천 코드·BI 파일은 수정하지 않았다.

- 전환 검증: 운영 Warehouse의 격리된 복사본에서 `dbt parse`가 통과했다. 증분 build는 `accepted_values_fct_subscription_payment_payment_status__COMPLETED__FAILED`만 실패했고 전체 새로고침 build는 통과했다. 기존 `completed` 2건·`failed` 3건이 `COMPLETED` 2건·`FAILED` 3건으로 바뀌었으며 5행·총액 149.50·성공 금액 59.80을 보존했다.

- [x] 구독 결제 상태 표준화: 대문자 상태에 맞춰 성공 금액·실패 코드 검증과 Python 계약·시점 결합 기대값을 갱신했다. 리뷰 후 운영 복구 절차를 추가하고 검증 기록을 이 문서로 통합했다.
- `README.md`, `docs/runbooks/r14-dbt-failure.md`: 첫 게시의 전체 새로고침 필수 명령, 기존 소문자 행으로 인한 허용값 실패와 게시 차단을 설명했다.
- `docs/phases/phase-05-bronze-catalog-and-staging.md`, `docs/phases/phase-06-dimensional-modeling.md`: 중복 검증 문단을 이 문서 참조로 바꿨다. 전환 증적에서 임시 파일 경로를 제거했다.
- `dbt/tests/fct_subscription_payment_completed_value_consistent.sql`, `dbt/tests/fct_subscription_payment_failure_code_consistent.sql`: `COMPLETED`·`FAILED` 조건으로 금액과 실패 코드의 정합성을 검사한다.
- `tests/test_warehouse_quality_contract.py`: Staging·Fact·Report의 대문자 허용값과 NULL 금지를 확인한다.
- `tests/integration/test_subscription_payment_temporal_join_integration.py`: 원천 소문자 상태에 대응하는 대문자 Fact 상태를 기대하도록 변경했다. 원천 생성·제약·생성기 테스트는 수정하지 않았다.

- [x] dbt 정리: 그룹화·`UNION`으로 보장되거나 Fact에서 같은 행·키를 검사하는 중복 SQL 테스트 7개를 제거하고 참조를 정리했다.
- `dbt/tests/`: `rpt_customer_order_activity_daily_unique.sql`, `rpt_membership_tier_performance_unique.sql`, `rpt_subscription_payment_outcomes_daily_unique.sql`, `int_affected_order_keys_unique.sql`, `int_affected_subscription_payment_keys_unique.sql`, `stg_order_items_unique.sql`, `stg_payments_unique.sql`을 삭제했다. `dim_subscription_version_unique.sql`은 R-10 증적이며 속성 해시가 다른 동일 계약·시작 시각의 중복을 단독 검증하므로 유지한다.
- `tests/test_warehouse_quality_contract.py`: 삭제한 파일 존재 단언을 제거하고 날짜·고객 구분 도메인 검증은 유지했다.
- `docs/phases/phase-10-bi.md`: 과거 추가 이력을 보존하면서 고객 활동 Report의 중복 유일성 테스트 제거를 기록했다.
- 검증: 최종 `dbt parse`, dbt 관련 Python·품질 계약 테스트 34건, 실제 dbt build 성공·품질 실패 차단·재게시 해시 보존 통합 테스트 1건이 통과했다. 세 통합 실행 플래그를 모두 활성화했으며 상세 명령은 Phase 6 변경 요약에 기록했다.

| 경로 | 구분 | 변경 내용 |
| ---- | ---- | --------- |
| `airflow/dags/warehouse_pipeline_dag.py`, `tests/test_airflow_dags.py` | 수정 | `full_refresh` 기본값 거짓과 명시적 참일 때만 dbt 전체 새로고침 인자 전달을 검증한다. 기존 두 서빙 작업의 AST를 보존했다. |
| `src/warehouse/publish.py`, `tests/test_warehouse_publish_cli.py` | 수정 | CLI 게시 성공 뒤에만 서빙 내보내기 실행을 stderr로 안내하며 실패·복구 전용 실행에서는 출력하지 않는다. |
| `PRD.bak/PRD_v1.24.md`, `PRD_v1.25.md`, `docs/phases/phase-07-data-quality-publish.md` | 이동·생성·수정 | 갱신·최신 상태 확인·카드 반영 전 검사와 DAG 전체 새로고침 계약을 문서화했다. |
| `PRD.bak/PRD_v1.23.md`, `PRD_v1.24.md`, `docs/phases/phase-07-data-quality-publish.md` | 이동·생성 | 기존 PRD를 보관하고 파일 식별값 기록·복구·내보내기, 9개 해시 대상과 기존 실행의 호환성 제한을 v1.24에 문서화했다. |
| `src/warehouse/duckdb_files.py`, `tests/test_duckdb_files.py` | 생성 | 공통 경로·WAL 경로·CHECKPOINT·디렉터리 fsync와 파일 식별값을 분리한다. 원자 교체 보존과 같은 inode의 수정 시각 변경을 검증한다. |
| `src/warehouse/mart_hash.py`, `tests/test_mart_hash.py` | 수정 | 단일 스캔으로 해시와 행 수를 계산하며 기존 공개 함수 시그니처와 빈 테이블 결과를 보존한다. |
| `sql/metadata/005_create_mart_publish_runs.sql`, `src/warehouse/publish_metadata.py`, `src/warehouse/publish.py`, `airflow/dags/warehouse_pipeline_dag.py` | 수정 | nullable `published_inode`·`published_mtime_ns`를 멱등적으로 추가하고 교체 직전 파일 식별값을 같은 상태 UPDATE와 DAG 전달값에 기록한다. 오래된 `PUBLISHING`은 해시 대신 inode·수정 시각이 모두 일치할 때만 `PUBLISHED`로 복구하며 누락·불일치는 `FAILED(UNKNOWN_ERROR)`로 닫는다. |
| `tests/integration/test_publish_metadata_integration.py`, `tests/integration/test_warehouse_publish_integration.py`, `tests/test_warehouse_publish_cli.py` | 수정 | 반복 마이그레이션·저장값 조회·원자 교체 뒤 식별값 보존과 확장된 CLI 결과를 검증한다. |
| `src/warehouse/errors.py`, `src/ingestion/errors.py` | 생성·수정 | Warehouse 오류 분류와 기본 `UNKNOWN_ERROR`를 추가했다. |
| `sql/metadata/005_create_mart_publish_runs.sql`, `src/warehouse/publish_metadata.py` | 생성 | Publish 상태 전이와 활성 실행 Mutex를 구현했다. |
| `src/warehouse/dbt_runner.py`, `src/warehouse/publish.py` | 생성 | 격리 Build, dbt 결과 분류, 원자 파일 교체, 복구 CLI를 구현했다. |
| `dbt/profiles.yml` | 수정 | WSL 자원 상한을 위해 DuckDB 내부 `memory_limit: '2GB'`와 `threads: 2`를 설정했다. |
| `tests/integration/test_corruption_matrix_integration.py`, `tests/integration/test_child_parent_references_integration.py` | 생성·수정 | Corruption Matrix 5종과 Snapshot 이후 Parent Commit 비가시성 검증을 추가했다. |
| `tests/integration/test_observability_run_status_integration.py` | 생성 | Batch별 성공·빈·실패·재실행·Publish 실패 상태를 관측 SQL로 검증한다. |
| `tests/integration/test_publish_gate_dbt_integration.py` | 생성 | 실제 dbt Canary 실패가 Published Warehouse Hash와 Row Count를 바꾸지 못함을 검증한다. |
| `tests/integration/test_order_e2e_and_late_order_mart_integration.py` | 수정 | 정상 E2E 2건에서 dbt `relationships` Test 실행·통과·Failure 0건을 `run_results.json`으로 직접 검증한다. |
| `scripts/verify_clean_clone.sh` | 수정 | Raw Dataset을 새 Clone의 `data/raw/` 아래에 정확히 복사하고 Phase 0~7 전체 검증 로그를 남긴다. |
| `src/warehouse/mart_hash.py` | 수정 | Mart별 행 수 계산을 추가했다. |
| `airflow/dags/warehouse_pipeline_dag.py`, `src/rebaseline.py` | 수정 | DAG Publish Task 체인과 Rebaseline 활성 Publish 방어를 추가했다. |
| `src/ingestion/rules.py`, `src/ingestion/validation.py`, `src/ingestion/corruption.py`, `src/ingestion/service.py` | 생성·수정 | 검증 규칙 등록부와 Page 단위 PK 중복 오염을 추가했다. |
| `dbt/tests/`, `dbt/models/marts/facts/schema.yml` | 생성·수정 | 상태·FK·금액·시간·합계·Canary 품질 Gate를 추가했다. |
| `dbt/models/marts/facts/schema.yml`, `dbt/tests/fct_order_payment_date_matches_order.sql` | 생성·수정 | 056 승인으로 주문 결제의 `purchase_date_key`(주문 구매일 키)에 NULL 금지·`dim_date.date_key` 참조를 추가하고 주문 Fact와 다른 구매일을 찾는 Singular Test를 추가했다. |
| `tests/test_warehouse_quality_contract.py`, `tests/test_order_payment_date_key.py` | 생성·수정 | 구매일 FK와 Singular Test 선언, UTC 날짜 경계·결제 행 보존·증분 필터·불일치 탐지를 검증했다. 관련 로컬 테스트 28건이 통과했으며 운영 Publish와 과거 Phase 8·9 해시 증거 재계산은 수행하지 않았다. |
| `sql/validation/observability_run_status.sql` | 생성 | Batch별 Ingestion·Publish 상태 단일 조회를 추가했다. |
| `scripts/verify_clean_clone.sh`, `README.md` | 생성·수정 | 새 Clone 재현 및 수동 Publish 절차를 추가했다. |
| `tests/` | 생성·수정 | 오류, 메타데이터, Publish, 규칙, 품질 계약 테스트를 추가했다. |

## 권장 Commit

```text
feat: enforce data quality and safe mart publishing
```

## 다음 Phase 인계

Phase 8은 새 기능 추가보다 Phase 0~7에서 구현한 실패·충돌·재처리 계약을 의도적으로 깨뜨리고, 탐지와 복구 과정을 Runbook으로 증명한다.
