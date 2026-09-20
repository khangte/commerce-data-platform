# Phase 7 Data Quality & Publish 설계

> 작성: architect, 2026-09-18
> 기준: [phase-07](../../phases/phase-07-data-quality-publish.md), PRD v1.12 §11·§13.2·§17·§18·§22, [mart-grain](../../reference/mart-grain.md)
> 상태: 승인 (2026-09-18, Q1~Q4 사용자 승인). 기준 PRD는 v1.13

## 1. 범위 요약

Phase 7 Task는 21개(`P7-01`~`P7-21`)다. 세 묶음으로 나뉜다.

| 묶음 | Task | 현재 상태 |
| ---- | ---- | --------- |
| Ingestion Quality | P7-01~05 | Phase 3에서 대부분 구현. Registry·Duplicate Corruption·0 Row 경계·5종 통합 테스트가 없다. |
| Warehouse Quality | P7-06~12 | Phase 6에서 SCD2·Unique·Unknown Key 테스트 구현. 금액 Non-negative·Timestamp 순서 테스트가 없다. |
| Publish Safety + Metadata + E2E | P7-13~21 | 미구현. 가장 큰 작업이다. |

## 2. 현재 구현 갭 (코드 조사 결과)

### 2.1 Publish 경계가 없다 (핵심 결함)

- `airflow/dags/warehouse_pipeline_dag.py`의 `dbt_build_task`는 `data/warehouse/warehouse.duckdb`에 직접 `dbt build`를 실행한다.
- `dbt build`는 Model 실행 뒤 Test를 실행한다. Test가 실패해도 Dimension Table 교체와 Fact Incremental Merge는 이미 반영됐다.
- 따라서 "Build/Test 실패 시 마지막 성공 Mart 유지"가 현재 성립하지 않는다.
- DuckDB는 한 Process가 Read-Write로 연 동안 다른 Process가 파일을 열 수 없다. Build 중 Metabase가 같은 파일을 읽을 수 없다.
- `control.dbt_processed_batch` 재계산 경계는 실패 시 전진하지 않는다. 하지만 Mart Table 자체는 실패 Build 상태로 남는다.

### 2.2 Ingestion Quality 갭

| Task | 갭 |
| ---- | -- |
| P7-01 | 오류 Code가 `validation.py` 문자열 리터럴, Domain·최소값이 `tables.py`에 흩어져 있다. 단일 Registry가 없다. |
| P7-02 | Row Error(Quarantine)와 Batch Error(`SourceContractError`) 분류는 구현됐다. 분류표와 코드 일치를 검증하는 테스트가 없다. |
| P7-03 | `references.py`에 Freeze 기준 검증이 있다. Snapshot 뒤 Parent 추가 시 격리되는지 확인하는 테스트 유무를 확인해야 한다. |
| P7-04 | `test_reject_rate_allows_five_percent_and_rejects_more`는 경계와 초과만 검증한다. 0 Row와 0 Reject 경우가 없다. |
| P7-05 | `corruption.py`에 `DUPLICATE` 종류가 없다. Corruption Matrix의 Duplicate를 재현할 수 없다. 5종 통합 테스트도 없다. |

### 2.3 Warehouse Quality 갭

| Task | 갭 |
| ---- | -- |
| P7-06 | Mart `schema.yml`에 Generic Test 선언이 약 43개 있다. PRD §17 목록 대비 누락 여부를 감사해야 한다. |
| P7-07 | `fct_order_item.item_price`, `fct_order_item.freight_value`, `fct_order_payment.payment_value`의 `>= 0` 테스트가 없다. |
| P7-08 | `delivered_at >= purchase_at`, `approved_at >= purchase_at` 테스트가 없다. `fct_order`에 Timestamp Column이 없으므로 `stg_orders`에 건다. |
| P7-09 | 완료. `dim_customer_*`, `dim_subscription_*` Overlap·Current 테스트가 있다. |
| P7-10 | Fact Unique 테스트는 있다. `relationships` 기반 FK Missing 커버리지를 감사해야 한다. |
| P7-11 | Phase 6 AC-12 판정(`architect-review/005`)에서 구현. 재확인만 한다. |
| P7-12 | Fan-out 회귀 테스트(주문 Header 금액 = Item 합계)가 명시적으로 없다. |

Olist Raw 측정값: `approved_at < purchase_at` 0건, `delivered_at < purchase_at` 0건, `carrier_at < purchase_at` 166건.
PRD §17 계약은 `approved_at`과 `delivered_at`만 요구한다. `carrier_at`은 테스트하지 않는다. 정상 데이터 False Positive 0을 지킨다.

### 2.4 Observability 갭

- `src/ingestion/errors.py`에 `DBT_BUILD_ERROR`, `DBT_TEST_ERROR`, `UNKNOWN_ERROR`가 없다.
- `classify_error`는 모르는 예외를 `CONFIGURATION_ERROR`로 분류한다. PRD §18 의미와 다르다.
- Publish Run을 기록할 Metadata Table이 없다.

## 3. 설계 결정

### D1. Publish 전략: 별도 Warehouse File Build → 검증 → 원자적 교체

**결정.** Published File과 Build File을 분리한다. Build File에서 `dbt build`가 성공한 경우에만 `os.replace`로 Published File을 교체한다.

| 경로 | 역할 |
| ---- | ---- |
| `data/warehouse/warehouse.duckdb` | Published File. 경로를 유지한다. Metabase와 `rebaseline.py` 경로 검증이 그대로 동작한다. |
| `data/warehouse/build/{publish_run_id}.duckdb` | Build File. 실행마다 새로 만든다. |
| `data/warehouse/failed/{publish_run_id}.duckdb` | 실패 Build 격리 위치. 최근 3개만 보존한다. |

모든 경로는 같은 Filesystem에 둔다. `os.replace`가 원자적이다.

**절차.**

1. `mart_publish_runs`에 `BUILDING` Row를 기록한다.
2. Published File이 있으면 Build File로 복사한다. 없으면 빈 Build File에서 시작한다.
3. `sync_bronze_catalog(settings, build_path)`로 Catalog를 Build File에 동기화한다.
4. `WAREHOUSE_PATH=build_path`와 실행별 `--target-path`로 `dbt build`를 실행한다.
5. 실패하면 `run_results.json`에서 오류 종류를 판정한다. Build File을 `failed/`로 옮기고 `FAILED`를 기록한다. Published File은 건드리지 않는다.
6. 성공하면 Build File에 `CHECKPOINT` 후 연결을 닫는다. `mart_logical_hashes`와 Row Count를 계산한다. `PUBLISHING`과 Hash를 기록한다.
7. `os.replace(build_path, published_path)` 후 Directory를 `fsync`한다. `PUBLISHED`를 기록한다.

**복구 (P7-15).** 시작 시 `BUILDING`/`PUBLISHING` 상태로 남은 Row를 정리한다.

- `PUBLISHING`이고 Published File Hash가 기록 Hash와 같으면 `PUBLISHED`로 확정한다.
- 그 외는 `FAILED(error_type=UNKNOWN_ERROR, error_message='abandoned')`로 닫고 Build File을 `failed/`로 옮긴다.

**이 구조로 해결되는 것.**

- Incremental Fact 이전 상태는 복사한 Build File에 그대로 있다. Incremental 전략을 바꿀 필요가 없다.
- `control.dbt_processed_batch`도 File 안에 있다. 실패 Build의 경계 전진은 Published File에 절대 도달하지 않는다.
- Build 중에도 Metabase는 Published File을 Read-only로 읽는다.
- Bronze·Watermark는 Postgres·SeaweedFS에 있다. Warehouse 실패가 이들을 변경하지 않는다.

**기각한 대안.**

| 대안 | 기각 사유 |
| ---- | --------- |
| 같은 File 안 Build Schema → Swap | DuckDB에 `ALTER SCHEMA RENAME`이 없다. Table별 Rename은 원자 단위가 아니다. `metrics` View가 Schema 이름에 묶인다. Incremental Fact 이전 상태를 Build Schema로 복사해야 한다. Build 중 Writer Lock으로 Metabase 읽기가 막힌다. |
| Published File에 `ATTACH` 후 Mart Table만 복사 | Catalog·Control·Mart를 따로 옮기는 코드가 늘어난다. 복사 도중 실패 시 부분 반영이 생긴다. |

**비용.** 현재 Warehouse File은 38.3MB다. 실행당 1회 복사 비용은 무시할 수 있다. Phase 9에서 측정한다.

**제약.** Published File은 Publish 절차만 쓴다. 동시 Publish는 `mart_publish_runs`의 활성 상태 부분 Unique Index로 막는다. Airflow Task는 별도 Process라 `fcntl` Lock이 Task 경계를 넘지 못하므로 DB Index를 Mutex로 쓴다. `rebaseline.py`는 활성 Publish가 있으면 거부한다. Ingestion CLI `--sync-catalog`의 Published File 직접 쓰기는 개발용 관리 동작으로 남기고 ADR 016 Consequences에 기록한다.

ADR: `docs/adr/016-publish-mart-via-warehouse-file-swap.md`에 위 결정과 관측 근거를 기록한다.

### D2. Publish Metadata Table

`sql/metadata/005_create_mart_publish_runs.sql`로 Postgres Metadata DB에 추가한다.

```text
mart_publish_runs
  publish_run_id           UUID PK
  pipeline_name            TEXT NOT NULL
  batch_id                 TEXT NULL        -- CLI 실행은 NULL
  dag_run_id               TEXT NULL
  dbt_invocation_id        TEXT NULL
  status                   BUILDING | PUBLISHING | PUBLISHED | FAILED
  error_type               DBT_BUILD_ERROR | DBT_TEST_ERROR | CONFIGURATION_ERROR | UNKNOWN_ERROR | NULL
  error_message            TEXT NULL
  previous_publish_run_id  UUID NULL        -- 직전 PUBLISHED Run
  mart_hashes              JSONB NULL       -- relation → logical hash
  mart_row_counts          JSONB NULL
  tests_passed / tests_failed  INTEGER NULL
  failed_path              TEXT NULL
  started_at / finished_at TIMESTAMPTZ
```

제약:

- `status IN ('BUILDING','PUBLISHING')` 부분 Unique Index로 활성 Run을 1개로 제한한다.
- `pipeline_runs`와 같은 형식의 상태·Error CHECK를 둔다.

### D3. Airflow Task Graph

PRD §13.2의 `sync_bronze_catalog → dbt_build → publish_run_summary`를 다음으로 바꾼다.

```text
verify_bronze_commit
  ↓
prepare_warehouse_build     # 복구, BUILDING 기록(Mutex), File 복사, Catalog Sync
  ↓
dbt_build                   # Build File 대상, 실패 시 격리·FAILED 기록 후 AirflowFailException
  ↓
publish_mart                # Hash 계산, PUBLISHING, os.replace, PUBLISHED
  ↓
publish_run_summary         # all_done
```

- Build Path와 `publish_run_id`는 XCom으로 전달한다. 값이 작다.
- dbt 실패는 Non-retryable이다. 현재 `AirflowFailException` 동작을 유지한다.
- Task 로직은 `src/warehouse/publish.py`에 둔다. DAG은 호출만 한다. CLI(`python -m src.warehouse.publish`)도 같은 함수를 쓴다.
- `publish_run_summary`는 `publish_run_id`, `status`, 교체 전후 Hash를 포함한다.

### D4. Error Type

- `src/ingestion/errors.py`에 `DBT_BUILD_ERROR`, `DBT_TEST_ERROR`, `UNKNOWN_ERROR`를 추가한다.
- dbt 오류 판정은 `run_results.json` 기준이다. `test` Node에 `fail`/`error`가 있으면 `DBT_TEST_ERROR`, 그 외는 `DBT_BUILD_ERROR`다.
- `classify_error`의 기본값을 `UNKNOWN_ERROR`로 바꾼다. 기존 테스트 중 기본값에 의존하는 것을 함께 수정한다. (승인 항목 Q2)

### D5. Ingestion Quality Registry (P7-01~05)

YAGNI 범위로 만든다. 새 추상화 계층을 만들지 않는다.

- `src/ingestion/rules.py`: `ValidationRule(code, order, scope)` 튜플 상수 하나. `scope`는 `ROW` 또는 `BATCH`.
- `validation.py`는 Code 문자열 대신 이 상수를 참조한다. 검증 로직 순서는 바꾸지 않는다.
- 테스트: Registry 순서가 PRD §11 순서와 같다. `validation.py`가 내보내는 Code가 전부 Registry에 있다.
- `corruption.py`에 `DUPLICATE_PRIMARY_KEY`를 추가한다. 지정 순번 Record의 PK를 직전 Record PK로 바꾼다.
- Reject Rate 테스트에 `total_rows=0`, `rejected_rows=0`, 정확히 5%, 5% 초과를 모두 넣는다.
- 통합 테스트 `tests/integration/test_corruption_matrix_integration.py`: 5종 Corruption을 `ingest_table` 경로로 주입한다. `quarantine_batches.error_counts`, Valid Row Commit, Source 무변경, Watermark 전진을 확인한다 (AC-08).

### D6. Warehouse Quality (P7-06~12)

- Generic Test `dbt/tests/generic/non_negative.sql` 1개를 만든다. `item_price`, `freight_value`, `payment_value`(주문·구독 결제)에 건다.
- Singular Test `stg_orders_timestamp_order.sql`: `approved_at < purchase_at` 또는 `delivered_at < purchase_at`인 Row를 반환한다. NULL은 통과다.
- Singular Test `fct_order_item_total_matches_order.sql`: `fct_order.gross_order_value`와 `fct_order_item.line_gross_value` 합계 차이를 반환한다 (P7-12 Fan-out). Grain 정의는 mart-grain 계약에서 확인 후 확정한다.
- P7-06·P7-10은 PRD §17 목록 대비 감사표를 phase-07 문서에 남기고 누락만 추가한다.

### D7. Metadata 조회 (P7-19, AC-13)

`sql/validation/observability_run_status.sql` 한 개로 성공·빈·실패·재실행 네 상태를 조회한다.

- `pipeline_runs`(Ingestion 상태)와 `mart_publish_runs`(Warehouse 상태)를 `batch_id`로 결합한다.
- 재실행은 `attempt_number > 1` 또는 `SKIPPED_ALREADY_COMMITTED`로 판정한다.
- 통합 테스트가 네 상태 Fixture를 만들고 SQL 한 번의 결과를 검증한다.

### D8. E2E Gate (P7-18, P7-20, P7-21)

- P7-18: 고정 주문 1건을 Source → `bronze_objects` → Catalog → `fct_order`까지 추적하는 통합 테스트. 기존 `test_order_e2e_and_late_order_mart_integration.py`를 확장한다.
- P7-20: `scripts/verify_clean_clone.sh`. 새 Clone 경로에서 Version·Health·Seed·Generator·Ingestion·Publish·dbt Test를 순서대로 실행하고 로그를 남긴다.
- P7-21: README에 위 명령을 반영한다.

## 4. 구현 순서 제안

1. D4 Error Type + D2 Metadata DDL (선행 의존성)
2. D1 `src/warehouse/publish.py` + 단위·통합 테스트 (Publish 실패 시나리오 5단계 포함)
3. D3 DAG 교체 + `tests/test_airflow_dags.py` 갱신
4. D5 Ingestion Registry·Corruption Matrix
5. D6 Warehouse Test 추가
6. D7 Observability SQL
7. D8 E2E·Clean Clone·README
8. ADR 016, phase-07 문서 `파일·폴더별 변경 요약`, 상태 갱신

1~3이 Publish Safety 핵심이다. 4~6은 서로 독립이라 병행할 수 있다.

## 5. lead 승인 필요 항목

- **Q1. PRD 변경.** D3는 PRD §13.2 Task Graph를, D2는 §12에 새 Table을 추가한다. PRD v1.13 작성과 `PRD.bak/` 이동이 필요하다. 사용자 승인 대상이다.
- **Q2. `classify_error` 기본값 변경.** `CONFIGURATION_ERROR` → `UNKNOWN_ERROR`. Phase 3 동작 변경이다.
- **Q3. Phase 6 상태.** `phase-06-dimensional-modeling.md` 헤더가 아직 `In Progress`다. 커밋 `5516261`은 Phase 6 종료를 기록했다. Phase 7 DoD 첫 항목이 "모든 P6 완료"라서 헤더를 `Done`으로 갱신해야 한다.
- **Q4. 실패 Build 보존 개수.** 3개로 제안한다. File당 약 40MB다.

## 6. 위험

| 위험 | 대응 |
| ---- | ---- |
| Published File에 WAL이 남은 상태로 복사 | Published File은 `CHECKPOINT` 후 닫힌 Build File만 교체로 만든다. 복사 전 `.wal` 존재 시 `CONFIGURATION_ERROR`로 중단한다. |
| Metabase가 교체 전 File Handle을 유지 | 기존 연결은 이전 inode를 계속 읽는다. 새 연결부터 새 File을 본다. Reload 정책은 Phase 10 ADR-012에서 정한다. |
| `rebaseline.py`가 Published File을 삭제 | 활성 Publish Run이 있으면 거부한다. 경로 검증은 그대로 둔다. |
| 테스트가 실행하는 `dbt` 명령이 기본 `WAREHOUSE_PATH`에 쓰기 | 기존 테스트는 `tmp_path`를 쓴다. 새 테스트도 같은 규칙을 따른다. |
