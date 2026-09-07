# Phase 4. Airflow Orchestration

> 상태: Planned  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 3. Incremental Ingestion](phase-03-incremental-ingestion.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.4](../../PRD_v1.4.md)

## 목표

Phase 3에서 독립적으로 검증된 Python Pipeline을 Apache Airflow가 일정, 의존성, 재시도, 실행 상태에 맞게 오케스트레이션하도록 한다. DAG는 데이터 처리 로직을 소유하지 않고 호출과 제어 흐름만 담당한다.

## 핵심 계약

- Airflow Task 내부에 Extract/Validate/Commit 로직을 복제하지 않는다.
- `warehouse_pipeline_dag`는 Run 전체에서 Global Source Lease를 보유한다.
- Table Task는 Dynamic Task Mapping으로 실행하며 각자 Table Lease와 Metadata 상태를 사용한다.
- COMMITTED Table Batch는 재실행 시 재사용하고 FAILED Table만 다시 실행한다.
- XCom에는 식별자와 작은 Metadata만 저장한다.
- dbt 실패는 이미 Commit된 Bronze와 Watermark를 되돌리지 않는다.
- DuckDB Single-writer 제약 때문에 Warehouse DAG는 `max_active_runs=1`이다.

## 선행 조건

- Phase 3 Python API가 Airflow 없이도 E2E 테스트를 통과했다.
- Airflow Metadata Database와 Connection/환경 변수 계약이 준비됐다.
- Source, Metadata, SeaweedFS Health Check가 제공된다.

## 구현 순서

### 1. Airflow Runtime

- [ ] `P4-01` Airflow 3.3.1 Compose Service와 LocalExecutor 구성
- [ ] `P4-02` Airflow Metadata DB 초기화와 Health Check 구성
- [ ] `P4-03` Secret을 코드에 넣지 않는 Connection 설정
- [ ] `P4-04` DAG Import/Parse Smoke Test 구성

### 2. Generator DAG

- [ ] `P4-05` `source_simulation_dag` 구현
- [ ] Generator Config/Logical Date를 Phase 2 API로 전달
- [ ] Source Mutation Lease 획득 실패를 명시적 상태로 기록
- [ ] Generator 결과 Count/Hash/Run ID만 XCom에 반환

### 3. Warehouse DAG

- [ ] `P4-06` `warehouse_pipeline_dag` 구현
- [ ] `P4-07` `initialize_run`에서 Batch/Run Identity와 Global Lease 구성
- [ ] `P4-08` 6개 Table `extract_validate_load` Dynamic Task Mapping
- [ ] `P4-09` `verify_bronze_commit` 구현
- [ ] `P4-10` `sync_bronze_catalog` 호출
- [ ] `P4-11` dbt Build 호출 경계 구성
- [ ] `P4-12` `publish_run_summary`와 최종 상태 기록
- [ ] `P4-13` 성공/실패에 관계없이 Global Lease를 해제하는 Cleanup Task

Task Graph:

```text
initialize_run / acquire_global_source_lease
    ↓
extract_validate_load[
    customers,
    products,
    sellers,
    orders,
    order_items,
    order_payments
]
    ↓
verify_bronze_commit
    ↓
sync_bronze_catalog
    ↓
dbt_build
    ↓
publish_run_summary

all terminal paths
    ↓
release_global_source_lease
```

### 4. Retry와 오류 분류

- [ ] `P4-14` Retryable/Non-retryable Error Taxonomy 구현
- [ ] `P4-15` Exponential Backoff와 최대 Retry 설정
- [ ] `P4-16` 부분 성공 재실행과 COMMITTED Table 재사용 구현
- [ ] `P4-17` Task Timeout/종료 시 Lease 만료 또는 해제 검증

재시도 가능:

```text
Network Error
Temporary Database Error
Temporary Object Storage Error
```

재시도하지 않음:

```text
Schema Contract Error
Batch Identity Conflict
Configuration Error
Data Quality Threshold Exceeded
```

### 5. XCom 제한

허용:

```text
batch_id / run_id / table_batch_id
object_key
row_count
watermark
status
logical_hash
```

금지:

```text
DataFrame / Arrow Table / Parquet Bytes
Raw Row / Raw Payload
Credential / Secret
```

## 범위 밖

- Phase 3 Python Pipeline의 재구현
- dbt Model의 상세 구현
- Mart Publish 교체 전략
- 최종 장애 Runbook 작성

## 테스트와 Gate

### DAG 검증

- DAG Import Error 0
- Task Graph와 Dependency가 문서와 일치
- `max_active_runs=1`
- Dynamic Task가 정확히 6개 Table로 확장
- XCom Payload Schema/Size 검증

### 부분 성공 시나리오

```text
customers SUCCESS
orders SUCCESS
order_payments FAILED
    ↓ rerun
customers SKIPPED_ALREADY_COMMITTED
orders SKIPPED_ALREADY_COMMITTED
order_payments 재실행
```

| AC    | 시나리오                  | 이 Phase의 증거                  |
| ----- | ------------------------- | -------------------------------- |
| AC-01 | Source→Bronze Catalog E2E | 고정 주문의 Count/Key 추적       |
| AC-03 | 동일 Batch 재실행         | COMMITTED Table 재사용, 중복 0   |
| AC-13 | Observability             | 성공/빈/실패/재실행 상태 조회    |
| AC-16 | 새 Clone의 Airflow 부분   | Compose 기동, DAG Parse/E2E 기록 |

AC-01과 AC-16의 Fact/dbt 부분은 Phase 5~6에서 완성한다.

## 요구사항 추적

| 구분 | 연결 항목                              |
| ---- | -------------------------------------- |
| PRD  | Section 9 Batch Identity와 재실행      |
| PRD  | Section 12 Pipeline Metadata           |
| PRD  | Section 13 Airflow 실행 계약           |
| PRD  | Section 18 Observability와 오류        |
| ADR  | ADR-010 Metadata-backed Bronze Catalog |
| ADR  | ADR-013 Source Mutation/Extract 동시성 |
| FR   | FR-08 Idempotency와 Orphan Recovery    |
| FR   | FR-09 Airflow Pipeline                 |

## 산출물

- Airflow Runtime/Compose 설정
- `source_simulation_dag`
- `warehouse_pipeline_dag`
- Dynamic Task Mapping과 Cleanup 경계
- Retry/Error Taxonomy
- 부분 성공 재사용 통합 테스트
- DAG Parse와 XCom Contract 테스트

## Definition of Done

- [ ] 모든 `P4-*` Task가 완료됐다.
- [ ] 두 DAG가 Import Error 없이 Parse된다.
- [ ] Warehouse E2E가 Phase 3 API를 통해 실행된다.
- [ ] Retryable Error만 재시도한다.
- [ ] 부분 성공 재실행에서 COMMITTED Table을 재사용한다.
- [ ] 모든 종료 경로에서 Global Source Lease가 해제되거나 안전하게 만료된다.
- [ ] XCom 금지 Payload가 존재하지 않는다.

## Portfolio Evidence

- DAG Graph와 Task별 책임
- 부분 성공 전후 `pipeline_runs` 비교
- Retryable/Non-retryable 실패 실행 기록
- Global Lease 획득부터 해제까지 Timeline

## 권장 Commit

```text
feat: orchestrate ingestion with airflow
```

## 다음 Phase 인계

Phase 5는 `sync_bronze_catalog`가 만든 Metadata-backed File 목록만 입력으로 사용한다. DAG의 `dbt_build` Task는 Phase 5 CLI를 호출하는 경계로 유지한다.
