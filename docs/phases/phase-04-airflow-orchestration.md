# Phase 4. Airflow Orchestration

> 상태: Planned  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 3. Incremental Ingestion](phase-03-incremental-ingestion.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.4](../../PRD_v1.4.md)

## 목표

Phase 3에서 독립적으로 검증된 Python Pipeline을 Apache Airflow가 일정, 의존성, 재시도, 실행 상태에 맞게 오케스트레이션하도록 한다. DAG는 데이터 처리 로직을 소유하지 않고 호출과 제어 흐름만 담당한다.

## 용어 설명

### 원천 데이터 동시성 잠금

Generator와 Warehouse Ingestion이 원천 PostgreSQL을 동시에 변경·수집하지 못하게 하는 시간 제한 잠금이다.
Warehouse가 6개 Table의 Snapshot을 읽는 동안 Generator의 원천 변경을 막아, Table 사이에 서로 다른
시점의 데이터를 관측하는 일을 방지한다. Generator도 Source 변경 전에 같은 Lease를 획득하므로 Warehouse
수집과 상호 배타적으로 동작한다.

```text
Warehouse: Lease 획득 → Source Snapshot/Bronze 적재 → 검증 → Lease 해제
Generator: Lease 획득 → Source 변경 → Lease 해제
```

### 테이블별 수집 잠금

동시에 실행된 Warehouse 작업끼리 같은 Table의 Watermark를 함께 갱신하지 못하게 하는 Table별 잠금이다.
원천 데이터 동시성 잠금이 Generator와 Warehouse 사이의 Source 일관성을 보호한다면, 테이블별 수집 잠금은 Warehouse
작업 사이의 동일 Table 수집 충돌을 방지한다.

## 핵심 계약

- Airflow Task 내부에 Extract/Validate/Commit 로직을 복제하지 않는다.
- `warehouse_pipeline_dag`는 Source Snapshot 구간에서만 원천 데이터 동시성 잠금을 보유하며, Bronze Commit 검증 뒤 즉시 해제한다.
- Table Task는 Dynamic Task Mapping으로 실행하며 각자 테이블별 수집 잠금과 Metadata 상태를 사용한다.
- COMMITTED Table Batch는 재실행 시 재사용하고 FAILED Table만 다시 실행한다.
- XCom에는 식별자와 작은 Metadata만 저장한다.
- dbt 실패는 이미 Commit된 Bronze와 Watermark를 되돌리지 않는다.
- DuckDB Single-writer 제약 때문에 Warehouse DAG는 `max_active_runs=1`이다.
- `sync_bronze_catalog`는 Metadata의 `COMMITTED` Object만 DuckDB Catalog에 반영한다.

## 구현 전 설계 보완 사항

Phase 3의 `warehouse_source_freeze()`는 하나의 Python 프로세스 안에서만 Lease를 유지하는 Context
Manager다. Airflow의 개별 Task는 서로 다른 프로세스에서 실행될 수 있으므로, Warehouse DAG에서 이
Context Manager를 Task 경계 전체에 걸쳐 사용할 수 없다. 아래 계약을 먼저 구현한다.

### 원천 데이터 동시성 잠금 전달 계약

1. `acquire_source_snapshot_lease` Task가 `WAREHOUSE` 원천 데이터 동시성 잠금을 획득한다.
2. 획득 결과는 아래처럼 JSON 직렬화 가능한 Lease Token만 XCom에 기록한다.

```json
{
  "owner_type": "WAREHOUSE",
  "owner_id": "UUID",
  "lease_expires_at": "UTC ISO-8601",
  "version": 1
}
```

3. 각 `extract_validate_load` Task는 Token으로 `SourceMutationLease`를 복원하고, 기존
   `LeaseHeartbeat`를 통해 자기 실행 동안 원천 데이터 동시성 잠금과 테이블별 수집 잠금을 갱신한다.
4. Source Snapshot, Upload, Commit 직전에는 Lease Fencing을 수행한다. 소유권·Version·만료 상태가
   달라진 Task는 안전하게 실패한다.
5. `release_source_snapshot_lease`는 모든 Table Task가 종료된 뒤 같은 Token으로 실행한다. Lease를
   획득하지 못한 초기화 실패 경로에서는 no-op이며, 이미 소유권을 잃은 경우에는 원인을 기록하고
   타 소유자의 Lease를 해제하지 않는다.

원천 데이터 동시성 잠금은 Source Snapshot 일관성을 위한 잠금이므로 Catalog 동기화, dbt, Summary 구간까지
보유하지 않는다. 이 구분으로 Generator가 DuckDB/dbt 실행 시간 동안 불필요하게 대기하지 않는다.

### 실행 식별자와 재실행 계약

- `batch_id`는 `{dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}`를 사용한다. 동일 DAG Run의 Airflow
  재시도와 같은 논리 실행의 수동 재실행은 같은 `batch_id`를 사용한다.
- `run_id`는 Task 실행 시도별 새 UUID다. `pipeline_runs`에는 시도별 이력을 남기고, 이미
  `COMMITTED`인 동일 `batch_id`/Table은 재사용한다.
- `attempt_number`는 Airflow `ti.try_number`를 관측값으로만 기록한다. 최종 Bronze 객체 Key와 Batch
  Identity에는 포함하지 않는다.
- Dynamic Mapping 입력은 고정 순서의 6개 Table 이름과 Batch/Logical Date/Lease Token 같은 작은
  식별 정보로 제한한다. Raw 데이터나 자격 증명은 전달하지 않는다.

### dbt와 Summary 경계

- Phase 4의 완료 가능한 Warehouse 경계는 `sync_bronze_catalog`까지다.
- `dbt_build` Task는 Phase 5 dbt Project와 CLI가 구현된 뒤 활성화하는 호출 경계로만 구성한다. dbt
  Project가 없는 상태에서 성공을 가장하는 no-op Task는 만들지 않는다.
- `publish_run_summary`는 Airflow DagRun/Task 상태와 Table별 `pipeline_runs`를 조회해 작은 JSON
  Summary를 로그와 XCom에 남긴다. 장기 실행 Summary 저장 테이블이 필요해지면 별도 Metadata Schema와
  마이그레이션으로 추가하며, Phase 4에서 암묵적으로 새 영속 상태를 만들지 않는다.

## 선행 조건

- Phase 3 Python API가 Airflow 없이도 E2E 테스트를 통과했다.
- Airflow Metadata Database와 환경 변수 계약이 준비됐다.
- Source, Metadata, SeaweedFS Health Check가 제공된다.
- 기존 6개 Bronze Object는 최초 Warehouse DAG 전에 `sync_bronze_catalog`로 1회 동기화하거나,
  첫 DAG의 Catalog 동기화 결과로 반영한다.

## 구현 순서

### 1. Airflow Runtime

- [ ] `P4-01` Airflow 3.3.1 Compose Service와 LocalExecutor 구성
- [ ] `P4-02` Airflow Metadata DB 초기화와 Health Check 구성
- [ ] `P4-03` Secret을 코드에 넣지 않는 환경 변수 설정
- [ ] `P4-04` DAG Import/Parse Smoke Test 구성

Runtime 계약:

- Airflow Image와 Python/Airflow Provider 버전을 고정하고, 프로젝트 의존성(`psycopg`, `boto3`,
  `pyarrow`, `duckdb`, dbt)을 Image Build 단계에서 설치한다. 컨테이너 시작 시 임의 `pip install`은 하지
  않는다.
- DAG와 `src/`는 read-only로 Mount하고, Task Local Parquet 임시 경로와 DuckDB Warehouse 경로는
  명시적 Volume/환경 변수로 제공한다.
- 컨테이너 내부 연결에는 `postgres`, `seaweedfs` Service Host를 사용한다. Host의 `localhost` 포트는
  Airflow 컨테이너 설정에 사용하지 않는다.
- Airflow Metadata DB URL, Source/Metadata PostgreSQL, SeaweedFS Endpoint와 Key는 `.env` 또는
  Docker Secret/환경 변수로만 주입한다. 코드·DAG·XCom·로그에 Credential을 기록하지 않는다.
- Compose에는 Airflow 버전에 맞는 API Server, Scheduler, Metadata DB Migration과 Health Check를
  명시하고, LocalExecutor가 Task를 실행할 수 있는 단일 개발 머신 구성을 검증한다.

### 2. Generator DAG

- [ ] `P4-05` `source_simulation_dag` 구현
- [ ] Generator Config/Logical Date를 Phase 2 API로 전달
- [ ] 원천 데이터 동시성 잠금 획득 실패를 명시적 상태로 기록
- [ ] Generator 결과 Count/Hash/Run ID만 XCom에 반환

Generator DAG는 `seed`, `logical_date`, 생성 건수와 Scenario Profile을 Airflow Params로 명시적으로
검증한 뒤 Phase 2 API에 전달한다. 임의 난수·현재 시각·숨은 기본값으로 Generator 결과가 달라지지 않게
한다.

### 3. Warehouse DAG

- [ ] `P4-06` `warehouse_pipeline_dag` 구현
- [ ] `P4-07` `initialize_run`에서 Batch/Run 식별 정보 구성, 별도 Task에서 원천 데이터 동시성 잠금 획득
- [ ] `P4-08` 6개 Table `extract_validate_load` Dynamic Task Mapping
- [ ] `P4-09` `verify_bronze_commit`에서 Metadata 기반 Commit 검증
- [ ] `P4-10` `sync_bronze_catalog` 호출
- [ ] `P4-11` Phase 5 이후 활성화할 dbt Build 호출 경계 구성
- [ ] `P4-12` `publish_run_summary`와 최종 상태 기록
- [ ] `P4-13` 성공/실패에 관계없이 원천 데이터 동시성 잠금을 해제하는 Cleanup Task

`P4-08`은 `partial(...).expand(...)`로 고정된 아래 6개 Table만 확장한다. 병렬도는 Source와
Object Storage 용량을 고려해 `max_active_tis_per_dag`로 제한하고, Map 입력 순서는 고정한다.

```text
customers → products → sellers → orders → order_items → order_payments
```

`P4-09`는 Map Task의 XCom 성공 응답만 신뢰하지 않는다. `pipeline_metadata.bronze_objects`에서
Batch의 6개 Table이 모두 `COMMITTED`인지 확인하고, Manifest/Object/Hash/Row Count/Watermark를
Phase 3 검증 API로 재확인한다. 하나라도 실패·미완료이면 Catalog와 dbt를 실행하지 않는다.

Task Graph:

```text
initialize_run
    ↓
acquire_source_snapshot_lease
    ↓
extract_validate_load[
    customers,
    products,
    sellers,
    orders,
    order_items,
    order_payments
]
    ├── verify_bronze_commit (all_success)
    └── release_source_snapshot_lease (all_done)
                 ↓
verify_bronze_commit + release_source_snapshot_lease
    ↓ (둘 다 성공한 경우)
sync_bronze_catalog
    ↓
dbt_build (Phase 5 구현 뒤 활성화)
    ↓
publish_run_summary
```

Lease 해제는 mapped Task 자체가 아니라 별도 Cleanup Task에서 `ALL_DONE`으로 실행한다. Cleanup은 모든
mapped Task의 종료를 기다리되, 검증 실패 또는 Cleanup 실패 시 Catalog/dbt로 진행하지 않는다.

### 4. Retry와 오류 분류

- [ ] `P4-14` Retryable/Non-retryable Error Taxonomy 구현
- [ ] `P4-15` Exponential Backoff와 최대 Retry 설정
- [ ] `P4-16` 부분 성공 재실행과 COMMITTED Table 재사용 구현
- [ ] `P4-17` Task Timeout/종료 시 Lease 만료 또는 해제 검증

재시도 가능:

```text
SOURCE_CONNECTION_ERROR
일시적 PostgreSQL 연결/조회 오류
일시적 Object Storage 연결·Timeout 오류
LeaseUnavailableError (다른 활성 실행 종료를 기다리는 bounded retry)
```

재시도하지 않음:

```text
CONFIGURATION_ERROR
SOURCE_CONTRACT_ERROR
VALIDATION_THRESHOLD_EXCEEDED
OBJECT_VERIFICATION_ERROR
WATERMARK_CONFLICT
BATCH_IDENTITY_CONFLICT
LeaseOwnershipLostError
DBT_BUILD_ERROR / DBT_TEST_ERROR
```

기본값은 PRD를 따라 `retries=2`, `retry_delay=60초`, exponential backoff, `max_retry_delay=10분`으로
시작한다. 구현 시 예외 타입을 위 Error Type으로 변환하는 단일 분류 함수를 두고, 최종 예외·재시도 횟수·
다음 재시도 시각을 Task Log와 `pipeline_runs`에 남긴다.

### 5. XCom 제한

허용:

```text
batch_id / run_id / table_batch_id
object_key
row_count
watermark
status
logical_hash
source_lease_token
```

금지:

```text
DataFrame / Arrow Table / Parquet Bytes
Raw Row / Raw Payload
Credential / Secret
```

`source_lease_token`에는 앞서 정의한 소유자·UUID·만료 시각·Version만 포함한다. Airflow Connection URI,
비밀번호, Object Storage Key는 포함하지 않는다.

## 범위 밖

- Phase 3 Python Pipeline의 재구현
- dbt Model의 상세 구현
- Phase 5 이전의 실제 dbt 실행 성공
- Mart Publish 교체 전략
- 최종 장애 Runbook 작성

## 테스트와 Gate

### DAG 검증

- DAG Import Error 0
- Task Graph와 Dependency가 문서와 일치
- `max_active_runs=1`
- Dynamic Task가 정확히 6개 Table로 확장
- Dynamic Task의 Map 입력 순서와 `max_active_tis_per_dag` 제한 검증
- XCom Payload Schema/Size 검증
- Lease Token 직렬화/복원, Task 프로세스 경계 Fencing, 초기화 실패 Cleanup no-op 검증
- 모든 Table 종료 뒤 Lease가 해제되고 Catalog/dbt 구간에서는 Generator가 Lease를 획득할 수 있는지 검증
- `verify_bronze_commit`이 Metadata·Manifest·Object·Row Count·Watermark 불일치를 차단하는지 검증
- Compose Fresh Boot, Metadata Migration, Health Check, LocalExecutor 실제 Task 실행 검증

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

| 구분 | 연결 항목                                |
| ---- | ---------------------------------------- |
| PRD  | Section 9 Batch Identity와 재실행        |
| PRD  | Section 12 Pipeline Metadata             |
| PRD  | Section 13 Airflow 실행 계약             |
| PRD  | Section 18 Observability와 오류          |
| ADR  | ADR-010 메타데이터 기반 Bronze 파일 목록 |
| ADR  | ADR-013 원천 변경/수집 동시성            |
| FR   | FR-08 Idempotency와 Orphan Recovery      |
| FR   | FR-09 Airflow Pipeline                   |

## 산출물

- Airflow Runtime/Compose 설정
- `source_simulation_dag`
- `warehouse_pipeline_dag`
- Dynamic Task Mapping과 Cleanup 경계
- Retry/Error Taxonomy
- 부분 성공 재사용 통합 테스트
- DAG Parse와 XCom Contract 테스트
- Airflow Lease Token/Task 경계 Fencing 테스트
- Compose Fresh Boot와 LocalExecutor E2E 실행 기록

## Definition of Done

- [ ] 모든 `P4-*` Task가 완료됐다.
- [ ] 두 DAG가 Import Error 없이 Parse된다.
- [ ] Warehouse E2E가 Phase 3 API를 통해 실행된다.
- [ ] Retryable Error만 재시도한다.
- [ ] 부분 성공 재실행에서 COMMITTED Table을 재사용한다.
- [ ] 모든 종료 경로에서 원천 데이터 동시성 잠금이 해제되거나 안전하게 만료된다.
- [ ] XCom 금지 Payload가 존재하지 않는다.
- [ ] Lease 해제 후 Catalog/dbt 구간에서 Generator가 원천 데이터 동시성 잠금을 획득할 수 있다.
- [ ] dbt Project 미구현 상태에서 Warehouse DAG가 dbt 성공을 가장하지 않는다.

## Portfolio Evidence

- DAG Graph와 Task별 책임
- 부분 성공 전후 `pipeline_runs` 비교
- Retryable/Non-retryable 실패 실행 기록
- 원천 데이터 동시성 잠금 획득부터 해제까지 Timeline

## 권장 Commit

```text
feat: orchestrate ingestion with airflow
```

## 다음 Phase 인계

Phase 5는 `sync_bronze_catalog`가 만든 메타데이터 기반 Bronze 파일 목록만 입력으로 사용한다. Phase 5가 dbt
Project/CLI와 Test를 완성한 뒤, Warehouse DAG의 `dbt_build` 호출 경계를 활성화한다.

## 파일·폴더별 변경 요약

| 경로                                            | 변경 내용                                                                                                                                     |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `docs/phases/phase-04-airflow-orchestration.md` | Phase 4 검토 결과를 반영해 내부 용어의 한국어 표기, Airflow Task 경계 Lease Token, DAG 순서, Runtime, 재시도, dbt 경계, 검증 기준을 보완했다. |
