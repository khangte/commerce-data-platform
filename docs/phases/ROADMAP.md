# ROADMAP: Commerce Analytics Data Platform

> 기준 PRD: PRD v1.7
> 목적: Phase 0부터 Phase 9까지의 실제 개발 순서와 검증 기준 정의  
> 원칙: 전체 아키텍처와 핵심 계약은 PRD를 기준으로 유지하고, 구현은 Phase 단위로 완료·검증한 뒤 다음 단계로 진행한다.

---

## Phase 실행 문서

ROADMAP은 전체 순서와 범위를 관리하고, 아래 문서는 Phase별 Task, 산출물, 검증 Evidence, Definition of Done을 관리한다. 공통 Architecture와 데이터 계약의 Source of Truth는 [PRD v1.7](../../PRD_v1.7.md)다.

| Phase | 실행 문서                                                      | 주요 Gate                        |
| ----- | -------------------------------------------------------------- | -------------------------------- |
| 0     | [Bootstrap](phase-00-bootstrap.md)                             | 환경/구조 재현                   |
| 1     | [Source Environment](phase-01-source-environment.md)           | AC-14, AC-18                     |
| 2     | [Deterministic Generator](phase-02-deterministic-generator.md) | AC-15, AC-20/21 생성 측          |
| 3     | [Incremental Ingestion](phase-03-incremental-ingestion.md)     | AC-02~06, 08, 20, 21, 23, 24     |
| 4     | [Airflow Orchestration](phase-04-airflow-orchestration.md)     | E2E, Retry, 부분 성공 재사용     |
| 5     | [dbt + DuckDB Modeling](phase-05-dbt-duckdb-modeling.md)       | AC-01, 09~12, 19, 22             |
| 6     | [Data Quality & Publish](phase-06-data-quality-publish.md)     | AC-01, 08, 12, 13, 16            |
| 7     | [Reliability Scenarios](phase-07-reliability.md)               | 실패/충돌/재처리 복구 Evidence   |
| 8     | [Benchmark](phase-08-benchmark.md)                             | AC-17                            |
| 9     | [BI](phase-09-bi.md)                                           | Mart-only Dashboard, Serving ADR |

Phase 문서의 상태는 `Planned → In Progress → Done`으로 변경한다. `Done`은 체크박스 개수가 아니라 해당 문서의 Definition of Done과 Acceptance Gate가 모두 통과했음을 뜻한다.

각 Phase 문서에는 `파일·폴더별 변경 요약` 섹션을 유지한다. 구현 중 생성·수정·삭제한 파일 또는 폴더와 변경 목적을 한 줄씩 기록하고, 해당 Phase 완료 전에 최신 상태로 갱신한다. 이 요약은 상세 Diff가 아니라 구현 범위를 빠르게 확인하기 위한 목록이다.

---

## 전체 진행 순서 요약

프로젝트는 전체 설계와 핵심 데이터 계약을 먼저 확정한 뒤, Phase 단위로 구현·검증·완료하는 방식으로 진행한다. 각 Phase는 이전 Phase의 결과물을 기반으로 하며, 해당 Phase의 Definition of Done과 Acceptance Criteria를 통과한 뒤 다음 단계로 넘어간다.

```text
Milestone 1. Source Foundation
│
├── Phase 0. Bootstrap
│   └── 개발환경, Repository, Python/uv, 기본 구조 구성
│
├── Phase 1. Source Environment
│   └── PostgreSQL Source Schema, Olist Raw/Seed 적재
│
└── Phase 2. Deterministic Generator
    └── 신규·변경·지연 데이터를 발생시키는 Source Simulator 구현

Milestone 2. Data Platform Core
│
├── Phase 3. Incremental Ingestion
│   └── Cursor, Watermark, Validation, Parquet Bronze, Quarantine
│
├── Phase 4. Airflow Orchestration
│   └── 검증된 Pipeline Logic을 DAG로 오케스트레이션
│
├── Phase 5. dbt + DuckDB Modeling
│   └── Staging, Intermediate, Dimension, Fact, SCD2 구축
│
└── Phase 6. Data Quality & Publish
    └── Ingestion/Warehouse 품질 검증과 안전한 Mart Publish


Milestone 3. Portfolio Evidence
│
├── Phase 7. Reliability
│   └── 실패, 충돌, Late Arrival, Backfill, 복구 시나리오 검증
│
├── Phase 8. Benchmark
│   └── 성능 Baseline, 병목 분석, 개선 전후 정량 비교
│
└── Phase 9. BI
    └── Metabase를 통한 최종 Data Mart 소비 가능성 검증
```

진행 흐름은 다음을 기본으로 한다.

```text
PRD Baseline
    ↓
Phase 구현
    ↓
Unit / Integration Test
    ↓
Acceptance Criteria 검증
    ↓
ADR / 문서 갱신
    ↓
Git Commit
    ↓
다음 Phase
```

구현 우선순위는 다음과 같이 본다.

```text
Phase 0~2
→ Source와 테스트 데이터 환경 확보

Phase 3~6
→ 프로젝트의 핵심 Data Engineering 구현

Phase 7~9
→ 신뢰성·성능·활용 가능성을 포트폴리오 증거로 정리
특히 프로젝트의 핵심 비중은 Phase 3, 5, 7, 8에 둔다.
```

---

## 1. 진행 원칙

프로젝트 전체 코드를 한 번에 생성한 뒤 수정하는 방식은 사용하지 않는다.

```text
PRD v1.7
전체 Architecture / Contract 확정
        ↓
Phase 0 구현
        ↓
Test / DoD / Commit
        ↓
Phase 1 구현
        ↓
Test / Acceptance Criteria / Commit
        ↓
...
```

핵심 원칙:

1. 미래 Phase의 요구사항을 고려한 Contract는 먼저 정의한다.
2. 미래 Phase의 구현은 미리 하지 않는다.
3. 각 Phase는 독립적으로 검증 가능한 상태에서 종료한다.
4. Phase 내부에서도 대표 Vertical Slice를 먼저 완성한 뒤 일반화한다.
5. 구현 결과가 PRD 가정과 다르면 관측 근거를 남기고 ADR/PRD를 수정한다.
6. 완료한 Phase의 Contract가 바뀌면 영향받는 Acceptance Test를 다시 실행한다.

---

# Milestone 1. Source Foundation

## Phase 0. Bootstrap

### 목표

데이터 파이프라인 구현 전 개발환경과 Repository 구조를 재현 가능한 상태로 만든다.

### Task

```text
P0-01 Repository 초기화
P0-02 Python 3.12 + uv 설정
P0-03 pyproject.toml 구성
P0-04 .python-version 생성
P0-05 Repository 기본 구조 생성
P0-06 .gitignore 작성
P0-07 .env.example 작성
P0-08 compose.yaml Skeleton 작성
P0-09 scripts/download_dataset.py 작성
P0-10 README.md 기본 실행 가이드 작성
P0-11 AGENTS.md 작성
P0-12 pytest / ruff 초기 구성
```

### 검증

```bash
uv run python --version
uv sync --frozen
uv run ruff check .
uv run pytest
docker compose config
```

### Phase 0에서 하지 않는 것

```text
PostgreSQL Table 생성
Olist Seed 적재
Synthetic Generator
SeaweedFS 구성
Airflow DAG
dbt Model
```

### 완료 조건

- Python 3.12.x 확인
- `uv sync --frozen` 성공
- Ruff 실행 성공
- pytest 실행 가능
- `docker compose config` 성공
- Git Ignore 정책 정상
- README만으로 Bootstrap 절차 재현 가능

### 권장 Commit

```text
chore: bootstrap commerce data platform
```

---

## Phase 1. Source Environment

### 목표

Olist Raw 데이터를 원본 Naming을 최대한 유지하는 PostgreSQL OLTP Source로 구성한다.

### 1-1. PostgreSQL 인프라

```text
P1-01 PostgreSQL 18.6 Compose Service
P1-02 Health Check
P1-03 역할별 Credential
P1-04 commerce_source DB
P1-05 airflow_metadata DB
P1-06 pipeline_metadata DB
```

### 1-2. Olist Raw Dataset

```text
P1-07 KaggleHub 다운로드
P1-08 필수 CSV 존재 검증
P1-09 CSV Header Contract 검증
P1-10 Raw Checksum 계산
```

필수 Dataset:

```text
customers
orders
order_items
order_payments
products
sellers
```

### 1-3. Source DDL

FK 의존성을 고려해 다음 순서로 구성한다.

```text
customers
products
sellers
    ↓
orders
    ↓
order_items
order_payments
```

Task:

```text
P1-11 customers DDL
P1-12 products DDL
P1-13 sellers DDL
P1-14 orders DDL
P1-15 order_items DDL
P1-16 order_payments DDL
P1-17 PK / FK / CHECK
P1-18 Incremental Index
```

Source에서는 다음과 같은 Olist Naming을 유지한다.

```text
customer_city
product_category_name
payment_sequential
order_purchase_timestamp
```

### 1-4. Seed Loader

```text
P1-19 임시 PostgreSQL Staging Schema
P1-20 CSV Type 변환
P1-21 프로젝트 Extension Field 계산
P1-22 Header 검증
P1-23 PK / FK 검증
P1-24 Transactional UPSERT
P1-25 seed_runs 기록
P1-26 Seed Guard
```

먼저 `customers` 한 Table로 Seed Flow를 검증한 뒤 6개 Table로 확장한다.

### 테스트

```text
동일 Raw + 동일 seeded_at
→ Seed 2회 실행
→ Row Count 동일
→ Content Hash 동일

Source Allowlist
→ 원본 Naming 유지

PK / FK
→ 위반 0
```

### Gate

- AC-14 Seed 재실행
- AC-18 Source Schema Allowlist

---

## Phase 2. Deterministic Generator

### 목표

정적 Olist Source를 지속적으로 변경되는 서비스형 OLTP Source로 만든다.

### 2-1. 결정성 기반

```text
P2-01 Generator Config
P2-02 random_seed
P2-03 logical_date
P2-04 generator_version
P2-05 Deterministic ID Utility
P2-06 generator_runs Metadata
```

Synthetic ID는 UUIDv5 또는 결정적 Hash를 사용한다.

### 2-2. Customer

```text
P2-07 신규 customer_unique_id
P2-08 신규 customer_id
P2-09 기존 고객 재구매
P2-10 주소 변경
P2-11 Membership 계산
```

Identity 계약:

```text
customer_unique_id
= 동일 인물 Business Key

customer_id
= 주문 시점 Customer Record
```

### 2-3. Order / Item / Payment

```text
P2-12 신규 Order 생성
P2-13 Order Item 생성
P2-14 Payment 생성
```

Order / Item / Payment 생성은 하나의 Transaction으로 처리한다.

### 2-4. 상태 변화

Order:

```text
created
→ approved
→ shipped
→ delivered

created / approved
→ canceled
```

Payment:

```text
pending
→ completed
→ refunded

pending
→ failed
```

### 2-5. Service-level Scenario

```text
P2-15 Late Order
P2-16 Delayed Payment
P2-17 과거 Event의 Late Update
P2-18 Membership Change
```

파이프라인 오류 주입은 Phase 3/6에서 처리한다.

### 2-6. 원천 변경 동시성

```text
P2-19 원천 데이터 동시성 잠금 스키마/클라이언트
P2-20 Lease 획득 실패 시 Source 변경 전 종료
P2-21 Generator Transaction 종료 후 Lease 해제
P2-22 Lease 소유권 상실 시 Fencing 검증
```

### 결정성 검증

동일 Snapshot에서 동일 입력:

```text
random_seed
logical_date
order_count
anomaly_profile
generator_version
```

을 사용하면 다음이 같아야 한다.

```text
생성 Key Set
상태
Count
Logical Hash
```

### Gate

- AC-15 Generator 재현
- 허용 State Transition 준수
- SCD2 추적 속성의 수집 구간 내 변경 횟수 계약 준수

---

# Milestone 2. Data Platform Core

## Phase 3. Incremental Ingestion

### 목표

PostgreSQL 변경 데이터를 안전하게 증분 추출해 SeaweedFS Bronze와 Quarantine으로 Commit한다.

Phase 3은 처음부터 6개 Table을 구현하지 않는다.

---

## Phase 3A. `orders` Vertical Slice

먼저 `orders` 하나로 전체 경로를 완성한다.

```text
PostgreSQL orders
        ↓
Composite Cursor
        ↓
Extract
        ↓
Validation
        ↓
Local Parquet
        ↓
SeaweedFS
        ↓
Manifest
        ↓
Metadata Commit
        ↓
Watermark
```

### 3A-1. Metadata

```text
P3-01 Metadata DDL
      ├── watermarks
      ├── pipeline_runs
      ├── bronze_objects
      └── quarantine_batches
```

### 3A-2. Cursor / Pagination

`orders` Cursor:

```text
(updated_at, order_id)
```

고정 범위:

```sql
WHERE cursor > :watermark_before
  AND cursor <= :extract_upper_bound
ORDER BY cursor
LIMIT :page_size
```

구현 항목:

```text
P3-02 orders Composite Cursor Query
P3-03 고정 Upper Bound + Keyset Pagination
      ├── watermark_before
      ├── extract_upper_bound
      ├── Page Size
      └── Empty Batch
```

### 3A-3. Parquet

```text
P3-04 Local Parquet
      ├── Arrow Schema
      ├── Page-based Write
      ├── Zstandard Compression
      └── Bronze Technical Columns
```

기술 컬럼:

```text
_batch_id
_run_id
_ingested_at
_source_table
_schema_version
```

### 3A-4. SeaweedFS Compatibility Smoke Test

먼저 프로젝트가 실제 사용하는 S3 API 범위를 검증한다.

```text
P3-05 SeaweedFS Compatibility
Bucket Create
PUT
GET
HEAD
LIST
DELETE
Overwrite Test
DuckDB Parquet Read
```

그 후:

```text
_staging/
bronze/
quarantine/
```

Prefix를 구성한다.

### 3A-5. Commit Protocol

```text
P3-06 Manifest 생성
P3-07 Metadata COMMITTED
P3-08 Watermark CAS

Upload
→ HEAD / Checksum / Row Count
→ Manifest
→ Metadata COMMITTED
→ Watermark CAS
```

---

## Phase 3B. 전체 Table 일반화

`orders`에서 검증한 구조를 다음 Table로 확장한다.

```text
customers
products
sellers
order_items
order_payments
```

Cursor 차이:

```text
Mutable
→ updated_at + PK

order_items
→ created_at + Composite PK
```

---

## Phase 3C. Quarantine

Pipeline-level Corruption:

```text
Duplicate
NULL Key
Broken FK
Invalid Status
Negative Value
```

처리:

```text
Valid
→ Bronze

Invalid
→ Quarantine
```

---

## Phase 3D. 동시성

핵심 Ingestion이 정상 동작한 뒤 추가한다.

```text
Lease
TTL
Lease Renewal
CAS
Concurrent Extract
원천 변경 동시성 조정
```

처음부터 동시성 기능까지 한 번에 만들지 않는다.

### Gate

- AC-02 Incremental
- AC-03 Idempotency
- AC-04 Failure / Watermark
- AC-05 Pagination
- AC-06 Concurrency
- AC-08 Corruption
- AC-20 Mutation Cursor Safety
- AC-21 Generator / Ingestion Concurrency
- AC-23 Metadata Commit Contract
- AC-24 Schema Version

---

## Phase 4. Airflow Orchestration

### 목표

Phase 3에서 검증된 Python Pipeline을 Airflow가 오케스트레이션하도록 한다.

Airflow DAG 내부에 ETL 구현을 복제하지 않는다.

### DAG

```text
P4-01 source_simulation_dag
P4-02 warehouse_pipeline_dag
```

### Task Graph

```text
initialize_run
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
```

### Retry

재시도 가능:

```text
Network Error
Temporary DB Error
Temporary Object Storage Error
```

재시도하지 않음:

```text
Schema Contract Error
Batch Identity Conflict
Configuration Error
```

### XCom

허용:

```text
Batch / Run ID
Object Key
Count
Watermark
Status
```

금지:

```text
DataFrame
Arrow Table
Parquet Bytes
Raw Rows
Credential
```

### 부분 성공 검증

예:

```text
customers SUCCESS
orders SUCCESS
order_payments FAILED
```

재실행 시:

```text
COMMITTED Table
→ 재사용

FAILED Table
→ 다시 실행
```

### Gate

- DAG E2E
- Retry
- Partial Success Reuse
- Metadata-only XCom
- `max_active_runs=1`

---

## Phase 5. dbt + DuckDB Modeling

### 목표

Committed Bronze만 읽어 Staging → Intermediate → Dimension / Fact를 구축한다.

---

## Phase 5A. Bronze File Catalog

```text
pipeline_metadata
        ↓
COMMITTED Objects
        ↓
DuckDB control.bronze_files
```

S3 Prefix 전체를 직접 Glob하지 않는다.

---

## Phase 5B. Staging

권장 순서:

```text
stg_products
stg_sellers
stg_order_items
stg_payments
stg_orders
stg_customers_current
stg_customer_observations
```

Source Naming은 Staging에서 처음 분석 Naming으로 변환한다.

예:

```text
order_purchase_timestamp
→ purchase_at

payment_sequential
→ payment_sequence

product_category_name
→ category_name
```

표준화 상태값도 Staging에서 처리한다.

```text
approved / processing / invoiced
→ APPROVED

shipped
→ SHIPPED
```

---

## Phase 5C. Intermediate

```text
int_orders_enriched
int_order_items_enriched
int_payment_summary
int_customer_history
int_affected_business_dates
```

---

## Phase 5D. Dimension

먼저 단순 Dimension:

```text
dim_product
dim_seller
dim_date
```

이후:

```text
dim_customer
```

SCD Type 2 구현.

---

## Phase 5E. Fact

권장 순서:

```text
fact_order_items
fact_payments
fact_orders
```

`fact_orders`는 Aggregate Measure가 있으므로 마지막에 구현한다.

Metric:

```text
item_subtotal
freight_total
gross_order_value
payment_total
order_count
```

서로 다른 Fact Grain을 직접 Join해 Measure를 합산하지 않는다.

---

## Phase 5F. SCD2

Synthetic Membership / Address 변경으로 검증한다.

```text
BRONZE
    ↓
SILVER
    ↓
GOLD
```

검증:

```text
Version 생성
valid_from
valid_to
is_current
Overlap 없음
Current 정확히 1개
```

---

## Phase 5G. Temporal Join

주문 `purchase_at` 기준으로 당시 유효했던 Customer Dimension Version을 선택한다.

### Gate

- AC-09 SCD2
- AC-10 Temporal Join
- AC-12 Referential Integrity
- AC-19 Staging Naming
- AC-22 표준화 상태값

---

## Phase 6. Data Quality + Publish

### 목표

Ingestion과 Warehouse의 품질 책임을 분리하고 실패한 Build가 마지막 성공 Mart를 훼손하지 않도록 한다.

### Ingestion Quality

```text
Schema
Type
NULL
Duplicate
Source Domain
Numeric Range
Broken Reference
Cursor Range
```

### Warehouse Quality

Generic:

```text
unique
not_null
relationships
accepted_values
```

Custom:

```text
payment_value >= 0
price >= 0
freight_value >= 0
delivered_at >= purchase_at
approved_at >= purchase_at
SCD2 overlap = 0
Current Customer Version = 1
Fact FK Missing = 0
Fact Business Key Duplicate = 0
Normal E2E Unknown Key = 0
```

### Publish Safety

목표:

```text
dbt Build/Test 실패
→ Bronze / Watermark 유지
→ 마지막 성공 Mart 유지
```

필요하면 Build Schema → Test → Swap 방식을 적용한다.

### Gate

- 지정 Corruption 탐지
- 정상 데이터 False Positive 0
- Warehouse Failure 시 Bronze 유지
- 마지막 성공 Mart 유지

---

# Milestone 3. Portfolio Evidence

## Phase 7. Reliability Scenarios

### 목표

새 기능 구현보다 기존 시스템을 의도적으로 실패시키고 복구 가능성을 검증한다.

각 시나리오 문서 형식:

```text
문제
→ 재현
→ 관측
→ 원인
→ 해결
→ 재검증
```

### 시나리오

```text
R-01 Duplicate Batch
R-02 Upload Failure
R-03 Metadata Failure
R-04 Watermark CAS Conflict
R-05 Expired Lease
R-06 Orphan Object
R-07 Broken Manifest
R-08 Late Order
R-09 Late Payment
R-10 Customer SCD2 Change
R-11 Missing Schedule
R-12 Backfill Replay
R-13 Re-extract
R-14 dbt Failure
R-15 Source Connection Failure
```

기록 위치:

```text
docs/runbooks/
docs/troubleshooting/
```

### Gate

각 핵심 장애가 다음을 만족해야 한다.

```text
재현 가능
탐지 가능
복구 가능
재검증 가능
```

---

## Phase 8. Benchmark

### 목표

절대 처리시간보다 Baseline과 개선 전후의 차이를 정량적으로 증명한다.

### Scale

```text
S = 100K Orders
M = 1M Orders
L = 5M Orders
```

100K에서 Benchmark Framework를 먼저 검증하고 이후 Scale을 확대한다.

### Experiment

```text
A. Full Extract vs Incremental Extract
B. CSV vs Parquet
C. Full Scan vs Filtered Scan
D. Cold vs Warm
```

### 반복

각 주요 실험:

```text
5회 실행
→ Raw Result 저장
→ Median 계산
```

환경 고정:

```text
Host / WSL Spec
Git Commit
Python Version
Dependency Lock Hash
Docker Image Version
Dataset Scale
Random Seed
Query / Scenario
```

### 개선 Loop

```text
Baseline
    ↓
Bottleneck 확인
    ↓
개선
    ↓
동일 조건 재측정
```

포트폴리오에서는 다음과 같은 형태의 근거를 남긴다.

```text
Baseline 18.2s
→ 개선 7.4s
→ 59% 감소
```

### Gate

- AC-17 Benchmark Evidence
- Raw 5회 결과 존재
- Median 존재
- Result Hash 존재
- 환경 Metadata 존재

---

## Phase 9. BI

### 목표

Mart가 실제 소비 가능한 구조인지 검증한다. BI 자체를 프로젝트 중심 기능으로 만들지 않는다.

### Connection Gate

우선:

```text
Metabase
    ↓
DuckDB
```

를 검증한다.

Driver / Lock 문제가 있으면:

```text
DuckDB Mart
    ↓
PostgreSQL Serving DB
    ↓
Metabase
```

를 대안으로 사용하고 ADR을 작성한다.

### Dashboard

#### Sales

```text
Daily GMV
Orders
AOV
```

#### Product

```text
Category GMV
Top Products
Sales Volume
```

#### Customer

```text
New Customers
Repeat Customers
Membership
Region
```

### Gate

- Raw Source 직접 조회 금지
- Mart만 사용
- Connection Strategy ADR
- Dashboard 재현 가능

---

# 전체 의존 순서

```text
Phase 0
Bootstrap
    ↓
Phase 1
Source
    ↓
Phase 2
Generator
    ↓
Phase 3
Ingestion + Bronze
    ↓
Phase 4
Airflow
    ↓
Phase 5
Warehouse / dbt
    ↓
Phase 6
Quality / Publish
    ↓
Phase 7
Reliability
    ↓
Phase 8
Benchmark
    ↓
Phase 9
BI
```

---

# 권장 Milestone

```text
Milestone 1 — Source Foundation
├── Phase 0
├── Phase 1
└── Phase 2

Milestone 2 — Data Platform Core
├── Phase 3
├── Phase 4
├── Phase 5
└── Phase 6

Milestone 3 — Portfolio Evidence
├── Phase 7
├── Phase 8
└── Phase 9
```

투입 비중은 Phase 3, 5, 7, 8을 가장 높게 잡는다.

```text
Phase 0       낮음
Phase 1       중간
Phase 2       중간
Phase 3       매우 높음
Phase 4       높음
Phase 5       매우 높음
Phase 6       높음
Phase 7       매우 높음
Phase 8       매우 높음
Phase 9       낮음~중간
```

---

# Codex 작업 단위 원칙

Codex에 한 번에 `Phase 3 전체 구현`을 지시하지 않는다.

좋은 Task 크기:

```text
P3-01 watermarks Metadata DDL 구현
P3-02 orders Composite Cursor Query 구현
P3-03 orders Keyset Pagination 구현
P3-04 orders → Local Parquet 구현
P3-05 SeaweedFS Upload 구현
P3-06 Manifest 생성 구현
P3-07 Metadata Commit 구현
P3-08 Watermark CAS 구현
```

각 Task는 반드시 테스트로 완료 여부를 판단할 수 있어야 한다.

기준:

> 하나의 Task가 끝났을 때 “정상 동작하는가?”를 자동 또는 명시적 검증으로 판정할 수 있어야 한다.

---

# 실제 시작 순서

```text
1. PRD v1.7 Baseline Commit
2. AGENTS.md 확정
3. Phase 0 구현
4. Phase 0 DoD 검증
5. Phase 0 Commit
6. Phase 1 PostgreSQL 구성
7. Olist Raw Download
8. Source DDL
9. Seed Loader
10. AC-14 / AC-18 검증
11. Phase 1 Commit
12. Phase 2 진행
13. Phase 3 진입 시 orders Vertical Slice부터 구현
```

PRD는 구현 중 발견된 실제 증거에 따라 변경할 수 있다.

변경 흐름:

```text
가설
→ 구현
→ 관측
→ 문제
→ 대안 검토
→ 결정
→ ADR
→ PRD Version Update
```

단순히 구현이 어려워졌다는 이유만으로 Contract를 조용히 변경하지 않는다.
