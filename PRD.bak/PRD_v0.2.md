# PRD: Commerce Analytics Data Platform

> Version: 0.2
> Status: Baseline Approved  
> Development Environment: WSL2 Ubuntu / Local  
> Primary Goal: 데이터엔지니어 취업 포트폴리오용 Batch DW·Data Mart 프로젝트

## 1. 프로젝트 개요

### 프로젝트명

**Commerce Analytics Data Platform**

### 한 줄 정의

전자상거래 원천 데이터를 수집·적재하고, 분석용 데이터웨어하우스 및 데이터 마트를 구축하여 **배치 파이프라인, 데이터 모델링, 데이터 품질, 재처리 및 운영 안정성**을 검증하는 데이터엔지니어링 프로젝트.

### 프로젝트 목적

단순히 데이터를 이동시키는 ETL 프로젝트가 아니라 다음 역량을 하나의 프로젝트에서 증명하는 것을 목표로 한다.

```text
Source Data
    ↓
수집 / 증분 적재
    ↓
Object Storage
    ↓
Transformation
    ↓
Dimensional Modeling
    ↓
Data Quality
    ↓
Data Mart
    ↓
BI / Analytics
```

핵심 포트폴리오 메시지는 다음과 같다.

> 정적 데이터 적재가 아니라 실제 서비스처럼 데이터가 지속적으로 변경되는 환경을 구성하고, 증분 처리·이력 관리·품질 검증·재처리까지 고려한 데이터 파이프라인을 설계하고 운영한다.

---

## 2. 개발 환경

### Host

```text
Windows
└── WSL2
    └── Ubuntu
```

현재 WSL 설정을 초기 기준으로 사용한다.

| 항목              |                 설정 |
| ----------------- | -------------------: |
| CPU               | 8 Logical Processors |
| RAM               |               약 8GB |
| Swap              |                  2GB |
| Host RAM          |                 16GB |
| 개발 위치         | WSL Linux Filesystem |
| Container Runtime |               Docker |
| Container 관리    |       Docker Compose |

프로젝트 경로:

```text
~/projects/commerce-data-platform
```

`/mnt/c/...`가 아닌 WSL 내부 파일 시스템을 사용한다.

### Docker Compose 파일명

프로젝트 루트의 Compose 파일은 `compose.yaml`을 사용한다.

```text
commerce-data-platform/
└── compose.yaml
```

`docker-compose.yaml`도 동작하지만, 본 프로젝트에서는 현재 Docker Compose의 간결한 기본 네이밍인 `compose.yaml`로 통일한다.

### Dockerfile 배치 원칙

커스텀 이미지가 필요한 서비스의 `Dockerfile`은 별도의 `docker/` 디렉터리에 중앙집중하지 않고 **해당 서비스를 소유하는 디렉터리 가까이에 배치**한다.

초기에는 커스텀 이미지가 필요할 가능성이 있는 Airflow만 다음 구조를 사용한다.

```text
airflow/
├── Dockerfile
└── dags/
```

PostgreSQL, MinIO, Metabase는 가능한 한 공식 이미지를 그대로 사용하며 별도 Dockerfile을 만들지 않는다.

Airflow 역시 공식 이미지와 Volume Mount만으로 요구사항을 충족할 수 있다면 `airflow/Dockerfile`을 만들지 않는다. 즉, Dockerfile은 실제 커스텀 Build 요구가 생길 때만 추가한다.

### 리소스 정책

초기에는 현재 설정을 유지한다.

```text
CPU     8
RAM     8GB
Swap    2GB
```

실제 사용 중 다음 상황이 발생할 때만 조정한다.

- 지속적인 Swap 사용
- OOM 발생
- DuckDB 대용량 Query 실패
- Airflow + Metabase 동시 실행 시 메모리 부족

---

## 3. 프로젝트 배경

일반적인 개인 데이터엔지니어링 프로젝트는 다음 수준에서 끝나는 경우가 많다.

```text
CSV
 ↓
Python
 ↓
Database
 ↓
Dashboard
```

이 구조만으로는 실제 데이터 파이프라인에서 발생하는 다음 문제를 보여주기 어렵다.

- 신규 데이터의 증분 적재
- 데이터 중복
- 변경 데이터
- 지연 도착 데이터
- 배치 실패
- 부분 재처리
- 데이터 품질 문제
- Dimension 이력 관리
- 원천 데이터와 분석 모델 분리

따라서 이번 프로젝트에서는 **원천 서비스 DB부터 분석용 Data Mart까지의 전체 데이터 흐름을 직접 구성**한다.

---

## 4. 프로젝트 목표

### G1. 데이터 파이프라인 구축

PostgreSQL 원천 데이터를 주기적으로 읽어 Object Storage에 적재한다.

```text
PostgreSQL
    ↓
Airflow
    ↓
MinIO
```

### G2. 증분 데이터 처리

매 실행마다 전체 데이터를 다시 처리하지 않고 변경된 데이터만 처리한다.

구현 대상:

- Watermark
- Incremental Extract
- Incremental Model
- MERGE / Upsert
- Idempotency

### G3. 분석 데이터 모델 구축

OLTP 형태의 원천 데이터를 Star Schema 기반 분석 모델로 변환한다.

```text
Source

customers
orders
order_items
products
payments
sellers

        ↓

Data Mart

dim_customer
dim_product
dim_seller
dim_date

fact_orders
fact_order_items
fact_payments
```

### G4. 변경 이력 관리

고객 속성 등의 변경을 SCD Type 2 방식으로 보존한다.

### G5. Data Quality 구축

데이터 오류를 자동으로 검출한다.

- unique
- not_null
- relationships
- accepted_values
- freshness
- custom tests

### G6. 파이프라인 장애 복구

단순 성공 실행뿐만 아니라 다음을 검증한다.

- Retry
- Backfill
- Re-run
- Late Arriving Data
- Duplicate Processing
- Partial Failure

---

## 5. 비목표

첫 버전에서는 다음 기능을 구현하지 않는다.

- Kafka 기반 Streaming
- Spark
- Debezium CDC
- Kubernetes
- Terraform
- AWS S3
- Snowflake
- Databricks

이유는 기존 스트리밍 프로젝트와 역할을 분리하고, 첫 버전에서는 **Batch + DW + Modeling + Data Quality**에 집중하기 위함이다.

Kafka/Spark를 추가하지 않는 것은 기술적 한계가 아니라 **프로젝트 범위 선택**이다.

---

## 6. 데이터셋

### 기본 데이터

**Olist Brazilian E-Commerce Dataset**을 초기 Seed Dataset으로 사용한다.

사용 대상:

- customers
- orders
- order_items
- products
- payments
- sellers

필요 시 후순위로 다음을 추가한다.

- reviews
- geolocation

### 초기 데이터의 역할

Olist 원본 CSV를 분석 데이터로 바로 사용하지 않는다.

```text
Olist CSV
    ↓
초기 Seed
    ↓
PostgreSQL Source DB
```

즉 PostgreSQL을 실제 애플리케이션의 Operational Database처럼 사용한다.

---

## 7. Synthetic Data Generator

정적 Olist 데이터만으로는 증분 처리와 변경 시나리오를 재현하기 어렵기 때문에 Python 기반 Generator를 별도로 구현한다.

### 정상 데이터

Generator는 다음 데이터를 지속적으로 생성한다.

- 신규 고객
- 신규 주문
- 신규 주문 상품
- 신규 결제
- 주문 상태 변경
- 결제 상태 변경
- 고객 속성 변경

초기 개발 시 실행 단위 예시:

```text
신규 고객       10~50
신규 주문      100~500
Order Item    200~1,500
Payment       주문 수에 비례
```

수치는 Config로 조정한다.

### 테스트용 이상 데이터

Generator에 장애 주입 기능을 추가한다.

예:

- duplicate_order
- null_customer
- invalid_product
- late_arriving_order
- invalid_order_status
- delayed_payment

예시:

```text
1,000 Orders

├─ 정상                    980
├─ Duplicate                5
├─ Late Arrival             5
├─ Invalid FK               5
└─ NULL / Invalid Value     5
```

실제 비율은 설정값으로 제어한다.

---

## 8. 시스템 아키텍처

```text
                    ┌──────────────────┐
                    │ Olist CSV Dataset│
                    └─────────┬────────┘
                              │ Seed
                              ▼

┌─────────────────────────────────────────────┐
│              PostgreSQL Source              │
│                                             │
│ customers                                   │
│ products                                    │
│ orders                                      │
│ order_items                                 │
│ payments                                    │
│ sellers                                     │
└───────────────────┬─────────────────────────┘
                    ▲
                    │
             Synthetic Generator
                    │
                    ▼

              Apache Airflow
                    │
            Incremental Extract
                    │
                    ▼
┌─────────────────────────────────────────────┐
│                   MinIO                     │
│                                             │
│ bronze/                                     │
│ ├── customers/                              │
│ ├── products/                               │
│ ├── orders/                                 │
│ ├── order_items/                            │
│ └── payments/                               │
│                                             │
│ Format: Parquet                             │
└───────────────────┬─────────────────────────┘
                    │
                    ▼
                  DuckDB
                    │
                    ▼
                   dbt
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼

    staging    intermediate     marts
                                  │
                                  ▼
                       Star Schema / SCD2
                                  │
                                  ▼
                              Metabase
```

---

## 9. 기술 스택

| 영역              | 기술                 |
| ----------------- | -------------------- |
| OS                | WSL2 Ubuntu          |
| Language          | Python               |
| Query             | SQL                  |
| Dependency        | uv                   |
| Source DB         | PostgreSQL           |
| Workflow          | Apache Airflow       |
| Object Storage    | MinIO                |
| Storage Format    | Parquet              |
| Analytical Engine | DuckDB               |
| Transformation    | dbt                  |
| Data Modeling     | Dimensional Modeling |
| Quality           | dbt tests            |
| Visualization     | Metabase             |
| Container         | Docker               |
| Infrastructure    | Docker Compose       |
| Version Control   | Git / GitHub         |

---

## 10. Source Database 설계

초기 핵심 엔티티:

- customers
- products
- sellers
- orders
- order_items
- payments

관계:

```text
customers
    │
    └──< orders
            │
            ├──< order_items >── products
            │          │
            │          └────── sellers
            │
            └──< payments
```

Source DB는 최대한 **OLTP 모델**로 유지한다.

분석 편의를 위해 Source DB 자체를 Star Schema 형태로 바꾸지 않는다.

---

## 11. 데이터 레이크 구조

MinIO에는 Parquet 기반 Bronze 데이터를 저장한다.

예:

```text
commerce-lake/

bronze/
├── customers/
│   └── extract_date=2026-09-03/
│       └── part-00001.parquet
│
├── orders/
│   └── extract_date=2026-09-03/
│       └── part-00001.parquet
│
├── order_items/
├── products/
└── payments/
```

초기에는 Bronze Layer만 Object Storage에 둔다.

`Silver / Gold`를 MinIO에 모두 복제하는 구조는 처음부터 만들지 않는다.

Transformation 책임은 dbt와 Analytical Layer에 둔다.

---

## 12. dbt Layer 설계

### Staging

원천 데이터 정리.

- stg_customers
- stg_products
- stg_orders
- stg_order_items
- stg_payments
- stg_sellers

책임:

- 컬럼명 표준화
- 타입 변환
- NULL 처리
- Timestamp 정규화
- 불필요 컬럼 제거
- 기본 데이터 검증

### Intermediate

복잡한 변환 로직을 분리한다.

- int_orders_enriched
- int_order_items_enriched
- int_customer_history
- int_payment_summary

### Mart

#### Dimensions

- dim_customer
- dim_product
- dim_seller
- dim_date

#### Facts

- fact_orders
- fact_order_items
- fact_payments

---

## 13. SCD Type 2

초기 SCD 대상은 `dim_customer` 하나로 제한한다.

예를 들어 다음 속성 변경을 추적한다.

- membership_level
- customer_city
- customer_state

모델:

```text
customer_key
customer_id
membership_level
valid_from
valid_to
is_current
```

예:

```text
customer_key | customer_id | grade  | valid_from | valid_to   | current
101          | C001        | SILVER | 2026-01-01 | 2026-05-31 | false
392          | C001        | GOLD   | 2026-06-01 | NULL       | true
```

Synthetic Generator가 실제 변경 이벤트를 발생시키도록 한다.

---

## 14. Airflow Pipeline

초기 DAG:

```text
generate_data
      │
      ▼
extract_source
      │
      ▼
load_minio
      │
      ▼
dbt_staging
      │
      ▼
dbt_intermediate
      │
      ▼
dbt_marts
      │
      ▼
dbt_test
```

실제 구성에서는 Generator를 별도 DAG로 분리할 수 있다.

권장 최종 구조:

```text
source_simulation_dag

warehouse_pipeline_dag
```

데이터 생성과 데이터 처리의 책임을 분리한다.

---

## 15. 증분 처리

### Extract

예:

```text
last_successful_watermark
        ↓

PostgreSQL

updated_at > watermark
        ↓

신규 / 변경 row
```

Watermark는 Airflow Metadata에만 암묵적으로 의존하지 않고 별도 상태로 관리할 수 있도록 설계한다.

### Idempotency

같은 배치를 다시 실행해도 결과가 변하지 않아야 한다.

잘못된 예:

```text
2026-09-03 Batch

첫 실행
→ 10,000 rows

동일 Batch 재실행
→ 10,000 rows

Final
→ 20,000 rows
```

기대 결과:

```text
Before 10,000
Re-run 10,000

Final 10,000
```

---

## 16. Late Arriving Data

Generator가 일부러 다음 데이터를 생성한다.

```text
event_date
2026-09-01

inserted_at
2026-09-03
```

단순한 다음 방식으로는 해당 데이터가 유실될 수 있다.

```sql
WHERE event_date = CURRENT_DATE
```

따라서 다음 중 하나를 적용하고 비교한다.

- Watermark
- Lookback Window
- MERGE

이 시나리오는 주요 포트폴리오 사례로 사용한다.

---

## 17. Data Quality

### 기본 테스트

dbt를 이용하여 최소 다음을 적용한다.

#### unique

예:

- order_id
- customer_key
- product_key

#### not_null

예:

- order_id
- customer_id
- order_date

#### relationships

예:

```text
fact_orders.customer_key
→
dim_customer.customer_key
```

#### accepted_values

예:

```text
order_status

created
approved
shipped
delivered
canceled
```

### 추가 테스트

직접 구현할 테스트:

```text
payment_value >= 0

quantity > 0

delivered_at >= purchased_at

order_total
=
sum(order_item)
```

---

## 18. 오류 데이터 처리

Data Quality 실패 데이터를 무조건 삭제하지 않는다.

초기 정책:

```text
Source
   ↓
Validation
   │
   ├── 정상
   │     ↓
   │    Mart
   │
   └── 오류
         ↓
      Reject / Quarantine
```

예시 테이블:

```text
invalid_records

record_id
source_table
error_type
error_message
detected_at
raw_payload
```

후속 단계에서 이 구조를 구현한다.

---

## 19. Backfill

예를 들어:

```text
2026-09-01 성공
2026-09-02 실패
2026-09-03 성공
```

이라면 9월 2일 데이터만 다시 처리할 수 있어야 한다.

요구사항:

```text
특정 logical date 지정
        ↓
해당 날짜 Extract
        ↓
재처리
        ↓
중복 없이 Merge
```

Airflow의 재실행 기능에만 의존하지 않고 데이터 처리 코드 자체가 Backfill 가능한 구조여야 한다.

---

## 20. BI 요구사항

Metabase는 프로젝트의 핵심이 아니라 **최종 데이터 검증 및 활용 계층**으로 사용한다.

초기 Dashboard는 3개 정도로 제한한다.

### Sales Overview

- 일별 매출
- 주문 건수
- 평균 주문 금액

### Product

- 카테고리별 매출
- Top Product
- 판매량

### Customer

- 신규 고객
- 재구매 고객
- 지역별 고객

대시보드 디자인에 과도하게 시간을 사용하지 않는다.

---

## 21. 핵심 기능 요구사항

| ID    | 요구사항                              | 우선순위 |
| ----- | ------------------------------------- | -------- |
| FR-01 | Olist 데이터를 PostgreSQL에 초기 적재 | P0       |
| FR-02 | Synthetic 신규 주문 데이터 생성       | P0       |
| FR-03 | PostgreSQL 증분 Extract               | P0       |
| FR-04 | MinIO Parquet 적재                    | P0       |
| FR-05 | Airflow DAG 실행                      | P0       |
| FR-06 | dbt Staging 구축                      | P0       |
| FR-07 | Star Schema 구축                      | P0       |
| FR-08 | dbt Data Quality Test                 | P0       |
| FR-09 | Pipeline Idempotency                  | P0       |
| FR-10 | Backfill                              | P0       |
| FR-11 | Late Arriving Data 처리               | P0       |
| FR-12 | SCD Type 2                            | P1       |
| FR-13 | Quarantine                            | P1       |
| FR-14 | Metabase Dashboard                    | P1       |
| FR-15 | 대용량 데이터 생성                    | P1       |
| FR-16 | 성능 비교                             | P1       |
| FR-17 | Cloud Migration                       | P2       |
| FR-18 | CDC                                   | P2       |

---

## 22. 비기능 요구사항

### 재현성

다른 환경에서도 다음을 중심으로 기본 인프라를 재현할 수 있어야 한다.

```bash
git clone <repository>
cd commerce-data-platform
docker compose up -d
```

### 설정 분리

다음 값은 코드에 직접 작성하지 않는다.

- DB Password
- MinIO Credentials
- Ports
- Database URL
- Bucket Name

`.env`로 관리한다.

실제 `.env`는 Git에 포함하지 않는다.

```text
.env
.env.example
```

구조를 사용한다.

### 관측 가능성

최소한 다음 정보를 확인할 수 있어야 한다.

- Batch Start
- Batch End
- Rows Extracted
- Rows Loaded
- Rows Rejected
- Execution Time
- Pipeline Status

향후 별도 메타데이터 테이블을 둘 수 있다.

예:

```text
pipeline_run

run_id
pipeline_name
started_at
finished_at
rows_read
rows_written
rows_failed
status
```

---

## 23. 성능 실험

기능 구현 이후 데이터 크기를 점진적으로 확대한다.

```text
100K
 ↓
1M
 ↓
5M
```

필요하면 10M까지 확장한다.

### Full Refresh vs Incremental

비교 지표:

- 처리 시간
- 읽은 Row 수
- 처리 Row 수

### CSV vs Parquet

비교 지표:

- 파일 크기
- 읽기 시간
- 분석 Query 시간

### 전체 스캔 vs Partition Filtering

비교 지표:

- Scan Size
- Execution Time

프로젝트 포트폴리오에 실제 측정값을 기록한다.

---

## 24. 성공 기준

| 지표                  | 목표                                    |
| --------------------- | --------------------------------------- |
| E2E Pipeline          | 자동 실행 가능                          |
| Incremental Load      | Full Refresh 없이 신규/변경 데이터 처리 |
| Idempotency           | 동일 Batch 재실행 시 중복 0건           |
| Backfill              | 날짜 지정 재처리 가능                   |
| Data Quality          | 의도적으로 생성한 오류 탐지 가능        |
| Referential Integrity | Fact-Dimension 관계 검증                |
| SCD2                  | 고객 속성 변경 이력 보존                |
| Late Arrival          | 지연 데이터 유실 없이 처리              |
| Data Mart             | Fact/Dimension 모델 완성                |
| BI                    | Mart 데이터 기반 조회 가능              |
| Reproducibility       | Docker 기반 환경 재현 가능              |

성능 수치는 구현 전에 임의로 목표값을 정하지 않는다.

**Baseline 측정 → 개선 → 개선율 산출** 방식으로 진행한다.

---

## 25. 디렉터리 구조

초기 목표:

폴더 구조는 다음 책임 분리를 기준으로 한다.

- 프로젝트 전체 실행/설정: 프로젝트 루트
- Python 데이터 처리 로직: `src/`
- Workflow 정의와 Airflow 전용 설정: `airflow/`
- Transformation 및 Dimensional Modeling: `dbt/`
- Source DB DDL 및 검증 SQL: `sql/`
- 로컬 데이터: `data/`
- 반복 실행 스크립트: `scripts/`
- 테스트: `tests/`
- 설계/의사결정/벤치마크 문서: `docs/`

Docker 관련 파일을 무조건 별도 `docker/` 디렉터리에 모으지 않는다. 현재처럼 커스텀 이미지 수가 적은 프로젝트에서는 서비스별 응집도를 우선하여 `airflow/Dockerfile`처럼 배치한다.

```text
commerce-data-platform/
│
├── README.md
├── PRD.md
├── compose.yaml
├── .env.example
├── pyproject.toml
│
├── airflow/
│   ├── Dockerfile              # 필요할 때만 생성
│   ├── dags/
│   └── logs/                   # Git 제외
│
├── src/
│   ├── generator/
│   ├── ingestion/
│   └── common/
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── snapshots/
│   └── tests/
│
├── data/
│   └── seed/
│
├── sql/
│   └── source/
│
├── scripts/
│
└── docs/
    ├── architecture/
    ├── decisions/
    └── benchmarks/
```

---

## 26. 개발 단계

### Phase 1. Source Environment

목표:

> 실제 서비스와 유사한 원천 데이터 환경 구성

구현:

- WSL 환경
- `compose.yaml` 기반 Docker Compose
- PostgreSQL 공식 이미지
- Olist Dataset
- Source Schema
- Seed Loader

완료 조건:

```text
Olist → PostgreSQL 적재 성공
테이블 관계 정상
기본 Query 정상
```

### Phase 2. Data Generator

구현:

- Customer Generator
- Order Generator
- Payment Generator
- State Transition
- Customer Update

완료 조건:

```text
Generator 실행
→ PostgreSQL 신규 데이터 발생
```

### Phase 3. Data Lake

구현:

- MinIO
- Parquet
- Incremental Extract
- Partition Layout

흐름:

```text
PostgreSQL
→ Python
→ Parquet
→ MinIO
```

### Phase 4. Airflow

구현:

- Scheduling
- Dependency
- Retry
- Backfill
- Watermark

### Phase 5. dbt + DuckDB

구현:

```text
Source
Staging
Intermediate
Mart
```

이 단계부터 SQL과 모델링 비중을 크게 가져간다.

### Phase 6. Data Quality

구현:

- dbt tests
- Custom Tests
- Invalid Data Generator
- Quarantine

### Phase 7. Reliability Scenarios

의도적으로 다음 문제를 발생시킨다.

- 중복 데이터
- Late Arrival
- Batch Failure
- Missing Data
- Broken FK
- NULL
- 재실행
- Backfill

이 단계가 **포트폴리오의 핵심 문제 해결 사례**가 된다.

### Phase 8. Benchmark

```text
100K
1M
5M
```

규모별 처리 성능을 측정한다.

### Phase 9. BI

Metabase Dashboard를 연결하여 Data Mart의 사용 가능성을 검증한다.

---

## 27. 향후 확장

첫 번째 버전이 완성된 이후에만 진행한다.

### Cloud Migration

```text
MinIO
→ AWS S3

DuckDB
→ Snowflake
```

소량의 데이터만 사용하여 Migration PoC를 진행한다.

### CDC

```text
PostgreSQL
 ↓
Debezium
 ↓
Kafka
 ↓
Warehouse
```

Batch 방식과 비교한다.

### CI

GitHub Actions에서 다음을 검토한다.

```text
dbt compile
dbt test
Python Test
Lint
```

---

## 28. 최종 포트폴리오에서 보여줄 내용

기술 스택 목록보다 다음 순서로 설명한다.

1. 왜 이 아키텍처를 선택했는가
2. 원천 데이터를 어떻게 증분 수집했는가
3. 분석 모델을 어떻게 설계했는가
4. 데이터 변경 이력을 어떻게 관리했는가
5. 데이터 오류를 어떻게 탐지했는가
6. Batch 실패 후 어떻게 복구했는가
7. 동일 Batch 재실행에서 어떻게 중복을 방지했는가
8. Late Arriving Data를 어떻게 처리했는가
9. 데이터 규모 증가에 따라 무엇이 병목이 되었는가
10. 측정 후 무엇을 개선했는가

### 프로젝트 구조 원칙

- Compose 진입점은 프로젝트 루트의 `compose.yaml`로 통일한다.
- PostgreSQL, MinIO, Metabase는 우선 공식 이미지를 사용한다.
- 커스텀 Dockerfile은 실제 Build 요구가 있는 서비스에만 둔다.
- Airflow 커스텀 이미지가 필요하면 `airflow/Dockerfile`에 배치한다.
- Airflow DAG에는 오케스트레이션 책임만 두고 실제 데이터 처리 로직은 `src/`에 둔다.
- dbt는 Transformation과 분석 모델링 책임을 담당한다.

초기 확정 범위를 한 줄로 정리하면 다음과 같다.

> **WSL2 Ubuntu + Docker 환경에서 Olist와 Synthetic 데이터를 PostgreSQL 원천 DB로 구성하고, Airflow → MinIO/Parquet → DuckDB/dbt → Data Mart → Metabase 파이프라인을 구축하여 증분 적재, SCD2, 데이터 품질, Backfill, Late Arrival, Idempotency를 검증한다.**
