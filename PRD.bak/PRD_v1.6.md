# PRD: Commerce Analytics Data Platform

> Version: 1.6
>
> Status: Implementation-Ready Baseline
>
> Baseline Date: 2026-09-09
>
> Environment: WSL2 Ubuntu / Local
>
> Goal: 데이터엔지니어 취업 포트폴리오용 Batch DW·Data Mart 프로젝트

---

## 0. v1.6 변경 요약

v1.6은 v1.5의 Source 원본 보존, 증분 수집, Bronze 불변성, SCD2, Late Arrival, 재처리 계약을 유지하면서 계정과 개인 멤버십의 분석 단위를 분리한다.

- `customers`는 계정 단위의 고정 속성만 보관하고, 개인 단위 멤버십 이력은 새 `customer_memberships` Source Table로 분리한다.
- `customers`의 증분 Cursor는 `created_at`, `customer_memberships`의 증분 Cursor는 `updated_at`으로 고정한다.
- 멤버십만 Staging과 Warehouse에서 SCD2로 관리한다. 도시·주 값은 계정 속성과 주문 발생 시점 스냅샷으로 유지한다.
- 결정적 Generator는 실제 멤버십 변경을 만들며, 재기준화 작업은 7개 Source Table의 Seed·수집·Catalog 동기화를 다시 검증한다.
- Staging의 `order_status`는 원천 8개 상태를 대문자로만 표준화해 세부 운영 상태를 보존한다. 다중 상태 집계는 반복 사용될 때만 dbt Macro 또는 Mapping Seed로 재사용한다.

v1.5에서 확정한 다음 계약은 유지한다.

- PostgreSQL과 Bronze에서 선택한 Olist 원본 Table/Column Name과 값을 최대한 보존
- `customer_id`는 Source PK/FK, `customer_unique_id`는 dbt 이후 분석 고객 Business Key로 역할 분리
- Entity Prefix 제거, Timestamp Rename, 상태 표준화는 dbt Staging 책임
- 증분용 `created_at`/`updated_at`과 Synthetic 시나리오 필드만 Source 확장으로 허용
- V1 미사용 컬럼 6개는 Raw CSV에만 보존
- 모든 운영 시간은 UTC `TIMESTAMPTZ`
- Seed Loader는 Staging + Transactional UPSERT
- Source PK/FK/CHECK/Numeric Precision/Incremental Index 정의
- 증분 범위는 `(watermark_before, extract_upper_bound]`
- Keyset Pagination, 빈 Batch, 테이블별 수집 잠금, Watermark CAS
- `batch_id`, `run_id`, `table_batch_id`, `reprocess_id` 의미 분리
- Bronze 기술 컬럼, Manifest, Checksum, Commit Protocol
- Quarantine은 SeaweedFS Parquet Prefix
- Reject가 있어도 Threshold 이하이면 Valid Row Commit 후 Watermark 전진
- Pipeline Metadata Physical Schema 및 상태 전이
- DuckDB Bronze File Catalog는 Metadata의 COMMITTED Object만 동기화
- Mutable 최신 상태, SCD2 관측 이력, Temporal Join
- Late Arrival 영향 범위와 Mart 재계산
- Airflow Task 경계, Retry, XCom 제한
- Source/Bronze Naming 보존과 dbt Staging Naming Mapping 자동 검증
- SeaweedFS 기반 Local S3-compatible Object Storage

기술 Version은 v1.5와 동일하다.

---

## 1. 프로젝트 개요

### 1.1 한 줄 정의

Olist 공개 데이터를 Seed로 서비스형 OLTP Source를 구성하고, 결정적 Synthetic 데이터로 변경을 발생시켜 **증분 수집 → S3-compatible Bronze → DW 변환·모델링 → 품질 검증 → 재처리·복구**를 구현한다.

### 1.2 핵심 메시지

> 정적 CSV를 한 번 적재하는 프로젝트가 아니라 변경·지연·중복·실패가 발생하는 환경에서 Batch Pipeline의 신뢰성과 분석 모델의 정합성을 설계하고 검증한다.

### 1.3 성공의 정의

1. 동일 입력은 동일 Source 변경과 동일 Mart 상태를 만든다.
2. 실패한 Table Batch는 해당 Table Watermark를 전진시키지 않는다.
3. 동일 Batch를 반복 실행해도 중복 결과가 생기지 않는다.
4. 늦게 도착한 과거 이벤트가 Cursor에 누락되지 않고 최종 Mart에 반영된다.
5. 주문은 발생 시점에 유효한 고객 Dimension Version을 참조한다.
6. 실행 결과를 Metadata와 Test Evidence로 추적할 수 있다.
7. Source Writer와 Warehouse Extract의 동시성으로 Parent/Child 관측 불일치가 발생하지 않는다.
8. 문서의 명령만으로 새 로컬 환경에서 재현할 수 있다.

---

## 2. 범위와 아키텍처

### 2.1 V1 흐름

```text
Olist CSV
   │ Seed Transformation
   ▼
PostgreSQL: commerce_source ◀── Deterministic Synthetic Generator
   │                              │
   │◀──── 원천 데이터 동시성 잠금 ────▶│
   ▼
Airflow LocalExecutor
   │ Incremental Extract / Validate
   ▼
SeaweedFS: Parquet Bronze + Quarantine
   │ 메타데이터 기반 커밋 파일 목록
   ▼
dbt + DuckDB
   ├── staging
   ├── intermediate
   └── marts ──▶ Metabase

PostgreSQL: pipeline_metadata
   └── Watermark / Lease / Run / Object / Reject / Seed / Generator
```

### 2.2 계층별 책임

| 계층        | 책임                                                              | 하지 않는 일                         |
| ----------- | ----------------------------------------------------------------- | ------------------------------------ |
| Source DB   | Olist 호환 Naming, 현재 서비스 상태, OLTP 무결성                  | 분석용 Naming, 집계, Corruption 저장 |
| Ingestion   | 수집 중 원천 변경 차단, 범위 고정, 추출, 기본 검증, Bronze Commit | Mart 계산                            |
| Bronze      | 재처리 가능한 Source 관측 이력                                    | 분석용 표준화, Silver/Gold 중복 저장 |
| DuckDB/dbt  | Naming/값 표준화, 최신 상태, 이력, Dimension/Fact                 | Source Watermark 관리                |
| Metadata DB | 제어 상태와 실행 증적, 메타데이터 커밋 상태 기준                  | 대용량 Payload 저장                  |
| Metabase    | 검증 완료 Mart 소비                                               | Raw Source 직접 조회                 |

### 2.3 불변 조건

- Watermark는 Metadata에서 `COMMITTED`인 Bronze 범위만 가리킨다.
- Commit된 Bronze Object는 수정·덮어쓰기·삭제하지 않는다.
- dbt는 Metadata에서 `COMMITTED`인 Object만 읽는다.
- PostgreSQL과 Bronze는 선택한 Olist Column Name을 유지하고 분석용 Rename은 dbt Staging에서만 수행한다.
- Business Event Time은 과거일 수 있지만 Mutable Row의 `updated_at`은 Source 변경을 놓치지 않도록 단조 증가한다.
- Warehouse Extract가 Source Snapshot Lease를 보유하는 동안 Generator는 Source를 변경하지 않는다.
- Fact Measure는 문서화된 Grain에서만 집계한다.
- Credential은 Git, SQL, dbt Model, Manifest에 기록하지 않는다.

### 2.4 비목표

Kafka, Spark, Debezium CDC, Source Delete/Tombstone, Kubernetes, Terraform, AWS S3/Snowflake/Databricks 상시 운영, 실시간 SLA, 외부 서비스의 무중단 Snapshot 문제, 다중 Worker Cluster는 V1 범위가 아니다.

---

## 3. 개발 환경과 Version Baseline

| 영역              | 기술                  |                    Version |
| ----------------- | --------------------- | -------------------------: |
| Language          | Python                |            3.12 Minor Line |
| Dependency        | uv                    |                     0.12.9 |
| Source / Metadata | PostgreSQL            |                       18.6 |
| Workflow          | Apache Airflow        |         3.3.1, Python 3.12 |
| Object Storage    | SeaweedFS             |                       4.45 |
| Storage Format    | PyArrow / Parquet     |                     25.0.1 |
| Warehouse         | DuckDB                |                      1.5.5 |
| Transformation    | dbt-core / dbt-duckdb |            1.12.3 / 1.11.0 |
| BI                | Metabase              | 0.63.16.1 Phase 9 Baseline |

직접 Python Dependency는 `pyproject.toml`, 전체 Resolution은 `uv.lock`으로 고정한다. Docker Image에는 `latest` Tag를 사용하지 않는다.

```text
duckdb              1.5.5
dbt-core            1.12.3
dbt-duckdb          1.11.0
pandas              3.0.5
pyarrow             25.0.1
psycopg[binary]     3.3.5
kagglehub           1.0.2
boto3               1.43.87
pytest              9.1.1   # development
ruff                0.16.5  # development
```

```text
postgres:18.6
apache/airflow:3.3.1-python3.12
chrislusf/seaweedfs:4.45
metabase/metabase:v0.63.16.1
```

Python 계약:

```toml
[project]
requires-python = ">=3.12,<3.13"
```

`.python-version`은 `3.12`다. 실제 Patch, Docker/WSL Version은 README와 Benchmark Metadata에 기록한다.

Version 변경 Gate:

```text
uv sync --frozen
pytest
ruff check .
dbt debug
dbt parse
dbt build
dbt test
DuckDB → SeaweedFS Parquet Read
Incremental Merge
동일 Batch Re-run
원천 데이터 동시성 잠금 Test
```

Root Compose 파일은 `compose.yaml`, Secret 원본은 Git 제외된 `.env`, 공개 Template은 `.env.example`이다.

---

## 4. 공통 데이터 계약

### 4.1 시간

- Source/Metadata Timestamp는 `TIMESTAMPTZ`, 저장·비교 기준은 UTC다.
- Python은 timezone-aware `datetime`만 허용한다.
- Parquet은 `timestamp[us, tz=UTC]`를 사용한다.
- Airflow Logical Date와 Data Interval은 UTC로 해석한다.
- Olist의 timezone 없는 Timestamp는 UTC naive 값으로 간주한다. 브라질 현지시각으로 추정하지 않으며 이 한계를 ADR에 남긴다.

| 시간                | 의미                                                                 |
| ------------------- | -------------------------------------------------------------------- |
| Business Event Time | 주문·승인·배송 등 실제 비즈니스 사건 시간                            |
| 원천 변경 시각      | 현재 Source Row Version이 생성·변경된 시간. `updated_at` Cursor 의미 |
| Ingestion Time      | Bronze에 수집한 시간                                                 |
| Logical Date        | Generator와 Batch의 결정적 실행 기준                                 |

#### `updated_at` 안전성 계약

`updated_at`은 Business Event Time이 아니다.

```text
order_purchase_timestamp = 2026-09-01T10:00:00Z
updated_at               = 2026-09-04T09:00:00Z
ingested_at              = 2026-09-04T10:00:00Z
```

위 Row는 9월 1일 주문이 9월 4일 Source에 늦게 반영된 Late Arrival이다.

Mutable Row 변경 규칙:

- Seed Row: `updated_at = --seeded-at`
- Synthetic 신규 Row: `created_at = updated_at = generator.logical_date`
- Synthetic 기존 Row 변경: 새 `updated_at`은 해당 Row의 기존 `updated_at`보다 반드시 커야 한다.
- Generator는 같은 Row를 동일 `updated_at`으로 서로 다른 값으로 갱신하지 않는다.
- `updated_at` 역행 또는 동일 Cursor 위치의 값 변경은 Source Contract 위반으로 실패한다.
- 과거 비즈니스 사건을 늦게 반영할 때는 Business Event Timestamp만 과거이고 `updated_at`은 현재 Generator Logical Date를 사용한다.

### 4.2 Naming과 타입

- Source/Bronze는 선택한 Olist 컬럼의 `snake_case` 이름과 Prefix를 그대로 보존한다.
- 프로젝트 확장 Column만 기존 Olist Column과 충돌하지 않는 `snake_case` 이름을 사용한다.
- dbt Staging이 Prefix 제거, Timestamp Suffix 통일과 표준화 상태값 변환을 담당한다.
- 표준화 상태값은 Staging 이후 대문자 문자열이다.
- 금액은 `NUMERIC(14,2)` / Parquet `decimal128(14,2)`다.
- 의미 없는 빈 문자열은 `NULL`로 정규화한다.
- Watermark Column과 PK는 `NOT NULL`이다.
- 문자열 Cursor 비교는 PostgreSQL `COLLATE "C"` 기준이다.

### 4.3 식별자

- Olist `customer_id`, `customer_unique_id`, `order_id`, `product_id`, `seller_id`를 문자열 그대로 보존한다.
- Source `orders.customer_id`는 원본처럼 `customers.customer_id`를 참조한다.
- 분석 고객 Business Key는 dbt Staging에서 Source `customer_unique_id`를 `customer_id`로 노출한다.
- 원본 `customer_id`는 Staging 이후 `source_customer_id`로 보존한다.
- 필요 시 원본 `customer_unique_id`는 Lineage용 `source_customer_unique_id`로 보존하되 분석 Join은 `customer_id`를 사용한다.
- Synthetic ID는 Seed/Logical Date를 포함한 UUIDv5 또는 결정적 Hash 문자열을 사용한다. Random UUIDv4는 Business ID로 사용하지 않는다.

---

## 5. Olist Raw-compatible Seed 계약

### 5.1 입력

- Dataset: `olistbr/brazilian-ecommerce`
- 필수: customers, orders, order_items, products, order_payments, sellers
- 선택: reviews, geolocation
- 다운로드: `uv run python scripts/download_dataset.py`
- 고정 입력 경로: `data/raw/olist/`

| CSV                                | PostgreSQL Source Table |
| ---------------------------------- | ----------------------- |
| `olist_customers_dataset.csv`      | `customers`             |
| `olist_orders_dataset.csv`         | `orders`                |
| `olist_order_items_dataset.csv`    | `order_items`           |
| `olist_order_payments_dataset.csv` | `order_payments`        |
| `olist_products_dataset.csv`       | `products`              |
| `olist_sellers_dataset.csv`        | `sellers`               |

### 5.2 보존 원칙

Seed Loader는 분석용 표준화를 수행하지 않는다.

1. V1 사용 컬럼을 명시적 Allowlist로 선택하고 선택된 CSV Header와 PostgreSQL Source Column Name을 동일하게 유지한다.
2. `customer_id`, `customer_unique_id`와 주문의 `customer_id`를 원본 그대로 적재한다.
3. `customer_city`, `seller_city`, `product_category_name` 같은 Entity Prefix를 제거하지 않는다.
4. `payment_sequential` 같은 원본 이름을 수정하지 않는다.
5. Olist 상태값은 소문자 원본 값을 유지한다.
6. Type 변환, 제약조건, 프로젝트 확장 필드 생성만 Seed 단계에서 수행한다.

| Table          | 확장 필드                                      | 목적                            |
| -------------- | ---------------------------------------------- | ------------------------------- |
| customers      | `created_at`                                   | 불변 주문 계정 증분 수집       |
| customer_memberships | `membership_level`, `created_at`, `updated_at` | 사람 단위 Synthetic 등급과 증분 수집 |
| products       | `created_at`, `updated_at`                     | 증분 수집                       |
| sellers        | `created_at`, `updated_at`                     | 증분 수집                       |
| orders         | `created_at`, `updated_at`                     | 생성/상태 변경 증분 수집        |
| order_items    | `created_at`                                   | Append-only 증분 수집           |
| order_payments | `payment_status`, `created_at`, `updated_at`   | Synthetic 결제 상태와 증분 수집 |

제외 컬럼:

| Source Table | 제외 컬럼                    | 제외 이유                                  |
| ------------ | ---------------------------- | ------------------------------------------ |
| customers    | `customer_zip_code_prefix`   | V1 고객 지역 분석은 city/state 사용        |
| order_items  | `shipping_limit_date`        | V1 KPI, 상태 전이, 품질검사에서 미사용     |
| products     | `product_name_lenght`        | 상품명 없이 길이만 제공되어 분석 가치 낮음 |
| products     | `product_description_lenght` | V1 Mart/Dashboard에서 미사용               |
| products     | `product_photos_qty`         | V1 Mart/Dashboard에서 미사용               |
| sellers      | `seller_zip_code_prefix`     | V1 판매자 지역 분석은 city/state 사용      |

제외 컬럼은 `data/raw/olist/` 원본 CSV에만 보존한다. 그 외 Allowlist 컬럼은 Source에서 Rename하거나 Merge하지 않는다.

### 5.3 확장 필드 생성 규칙

| Table            | 규칙                                                                                     |
| ---------------- | ---------------------------------------------------------------------------------------- |
| customers        | 연결 주문의 최초 `order_purchase_timestamp` → `created_at`                                |
| customer_memberships | 사람별 최소 계정 `created_at` → `created_at`, `--seeded-at` → `updated_at`          |
| orders           | `order_purchase_timestamp` → `created_at`, `--seeded-at` → `updated_at`                  |
| order_items      | 연결 주문의 `order_purchase_timestamp` → `created_at`                                    |
| products/sellers | `created_at = updated_at = --seeded-at`                                                  |
| order_payments   | 연결 주문의 `order_purchase_timestamp` → `created_at`, `--seeded-at` → `updated_at`      |

Seed `membership_level`은 `customer_unique_id`별 `order_status='delivered'` 주문 수로 계산해
사람 단위 `customer_memberships`에 정확히 한 행으로 기록한다.

Seed `payment_status`는 연결 주문이 `canceled` 또는 `unavailable`이면 `failed`, 그 외에는 `completed`다.

```text
bronze 0~4
silver 5~14
gold   15+
```

### 5.4 Seed CLI와 재실행

```bash
uv run python -m src.seed   --input-dir data/raw/olist   --seeded-at 2026-09-03T00:00:00Z
```

`--seeded-at`은 필수이며 모든 Raw Event Timestamp 이상이어야 한다. Seed Dataset 전체를 해당 시점에 관측한 Baseline Snapshot으로 취급한다.

실행 순서:

```text
Header/File 검증
→ 임시 PostgreSQL Schema Load
→ PK/FK/Status/Count 검증
→ 한 Transaction의 INSERT ... ON CONFLICT DO UPDATE
→ seed_runs 기록
```

- 입력 계산값만 갱신하고 Wall Clock으로 `updated_at`을 바꾸지 않는다.
- Target에만 있는 Synthetic Row는 삭제하지 않는다.
- 동일 Raw Checksum과 `seeded_at` 재실행 후 Count와 Content Hash가 같아야 한다.
- 전체 삭제는 `scripts/reset.sh` 책임이다.
- Generator 성공 이력이 존재하면 Seed Loader는 실행을 거부한다.

---

## 6. PostgreSQL Source 계약

한 PostgreSQL Container에 `commerce_source`, `airflow_metadata`, `pipeline_metadata` Database와 역할별 계정을 둔다.

### 6.1 Physical Table

#### customers

| Column               | Type         | Constraint                     |
| -------------------- | ------------ | ------------------------------ |
| `customer_id`        | VARCHAR(64)  | PK, Olist 원본                 |
| `customer_unique_id` | VARCHAR(64)  | NOT NULL, 중복 허용            |
| `customer_city`      | VARCHAR(128) | 원본 Prefix 보존               |
| `customer_state`     | CHAR(2)      | 원본 Prefix 보존               |
| `created_at`         | TIMESTAMPTZ  | NOT NULL                       |

#### customer_memberships

| Column               | Type         | Constraint                     |
| -------------------- | ------------ | ------------------------------ |
| `customer_unique_id` | VARCHAR(64)  | PK, 사람 Business Key          |
| `membership_level`   | VARCHAR(16)  | NOT NULL, `bronze/silver/gold` |
| `created_at`         | TIMESTAMPTZ  | NOT NULL                       |
| `updated_at`         | TIMESTAMPTZ  | NOT NULL, `>= created_at`      |

#### products

| Column                  | Type         | Constraint                |
| ----------------------- | ------------ | ------------------------- |
| `product_id`            | VARCHAR(64)  | PK                        |
| `product_category_name` | VARCHAR(256) | 원본 Prefix 보존          |
| `product_weight_g`      | INTEGER      | NULL 또는 `>= 0`          |
| `product_length_cm`     | INTEGER      | NULL 또는 `>= 0`          |
| `product_height_cm`     | INTEGER      | NULL 또는 `>= 0`          |
| `product_width_cm`      | INTEGER      | NULL 또는 `>= 0`          |
| `created_at`            | TIMESTAMPTZ  | NOT NULL                  |
| `updated_at`            | TIMESTAMPTZ  | NOT NULL, `>= created_at` |

#### sellers

| Column         | Type         | Constraint                |
| -------------- | ------------ | ------------------------- |
| `seller_id`    | VARCHAR(64)  | PK                        |
| `seller_city`  | VARCHAR(128) | 원본 Prefix 보존          |
| `seller_state` | CHAR(2)      | 원본 Prefix 보존          |
| `created_at`   | TIMESTAMPTZ  | NOT NULL                  |
| `updated_at`   | TIMESTAMPTZ  | NOT NULL, `>= created_at` |

#### orders

| Column                          | Type        | Constraint                         |
| ------------------------------- | ----------- | ---------------------------------- |
| `order_id`                      | VARCHAR(64) | PK                                 |
| `customer_id`                   | VARCHAR(64) | NOT NULL, FK customers             |
| `order_status`                  | VARCHAR(16) | NOT NULL, 원본 소문자 Domain CHECK |
| `order_purchase_timestamp`      | TIMESTAMPTZ | NOT NULL                           |
| `order_approved_at`             | TIMESTAMPTZ | NULL 가능                          |
| `order_delivered_carrier_date`  | TIMESTAMPTZ | NULL 가능                          |
| `order_delivered_customer_date` | TIMESTAMPTZ | NULL 가능                          |
| `order_estimated_delivery_date` | TIMESTAMPTZ | NULL 가능                          |
| `created_at`                    | TIMESTAMPTZ | NOT NULL                           |
| `updated_at`                    | TIMESTAMPTZ | NOT NULL, `>= created_at`          |

`order_status` Domain:

```text
created
approved
processing
invoiced
shipped
delivered
canceled
unavailable
```

#### order_items

| Column          | Type          | Constraint            |
| --------------- | ------------- | --------------------- |
| `order_id`      | VARCHAR(64)   | PK, FK orders         |
| `order_item_id` | INTEGER       | PK, `> 0`             |
| `product_id`    | VARCHAR(64)   | NOT NULL, FK products |
| `seller_id`     | VARCHAR(64)   | NOT NULL, FK sellers  |
| `price`         | NUMERIC(14,2) | NOT NULL, `>= 0`      |
| `freight_value` | NUMERIC(14,2) | NOT NULL, `>= 0`      |
| `created_at`    | TIMESTAMPTZ   | NOT NULL              |

#### order_payments

| Column                 | Type          | Constraint                    |
| ---------------------- | ------------- | ----------------------------- |
| `order_id`             | VARCHAR(64)   | PK, FK orders                 |
| `payment_sequential`   | INTEGER       | PK, `> 0`, 원본 이름 보존     |
| `payment_type`         | VARCHAR(32)   | NOT NULL                      |
| `payment_installments` | INTEGER       | NULL 또는 `>= 0`              |
| `payment_value`        | NUMERIC(14,2) | NOT NULL, `>= 0`              |
| `payment_status`       | VARCHAR(16)   | NOT NULL, 소문자 Domain CHECK |
| `created_at`           | TIMESTAMPTZ   | NOT NULL                      |
| `updated_at`           | TIMESTAMPTZ   | NOT NULL, `>= created_at`     |

`payment_status` Domain:

```text
pending
completed
failed
refunded
```

### 6.2 증분 Index

```text
customers       (created_at, customer_id)
customer_memberships (updated_at, customer_unique_id)
products        (updated_at, product_id)
sellers         (updated_at, seller_id)
orders          (updated_at, order_id)
order_items     (created_at, order_id, order_item_id)
order_payments  (updated_at, order_id, payment_sequential)
```

Index와 Extract 정렬 순서는 같아야 하며 문자열 Key는 `COLLATE "C"` 비교 규칙과 일치시킨다.

### 6.3 Transaction과 원천 변경 시각

- Order/Item/Payment 생성은 하나의 Transaction이다.
- 상태와 관련 Timestamp 변경은 하나의 Transaction이다.
- 완료 주문 수 재계산과 동일 `customer_unique_id`의 모든 Customer Row Membership 갱신도 같은 Transaction에 포함한다.
- `updated_at`은 **결정적으로 주입된 원천 변경 시각**이다.
- Generator에서는 `logical_date`가 원천 변경 시각 역할을 한다.
- 기존 Row를 변경하는 Transaction은 해당 Row의 직전 `updated_at`보다 큰 원천 변경 시각만 허용한다.
- Business Event Time은 원천 변경 시각보다 과거일 수 있다.
- `updated_at` 역행 및 동일 Cursor 위치의 서로 다른 내용은 금지한다.

---

## 7. Generator와 Anomaly

### 7.1 상태 전이와 Canonical Mapping

Generator 신규 전이:

```text
Source order_status
created → approved → shipped → delivered
   └──────────────→ canceled
          └──────→ canceled

Source payment_status
pending → completed → refunded
   └──→ failed
```

Olist Seed의 `processing`, `invoiced`, `unavailable`도 Source Domain에 포함하지만 Generator 신규 전이에는 사용하지 않는다.

dbt Staging Order Status Mapping:

| Source        | `order_status` |
| ------------- | -------------- |
| `created`     | `CREATED`      |
| `approved`    | `APPROVED`     |
| `processing`  | `PROCESSING`   |
| `invoiced`    | `INVOICED`     |
| `shipped`     | `SHIPPED`      |
| `delivered`   | `DELIVERED`    |
| `canceled`    | `CANCELED`     |
| `unavailable` | `UNAVAILABLE`  |

`order_status`는 운영 세부 상태를 보존하는 표준 상태다. 여러 상태를 묶는 집계가 반복될 때만
dbt Macro 또는 Mapping Seed로 조건을 재사용하며, Staging·Intermediate·Fact에 별도 분석 그룹
컬럼을 저장하지 않는다.

Payment Mapping:

```text
pending   → PENDING
completed → COMPLETED
failed    → FAILED
refunded  → REFUNDED
```

### 7.2 결정 입력

```bash
uv run python -m src.generator   --seed 42   --logical-date 2026-09-04T00:00:00Z   --orders 1000   --anomaly-profile default
```

결정성 입력:

```text
random_seed
logical_date
order_count
anomaly_profile
generator_version
```

후보 DB Row는 Business Key로 정렬한 뒤 선택한다. 같은 결정 ID가 이미 존재하면 기대값과 비교해 같으면 Skip, 다르면 결정성 위반으로 실패한다.

Synthetic 고객:

- `customer_unique_id`: 동일 인물의 안정적 ID
- `customer_id`: 주문 시점 Customer Record ID
- 재구매는 기존 `customer_unique_id` + 새 `customer_id`
- 주문은 새 `customer_id`를 FK로 참조
- 주소 변경은 새 Customer Record에 반영하고 과거 Row는 유지
- Membership 변경은 같은 `customer_unique_id`의 모든 Row를 동일 Transaction에서 갱신

### 7.3 원천 데이터 동시성 잠금

V1에서 Source Writer는 Seed Loader와 Synthetic Generator뿐이다. Seed는 Generator 시작 전만 허용하므로 Runtime 동시성 대상은 Generator와 Warehouse다.

`pipeline_metadata.source_mutation_leases`:

```text
resource_name PK = 'commerce_source'
owner_type          GENERATOR | WAREHOUSE
owner_id            UUID
lease_expires_at    TIMESTAMPTZ
version             BIGINT
updated_at          TIMESTAMPTZ
```

규칙:

- Generator는 Source Transaction 전 `GENERATOR` Lease 획득
- Warehouse는 모든 Table Upper Bound 계산 전 `WAREHOUSE` Lease 획득
- 두 Lease는 상호 배타적
- Warehouse는 모든 Table Extract/Validation 종료까지 Lease 유지
- Warehouse의 원천 데이터 동시성 잠금 중 Generator 원천 변경 금지
- 기본 TTL 30분, 5분마다 연장
- 실패 경로 Release Task는 `all_done`
- 만료 Lease 인수는 CAS/version 검증

이 계약은 Synthetic Source를 사용하는 V1용이다. 실제 외부 운영 DB의 무중단 일관 Snapshot 문제를 일반화하지 않는다.

### 7.4 Anomaly

- Service-level: Late Order, Delayed Payment, 과거 Event의 늦은 Update, Membership 변경
- Pipeline-level: Duplicate, NULL Key, Broken FK, Invalid Status, 음수 금액

파이프라인 오류 주입은 Extract 후 Validation 직전 복제본에 결정적으로 주입하며 Source에는 쓰지 않는다.

Broken Reference는 원천 데이터 동시성 잠금이 유지되는 동안 **변경이 차단된 원천 상위 테이블 키**를 기준으로 검증한다. 병렬 Parent Task의 완료 순서에 의존하지 않는다.

### 7.5 SCD2 관측 한계

Source는 현재 상태만 보관하므로 성공 수집 사이의 중간 변경은 복원할 수 없다. V1 Generator는 SCD2 추적 속성에 대해 고객당 수집 구간 내 최대 1회만 변경한다.

---

## 8. 증분 추출과 동시성

### 8.1 Cursor

| Table          | Cursor Tuple                                 |
| -------------- | -------------------------------------------- |
| customers      | `(created_at, customer_id)`                  |
| customer_memberships | `(updated_at, customer_unique_id)`     |
| products       | `(updated_at, product_id)`                   |
| sellers        | `(updated_at, seller_id)`                    |
| orders         | `(updated_at, order_id)`                     |
| order_items    | `(created_at, order_id, order_item_id)`      |
| order_payments | `(updated_at, order_id, payment_sequential)` |

초기 Watermark는 논리적 `-infinity`와 Table별 최소 Key로 해석한다.

### 8.2 수집 중 원천 변경 차단 구간

```text
원천 데이터 동시성 잠금 획득
→ 7개 Table Task 병렬 시작
→ Table별 REPEATABLE READ Read-only Snapshot
→ Table별 Upper Bound 고정
→ Extract / Validate / Commit
→ 모든 Table 종료
→ 원천 데이터 동시성 잠금 해제
```

원천 데이터 동시성 잠금은 Table Snapshot이 같은 PostgreSQL Transaction Snapshot을 공유한다는 뜻은 아니다. **추출 중 Source Writer가 새 Transaction을 Commit하지 못하게 해 Cross-table 관측 차이를 방지**한다.

### 8.3 고정 범위와 Pagination

각 Table Snapshot에서 `MAX(cursor_tuple)`을 `extract_upper_bound`로 고정한다.

```sql
WHERE cursor_tuple > :watermark_before
  AND cursor_tuple <= :extract_upper_bound
ORDER BY cursor_tuple
LIMIT :page_size
```

- Page Lower Bound는 직전 Page 마지막 Cursor
- 기본 Page Size 50,000
- `INGESTION_PAGE_SIZE`로 변경
- Page별 Arrow Table을 Parquet Writer에 순차 기록
- 전체 Table 메모리 Load 금지
- 빈 범위는 Object 없이 `SUCCESS_NO_DATA`, Watermark 유지
- Composite PK 전체 Tie-breaker 포함

### 8.4 테이블별 수집 잠금과 CAS

- 동일 `pipeline_name + source_table` 실행은 하나만 허용
- Metadata Watermark Row의 `lease_owner`, `lease_expires_at` 원자적 획득
- 기본 TTL 30분, 5분마다 연장
- Lock 실패 Run은 Source Read/Object 생성 금지
- 최종 Commit은 Watermark Compare-and-swap

Lease 책임:

```text
원천 데이터 동시성 잠금
→ Generator와 Warehouse의 원천 변경 충돌 방지

테이블별 수집 잠금
→ Warehouse Run끼리 동일 Table Watermark 충돌 방지
```

### 8.5 Commit 순서

```text
원천 데이터 동시성 잠금
→ 테이블별 수집 잠금
→ 범위 고정
→ Extract
→ Validate
→ Local Parquet
→ SeaweedFS 최종 Bronze 객체 Upload
→ HEAD / Checksum / Row Count 검증
→ Manifest(object_state=VERIFIED)
→ Metadata Transaction
   - bronze_objects COMMITTED
   - pipeline_runs SUCCESS
   - watermark CAS Update
→ 테이블별 수집 잠금 해제
→ 모든 Table 종료 후 원천 데이터 동시성 잠금 해제
```

Validation/Upload/검증/Metadata Commit 실패 시 해당 Table Watermark는 유지한다. dbt 실패는 Bronze Commit 이후이므로 Bronze와 Watermark를 유지한다.

---

## 9. Batch Identity와 재실행

```text
batch_id        = {dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}
run_id          = UUIDv4 per execution attempt
table_batch_id  = {batch_id}__{source_table}
```

- 동일 Logical Date 재실행 → 같은 `batch_id`, 새 `run_id`
- 같은 Table Batch가 `COMMITTED`이고 Range/Schema Version 같음 → Skip
- Commit Range 다름 → `BATCH_IDENTITY_CONFLICT`
- 최종 Bronze 객체 존재 + Metadata 없음 → Orphan 후보
- Orphan은 Manifest/Checksum/Range/Schema Version 검증 후 Reconciliation
- Commit 최종 Bronze 객체는 덮어쓰기/삭제 금지
- 같은 Logical Date 다른 범위 강제 처리 → `reprocess_id`가 붙은 Backfill Batch

Checksum:

```text
content_sha256 = Parquet Byte
logical_hash   = PK 정렬 후 Business Column Canonical JSON
```

재실행 정합성은 `row_count + logical_hash`.

---

## 10. SeaweedFS Bronze와 Quarantine

### 10.1 저장 구조

```text
commerce-lake/
├── _staging/{run_id}/{table}/
├── bronze/{table}/ingestion_date=YYYY-MM-DD/batch_id={batch_id}/
│   ├── data.parquet
│   └── manifest.json
└── quarantine/{table}/ingestion_date=YYYY-MM-DD/batch_id={batch_id}/
    ├── records.parquet
    └── manifest.json
```

- Endpoint: `http://seaweedfs:8333`
- Bucket: `commerce-lake`
- boto3 Path-style
- Credential은 환경변수

`_staging`은 실제 Object Storage 임시 Upload가 필요한 경우에만 사용한다. 기본은 Local Parquet 검증 후 Final Key Upload다.

### 10.2 Bronze 규칙

기술 컬럼:

```text
_batch_id
_run_id
_ingested_at
_source_table
_schema_version
```

Parquet:

```text
Compression      Zstandard
Timestamp        UTC microseconds
Decimal          decimal128(14,2)
Row Group Target 128K rows
Default File     Table Batch당 1개
```

512MB 초과가 관측되면 `part-00000.parquet` 방식으로 전환하고 ADR을 갱신한다.

Manifest 최소 필드:

```text
manifest_version
schema_version
batch_id
run_id
source_table
logical_date
watermark_before
extract_upper_bound
rows_extracted
rows_valid
rows_rejected
object_key
object_size
content_sha256
logical_hash
created_at
object_state=VERIFIED
```

`VERIFIED`는 Object Upload/검증 완료를 뜻하며 Pipeline Commit을 의미하지 않는다.

Manifest 금지:

```text
Credential
Secret
Local Absolute Path
Raw Payload
status=COMMITTED
```

### 10.3 메타데이터 커밋 상태 기준

Commit Source of Truth:

```text
pipeline_metadata.bronze_objects.status = COMMITTED
```

```text
Object + VERIFIED Manifest
        │
        ├─ Metadata COMMITTED 없음 → Orphan 후보 / dbt Read 금지
        │
        └─ Metadata COMMITTED 있음 → 공식 Bronze / Catalog 대상
```

- Manifest만 존재하면 Catalog에 동기화하지 않는다.
- Metadata가 COMMITTED가 아니면 dbt Read 금지
- Reconciliation은 Checksum/Range/Schema Version 검증
- 불일치하면 자동 Commit하지 않고 실패

### 10.4 Commit File Catalog

`sync_bronze_catalog`가 Metadata의 `COMMITTED` Object만 DuckDB `control.bronze_files`에 동기화한다.

```text
source_table
object_key
schema_version
batch_id
committed_at
row_count
logical_hash
```

dbt Macro는 Catalog의 Object 목록으로 `read_parquet([...])`를 만든다. Object Glob 직접 Read 금지.

### 10.5 Bronze Schema Version

초기 `schema_version = 1`.

- Source/Bronze Column 추가·삭제·Type/Nullability 변경 → Schema Version 증가
- Manifest 자체 변경 → `manifest_version` 증가
- Commit된 Object Schema Version 수정 금지
- dbt Staging은 지원 Schema Version 명시
- 미지원 Version → `SOURCE_CONTRACT_ERROR`
- Additive Nullable Column은 Contract Test 후 `union_by_name=true` 허용 가능
- Column Rename은 Bronze가 아니라 Staging Alias
- Breaking Change는 ADR + Migration/Backfill 범위 정의

### 10.6 Quarantine

```text
_record_id
_batch_id
_run_id
_source_table
_error_codes
_error_message
_detected_at
_raw_payload
```

`_record_id`는 `table_batch_id + row ordinal` 기반 결정 ID다. Metadata에는 Raw Payload가 아니라 Error Count와 Object Key만 기록한다.

---

## 11. Ingestion Validation

순서:

```text
Schema / 필수 Column
→ Type
→ Key NULL
→ Batch Duplicate
→ Source Status Domain
→ Numeric Range
→ Broken Reference
→ Cursor 범위
```

규칙:

- Row 오류 → Quarantine, Valid Row 계속 처리
- Schema 누락/Cursor 위반 → Batch 실패
- Broken Reference → Freeze된 Source Parent Table의 DISTINCT Key 기준
- Threshold 이하 Reject가 있어도 Valid Row Commit 후 Watermark Upper까지 전진
- Reject 영구 격리
- `MAX_REJECT_RATE=5%`
- Threshold 초과 → 전체 실패, Watermark 유지
- 0 Row Reject Rate = 0
- Corruption 주입 전후 Count 별도 기록

---

## 12. Pipeline Metadata

### 12.1 watermarks

| Column                | Type        | Constraint          |
| --------------------- | ----------- | ------------------- |
| `pipeline_name`       | TEXT        | PK                  |
| `source_table`        | TEXT        | PK                  |
| `watermark_timestamp` | TIMESTAMPTZ | NULL은 초기         |
| `watermark_keys`      | JSONB       | NOT NULL, 기본 `[]` |
| `lease_owner`         | UUID        | NULL                |
| `lease_expires_at`    | TIMESTAMPTZ | NULL                |
| `version`             | BIGINT      | NOT NULL, 기본 0    |
| `updated_at`          | TIMESTAMPTZ | NOT NULL            |

### 12.2 source_mutation_leases

```text
resource_name PK
owner_type
owner_id
lease_expires_at
version
updated_at
```

V1에서는 `resource_name='commerce_source'` 한 Row 사용.

### 12.3 pipeline_runs

```text
run_id + source_table Composite PK
batch_id
dag_id
logical_date
attempt_number
started_at
finished_at
watermark_before
extract_upper_bound
rows_extracted
rows_valid
rows_rejected
rows_loaded
status
error_type
error_message
```

상태:

```text
RUNNING
SUCCESS
SUCCESS_NO_DATA
SKIPPED_ALREADY_COMMITTED
FAILED
```

### 12.4 bronze_objects

```text
table_batch_id PK
source_table
batch_id
object_key UNIQUE
manifest_key
schema_version
row_count
content_sha256
logical_hash
watermark_before
watermark_after
status: STAGED | COMMITTED | ORPHANED
committed_at
```

### 12.5 quarantine_batches

```text
table_batch_id PK
object_key
row_count
error_counts JSONB
created_at
```

### 12.6 seed_runs / generator_runs

`seed_runs`:

```text
seed_run_id UUID PK
raw_checksum
seeded_at
started_at
finished_at
table_row_counts JSONB
table_content_hashes JSONB
status
error_message
```

`generator_runs`:

```text
generator_run_id UUID PK
random_seed
logical_date
order_count
anomaly_profile
generator_version
result_counts JSONB
logical_hash
started_at
finished_at
status
error_message

UNIQUE(random_seed, logical_date, order_count, anomaly_profile, generator_version)
```

### 12.7 Metadata Transaction

Table Commit:

```text
bronze_objects → COMMITTED
pipeline_runs  → SUCCESS
watermark      → CAS Update
```

를 하나의 Metadata Transaction으로 처리한다.

---

## 13. Airflow 실행 계약

### 13.1 기본값

- Executor: `LocalExecutor`
- `source_simulation_dag`: Manual
- `warehouse_pipeline_dag`: `@daily`
- 개발 시 `WAREHOUSE_DAG_SCHEDULE` 빈 값으로 비활성화
- `catchup=False`
- Backfill은 명시 Parameter
- `max_active_runs=1`
- DuckDB Writer 1 Process

### 13.2 Warehouse Task Graph

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
  ↓
verify_bronze_commit
  ↓
release_source_snapshot_lease  # all_done
  ↓
sync_bronze_catalog
  ↓
dbt_build
  ↓
publish_run_summary
```

Table Task 내부:

```text
테이블별 수집 잠금
→ Snapshot / Upper Bound
→ Extract
→ Validation
→ Quarantine
→ Upload
→ Object Verify
→ Manifest
→ Metadata Commit
→ Watermark CAS
```

한 Table 실패 시 dbt는 실행하지 않는다. 성공한 다른 Table Watermark는 되돌리지 않고 같은 Batch 재실행에서 재사용한다.

### 13.3 Generator DAG

```text
initialize_generator
→ acquire_source_mutation_lease
→ generate_transactional_changes
→ record_generator_run
→ release_source_mutation_lease
```

### 13.4 Retry와 XCom

```text
retries = 2
retry_delay = 60 seconds
retry_exponential_backoff = true
max_retry_delay = 10 minutes
```

일시 Network/DB/Object Storage 오류만 Retry. Contract/Identity/Determinism/Watermark Conflict는 자동 Retry하지 않는다.

XCom 허용:

```text
batch_id
run_id
object_key
row_count
watermark JSON
status
```

XCom 금지:

```text
DataFrame
Arrow Table
Parquet Byte
Raw Row
Credential
```

DAG는 Orchestration만 담당한다.

---

## 14. dbt + DuckDB 모델

Warehouse:

```text
data/warehouse/warehouse.duckdb
```

Schema:

```text
control
staging
intermediate
marts
```

| Layer        | Materialization        |
| ------------ | ---------------------- |
| control      | table                  |
| staging      | view                   |
| intermediate | view                   |
| dimensions   | table 또는 incremental |
| facts        | incremental            |

### 14.1 Staging

```text
stg_customers_current
stg_customer_observations
stg_products
stg_sellers
stg_orders
stg_order_items
stg_payments
```

Customer Mapping:

| Source                  | Staging                   |
| ----------------------- | ------------------------- |
| `customer_id`           | `source_customer_id`      |
| `customer_unique_id`    | `customer_id`             |
| `customer_city`         | `city`                    |
| `customer_state`        | `state`                   |
| `customers.created_at` | `created_at`              |
| `customer_memberships.membership_level` | `stg_customer_observations.membership_level` |
| `customer_memberships.created_at` | `stg_customer_observations.created_at` |
| `customer_memberships.updated_at` | `stg_customer_observations.updated_at` |

`stg_customers_current`는 불변 계정의 `source_customer_id` 1행과 해당 주문 주소 스냅샷을
보존한다. 사람 단위 Membership 관측은 `customer_memberships` Bronze 누적 행에서 별도로 만든다.

Order Mapping:

| Source                              | Staging                  |
| ----------------------------------- | ------------------------ |
| `customer_id`                       | `source_customer_id`     |
| customers Join `customer_unique_id` | `customer_id`            |
| `order_purchase_timestamp`          | `purchase_at`            |
| `order_approved_at`                 | `approved_at`            |
| `order_delivered_carrier_date`      | `carrier_at`             |
| `order_delivered_customer_date`     | `delivered_at`           |
| `order_estimated_delivery_date`     | `estimated_delivery_at`  |
| `order_status`                      | 대문자 8개 `order_status` |

Order Status Mapping은 7.1을 단일 Macro/Seed Mapping Source로 재사용한다.

Payment Mapping:

| Source                 | Staging                 |
| ---------------------- | ----------------------- |
| `payment_sequential`   | `payment_sequence`      |
| `payment_installments` | `installments`          |
| `payment_status`       | 대문자 `payment_status` |

Products/Sellers Mapping:

| Source                  | Staging         |
| ----------------------- | --------------- |
| `product_category_name` | `category_name` |
| `product_weight_g`      | `weight_g`      |
| `product_length_cm`     | `length_cm`     |
| `product_height_cm`     | `height_cm`     |
| `product_width_cm`      | `width_cm`      |
| `seller_city`           | `city`          |
| `seller_state`          | `state`         |

Intermediate/Mart는 Source Prefix를 직접 참조하지 않는다.

Mutable Current:

```sql
row_number() over (
  partition by business_key
  order by updated_at desc, _ingested_at desc, _batch_id desc
) = 1
```

Customer Observation은 `customer_id + updated_at + tracked_attribute_hash` 기준 Deduplicate.

### 14.2 Intermediate

```text
int_orders_enriched
int_order_items_enriched
int_payment_summary
int_customer_history
int_affected_business_dates
```

### 14.3 Mart Grain

| Model            | Grain             | Unique Key                     |
| ---------------- | ----------------- | ------------------------------ |
| dim_customer     | 고객 Version 1행  | `customer_key`                 |
| dim_product      | 상품 1행          | `product_id`                   |
| dim_seller       | 판매자 1행        | `seller_id`                    |
| dim_date         | UTC Date 1행      | `date_key`                     |
| fact_orders      | 주문 1행          | `order_id`                     |
| fact_order_items | 주문 Line 1행     | `(order_id, order_item_id)`    |
| fact_payments    | 결제 Sequence 1행 | `(order_id, payment_sequence)` |

### 14.4 Measure 계약

`fact_order_items`:

```text
item_price       = price
freight_value    = freight_value
line_gross_value = item_price + freight_value
```

`fact_payments`:

```text
payment_value
payment_status
```

`fact_orders`는 Item/Payment를 각각 주문 Grain으로 먼저 집계한 뒤 Join한다.

```text
item_subtotal     = SUM(item.price)
freight_total     = SUM(item.freight_value)
gross_order_value = item_subtotal + freight_total
payment_total     = SUM(payment.payment_value)
order_count       = 1
```

규칙:

- Item/Payment Raw Grain 직접 다대다 Join 후 SUM 금지
- `gross_order_value`와 `payment_total` 의미 분리
- 기본 Sales/GMV는 `order_status='DELIVERED'`의 `gross_order_value`
- Refund/Failed 분석은 `fact_payments.payment_status`
- `payment_total`을 Revenue와 동일시하지 않는다.

### 14.5 Incremental

변경 Key는 DuckDB Transaction의 `DELETE + INSERT` 또는 검증된 `MERGE`로 교체한다.

- 여러 Bronze Version 중 Current만 적재
- Full Refresh와 Key 정렬 Logical Hash 비교
- Model별 `unique_key` 명시

---

## 15. SCD Type 2와 Temporal Join

Business Key:

```text
dbt Staging customer_id
= Source customer_unique_id
```

추적 속성:

```text
membership_level
```

```text
customer_key = Hash(customer_id, valid_from, attribute_hash)
```

Schema:

```text
customer_key
customer_id
source_customer_unique_id
membership_level
attribute_hash
valid_from
valid_to
is_current
```

Version 규칙:

1. `(customer_id, updated_at, _ingested_at, _batch_id)` 정렬
2. 추적 속성 Hash 동일 → Version 추가 안 함
3. 최초 Version → `created_at`
4. 이후 변경 Version → `updated_at`
5. 다음 Version 시작 = 현재 `valid_to`
6. 마지막만 `valid_to=NULL`, `is_current=true`
7. 유효 구간 `[valid_from, valid_to)`
8. 동일 고객·동일 `updated_at`의 서로 다른 Hash → Contract 위반

Olist Seed는 과거 속성 이력이 없으므로 최초 Version을 Baseline Snapshot으로 간주한다.

Temporal Join:

```sql
order.purchase_at >= dim_customer.valid_from
AND order.purchase_at < COALESCE(dim_customer.valid_to, TIMESTAMPTZ 'infinity')
```

정상 E2E Unknown Fact Count는 0이어야 한다.

---

## 16. Late Arrival, Backfill, Re-run

Late Arrival의 핵심은 Business Event Time과 원천 변경 시각 분리다.

```text
Business Event Time < 원천 변경 시각
```

추출 여부는 `updated_at` Cursor가 결정한다.

영향 범위:

- orders: 구매일
- order_items/order_payments: 연결 주문 구매일
- customers: 변경 SCD2 구간과 겹치는 주문일
- products/sellers: 해당 Entity 사용 주문일

`control.affected_keys`에 영향 Key/Date와 dbt Invocation ID를 기록한다.

Fact/Aggregate는 영향 Key/Date를 Transactional `DELETE + INSERT`한다.

Backfill:

1. **Replay**: Commit Bronze를 재적용, Source Read 안 함
2. **Re-extract**: 명시 Cursor 범위를 새 `reprocess_id`로 추출

기본은 Replay.

---

## 17. Data Quality와 Publish

Generic Test:

```text
unique
not_null
relationships
accepted_values
```

Custom Test:

```text
payment_value >= 0
price >= 0
freight_value >= 0
delivered_at >= purchase_at
approved_at >= purchase_at when not null
customer당 current Version = 1
SCD2 유효 구간 중첩 = 0
Fact FK 누락 = 0
Fact Business Key 중복 = 0
정상 E2E Unknown 참조 = 0
표준화 상태값 매핑 누락 = 0
fact_orders Item/Payment Fan-out = 0
```

Warehouse 실패 시 Bronze/Watermark는 유지한다. Metabase는 마지막 성공 Mart만 읽는다.

---

## 18. Observability와 오류

필수 조회:

```text
Batch / Run / Logical Date
원천 데이터 동시성 잠금
테이블별 수집 잠금
Table Status
Cursor Before / Upper / After
단계별 Count
Object / Manifest / Hash
Schema Version
Duration / Retry
Error
dbt Invocation / Test
```

Error Type:

```text
CONFIGURATION_ERROR
SOURCE_CONNECTION_ERROR
SOURCE_CONTRACT_ERROR
SOURCE_MUTATION_CONFLICT
VALIDATION_THRESHOLD_EXCEEDED
OBJECT_STORAGE_ERROR
OBJECT_VERIFICATION_ERROR
WATERMARK_CONFLICT
BATCH_IDENTITY_CONFLICT
DBT_BUILD_ERROR
DBT_TEST_ERROR
UNKNOWN_ERROR
```

---

## 19. BI

Phase 9 직전에 Metabase Patch와 DuckDB Driver를 재검증한다.

우선 마지막 성공 DuckDB Mart를 Read-only 조회하고 Lock/Driver 문제가 관측되면 PostgreSQL Serving DB로 Publish한다.

Dashboard:

```text
Sales Overview
Product
Customer
```

Raw Source/Bronze 직접 참조 금지.

---

## 20. 비기능 요구사항과 Benchmark

재현 절차:

```bash
git clone <repository>
cd commerce-data-platform
cp .env.example .env
uv sync --frozen
docker compose up -d
uv run python scripts/download_dataset.py
uv run python -m src.seed   --input-dir data/raw/olist   --seeded-at <UTC_TIMESTAMP>
```

Benchmark:

```text
S = 100K Orders
M = 1M Orders
L = 5M Orders
```

비교:

```text
Full vs Incremental
CSV vs Parquet
Full Scan vs Filtering
Cold vs Warm
```

동일 환경에서 5회 실행하고 Median + Raw 값을 기록한다.

```text
benchmark_id
git_commit
started_at
host_spec
wsl_spec
python_version
dependency_lock_hash
image_versions
dataset_scale
random_seed
scenario
run_number
is_cold_run
duration_seconds
rows_scanned
rows_changed
input_bytes
output_bytes
result_hash
```

---

## 21. 기능 요구사항

| ID    | 요구사항                                       | 우선순위 |
| ----- | ---------------------------------------------- | -------- |
| FR-01 | Raw-compatible Olist Seed                      | P0       |
| FR-02 | Deterministic Generator                        | P0       |
| FR-03 | 고정 범위 Incremental Extract                  | P0       |
| FR-04 | Composite Watermark + 테이블별 수집 잠금 + CAS | P0       |
| FR-05 | 원천 데이터 동시성 잠금                        | P0       |
| FR-06 | Run/Object Metadata                            | P0       |
| FR-07 | SeaweedFS Parquet Bronze                       | P0       |
| FR-08 | Idempotency와 Orphan Recovery                  | P0       |
| FR-09 | Airflow Pipeline                               | P0       |
| FR-10 | dbt Staging/Intermediate/Mart                  | P0       |
| FR-11 | Star Schema/Fact Grain                         | P0       |
| FR-12 | Ingestion/Warehouse Quality                    | P0       |
| FR-13 | Backfill Replay/Re-extract                     | P0       |
| FR-14 | Late Arrival 재처리                            | P0       |
| FR-15 | SCD2/Temporal Join                             | P0       |
| FR-16 | Quarantine                                     | P0       |
| FR-17 | Bronze Schema Version                          | P0       |
| FR-18 | Metabase                                       | P1       |
| FR-19 | 1M+ Scale                                      | P1       |
| FR-20 | Benchmark                                      | P1       |
| FR-21 | Cloud PoC                                      | P2       |
| FR-22 | CDC                                            | P2       |

---

## 22. Acceptance Criteria

| ID    | 시나리오                     | 합격 조건                                                                                  |
| ----- | ---------------------------- | ------------------------------------------------------------------------------------------ |
| AC-01 | E2E                          | 고정 주문이 Source→Bronze Catalog→Fact에 존재, Count 추적 가능                             |
| AC-02 | Incremental                  | Cursor 조건과 실제 신규·변경 Key Set 일치                                                  |
| AC-03 | 동일 Batch 3회               | Object 수/Key Count/Mart Hash 동일, 중복 0                                                 |
| AC-04 | Upload 전후 실패             | 실패 중 Watermark 유지, 성공 뒤 Upper로 전진                                               |
| AC-05 | 동일 Timestamp가 Page 초과   | Page/Batch 경계 누락·중복 0                                                                |
| AC-06 | 동시 Extract                 | 하나만 테이블별 수집 잠금, 다른 Run은 Object 없이 Conflict                                 |
| AC-07 | Backfill Replay              | 같은 범위 Full Refresh와 Key별 값/Hash 동일                                                |
| AC-08 | 5종 Corruption               | 기대 Reject 일치, Source 무오염, Record 추적 가능                                          |
| AC-09 | BRONZE→SILVER→GOLD           | 3 Version, 구간 중첩 0, Current 1                                                          |
| AC-10 | 구간별 주문                  | 발생 시점 Customer Key 참조                                                                |
| AC-11 | 3일 전 Late Order            | 과거 Business Time + 새 `updated_at`으로 정확히 수집, 과거 Mart 갱신                       |
| AC-12 | Referential Integrity        | FK/Unique 통과, 정상 Unknown 0                                                             |
| AC-13 | Observability                | 성공/빈/실패/재실행을 SQL 한 번으로 조회                                                   |
| AC-14 | Seed 2회                     | Count/Content Hash 동일, PK/FK 위반 0                                                      |
| AC-15 | Generator 재현               | 동일 Snapshot/Input Key Set/Hash 동일                                                      |
| AC-16 | 새 Clone                     | Version/Health/Seed/E2E/dbt Test 성공                                                      |
| AC-17 | Benchmark                    | Raw 5회/Median/Hash/환경 Metadata 존재                                                     |
| AC-18 | Source Schema Allowlist      | 6개 제외 컬럼이 PostgreSQL/Bronze에 없고 나머지 선택 컬럼 이름/값 유지                     |
| AC-19 | Staging Naming               | Alias/상태 Mapping이 값 손실 없이 적용, Intermediate가 Raw Prefix 직접 참조하지 않음       |
| AC-20 | 원천 변경 시각 Cursor Safety | 과거 Business Event를 새 `updated_at`으로 갱신하면 다음 Batch에서 정확히 1회 수집          |
| AC-21 | Generator vs Warehouse       | Warehouse의 원천 데이터 동시성 잠금 중 Generator Source 변경 0, Parent/Child 관측 불일치 0 |
| AC-22 | 표준화 상태값                | 8개 `order_status` 정의 Mapping 100% 일치                                                   |
| AC-23 | Manifest vs Metadata         | VERIFIED Manifest만 있고 Metadata COMMITTED가 없으면 Catalog/dbt Read 0                    |
| AC-24 | Schema Version               | 지원하지 않는 Bronze Schema Version은 dbt Build 전에 Contract Error                        |

절대 처리시간 목표 대신 Baseline과 개선 전후를 비교한다.

---

## 23. Repository Structure

```text
commerce-data-platform/
├── README.md
├── PRD_v1.6.md
├── PRD.bak/
│   ├── PRD_v1.4.md
│   └── PRD_v1.5.md
├── AGENTS.md
├── compose.yaml
├── pyproject.toml
├── uv.lock
├── .python-version
├── .env.example
├── .gitignore
│
├── airflow/
│   └── dags/
│
├── src/
│   ├── seed/
│   ├── generator/
│   ├── ingestion/
│   └── common/
│
├── dbt/
│   ├── models/
│   ├── tests/
│   └── macros/
│
├── sql/
│   ├── source/
│   ├── metadata/
│   └── validation/
│
├── data/
│   ├── raw/olist/
│   ├── generated/
│   ├── warehouse/
│   └── samples/
│
├── scripts/
│   ├── init.sh
│   ├── download_dataset.py
│   ├── seed.sh
│   ├── cleanup_staging.py
│   └── reset.sh
│
├── tests/
│   ├── seed/
│   ├── generator/
│   ├── ingestion/
│   └── integration/
│
└── docs/
    ├── architecture/
    ├── adr/
    ├── benchmarks/
    ├── runbooks/
    └── troubleshooting/
```

---

## 24. 개발 단계와 Definition of Done

| Phase         | 구현                                                                                | 완료 조건                           |
| ------------- | ----------------------------------------------------------------------------------- | ----------------------------------- |
| 0 Bootstrap   | Python/uv, 구조, 환경, Compose, Download                                            | Sync/Version/Compose/Git Ignore     |
| 1 Source      | PostgreSQL, Raw-compatible DDL/Seed                                                 | 원본 Naming/PK/FK, Seed Hash, AC-18 |
| 2 Generator   | 결정 ID, 상태, 원천 변경 시각, 원천 데이터 동시성 잠금, Membership, Late            | AC-15/20/21, 허용 전이              |
| 3 Ingestion   | Cursor, 테이블별 수집 잠금, Metadata, Bronze, Quarantine, 메타데이터 커밋 상태 기준 | AC-02~06, AC-08, AC-23/24           |
| 4 Airflow     | DAG, Dynamic Task, Retry, XCom, 원천 데이터 동시성 잠금 해제                        | E2E, Retry, 부분 성공 재사용        |
| 5 Modeling    | Catalog, Naming Staging, Mart, SCD2, Measure Contract                               | AC-09/10/12/19/22                   |
| 6 Quality     | Corruption, Threshold, Test, Publish                                                | 지정 오류 탐지, 정상 오탐 0         |
| 7 Reliability | 실패/충돌/Late/Backfill/Orphan Runbook                                              | 문제→재현→관측→원인→해결→재검증     |
| 8 Benchmark   | 고정 조건, 5회, Median                                                              | Raw/Hash/비교 문서                  |
| 9 BI          | Driver Gate, 3 Dashboard                                                            | Mart만 조회, Serving ADR            |

---

## 25. ADR 목록

```text
001-use-seaweedfs-as-local-s3-compatible-storage.md
002-use-duckdb-as-local-warehouse.md
003-use-parquet-for-bronze.md
004-use-table-specific-incremental-strategy.md
005-use-composite-watermark-and-fixed-upper-bound.md
006-use-single-postgres-container.md
007-version-pinning-policy.md
008-preserve-olist-source-schema-and-standardize-in-dbt-staging.md
009-use-observed-history-for-customer-scd2.md
010-use-metadata-backed-bronze-file-catalog.md
011-late-arrival-reprocessing-strategy.md
012-metabase-serving-strategy.md
013-source-mutation-and-warehouse-extract-concurrency.md
014-use-metadata-as-bronze-commit-authority.md
015-bronze-schema-evolution-policy.md
```

ADR 형식:

```text
Status
Context
Decision
Alternatives
Consequences
Validation
```

---

## 26. 알려진 제한과 대응

| 제한                                             | V1 대응                                                                                            |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| Source 현재 상태만 보관                          | 중간 변경 관측 불가 명시, Generator 변경 제한                                                      |
| Source Writer가 Synthetic Generator뿐            | 원천 데이터 동시성 잠금으로 수집 중 변경 차단 가능, 실제 외부 Source는 CDC/DB-native Snapshot 필요 |
| Delete 미지원                                    | Tombstone/CDC는 V2                                                                                 |
| Olist timezone 부재                              | UTC naive로 일관 해석                                                                              |
| S3 원자 Rename 부재                              | 최종 Bronze 객체 Verify + 메타데이터 커밋 상태 기준 + Orphan Reconciliation                        |
| Object-Metadata 분산 Commit                      | Checksum, VERIFIED Manifest, Metadata COMMITTED                                                    |
| DuckDB Single-writer                             | `max_active_runs=1`, dbt 단일 Process                                                              |
| Metabase 호환성                                  | Phase 9 Gate, Serving DB 대안                                                                      |
| 8GB RAM의 5M Scale                               | Page Write, 관측 후 조정                                                                           |
| Reject 후 Watermark 전진                         | 영구 Quarantine, Reject Threshold                                                                  |
| Seed에 실제 과거 Customer Attribute History 없음 | Baseline Snapshot으로만 취급                                                                       |
| SeaweedFS S3 API ≠ AWS S3 100%                   | Phase 3 Compatibility Smoke Test                                                                   |

---

## 27. Portfolio Evidence

```text
Architecture Diagram
Source 원본 Naming 보존
Staging Naming Mapping
Customer Business Key 변환
원천 변경 시각 vs Business Event Time
원천 데이터 동시성 잠금 동시성 Test
Cursor / Index / Upper Bound
Watermark Failure
동일 Batch Hash
Manifest VERIFIED vs Metadata COMMITTED
Orphan Reconciliation
Quarantine
Fact Grain / Measure Contract
SCD2 Timeline
Late Arrival Diff
Backfill-Full Hash
Schema Version Contract Test
Scale Benchmark
Failure Runbook
새 Clone 재현 기록
```

면접 핵심 설명:

> Source/Bronze는 원본 계보와 재처리 가능성을 위해 원천 보존 상태를 유지하고, 분석 Naming과 표준화는 dbt Staging으로 분리했다. `updated_at`은 Business Event Time이 아니라 원천 변경 시각으로 정의해 Incremental Cursor의 누락을 방지했으며, Synthetic Source Writer와 Warehouse Extract 사이에는 원천 데이터 동시성 잠금을 두어 Cross-table 관측 일관성을 보장했다. Bronze Object 검증과 Pipeline Commit을 분리하고 Metadata의 COMMITTED 상태만 읽도록 해 Object Storage와 제어 상태의 불일치를 명시적으로 처리한다.

---

## 28. 최종 확정 문장

> **Python 3.12 + WSL2 Ubuntu + Docker 환경에서 V1 사용 컬럼을 Allowlist로 선택하고 Olist Naming을 보존한 PostgreSQL OLTP Source를 구성한다. Business Event Time과 원천 변경 시각을 분리하고 Mutable Row의 `updated_at` 단조 증가를 증분 Cursor 계약으로 사용한다. Synthetic Generator와 Warehouse Extract는 원천 데이터 동시성 잠금으로 상호 배타적으로 동작하며, Airflow는 Table별 Composite Cursor와 고정 Upper Bound로 데이터를 수집한다. 수집 결과는 SeaweedFS의 불변 Parquet Bronze로 저장하고 Object 검증은 VERIFIED Manifest, 최종 Commit은 Metadata의 COMMITTED 상태로 분리한다. dbt Staging에서 Prefix 제거·Timestamp 표준화·상태 표준화를 수행하고 DuckDB에서 Star Schema, Incremental Fact, SCD Type 2와 Temporal Join을 구축한다. Lease, Metadata, Manifest, Checksum, Schema Version, Data Quality, Quarantine, Backfill, Late Arrival, Orphan Recovery Test로 동시성·실패·재실행을 검증하고 Version Pinning과 실행 Metadata로 재현성을 확보한다.**
