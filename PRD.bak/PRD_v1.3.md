# PRD: Commerce Analytics Data Platform

> Version: 1.3
>
> Status: Implementation-Ready Baseline
>
> Baseline Date: 2026-09-04
>
> Environment: WSL2 Ubuntu / Local
>
> Goal: 데이터엔지니어 취업 포트폴리오용 Batch DW·Data Mart 프로젝트

---

## 0. v1.3 변경 요약

v1.3은 v1.2의 기능 범위와 신뢰성 계약을 유지하면서 Source 계층의 원본 추적 가능성과 dbt 계층의 책임을 명확히 한다.

- PostgreSQL과 Bronze에서 Olist 원본 Table/Column Name과 값을 최대한 보존
- `customer_id`는 Source PK/FK, `customer_unique_id`는 dbt 이후 분석 고객 Business Key로 역할 분리
- 선택 컬럼의 Entity Prefix 제거, Timestamp Rename과 상태 Canonicalization을 dbt Staging 책임으로 이동
- 증분용 `created_at`/`updated_at`과 Synthetic 시나리오 필드만 Source 확장으로 허용
- V1 미사용 컬럼 6개를 Source/Bronze Allowlist에서 제외하고 Raw CSV에만 보존
- Source Schema Allowlist와 Staging Naming Mapping을 검증하는 AC-18, AC-19 추가

v1.2에서 확정한 다음 계약도 그대로 유지한다.

- 모든 운영 시간을 UTC `TIMESTAMPTZ`로 통일
- Seed Loader를 Staging + Transactional UPSERT 방식으로 확정
- Source PK/FK/CHECK/Numeric Precision/Incremental Index 정의
- 증분 범위를 고정된 `(watermark_before, extract_upper_bound]`로 정의
- Keyset Pagination, 빈 Batch, 동시 실행, Lease와 Watermark CAS 정책 정의
- `batch_id`, `run_id`, `table_batch_id` 및 재실행 의미 분리
- Bronze 기술 컬럼, Schema Version, Manifest, Checksum, Commit Protocol 정의
- Quarantine을 SeaweedFS Parquet Prefix로 확정
- Watermark 전진 시 Reject 유실을 막기 위해 Quarantine을 P1에서 P0로 조정
- Pipeline Metadata의 Physical Schema와 상태 전이 정의
- Commit된 Object만 읽는 DuckDB Bronze File Catalog 추가
- Mutable 최신 상태, SCD2 관측 이력, Temporal Join 규칙 정의
- Late Arrival 영향 범위와 Mart 재계산 방식 확정
- Airflow 동시성·Task 경계·재시도 책임 구체화
- Acceptance Criteria별 검증 방법과 합격 조건 추가
- MinIO 잔여 ADR 명칭을 SeaweedFS 기준으로 정정

기술 Version은 v1.2와 동일하다.

---

## 1. 프로젝트 개요

### 1.1 한 줄 정의

Olist 공개 데이터를 Seed로 서비스형 OLTP Source를 구성하고, 결정적 Synthetic 데이터로 변경을 발생시켜 **증분 수집 → S3-compatible Bronze → DW 변환·모델링 → 품질 검증 → 재처리·복구**를 구현한다.

### 1.2 핵심 메시지

> 정적 CSV를 한 번 적재하는 프로젝트가 아니라 변경·지연·중복·실패가 발생하는 환경에서 Batch Pipeline의 신뢰성과 분석 모델의 정합성을 설계하고 검증한다.

### 1.3 성공의 정의

1. 동일 입력은 동일 Source 변경과 동일 Mart 상태를 만든다.
2. 실패한 Batch는 Watermark를 전진시키지 않는다.
3. 동일 Batch를 반복 실행해도 중복 결과가 생기지 않는다.
4. 늦게 도착한 과거 이벤트가 최종 Mart에 반영된다.
5. 주문은 발생 시점에 유효한 고객 Dimension Version을 참조한다.
6. 실행 결과를 Metadata와 Test Evidence로 추적할 수 있다.
7. 문서의 명령만으로 새 로컬 환경에서 재현할 수 있다.

---

## 2. 범위와 아키텍처

### 2.1 V1 흐름

```text
Olist CSV
   │ Seed Transformation
   ▼
PostgreSQL: commerce_source ◀── Deterministic Synthetic Generator
   │
   ▼
Airflow LocalExecutor
   │ Incremental Extract / Validate
   ▼
SeaweedFS: Parquet Bronze + Quarantine
   │ Committed File Catalog
   ▼
dbt + DuckDB
   ├── staging
   ├── intermediate
   └── marts ──▶ Metabase

PostgreSQL: pipeline_metadata
   └── Watermark / Run / Object / Reject Summary
```

### 2.2 계층별 책임

| 계층        | 책임                                              | 하지 않는 일                         |
| ----------- | ------------------------------------------------- | ------------------------------------ |
| Source DB   | Olist 호환 Naming, 현재 서비스 상태, OLTP 무결성  | 분석용 Naming, 집계, Corruption 저장 |
| Ingestion   | 범위 고정, 추출, 기본 검증, Bronze Commit         | Mart 계산                            |
| Bronze      | 재처리 가능한 Source 관측 이력                    | Silver/Gold 중복 저장                |
| DuckDB/dbt  | Naming/값 표준화, 최신 상태, 이력, Dimension/Fact | Source Watermark 관리                |
| Metadata DB | 제어 상태와 실행 증적                             | 대용량 Payload 저장                  |
| Metabase    | Mart 소비 가능성 검증                             | Raw Source 직접 조회                 |

### 2.3 불변 조건

- Watermark는 Commit된 Bronze 범위만 가리킨다.
- Commit된 Bronze Object는 수정하지 않는다.
- dbt는 Metadata에서 `COMMITTED`인 Object만 읽는다.
- PostgreSQL과 Bronze는 원본 Olist Column Name을 유지하고 분석용 Rename은 dbt Staging에서만 수행한다.
- Fact Measure는 문서화된 Grain에서만 집계한다.
- Credential은 Git, SQL, dbt Model, Manifest에 기록하지 않는다.

### 2.4 비목표

Kafka, Spark, Debezium CDC, Source Delete/Tombstone, Kubernetes, Terraform, AWS S3/Snowflake/Databricks 상시 운영, 실시간 SLA, 다중 Worker Cluster는 V1 범위가 아니다.

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

직접 Python Dependency는 `pyproject.toml`, 전체 Resolution은 `uv.lock`으로 고정한다. Docker Image는 다음을 사용하며 `latest` Tag를 금지한다.

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
dbt debug / parse / build / test
DuckDB → SeaweedFS Parquet Read
Incremental Merge 및 동일 Batch Re-run
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

| 시간                | 의미                          |
| ------------------- | ----------------------------- |
| Business Event Time | 주문·결제 사건 발생 시간      |
| Source Updated Time | 현재 Source Version 기록 시간 |
| Ingestion Time      | Bronze 수집 시간              |
| Logical Date        | 결정적 Batch 식별 기준 시간   |

### 4.2 Naming과 타입

- Source/Bronze는 선택한 Olist 컬럼의 `snake_case` 이름과 Prefix를 그대로 보존한다.
- 프로젝트 확장 Column만 기존 Olist Column과 충돌하지 않는 `snake_case` 이름을 사용한다.
- dbt Staging이 Prefix 제거, Timestamp Suffix 통일과 Canonical Status 변환을 담당한다.
- Canonical Status는 Staging 이후 대문자 문자열이다.
- 금액은 `NUMERIC(14,2)` / Parquet `decimal128(14,2)`다.
- 의미 없는 빈 문자열은 `NULL`로 정규화한다.
- Watermark Column과 PK는 `NOT NULL`이다.
- 문자열 Cursor 비교는 PostgreSQL `COLLATE "C"` 기준이다.

### 4.3 식별자

- Olist `customer_id`, `customer_unique_id`, `order_id`, `product_id`, `seller_id`를 문자열 그대로 보존한다.
- Source `orders.customer_id`는 원본처럼 `customers.customer_id`를 참조한다.
- 분석 고객의 Business Key는 dbt Staging에서 노출하는 `customer_unique_id`다.
- Synthetic ID는 Seed/Logical Date를 포함한 UUIDv5 또는 결정적 Hash 문자열을 사용한다. Random UUIDv4는 금지한다.

---

## 5. Olist Raw-compatible Seed 계약

### 5.1 입력

- Dataset: `olistbr/brazilian-ecommerce`
- 필수: customers, orders, order_items, products, order_payments, sellers
- 선택: reviews, geolocation
- 다운로드: `uv run python scripts/download_dataset.py`
- 고정 입력 경로: `data/raw/olist/`

Source Table Mapping:

| CSV                                | PostgreSQL Source Table |
| ---------------------------------- | ----------------------- |
| `olist_customers_dataset.csv`      | `customers`             |
| `olist_orders_dataset.csv`         | `orders`                |
| `olist_order_items_dataset.csv`    | `order_items`           |
| `olist_order_payments_dataset.csv` | `order_payments`        |
| `olist_products_dataset.csv`       | `products`              |
| `olist_sellers_dataset.csv`        | `sellers`               |

### 5.2 보존 원칙

Seed Loader는 분석용 Canonicalization을 수행하지 않는다.

1. V1 사용 컬럼을 명시적 Allowlist로 선택하고 선택된 CSV Header와 PostgreSQL Source Column Name을 동일하게 유지한다.
2. `customer_id`, `customer_unique_id`와 주문의 `customer_id`를 원본 그대로 적재한다.
3. `customer_city`, `seller_city`, `product_category_name` 같은 선택 컬럼의 Entity Prefix를 제거하지 않는다.
4. `payment_sequential` 같은 선택 컬럼의 원본 이름을 수정하지 않는다.
5. Olist 상태값은 소문자 원본 값을 유지한다.
6. Type 변환, 제약조건과 프로젝트 확장 필드 생성만 Seed 단계에서 수행한다.

Source에 추가할 수 있는 필드:

| Table          | 확장 필드                                      | 목적                            |
| -------------- | ---------------------------------------------- | ------------------------------- |
| customers      | `membership_level`, `created_at`, `updated_at` | Synthetic 고객 등급과 증분 수집 |
| products       | `created_at`, `updated_at`                     | 증분 수집                       |
| sellers        | `created_at`, `updated_at`                     | 증분 수집                       |
| orders         | `created_at`, `updated_at`                     | 생성/상태 변경 증분 수집        |
| order_items    | `created_at`                                   | Append-only 증분 수집           |
| order_payments | `payment_status`, `created_at`, `updated_at`   | Synthetic 결제 상태와 증분 수집 |

제외 컬럼은 다음 6개로 고정한다.

| Source Table | 제외 컬럼 | 제외 이유 |
|---|---|---|
| customers | `customer_zip_code_prefix` | V1 고객 지역 분석은 city/state를 사용 |
| order_items | `shipping_limit_date` | V1 KPI, 상태 전이, 품질검사에서 미사용 |
| products | `product_name_lenght` | 상품명 없이 길이만 제공되어 분석 가치가 낮음 |
| products | `product_description_lenght` | V1 Mart와 Dashboard에서 미사용 |
| products | `product_photos_qty` | V1 Mart와 Dashboard에서 미사용 |
| sellers | `seller_zip_code_prefix` | V1 판매자 지역 분석은 city/state를 사용 |

제외 컬럼은 `data/raw/olist/`의 원본 CSV에만 보존하며 PostgreSQL, Bronze, dbt Model에는 적재하지 않는다. 그 외 Allowlist 컬럼은 Source에서 Rename하거나 Merge하지 않는다. 제외 목록을 변경할 때는 PRD와 Seed Schema Contract Test를 함께 변경한다.

### 5.3 확장 필드 생성 규칙

| Table            | 규칙                                                                                              |
| ---------------- | ------------------------------------------------------------------------------------------------- |
| customers        | 연결 주문의 최초 `order_purchase_timestamp`를 `created_at`, `--seeded-at`을 `updated_at`으로 사용 |
| orders           | `order_purchase_timestamp`를 `created_at`, `--seeded-at`을 `updated_at`으로 사용                  |
| order_items      | 연결 주문의 `order_purchase_timestamp`를 `created_at`으로 사용                                    |
| products/sellers | 자체 Event Timestamp가 없어 `created_at = updated_at = --seeded-at`                               |
| order_payments   | 연결 주문의 `order_purchase_timestamp`를 `created_at`, `--seeded-at`을 `updated_at`으로 사용      |

Seed `membership_level`은 `customer_unique_id`별 원본 `order_status='delivered'` 주문 수로 계산하고 같은 `customer_unique_id`의 모든 Customer Row에 동일하게 기록한다.

Seed `payment_status`는 연결 주문이 `canceled` 또는 `unavailable`이면 `failed`, 그 외에는 `completed`다. 이는 Olist 원본에 없는 프로젝트 확장값이며 `pending`, `refunded`는 Generator가 만든다.

```text
bronze 0~4 / silver 5~14 / gold 15+
```

### 5.4 Seed CLI와 재실행

```bash
uv run python -m src.seed \
  --input-dir data/raw/olist \
  --seeded-at 2026-09-03T00:00:00Z
```

`--seeded-at`은 필수이며 모든 Raw Event Timestamp 이상이어야 한다. Seed Dataset 전체를 해당 시점에 관측한 Baseline Snapshot으로 취급한다. 여기서 Staging은 Seed Loader의 임시 PostgreSQL Schema이며 dbt Staging과 다르다. Loader는 Header/파일 검증 → 임시 Load → PK/FK/상태/Count 검증 → 한 Transaction의 `INSERT ... ON CONFLICT DO UPDATE` 순으로 실행한다.

- 입력 계산값만 갱신하고 실행 현재시각으로 `updated_at`을 바꾸지 않는다.
- Target에만 있는 Synthetic Row는 삭제하지 않는다.
- 동일 Raw Checksum과 `seeded_at` 재실행 후 Count와 Content Hash가 같아야 한다.
- 전체 삭제는 Loader가 아니라 명시적인 `scripts/reset.sh` 책임이다.
- Seed Loader는 Synthetic Generator가 한 번이라도 성공한 환경에서는 실행을 거부한다. Seed 재실행은 Generator 시작 전 Bootstrap 검증에만 허용하며, 이후 재초기화는 `scripts/reset.sh` 후 수행한다.

`pipeline_metadata.seed_runs`는 `raw_checksum`, `seeded_at`, Table별 Row Count, Content Hash, Status를 기록한다. `generator_runs`의 성공 Row가 존재하면 Seed Guard가 동작한다.

Naming 표준화와 상태 Mapping은 Seed 성공 조건이 아니라 dbt Staging의 책임이며 14.1에서 정의한다.

---

## 6. PostgreSQL Source 계약

한 PostgreSQL Container에 `commerce_source`, `airflow_metadata`, `pipeline_metadata` Database와 역할별 계정을 둔다.

### 6.1 Table

| Table / Grain                      | 주요 Column과 Constraint                                         |
| ---------------------------------- | ---------------------------------------------------------------- |
| customers / Olist 고객 Record 1행  | 선택 원본 4개 Column + `membership_level`, `created_at`, `updated_at` |
| products / 상품 1행                | 선택 원본 6개 Column + `created_at`, `updated_at`                     |
| sellers / 판매자 1행               | 선택 원본 3개 Column + `created_at`, `updated_at`                     |
| orders / 주문 1행                  | 원본 8개 Column + `created_at`, `updated_at`                     |
| order_items / 주문 Line 1행        | 선택 원본 6개 Column + `created_at`                              |
| order_payments / 결제 Sequence 1행 | 원본 5개 Column + `payment_status`, `created_at`, `updated_at`   |

모든 Timestamp CSV 값은 UTC로 해석해 `TIMESTAMPTZ`로 적재한다. 선택 Event Timestamp만 NULL을 허용한다. Source 상태 `CHECK`는 원본 소문자 Domain을 사용하고 대문자 Canonical Value는 dbt Staging에서 만든다.

Physical Column 계약:

#### customers

| Column                   | Type         | Constraint                     |
| ------------------------ | ------------ | ------------------------------ |
| customer_id              | VARCHAR(64)  | PK, Olist 원본                 |
| customer_unique_id       | VARCHAR(64)  | NOT NULL, 중복 허용            |
| customer_city            | VARCHAR(128) | 원본 Prefix 보존               |
| customer_state           | CHAR(2)      | 원본 Prefix 보존               |
| membership_level         | VARCHAR(16)  | NOT NULL, `bronze/silver/gold` |
| created_at               | TIMESTAMPTZ  | NOT NULL                       |
| updated_at               | TIMESTAMPTZ  | NOT NULL, `>= created_at`      |

#### products

| Column                     | Type         | Constraint                         |
| -------------------------- | ------------ | ---------------------------------- |
| product_id                 | VARCHAR(64)  | PK                                 |
| product_category_name      | VARCHAR(256) | 원본 Prefix 보존                   |
| product_weight_g           | INTEGER      | NULL 또는 `>= 0`                   |
| product_length_cm          | INTEGER      | NULL 또는 `>= 0`                   |
| product_height_cm          | INTEGER      | NULL 또는 `>= 0`                   |
| product_width_cm           | INTEGER      | NULL 또는 `>= 0`                   |
| created_at                 | TIMESTAMPTZ  | NOT NULL                           |
| updated_at                 | TIMESTAMPTZ  | NOT NULL, `>= created_at`          |

#### sellers

| Column                 | Type         | Constraint                |
| ---------------------- | ------------ | ------------------------- |
| seller_id              | VARCHAR(64)  | PK                        |
| seller_city            | VARCHAR(128) | 원본 Prefix 보존          |
| seller_state           | CHAR(2)      | 원본 Prefix 보존          |
| created_at             | TIMESTAMPTZ  | NOT NULL                  |
| updated_at             | TIMESTAMPTZ  | NOT NULL, `>= created_at` |

#### orders

| Column                        | Type        | Constraint                           |
| ----------------------------- | ----------- | ------------------------------------ |
| order_id                      | VARCHAR(64) | PK                                   |
| customer_id                   | VARCHAR(64) | NOT NULL, FK customers, 원본 값 보존 |
| order_status                  | VARCHAR(16) | NOT NULL, 원본 소문자 Domain CHECK   |
| order_purchase_timestamp      | TIMESTAMPTZ | NOT NULL                             |
| order_approved_at             | TIMESTAMPTZ | NULL 가능                            |
| order_delivered_carrier_date  | TIMESTAMPTZ | NULL 가능                            |
| order_delivered_customer_date | TIMESTAMPTZ | NULL 가능                            |
| order_estimated_delivery_date | TIMESTAMPTZ | NULL 가능                            |
| created_at                    | TIMESTAMPTZ | NOT NULL                             |
| updated_at                    | TIMESTAMPTZ | NOT NULL, `>= created_at`            |

`order_status` Source Domain은 `created`, `approved`, `processing`, `invoiced`, `shipped`, `delivered`, `canceled`, `unavailable`이다.

#### order_items

| Column              | Type          | Constraint                |
| ------------------- | ------------- | ------------------------- |
| order_id            | VARCHAR(64)   | PK, FK orders             |
| order_item_id       | INTEGER       | PK, `> 0`                 |
| product_id          | VARCHAR(64)   | NOT NULL, FK products     |
| seller_id           | VARCHAR(64)   | NOT NULL, FK sellers      |
| price               | NUMERIC(14,2) | NOT NULL, `>= 0`          |
| freight_value       | NUMERIC(14,2) | NOT NULL, `>= 0`          |
| created_at          | TIMESTAMPTZ   | NOT NULL                  |

#### order_payments

| Column               | Type          | Constraint                         |
| -------------------- | ------------- | ---------------------------------- |
| order_id             | VARCHAR(64)   | PK, FK orders                      |
| payment_sequential   | INTEGER       | PK, `> 0`, 원본 이름 보존          |
| payment_type         | VARCHAR(32)   | NOT NULL                           |
| payment_installments | INTEGER       | NULL 또는 `>= 0`, 원본 Prefix 보존 |
| payment_value        | NUMERIC(14,2) | NOT NULL, `>= 0`                   |
| payment_status       | VARCHAR(16)   | NOT NULL, 소문자 Domain CHECK      |
| created_at           | TIMESTAMPTZ   | NOT NULL                           |
| updated_at           | TIMESTAMPTZ   | NOT NULL, `>= created_at`          |

`payment_status` Source Domain은 `pending`, `completed`, `failed`, `refunded`다.

### 6.2 증분 Index

```text
customers   (updated_at, customer_id)
products    (updated_at, product_id)
sellers     (updated_at, seller_id)
orders      (updated_at, order_id)
order_items (created_at, order_id, order_item_id)
order_payments (updated_at, order_id, payment_sequential)
```

Index와 Extract 정렬 순서는 같아야 한다.

### 6.3 Transaction

- Order/Item/Payment 생성은 하나의 Transaction이다.
- 상태와 관련 Timestamp 변경은 하나의 Transaction이다.
- 완료 주문 수 재계산과 동일 `customer_unique_id`의 모든 Customer Row Membership 갱신도 같은 Transaction에 포함한다.
- `updated_at`은 Logical Event Time이며 Wall Clock을 직접 쓰지 않는다.
- Source `updated_at` 역행은 금지한다.

---

## 7. Generator와 Anomaly

### 7.1 상태 전이

```text
Source order_status
created → approved → shipped → delivered
   └──────────────→ canceled
          └──────→ canceled

Source payment_status
pending → completed → refunded
    └──→ failed
```

Olist Seed의 `processing`, `invoiced`, `unavailable`도 Source Domain에 포함하지만 Generator 신규 전이에는 사용하지 않는다. dbt Staging은 `created`, `approved/processing`, `shipped/invoiced`, `delivered`, `canceled/unavailable`을 각각 `CREATED`, `APPROVED`, `SHIPPED`, `DELIVERED`, `CANCELED`로 변환한다.

### 7.2 결정 입력

```bash
uv run python -m src.generator \
  --seed 42 \
  --logical-date 2026-09-03T00:00:00Z \
  --orders 1000 \
  --anomaly-profile default
```

결정성 입력은 `random_seed`, `logical_date`, `order_count`, `anomaly_profile`, `generator_version`이다. 후보 DB Row는 Business Key로 정렬한 뒤 선택한다.

이미 같은 결정 ID가 있으면 기대값과 비교해 같으면 Skip하고 다르면 결정성 위반으로 실패한다.

Synthetic 고객도 Olist 식별 의미를 따른다.

- `customer_unique_id`: 동일 인물을 나타내는 안정적인 결정 ID
- `customer_id`: 주문 시점의 Customer Record를 나타내는 결정 ID
- 재구매 주문은 기존 `customer_unique_id`와 새 `customer_id`를 사용한다.
- 주문은 새 `customer_id`를 FK로 참조한다.
- 주소 변경은 새 Customer Record의 Prefixed 주소 Column에 반영하고 과거 Customer Row는 유지한다.
- Membership 변경은 동일 `customer_unique_id`의 모든 Customer Row를 한 Transaction에서 같은 값과 `updated_at`으로 갱신한다.

### 7.3 Anomaly

- Service-level: Late Order, Delayed Payment, 과거 Event의 늦은 Update, Membership 변경
- Pipeline-level: Duplicate, NULL Key, Broken FK, Invalid Status, 음수 금액

Pipeline Corruption은 Extract 후 Validation 직전 복제본에 결정적으로 주입하며 Source에는 쓰지 않는다.

### 7.4 SCD2 관측 한계

Source는 현재 상태만 보관하므로 성공 수집 사이의 중간 변경은 복원할 수 없다. V1 Generator는 SCD2 추적 속성에 대해 고객당 수집 구간 내 최대 1회만 변경한다. 이 제약과 CDC 확장 필요성을 ADR에 남긴다.

---

## 8. 증분 추출과 동시성

### 8.1 Cursor

| Table          | Cursor Tuple                                 |
| -------------- | -------------------------------------------- |
| customers      | `(updated_at, customer_id)`                  |
| products       | `(updated_at, product_id)`                   |
| sellers        | `(updated_at, seller_id)`                    |
| orders         | `(updated_at, order_id)`                     |
| order_items    | `(created_at, order_id, order_item_id)`      |
| order_payments | `(updated_at, order_id, payment_sequential)` |

초기 Watermark는 논리적 `-infinity`와 Table별 최소 Key로 해석한다.

### 8.2 고정 범위와 Pagination

Source `REPEATABLE READ`, Read-only Snapshot에서 `MAX(cursor_tuple)`을 `extract_upper_bound`로 고정한 뒤 다음 범위만 읽는다.

```sql
WHERE cursor_tuple > :watermark_before
  AND cursor_tuple <= :extract_upper_bound
ORDER BY cursor_tuple
LIMIT :page_size
```

다음 Page Lower Bound는 직전 Page 마지막 Cursor다. 기본 Page Size는 50,000이며 `INGESTION_PAGE_SIZE`로 바꾼다. Page별 Arrow Table을 Parquet Writer에 순차 기록하고 전체 Table을 메모리에 올리지 않는다.

- 추출 중 Upper Bound 뒤에 들어온 Row는 다음 Batch 대상이다.
- 빈 범위는 Object 없이 `SUCCESS_NO_DATA`, Watermark 유지로 기록한다.
- Composite PK 전체를 Tie-breaker에 포함한다.

### 8.3 Lease와 CAS

- 동일 `pipeline_name + source_table` 실행은 하나만 허용한다.
- Metadata Watermark Row에서 `lease_owner`, `lease_expires_at`을 원자적으로 획득한다.
- 기본 TTL 30분, 5분마다 연장하며 만료 Lease만 인수할 수 있다.
- Lock 실패 Run은 Source를 읽거나 Object를 만들지 않는다.
- 최종 Commit은 현재 Watermark가 시작 시 읽은 값과 같은지 Compare-and-swap한다.

### 8.4 Commit 순서

```text
Lease → 범위 고정 → Extract → Validate → Local Parquet
→ SeaweedFS Upload → HEAD/Checksum/Row Count 검증 → Manifest
→ Metadata Transaction(Object COMMITTED + Run SUCCESS + Watermark)
→ Lease 해제
```

Validation/Upload/검증 실패 시 Watermark는 유지한다. dbt 실패는 Bronze Commit 이후이므로 Bronze와 Watermark를 유지한다.

---

## 9. Batch Identity와 재실행

```text
batch_id       = {dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}
run_id         = UUIDv4 per execution attempt
table_batch_id = {batch_id}__{source_table}
```

- 동일 Logical Date 재실행은 같은 `batch_id`, 새 `run_id`를 쓴다.
- 같은 Table Batch가 `COMMITTED`이고 Range/Schema Version이 같으면 Skip한다.
- Commit Range가 다르면 `BATCH_IDENTITY_CONFLICT`로 실패한다.
- 임시 Object만 있으면 제거 후 재실행한다.
- Final Object는 있으나 Metadata가 없으면 Manifest/Checksum으로 복구하거나 불일치 시 실패한다.
- Commit된 Final Object는 덮어쓰거나 삭제하지 않는다.
- 같은 Logical Date를 다른 범위로 강제 처리하면 `reprocess_id`가 붙은 새 Backfill Batch를 쓴다.

Checksum:

- `content_sha256`: Parquet Byte 기준
- `logical_hash`: PK 정렬 후 Business Column Canonical JSON 기준
- 재실행 정합성은 `row_count + logical_hash`로 판단한다.

---

## 10. SeaweedFS Bronze와 Quarantine

### 10.1 저장 구조

```text
commerce-lake/
├── _staging/{run_id}/{table}/data.parquet
├── bronze/{table}/ingestion_date=YYYY-MM-DD/batch_id={batch_id}/
│   ├── data.parquet
│   └── manifest.json
└── quarantine/{table}/ingestion_date=YYYY-MM-DD/batch_id={batch_id}/
    ├── records.parquet
    └── manifest.json
```

Endpoint는 `http://seaweedfs:8333`, Bucket은 `commerce-lake`, boto3 Path-style 접근을 사용한다. 설정과 Credential은 환경변수로 주입한다.

`_staging`은 성공 후 지우고 7일 이상 실패 잔여분은 `scripts/cleanup_staging.py`로 정리한다.

### 10.2 Bronze 규칙

기술 컬럼:

```text
_batch_id, _run_id, _ingested_at, _source_table, _schema_version
```

Parquet은 Zstandard, UTC microseconds, `decimal128(14,2)`, 목표 Row Group 128K를 사용한다. 기본은 Table Batch당 1개 파일이다. 512MB 초과가 관측되면 `part-00000.parquet` 방식으로 전환하고 ADR을 갱신한다.

Manifest 최소 필드:

```text
manifest_version, schema_version, batch_id, run_id, source_table
logical_date, watermark_before, extract_upper_bound
rows_extracted, rows_valid, rows_rejected
object_key, object_size, content_sha256, logical_hash
created_at, status=COMMITTED
```

Secret, Local Absolute Path, Raw Payload는 Manifest에 넣지 않는다.

### 10.3 Commit File Catalog

Object Glob을 직접 읽지 않는다. `sync_bronze_catalog`가 Metadata의 `COMMITTED` Object를 DuckDB `control.bronze_files`에 동기화한다.

```text
source_table, object_key, schema_version, batch_id
committed_at, row_count, logical_hash
```

dbt Macro는 Catalog의 Table별 Object 목록으로 `read_parquet([...])`를 만든다. Catalog에 없는 Orphan은 읽지 않는다.

### 10.4 Quarantine

Reject 원문은 `quarantine/` Parquet에 다음 기술 컬럼과 함께 저장한다.

```text
_record_id, _batch_id, _run_id, _source_table
_error_codes, _error_message, _detected_at, _raw_payload
```

`_record_id`는 `table_batch_id + row ordinal` 기반의 결정 ID다. Metadata DB에는 Raw Payload 대신 Error Count와 Object Key만 기록한다.

---

## 11. Ingestion Validation

검증 순서는 Schema/필수 Column → Type → Key NULL → Batch 내 중복 → Source Status Domain → Numeric Range → Broken Reference → Cursor 범위다. Canonical Status 검사는 dbt Staging 이후 Warehouse Test가 담당한다.

- Row 오류는 Quarantine하고 Valid Row는 계속 처리한다.
- Schema 누락이나 Cursor 범위 위반은 Batch 전체 실패다.
- Broken Reference는 같은 Batch Parent와 Commit된 최신 Parent Key를 함께 검사한다.
- Valid Row가 Commit되면 Reject가 있어도 Watermark는 Upper Bound까지 전진한다. Reject는 영구 격리한다.
- `MAX_REJECT_RATE` 기본값은 5%다. 초과 시 전체 실패하고 Watermark를 유지한다.
- 0 Row의 Reject Rate는 0이다.
- Corruption 주입 전후 Row Count를 따로 기록한다.

---

## 12. Pipeline Metadata

### 12.1 watermarks

| Column              | Type        | Constraint          |
| ------------------- | ----------- | ------------------- |
| pipeline_name       | TEXT        | PK                  |
| source_table        | TEXT        | PK                  |
| watermark_timestamp | TIMESTAMPTZ | NULL은 초기 상태    |
| watermark_keys      | JSONB       | NOT NULL, 기본 `[]` |
| lease_owner         | UUID        | NULL 가능           |
| lease_expires_at    | TIMESTAMPTZ | NULL 가능           |
| version             | BIGINT      | NOT NULL, 기본 0    |
| updated_at          | TIMESTAMPTZ | NOT NULL            |

`watermark_keys`는 PK 순서의 JSON Array이며 읽을 때 길이와 Type을 Table Contract로 검증한다.

### 12.2 pipeline_runs

| Column                                | 의미              |
| ------------------------------------- | ----------------- |
| run_id + source_table                 | Composite PK      |
| batch_id, dag_id, logical_date        | Logical 실행 식별 |
| attempt_number                        | 1부터 시작        |
| started_at, finished_at               | 실행 구간         |
| watermark_before, extract_upper_bound | Cursor JSON       |
| rows_extracted/valid/rejected/loaded  | 단계별 Count      |
| status, error_type, error_message     | 결과와 오류       |

상태는 `RUNNING`, `SUCCESS`, `SUCCESS_NO_DATA`, `SKIPPED_ALREADY_COMMITTED`, `FAILED`다. 오류 메시지는 최대 4,000자다.

### 12.3 bronze_objects

```text
table_batch_id PK
source_table, batch_id
object_key UNIQUE, manifest_key
schema_version, row_count
content_sha256, logical_hash
watermark_before, watermark_after
status: STAGED | COMMITTED | ORPHANED
committed_at
```

파일 분할 시 `bronze_object_files` Child Table을 추가한다.

### 12.4 quarantine_batches

```text
table_batch_id PK
object_key, row_count, error_counts JSONB, created_at
```

Watermark Update, Object Commit, Run Success는 하나의 Metadata Transaction이다. Commit된 Metadata를 제어 상태의 Source of Truth로 사용한다.

### 12.5 seed_runs와 generator_runs

`seed_runs`:

```text
seed_run_id UUID PK
raw_checksum, seeded_at, started_at, finished_at
table_row_counts JSONB, table_content_hashes JSONB
status, error_message
```

`generator_runs`:

```text
generator_run_id UUID PK
random_seed, logical_date, order_count
anomaly_profile, generator_version
result_counts JSONB, logical_hash
started_at, finished_at, status, error_message
UNIQUE(random_seed, logical_date, order_count, anomaly_profile, generator_version)
```

동일 Generator 입력 재실행은 기존 성공 Row와 Logical Hash를 비교한다. `seed_runs`와 `generator_runs`도 Secret이나 Row Payload를 저장하지 않는다.

---

## 13. Airflow 실행 계약

### 13.1 기본값

- Executor: `LocalExecutor`
- `source_simulation_dag`: Manual
- `warehouse_pipeline_dag`: `@daily`
- 개발 시 `WAREHOUSE_DAG_SCHEDULE` 빈 값으로 비활성화
- 기본 `catchup=False`; Backfill은 명시 Parameter 사용
- `max_active_runs=1`; DuckDB Writer와 Watermark 충돌 방지

### 13.2 Task Graph

```text
initialize_run
  ↓
extract_validate_load[customers, products, sellers, orders, order_items, order_payments]
  ↓
verify_bronze_commit
  ↓
sync_bronze_catalog
  ↓
dbt_build
  ↓
publish_run_summary
```

Table Task 내부에서 Extract → Validate → Upload → Watermark Commit을 수행한다. 한 Table이 실패하면 dbt는 실행하지 않는다. 성공한 다른 Table Watermark는 되돌리지 않고 같은 Batch 재실행에서 재사용한다.

### 13.3 Retry와 XCom

```text
retries = 2
retry_delay = 60 seconds
retry_exponential_backoff = true
max_retry_delay = 10 minutes
```

Network/DB/Object Storage 일시 오류만 재시도한다. Contract와 Identity 충돌은 재시도하지 않는다.

XCom에는 Batch/Run ID, Object Key, Count, Watermark JSON, 상태만 허용한다. DataFrame, Arrow Table, Parquet Byte, Raw Row, Credential은 금지한다.

DAG는 Orchestration만 담당하고 Generator/Ingestion은 `src/`, Transformation/Test는 `dbt/`에 둔다.

---

## 14. dbt + DuckDB 모델

Warehouse는 `data/warehouse/warehouse.duckdb`, Schema는 `control`, `staging`, `intermediate`, `marts`다. Credential은 Runtime DuckDB Secret/Session Setting으로 주입한다.

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

Staging Naming Mapping:

#### stg_customers_current / stg_customer_observations

| Source                     | Staging                          |
| -------------------------- | -------------------------------- |
| `customer_id`              | `source_customer_id`             |
| `customer_unique_id`       | `customer_id`                    |
| `customer_city`            | `city`                           |
| `customer_state`           | `state`                          |
| `membership_level`         | `membership_level` + 대문자 변환 |
| Group 내 최소 `created_at` | `created_at`                     |
| Group 내 최대 `updated_at` | `updated_at`                     |

한 `customer_unique_id`에 Source Row가 여러 개면 분석 고객 1개로 통합한다. `updated_at DESC`, 연결 주문의 `order_purchase_timestamp DESC`, `order_id DESC`, `source_customer_id DESC` 순으로 대표 Row를 선택한다. Membership은 같은 고객의 모든 Row에서 동기화되고, 주소는 가장 최근 주문 Customer Record에서 선택된다.

#### stg_orders

| Source                                | Staging                                   |
| ------------------------------------- | ----------------------------------------- |
| `customer_id`                         | `source_customer_id`                      |
| customers Join의 `customer_unique_id` | `customer_id`                             |
| `order_purchase_timestamp`            | `purchase_at`                             |
| `order_approved_at`                   | `approved_at`                             |
| `order_delivered_carrier_date`        | `carrier_at`                              |
| `order_delivered_customer_date`       | `delivered_at`                            |
| `order_estimated_delivery_date`       | `estimated_delivery_at`                   |
| `order_status`                        | `order_status` + Canonical 대문자 Mapping |

#### stg_order_items / stg_payments

`stg_order_items`는 선택한 Business Column 이름을 그대로 유지하고 Type과 기술 Column만 표준화한다.

| Source                 | Staging                        |
| ---------------------- | ------------------------------ |
| `payment_sequential`   | `payment_sequence`             |
| `payment_installments` | `installments`                 |
| `payment_status`       | `payment_status` + 대문자 변환 |

`stg_payments`의 Source Table은 `order_payments`다.

#### stg_products / stg_sellers

| Source                       | Staging                      |
| ---------------------------- | ---------------------------- |
| `product_category_name`      | `category_name`              |
| `product_weight_g`           | `weight_g`                   |
| `product_length_cm`          | `length_cm`                  |
| `product_height_cm`          | `height_cm`                  |
| `product_width_cm`           | `width_cm`                   |
| `seller_city`                | `city`                       |
| `seller_state`               | `state`                      |

Source/Bronze Column은 Staging SQL의 입력 Alias에서만 Rename한다. Intermediate와 Mart는 Source Prefix를 직접 참조하지 않는다.

Mutable Current Model의 우선순위:

```sql
row_number() over (
  partition by business_key
  order by updated_at desc, _ingested_at desc, _batch_id desc
) = 1
```

Customer Observation은 Staging에서 파생한 분석 `customer_id` 기준으로 모든 관측 Version을 유지하되 동일 `customer_id + updated_at + tracked_attribute_hash`를 Deduplicate한다.

### 14.2 Intermediate

```text
int_orders_enriched
int_order_items_enriched
int_payment_summary
int_customer_history
int_affected_business_dates
```

Affected Date는 새 Commit Batch의 변경 Order 구매일이다. Item/Payment는 `order_id`로 주문일을 찾는다.

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

각 YAML에 Grain, Key, Measure, 시간 기준, Test를 기록한다. `fact_orders`는 Item 금액 합, 배송비 합, Payment 합, `order_count=1`을 가진다. 서로 다른 Grain의 Fact를 직접 Join해 합산하지 않는다.

### 14.4 Incremental

변경 Key는 DuckDB Transaction의 `DELETE + INSERT` 또는 검증된 `MERGE`로 교체한다. 여러 Bronze Version 중 Current만 적재하고 Full Refresh와 Key 정렬 Logical Hash를 비교한다.

---

## 15. SCD Type 2와 Temporal Join

- Business Key: dbt Staging에서 `customer_unique_id`로부터 만든 분석 `customer_id`
- 추적 속성: `membership_level`, `city`, `state`
- `customer_key = Hash(customer_id, valid_from, attribute_hash)`

```text
customer_key, customer_id, customer_unique_id
membership_level, city, state, attribute_hash
valid_from, valid_to, is_current
```

Version 규칙:

1. `(customer_id, updated_at, _ingested_at, _batch_id)`로 정렬한다.
2. 추적 속성 Hash가 직전과 같으면 Version을 추가하지 않는다.
3. 고객의 최초 관측 Version은 `created_at`, 이후 변경 Version은 `updated_at`을 `valid_from`으로 쓴다.
4. 다음 Version의 시작이 현재 `valid_to`다.
5. 마지막만 `valid_to=NULL`, `is_current=true`다.
6. 유효 구간은 `[valid_from, valid_to)`다.

동일 고객·동일 `updated_at`의 서로 다른 Hash는 Contract 위반이다.

Olist Seed는 과거 속성 이력을 제공하지 않으므로 최초 Version의 Membership과 주소는 `created_at`부터 유효한 Baseline Snapshot으로 간주한다. Olist 과거 시점의 실제 등급·주소를 복원했다는 의미가 아니며, 정확한 SCD2 검증은 Seed 이후 Synthetic 변경을 대상으로 한다.

Temporal Join:

```sql
order.purchase_at >= dim_customer.valid_from
AND order.purchase_at < COALESCE(dim_customer.valid_to, TIMESTAMPTZ 'infinity')
```

유효 고객 Version이 아직 없으면 고정 Unknown Key를 사용하고 고객 도착 시 주문을 재처리한다. 정상 E2E의 Unknown Fact Count는 0이어야 한다.

---

## 16. Late Arrival, Backfill, Re-run

`business_event_time::date < logical_date::date`이면 Late Arrival 후보다. 추출 여부는 `updated_at` Cursor가 결정한다.

영향 범위:

- orders: 해당 구매일
- order_items/order_payments: 연결된 주문 구매일
- customers: 바뀐 SCD2 구간과 겹치는 주문일
- products/sellers: 해당 Entity 사용 주문일

영향 날짜와 Key를 `control.affected_keys`에 dbt Invocation ID와 기록한다. Fact/Aggregate는 영향 Key/Date를 Transactional `DELETE + INSERT`한다. 고객 Version 변경 시 유효 구간 주문의 Customer Key도 갱신한다. 영향 범위가 전체의 30%를 넘으면 Full Refresh 권고 Warning만 낸다.

Backfill Mode:

1. **Replay**: Commit Bronze를 dbt에 재적용하고 Source를 읽지 않는다.
2. **Re-extract**: 명시 Cursor 범위를 새 `reprocess_id`로 추출하고 기존 Object를 보존한다.

기본은 Replay다. Re-extract는 Source 현재 상태가 과거와 다를 수 있으므로 범위를 명시해야 한다.

누락 Schedule은 Metadata로 탐지한다. 다음 성공 Cursor가 Source Row를 수집하지만 운영 증적을 위해 누락 날짜 Backfill을 별도 실행한다.

---

## 17. Data Quality와 Publish

Generic Test는 `unique`, `not_null`, `relationships`, `accepted_values`다.

Custom Test:

```text
payment_value, price, freight_value >= 0
delivered_at >= purchase_at
approved_at >= purchase_at when not null
customer당 current Version = 1
SCD2 유효 구간 중첩 = 0
Fact FK 누락 = 0
Fact Business Key 중복 = 0
정상 E2E Unknown 참조 = 0
```

Ingestion은 Bronze Contract, Warehouse는 관계·시간·Grain을 검증한다. Warehouse 실패 시 Bronze/Watermark는 유지한다.

Metabase는 검증 완료 Mart만 읽는다. 실패 Build는 `control.warehouse_builds`에 기록하고 마지막 성공 Mart를 유지한다. 필요하면 Build Schema 후 Rename/Swap을 Phase 5에서 적용한다.

---

## 18. Observability와 오류

필수 조회: Batch/Run/Logical Date, Table Status, Cursor Before/Upper/After, 단계별 Count, Object/Manifest/Hash, Duration/Retry, Error, dbt Invocation/Test.

```text
CONFIGURATION_ERROR
SOURCE_CONNECTION_ERROR
SOURCE_CONTRACT_ERROR
VALIDATION_THRESHOLD_EXCEEDED
OBJECT_STORAGE_ERROR
OBJECT_VERIFICATION_ERROR
WATERMARK_CONFLICT
BATCH_IDENTITY_CONFLICT
DBT_BUILD_ERROR
DBT_TEST_ERROR
UNKNOWN_ERROR
```

Stack Trace는 Airflow Log, 요약은 Metadata에 저장하고 URL/Secret은 Mask한다. Run Summary는 Table/dbt 결과의 JSON Log와 읽기 쉬운 요약을 남긴다.

---

## 19. BI

Phase 9 직전에 Metabase Patch와 DuckDB Driver를 재검증한다. 우선 마지막 성공 DuckDB Mart를 Read-only 조회하고 Lock/Driver 문제가 관측되면 PostgreSQL Serving DB로 Publish한다. 선택 근거는 ADR로 남긴다.

Dashboard는 Sales Overview, Product, Customer 세 영역이며 Raw Source를 직접 참조하지 않는다.

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
uv run python -m src.seed --input-dir data/raw/olist --seeded-at <UTC_TIMESTAMP>
```

README에 Tool, 환경변수, Health Check, 초기화, E2E, 종료 절차를 포함한다. `.env`, Raw/Generated Data, DuckDB, Log는 Git 제외하고 Manifest/Metadata/Log에 Secret을 남기지 않는다.

Benchmark Scale은 S=100K, M=1M, L=5M Orders다. Full/Incremental, CSV/Parquet, Full Scan/Filtering, Cold/Warm을 동일 환경에서 기본 5회 실행하고 Median과 Raw 값을 저장한다.

```text
benchmark_id, git_commit, started_at, host_spec, wsl_spec
python_version, dependency_lock_hash, image_versions
dataset_scale, random_seed, scenario, run_number, is_cold_run
duration_seconds, rows_scanned, rows_changed
input_bytes, output_bytes, result_hash
```

---

## 21. 기능 요구사항

| ID    | 요구사항                          | 우선순위 |
| ----- | --------------------------------- | -------- |
| FR-01 | Raw-compatible Olist Seed         | P0       |
| FR-02 | Deterministic Generator           | P0       |
| FR-03 | 고정 범위 Incremental Extract     | P0       |
| FR-04 | Composite Watermark + Lease + CAS | P0       |
| FR-05 | Run/Object Metadata               | P0       |
| FR-06 | SeaweedFS Parquet Bronze          | P0       |
| FR-07 | Idempotency와 Orphan Recovery     | P0       |
| FR-08 | Airflow Pipeline                  | P0       |
| FR-09 | dbt Staging/Intermediate/Mart     | P0       |
| FR-10 | Star Schema/Fact Grain            | P0       |
| FR-11 | Ingestion/Warehouse Quality       | P0       |
| FR-12 | Backfill Replay/Re-extract        | P0       |
| FR-13 | Late Arrival 재처리               | P0       |
| FR-14 | SCD2/Temporal Join                | P0       |
| FR-15 | Quarantine                        | P0       |
| FR-16 | Metabase                          | P1       |
| FR-17 | 1M+ Scale                         | P1       |
| FR-18 | Benchmark                         | P1       |
| FR-19 | Cloud PoC                         | P2       |
| FR-20 | CDC                               | P2       |

FR-01과 FR-09의 결합 조건으로 Source/Bronze Naming 보존과 dbt Staging의 분석 Naming 변환을 자동 검증한다.

---

## 22. Acceptance Criteria

| ID    | 시나리오                   | 합격 조건                                                                                        |
| ----- | -------------------------- | ------------------------------------------------------------------------------------------------ |
| AC-01 | E2E                        | 고정 주문이 Source→Bronze Catalog→Fact에 존재하고 Count 추적 가능                                |
| AC-02 | Incremental                | Cursor 조건과 실제 신규·변경 Key Set 일치                                                        |
| AC-03 | 동일 Batch 3회             | Object 수/Key Count/Mart Hash 동일, 중복 0                                                       |
| AC-04 | Upload 전후 실패           | 실패 중 Watermark 유지, 성공 뒤 Upper로 전진                                                     |
| AC-05 | 동일 Timestamp가 Page 초과 | Page/Batch 경계 누락·중복 0                                                                      |
| AC-06 | 동시 Extract               | 하나만 Lease, 다른 Run은 Object 없이 Conflict                                                    |
| AC-07 | Backfill Replay            | 같은 범위 Full Refresh와 Key별 값/Hash 동일                                                      |
| AC-08 | 5종 Corruption             | 기대 Reject 일치, Source 무오염, Record 추적 가능                                                |
| AC-09 | BRONZE→SILVER→GOLD         | 3 Version, 구간 중첩 0, Current 1                                                                |
| AC-10 | 구간별 주문                | 발생 시점 Customer Key 참조                                                                      |
| AC-11 | 3일 전 Late Order          | 현재 수집, 과거 Mart 갱신, Full과 동일                                                           |
| AC-12 | Referential Integrity      | FK/Unique 통과, 정상 Unknown 0                                                                   |
| AC-13 | Observability              | 성공/빈/실패/재실행을 SQL 한 번으로 조회                                                         |
| AC-14 | Seed 2회                   | Count/Content Hash 동일, PK/FK 위반 0                                                            |
| AC-15 | Generator 재현             | 동일 Snapshot/Input Key Set/Hash 동일                                                            |
| AC-16 | 새 Clone                   | Version/Health/Seed/E2E/dbt Test 성공                                                            |
| AC-17 | Benchmark                  | Raw 5회/Median/Hash/환경 Metadata 존재                                                           |
| AC-18 | Source Schema Allowlist    | 지정한 6개 제외 컬럼이 PostgreSQL/Bronze에 없고, 나머지 선택 컬럼은 원본 이름과 값을 유지함      |
| AC-19 | Staging Naming             | 정의된 Alias/상태 Mapping이 값 손실 없이 적용되고 Intermediate가 Raw Prefix를 직접 참조하지 않음 |

절대 처리시간 목표 대신 Baseline과 개선 전후를 비교한다.

---

## 23. Repository Structure

```text
commerce-data-platform/
├── README.md / PRD_v1.3.md / AGENTS.md
├── compose.yaml / pyproject.toml / uv.lock / .python-version
├── .env.example / .gitignore
├── airflow/dags/
├── src/{seed,generator,ingestion,common}/
├── dbt/{models,tests,macros}/
├── sql/{source,metadata,validation}/
├── data/{raw/olist,generated,warehouse,samples}/
├── scripts/{init.sh,download_dataset.py,seed.sh,cleanup_staging.py,reset.sh}
├── tests/{seed,generator,ingestion,integration}/
└── docs/{architecture,adr,benchmarks,runbooks,troubleshooting}/
```

---

## 24. 개발 단계와 Definition of Done

| Phase         | 구현                                        | 완료 조건                            |
| ------------- | ------------------------------------------- | ------------------------------------ |
| 0 Bootstrap   | Python/uv, 구조, 환경, Compose, Download    | Sync/Version/Compose/Git Ignore 통과 |
| 1 Source      | PostgreSQL, Raw-compatible DDL/Seed         | 원본 Naming/PK/FK, Seed Hash, AC-18  |
| 2 Generator   | 결정 ID, 상태, Membership, Late             | AC-15, 허용 전이, 변경 횟수 제약     |
| 3 Ingestion   | Cursor, Lease, Metadata, Bronze, Quarantine | AC-02~06, AC-08                      |
| 4 Airflow     | DAG, Dynamic Task, Retry, XCom              | E2E, Retry, 부분 성공 재사용         |
| 5 Modeling    | Catalog, Naming Staging, Mart, SCD2         | Grain, AC-09/10/12/19                |
| 6 Quality     | Corruption, Threshold, Test, Publish        | 지정 오류 탐지, 정상 오탐 0          |
| 7 Reliability | 실패/충돌/Late/Backfill Runbook             | 문제→재현→관측→원인→해결→재검증      |
| 8 Benchmark   | 고정 조건, 5회, Median                      | Raw/Hash/비교 문서                   |
| 9 BI          | Driver Gate, 3 Dashboard                    | Mart만 조회, Serving ADR             |

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
```

형식은 `Status / Context / Decision / Alternatives / Consequences / Validation`이다.

---

## 26. 알려진 제한과 대응

| 제한                        | V1 대응                                       |
| --------------------------- | --------------------------------------------- |
| Source 현재 상태만 보관     | 중간 변경 관측 불가 명시, Generator 변경 제한 |
| Delete 미지원               | Tombstone/CDC는 V2                            |
| Olist timezone 부재         | UTC naive로 일관 해석                         |
| S3 원자 Rename 부재         | Staging/Manifest/Catalog/복구                 |
| Object-Metadata 분산 Commit | Orphan Reconciliation + Checksum              |
| DuckDB Single-writer        | `max_active_runs=1`, dbt 단일 Process         |
| Metabase 호환성             | Phase 9 Gate, Serving DB 대안                 |
| 8GB RAM의 5M Scale          | Page Write, 관측 후 조정                      |
| Reject 후 Watermark 전진    | 영구 Quarantine, Reject Threshold             |

---

## 27. Portfolio Evidence

Architecture, Source 원본 보존과 Staging Naming Mapping, 고객 Business Key 변환, Cursor/Index, Watermark Failure, 동일 Batch Hash, Manifest/Metadata, Quarantine, Fact Grain, SCD2 Timeline, Late Arrival Diff, Backfill-Full Hash, Scale Benchmark, Failure Runbook, 새 Clone 기록을 최종 증적으로 남긴다.

---

## 28. 최종 확정 문장

> **Python 3.12 + WSL2 Ubuntu + Docker 환경에서 V1 사용 컬럼을 Allowlist로 선택하고 Olist Naming을 보존한 PostgreSQL OLTP Source를 구성한다. Airflow는 원본 호환 Schema를 Table별 Composite Cursor로 수집해 SeaweedFS의 불변 Parquet Bronze로 Commit한다. dbt Staging은 Prefix 제거, Timestamp와 상태값 Canonicalization을 수행하고, 이후 DuckDB에서 Star Schema, Incremental Fact, SCD Type 2와 Temporal Join을 구축한다. Lease, Metadata, Manifest, Checksum, Data Quality, Quarantine, Backfill과 Late Arrival Test로 동시성·실패·재실행을 검증하고 Version Pinning과 실행 Metadata로 재현성을 확보한다.**
