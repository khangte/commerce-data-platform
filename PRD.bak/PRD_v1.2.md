# PRD: Commerce Analytics Data Platform

> Version: 1.2
>
> Status: Implementation-Ready Baseline
>
> Baseline Date: 2026-09-03
>
> Environment: WSL2 Ubuntu / Local
>
> Goal: 데이터엔지니어 취업 포트폴리오용 Batch DW·Data Mart 프로젝트

---

## 0. v1.2 변경 요약

v1.2는 v1.1의 기능 범위를 유지하면서 구현자가 추가 결정을 내리지 않아도 되는 수준으로 계약을 구체화한다.

- 모든 운영 시간을 UTC `TIMESTAMPTZ`로 통일
- Olist `customer_id`와 `customer_unique_id`의 역할 및 Project 고객 ID 생성 규칙 확정
- Olist → Source 필드·상태·시간 Mapping 추가
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

기술 Version은 v1.1과 동일하다.

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

| 계층        | 책임                                      | 하지 않는 일               |
| ----------- | ----------------------------------------- | -------------------------- |
| Source DB   | 현재 서비스 상태와 OLTP 무결성            | 분석 집계, Corruption 저장 |
| Ingestion   | 범위 고정, 추출, 기본 검증, Bronze Commit | Mart 계산                  |
| Bronze      | 재처리 가능한 Source 관측 이력            | Silver/Gold 중복 저장      |
| DuckDB/dbt  | 정규화, 최신 상태, 이력, Dimension/Fact   | Source Watermark 관리      |
| Metadata DB | 제어 상태와 실행 증적                     | 대용량 Payload 저장        |
| Metabase    | Mart 소비 가능성 검증                     | Raw Source 직접 조회       |

### 2.3 불변 조건

- Watermark는 Commit된 Bronze 범위만 가리킨다.
- Commit된 Bronze Object는 수정하지 않는다.
- dbt는 Metadata에서 `COMMITTED`인 Object만 읽는다.
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

### 4.2 타입과 값

- 이름은 `snake_case`, Canonical Status는 대문자 문자열이다.
- 금액은 `NUMERIC(14,2)` / Parquet `decimal128(14,2)`다.
- 의미 없는 빈 문자열은 `NULL`로 정규화한다.
- Watermark Column과 PK는 `NOT NULL`이다.
- 문자열 Cursor 비교는 PostgreSQL `COLLATE "C"` 기준이다.

### 4.3 식별자

- Olist `order_id`, `product_id`, `seller_id`는 보존한다.
- Project 고객 ID는 `UUIDv5(project_namespace, "olist:" + customer_unique_id)`다.
- `project_namespace`는 코드 상수이며 변경을 Migration으로 취급한다.
- Synthetic Business Key는 Seed/Logical Date를 포함한 UUIDv5 또는 결정적 Hash를 쓴다. UUIDv4는 금지한다.

---

## 5. Olist Seed 변환 계약

### 5.1 입력

- Dataset: `olistbr/brazilian-ecommerce`
- 필수: customers, orders, order_items, products, payments, sellers
- 선택: reviews, geolocation
- 다운로드: `uv run python scripts/download_dataset.py`
- 고정 입력 경로: `data/raw/olist/`

### 5.2 고객 Canonicalization

Olist `customer_id`는 주문별 식별자이므로 Project 고객 Entity로 사용하지 않는다.

1. `olist customer_id → customer_unique_id` Mapping을 만든다.
2. 4.3 규칙으로 Project `customer_id`를 만든다.
3. 주문의 고객 ID를 Project ID로 치환한다.
4. 한 고객의 주소가 여러 개면 가장 최근 `purchase_at` 주문 주소를 선택한다.
5. 동률은 `order_id DESC`, 다시 같으면 Olist `customer_id DESC`로 결정한다.
6. `customer_unique_id` 원본은 보존하고 `UNIQUE`를 적용한다.

Membership과 SCD2 Business Key는 Project `customer_id`로 통일한다.

### 5.3 Table Mapping

| Table            | 규칙                                                                                             |
| ---------------- | ------------------------------------------------------------------------------------------------ |
| customers        | Canonical ID, 최신 주소, 최초 구매를 `created_at`, `--seeded-at`을 `updated_at`으로 사용         |
| orders           | 고객 ID 치환, Olist Event Timestamp 보존, 존재하는 상태 Timestamp 최댓값을 `updated_at`으로 사용 |
| order_items      | 원본 PK/Product/Seller/금액 보존, Order 구매시각을 `created_at`으로 사용                         |
| products/sellers | 원본 ID/속성 보존, 자체 시간이 없어 `created_at = updated_at = --seeded-at`                      |
| payments         | 원본 Sequence/Type/Value 보존, Order 구매/갱신시각을 생성/갱신시각으로 사용                      |

Order 상태 Mapping:

| Olist                 | Canonical |
| --------------------- | --------- |
| created               | CREATED   |
| approved, processing  | APPROVED  |
| invoiced, shipped     | SHIPPED   |
| delivered             | DELIVERED |
| canceled, unavailable | CANCELED  |

알 수 없는 상태는 자동 보정하지 않고 Seed를 실패시킨다. Payment는 CANCELED Order면 `FAILED`, 그 외 Olist Seed는 `COMPLETED`로 둔다. `PENDING`, `REFUNDED`는 Generator가 만든다.

Membership은 고객별 `DELIVERED` 주문 수로 계산한다.

```text
BRONZE 0~4 / SILVER 5~14 / GOLD 15+
```

### 5.4 Seed CLI와 재실행

```bash
uv run python -m src.seed \
  --input-dir data/raw/olist \
  --seeded-at 2026-09-03T00:00:00Z
```

`--seeded-at`은 필수다. Loader는 Header/파일 검증 → Staging Load → PK/FK/상태/Count 검증 → 한 Transaction의 `INSERT ... ON CONFLICT DO UPDATE` 순으로 실행한다.

- 입력 계산값만 갱신하고 실행 현재시각으로 `updated_at`을 바꾸지 않는다.
- Target에만 있는 Synthetic Row는 삭제하지 않는다.
- 동일 Raw Checksum과 `seeded_at` 재실행 후 Count와 Content Hash가 같아야 한다.
- 전체 삭제는 Loader가 아니라 명시적인 `scripts/reset.sh` 책임이다.
- Seed Loader는 Synthetic Generator가 한 번이라도 성공한 환경에서는 실행을 거부한다. Seed 재실행은 Generator 시작 전 Bootstrap 검증에만 허용하며, 이후 재초기화는 `scripts/reset.sh` 후 수행한다.

`pipeline_metadata.seed_runs`는 `raw_checksum`, `seeded_at`, Table별 Row Count, Content Hash, Status를 기록한다. `generator_runs`의 성공 Row가 존재하면 Seed Guard가 동작한다.

---

## 6. PostgreSQL Source 계약

한 PostgreSQL Container에 `commerce_source`, `airflow_metadata`, `pipeline_metadata` Database와 역할별 계정을 둔다.

### 6.1 Table

| Table / Grain                | 주요 Column과 Constraint                                                                                                   |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| customers / 고객 1행         | `customer_id UUID PK`, `customer_unique_id VARCHAR(64) UNIQUE`, 주소, `membership_level CHECK`, `created_at`, `updated_at` |
| products / 상품 1행          | `product_id VARCHAR(64) PK`, category, non-negative dimensions, `created_at`, `updated_at`                                 |
| sellers / 판매자 1행         | `seller_id VARCHAR(64) PK`, 주소, `created_at`, `updated_at`                                                               |
| orders / 주문 1행            | `order_id VARCHAR(64) PK`, `customer_id FK`, Canonical status, Event Timestamp, `created_at`, `updated_at`                 |
| order_items / Line 1행       | PK `(order_id, order_item_id)`, Product/Seller FK, non-negative `NUMERIC(14,2)` price/freight, `created_at`                |
| payments / 결제 Sequence 1행 | PK `(order_id, payment_sequence)`, Order FK, type/installments/value/status, `created_at`, `updated_at`                    |

모든 운영 Timestamp는 `TIMESTAMPTZ NOT NULL`이며 선택 Event Timestamp만 NULL을 허용한다. `updated_at >= created_at`, 양수 Sequence, Canonical Status를 `CHECK`한다. Event 시간 순서는 Canceled 사례를 허용하기 위해 Source CHECK가 아닌 Generator와 Warehouse Test에서 검증한다.

Physical Column 계약:

#### customers

| Column             | Type         | Constraint                     |
| ------------------ | ------------ | ------------------------------ |
| customer_id        | UUID         | PK                             |
| customer_unique_id | VARCHAR(64)  | NOT NULL, UNIQUE               |
| zip_code_prefix    | VARCHAR(16)  |                                |
| city               | VARCHAR(128) |                                |
| state              | CHAR(2)      |                                |
| membership_level   | VARCHAR(16)  | NOT NULL, `BRONZE/SILVER/GOLD` |
| created_at         | TIMESTAMPTZ  | NOT NULL                       |
| updated_at         | TIMESTAMPTZ  | NOT NULL, `>= created_at`      |

#### products

| Column        | Type         | Constraint                |
| ------------- | ------------ | ------------------------- |
| product_id    | VARCHAR(64)  | PK                        |
| category_name | VARCHAR(256) |                           |
| weight_g      | INTEGER      | NULL 또는 `>= 0`          |
| length_cm     | INTEGER      | NULL 또는 `>= 0`          |
| height_cm     | INTEGER      | NULL 또는 `>= 0`          |
| width_cm      | INTEGER      | NULL 또는 `>= 0`          |
| created_at    | TIMESTAMPTZ  | NOT NULL                  |
| updated_at    | TIMESTAMPTZ  | NOT NULL, `>= created_at` |

#### sellers

| Column          | Type         | Constraint                |
| --------------- | ------------ | ------------------------- |
| seller_id       | VARCHAR(64)  | PK                        |
| zip_code_prefix | VARCHAR(16)  |                           |
| city            | VARCHAR(128) |                           |
| state           | CHAR(2)      |                           |
| created_at      | TIMESTAMPTZ  | NOT NULL                  |
| updated_at      | TIMESTAMPTZ  | NOT NULL, `>= created_at` |

#### orders

| Column                | Type        | Constraint                |
| --------------------- | ----------- | ------------------------- |
| order_id              | VARCHAR(64) | PK                        |
| customer_id           | UUID        | NOT NULL, FK customers    |
| order_status          | VARCHAR(16) | NOT NULL, Canonical CHECK |
| purchase_at           | TIMESTAMPTZ | NOT NULL                  |
| approved_at           | TIMESTAMPTZ | NULL 가능                 |
| carrier_at            | TIMESTAMPTZ | NULL 가능                 |
| delivered_at          | TIMESTAMPTZ | NULL 가능                 |
| estimated_delivery_at | TIMESTAMPTZ | NULL 가능                 |
| created_at            | TIMESTAMPTZ | NOT NULL                  |
| updated_at            | TIMESTAMPTZ | NOT NULL, `>= created_at` |

#### order_items

| Column            | Type          | Constraint            |
| ----------------- | ------------- | --------------------- |
| order_id          | VARCHAR(64)   | PK, FK orders         |
| order_item_id     | INTEGER       | PK, `> 0`             |
| product_id        | VARCHAR(64)   | NOT NULL, FK products |
| seller_id         | VARCHAR(64)   | NOT NULL, FK sellers  |
| shipping_limit_at | TIMESTAMPTZ   | NULL 가능             |
| price             | NUMERIC(14,2) | NOT NULL, `>= 0`      |
| freight_value     | NUMERIC(14,2) | NOT NULL, `>= 0`      |
| created_at        | TIMESTAMPTZ   | NOT NULL              |

#### payments

| Column           | Type          | Constraint                |
| ---------------- | ------------- | ------------------------- |
| order_id         | VARCHAR(64)   | PK, FK orders             |
| payment_sequence | INTEGER       | PK, `> 0`                 |
| payment_type     | VARCHAR(32)   | NOT NULL                  |
| installments     | INTEGER       | NULL 또는 `>= 0`          |
| payment_value    | NUMERIC(14,2) | NOT NULL, `>= 0`          |
| payment_status   | VARCHAR(16)   | NOT NULL, Canonical CHECK |
| created_at       | TIMESTAMPTZ   | NOT NULL                  |
| updated_at       | TIMESTAMPTZ   | NOT NULL, `>= created_at` |

### 6.2 증분 Index

```text
customers   (updated_at, customer_id)
products    (updated_at, product_id)
sellers     (updated_at, seller_id)
orders      (updated_at, order_id)
order_items (created_at, order_id, order_item_id)
payments    (updated_at, order_id, payment_sequence)
```

Index와 Extract 정렬 순서는 같아야 한다.

### 6.3 Transaction

- Order/Item/Payment 생성은 하나의 Transaction이다.
- 상태와 관련 Timestamp 변경은 하나의 Transaction이다.
- 완료 주문 수 재계산과 Membership 갱신도 같은 Transaction에 포함한다.
- `updated_at`은 Logical Event Time이며 Wall Clock을 직접 쓰지 않는다.
- Source `updated_at` 역행은 금지한다.

---

## 7. Generator와 Anomaly

### 7.1 상태 전이

```text
CREATED → APPROVED → SHIPPED → DELIVERED
   └───────────────→ CANCELED
           └──────→ CANCELED

PENDING → COMPLETED → REFUNDED
    └───→ FAILED
```

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

### 7.3 Anomaly

- Service-level: Late Order, Delayed Payment, 과거 Event의 늦은 Update, Membership 변경
- Pipeline-level: Duplicate, NULL Key, Broken FK, Invalid Status, 음수 금액

Pipeline Corruption은 Extract 후 Validation 직전 복제본에 결정적으로 주입하며 Source에는 쓰지 않는다.

### 7.4 SCD2 관측 한계

Source는 현재 상태만 보관하므로 성공 수집 사이의 중간 변경은 복원할 수 없다. V1 Generator는 SCD2 추적 속성에 대해 고객당 수집 구간 내 최대 1회만 변경한다. 이 제약과 CDC 확장 필요성을 ADR에 남긴다.

---

## 8. 증분 추출과 동시성

### 8.1 Cursor

| Table       | Cursor Tuple                               |
| ----------- | ------------------------------------------ |
| customers   | `(updated_at, customer_id)`                |
| products    | `(updated_at, product_id)`                 |
| sellers     | `(updated_at, seller_id)`                  |
| orders      | `(updated_at, order_id)`                   |
| order_items | `(created_at, order_id, order_item_id)`    |
| payments    | `(updated_at, order_id, payment_sequence)` |

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

검증 순서는 Schema/필수 Column → Type → Key NULL → Batch 내 중복 → Canonical Status → Numeric Range → Broken Reference → Cursor 범위다.

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
extract_validate_load[customers, products, sellers, orders, order_items, payments]
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

Mutable Current Model의 우선순위:

```sql
row_number() over (
  partition by business_key
  order by updated_at desc, _ingested_at desc, _batch_id desc
) = 1
```

Customer Observation은 모든 관측 Version을 유지하되 동일 `customer_id + updated_at + tracked_attribute_hash`를 Deduplicate한다.

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

- Business Key: `customer_id`
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
- order_items/payments: 연결된 주문 구매일
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
| FR-01 | Canonical Olist Seed              | P0       |
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

---

## 22. Acceptance Criteria

| ID    | 시나리오                   | 합격 조건                                                         |
| ----- | -------------------------- | ----------------------------------------------------------------- |
| AC-01 | E2E                        | 고정 주문이 Source→Bronze Catalog→Fact에 존재하고 Count 추적 가능 |
| AC-02 | Incremental                | Cursor 조건과 실제 신규·변경 Key Set 일치                         |
| AC-03 | 동일 Batch 3회             | Object 수/Key Count/Mart Hash 동일, 중복 0                        |
| AC-04 | Upload 전후 실패           | 실패 중 Watermark 유지, 성공 뒤 Upper로 전진                      |
| AC-05 | 동일 Timestamp가 Page 초과 | Page/Batch 경계 누락·중복 0                                       |
| AC-06 | 동시 Extract               | 하나만 Lease, 다른 Run은 Object 없이 Conflict                     |
| AC-07 | Backfill Replay            | 같은 범위 Full Refresh와 Key별 값/Hash 동일                       |
| AC-08 | 5종 Corruption             | 기대 Reject 일치, Source 무오염, Record 추적 가능                 |
| AC-09 | BRONZE→SILVER→GOLD         | 3 Version, 구간 중첩 0, Current 1                                 |
| AC-10 | 구간별 주문                | 발생 시점 Customer Key 참조                                       |
| AC-11 | 3일 전 Late Order          | 현재 수집, 과거 Mart 갱신, Full과 동일                            |
| AC-12 | Referential Integrity      | FK/Unique 통과, 정상 Unknown 0                                    |
| AC-13 | Observability              | 성공/빈/실패/재실행을 SQL 한 번으로 조회                          |
| AC-14 | Seed 2회                   | Count/Content Hash 동일, PK/FK 위반 0                             |
| AC-15 | Generator 재현             | 동일 Snapshot/Input Key Set/Hash 동일                             |
| AC-16 | 새 Clone                   | Version/Health/Seed/E2E/dbt Test 성공                             |
| AC-17 | Benchmark                  | Raw 5회/Median/Hash/환경 Metadata 존재                            |

절대 처리시간 목표 대신 Baseline과 개선 전후를 비교한다.

---

## 23. Repository Structure

```text
commerce-data-platform/
├── README.md / PRD_v1.2.md / AGENTS.md
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
| 1 Source      | PostgreSQL, DDL/Index, Canonical Seed       | 고객 Mapping, PK/FK, Seed 2회 Hash   |
| 2 Generator   | 결정 ID, 상태, Membership, Late             | AC-15, 허용 전이, 변경 횟수 제약     |
| 3 Ingestion   | Cursor, Lease, Metadata, Bronze, Quarantine | AC-02~06, AC-08                      |
| 4 Airflow     | DAG, Dynamic Task, Retry, XCom              | E2E, Retry, 부분 성공 재사용         |
| 5 Modeling    | Catalog, Staging, Mart, SCD2                | Grain, AC-09/10/12                   |
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
008-canonicalize-olist-customer-identity.md
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

Architecture, 고객 Canonicalization, Cursor/Index, Watermark Failure, 동일 Batch Hash, Manifest/Metadata, Quarantine, Fact Grain, SCD2 Timeline, Late Arrival Diff, Backfill-Full Hash, Scale Benchmark, Failure Runbook, 새 Clone 기록을 최종 증적으로 남긴다.

---

## 28. 최종 확정 문장

> **Python 3.12 + WSL2 Ubuntu + Docker 환경에서 Olist를 Canonical PostgreSQL OLTP Source로 변환하고 결정적 Synthetic Generator로 변경을 만든다. Airflow는 Table별 Composite Cursor의 고정 범위를 추출하며 Lease, Batch Metadata, Manifest와 Checksum으로 동시성·실패·재실행을 통제한다. 검증 데이터는 SeaweedFS의 불변 Parquet Bronze로 Commit하고, dbt + DuckDB는 Metadata Catalog의 Object만 읽어 Star Schema, Incremental Fact, SCD Type 2와 Temporal Join을 구축한다. Data Quality, Quarantine, Backfill, Late Arrival, Idempotency와 Failure Recovery를 결정적 Test와 Result Hash로 검증하고 Version Pinning과 실행 Metadata로 재현성을 확보한다.**
