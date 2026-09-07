# PRD: Commerce Analytics Data Platform

> Version: 1.1  
> Status: Baseline Approved / Version Baseline Reviewed 2026-09-03  
> Development Environment: WSL2 Ubuntu / Local  
> Primary Goal: 데이터엔지니어 취업 포트폴리오용 Batch DW·Data Mart 프로젝트

### Version 1.1 변경 요약

Version 1.1은 기능 범위를 확장하기보다 **2026-09-03 기준 기술 버전, 배포 방식, 재현성**을 다시 검토해 Baseline을 정리한 버전이다.

주요 변경:

- Python `3.12` 확정
- PostgreSQL `18.6`으로 Baseline 갱신
- Apache Airflow `3.3.1` + Python 3.12 이미지 사용
- DuckDB `1.5.5`
- dbt-core `1.12.3`
- dbt-duckdb `1.11.0`
- Olist 다운로드를 Kaggle CLI가 아닌 `kagglehub` 기반 Python Script로 통일
- MinIO Community Edition의 현재 배포/유지보수 정책을 재검토하여 Local S3-compatible Object Storage를 **SeaweedFS 4.45**로 변경
- S3-compatible Client는 특정 Storage SDK 대신 `boto3`를 사용
- Docker Image와 직접 Dependency에 Version Pinning Policy 추가
- `latest` Docker Tag 사용 금지
- BI는 Metabase `0.63.16.1`을 Phase 9 Baseline으로 두되 실제 구현 직전에 재검증
- Phase 0 `Project Bootstrap` 추가
- 기존 Phase 1의 Repository/WSL/기본 파일 초기화 책임을 Phase 0으로 이동
- Phase 1을 PostgreSQL Source Environment + Olist Seed 적재 단계로 재정의

Object Storage 변경은 데이터 레이크의 논리 구조를 바꾸지 않는다.

```text
기존
MinIO
  ↓
S3-compatible Object Storage

v1.1
SeaweedFS
  ↓
S3-compatible Object Storage
```

Bucket, Object Key, Parquet, Batch Manifest, DuckDB의 S3 접근 방식은 동일한 추상화를 유지한다.

---

## 1. 프로젝트 개요

### 1.1 프로젝트명

**Commerce Analytics Data Platform**

### 1.2 한 줄 정의

실제 전자상거래 공개 데이터를 Seed로 활용해 서비스형 OLTP Source를 구성하고, 지속적으로 변하는 Synthetic 데이터를 생성하여 **증분 수집 → Object Storage 적재 → DW 변환·모델링 → 품질 검증 → 재처리·복구**까지 구현하는 로컬 데이터 플랫폼 프로젝트.

### 1.3 핵심 포트폴리오 메시지

> 정적 CSV를 한 번 적재하는 프로젝트가 아니라, 변경·지연·중복·실패가 발생하는 데이터 환경에서 배치 파이프라인의 신뢰성과 분석 모델의 정합성을 설계하고 검증한다.

### 1.4 핵심 역량

- Batch Data Pipeline
- Incremental Load
- Watermark / Checkpoint
- Idempotency
- Backfill / Re-run
- Late Arriving Data
- Dimensional Modeling
- Star Schema
- SCD Type 2
- Data Quality
- Pipeline Metadata / Observability
- 성능 측정 및 개선

---

## 2. 프로젝트 범위

### 2.1 V1 포함 범위

```text
Olist Dataset
      │
      │ Seed Transformation
      ▼
PostgreSQL OLTP Source
      ▲
      │ Synthetic Data Generator
      │
      ▼
Apache Airflow
      │
      │ Incremental Extract
      ▼
SeaweedFS / Parquet
      │
      ▼
dbt + DuckDB
      │
      ├── staging
      ├── intermediate
      └── marts
             │
             ├── Dimensions
             └── Facts
                    │
                    ▼
                  BI
```

### 2.2 V1 비목표

첫 버전에서는 다음을 구현하지 않는다.

- Kafka 기반 Streaming
- Spark
- Debezium CDC
- Kubernetes
- Terraform
- AWS S3 상시 운영
- Snowflake 상시 운영
- Databricks

이 프로젝트는 기존 Streaming 프로젝트와 역할을 분리하고 **Batch + DW + Modeling + Data Quality + Reliability**에 집중한다.

### 2.3 향후 확장

V1 완료 후 선택적으로 진행한다.

```text
SeaweedFS   → AWS S3
DuckDB  → Snowflake

PostgreSQL
    ↓
Debezium
    ↓
Kafka
    ↓
Warehouse
```

---

## 3. 개발 환경

### 3.1 Host

```text
Windows
└── WSL2
    └── Ubuntu
```

| 항목              |                 설정 |
| ----------------- | -------------------: |
| Host RAM          |                 16GB |
| WSL CPU           | 8 Logical Processors |
| WSL RAM           |               약 8GB |
| WSL Swap          |                  2GB |
| 개발 위치         | WSL Linux Filesystem |
| Container Runtime |               Docker |
| Container 관리    |       Docker Compose |

프로젝트 경로:

```text
~/projects/commerce-data-platform
```

`/mnt/c/...`가 아닌 WSL 내부 파일 시스템을 사용한다.

### 3.2 Docker Compose 파일명

프로젝트 루트의 Compose 파일은 다음으로 통일한다.

```text
compose.yaml
```

### 3.3 Dockerfile 배치 원칙

Docker 관련 파일을 별도의 `docker/` 디렉터리에 무조건 모으지 않는다.

- PostgreSQL: `postgres:18.6` 사용
- SeaweedFS: `chrislusf/seaweedfs:4.45` 사용
- Metabase: Phase 9에서 검증한 공식 Version Tag 사용
- Airflow: `apache/airflow:3.3.1-python3.12` 기반
- 커스텀 Build가 실제 필요한 경우에만 해당 서비스 디렉터리에 `Dockerfile` 추가

예:

```text
airflow/
├── Dockerfile   # 필요한 경우에만 생성
└── dags/
```

### 3.4 리소스 조정 기준

초기 설정을 유지하고 다음 상황이 실제 관측될 때만 조정한다.

- 지속적인 Swap 사용
- OOM 발생
- DuckDB 대용량 Query 실패
- Airflow + BI 동시 실행 시 메모리 부족
- Container 반복 종료

---

## 4. 기술 스택 및 Version Baseline

### 4.1 Platform / Infrastructure

| 영역                 | 기술                      | v1.1 Baseline                  |
| -------------------- | ------------------------- | ------------------------------ |
| OS                   | WSL2 Ubuntu               | WSL 내부 Linux Filesystem 사용 |
| Language             | Python                    | **3.12**                       |
| Query                | SQL                       | -                              |
| Python Dependency    | uv                        | **0.12.9** 기준                |
| Source / Metadata DB | PostgreSQL                | **18.6**                       |
| Workflow             | Apache Airflow            | **3.3.1**                      |
| Airflow Python       | CPython                   | **3.12**                       |
| Object Storage       | SeaweedFS (S3-compatible) | **4.45**                       |
| Storage Format       | Apache Parquet            | PyArrow 25.0.1                 |
| Analytical Engine    | DuckDB                    | **1.5.5**                      |
| Transformation       | dbt-core                  | **1.12.3**                     |
| dbt Adapter          | dbt-duckdb                | **1.11.0**                     |
| Data Modeling        | Dimensional Modeling      | Star Schema / SCD2             |
| BI                   | Metabase                  | **0.63.16.1** Phase 9 Baseline |
| Container            | Docker                    | Host 설치 버전 기록            |
| Infrastructure       | Docker Compose            | Compose Specification          |
| Version Control      | Git / GitHub              | -                              |

### 4.2 Python Runtime Dependency

초기 직접 Dependency Baseline:

```text
Python              3.12
duckdb              1.5.5
dbt-core            1.12.3
dbt-duckdb          1.11.0
pandas              3.0.5
pyarrow             25.0.1
psycopg[binary]     3.3.5
kagglehub           1.0.2
boto3               1.43.87
```

개발 Dependency:

```text
pytest              9.1.1
ruff                0.16.5
```

### 4.3 Docker Image Baseline

```text
postgres:18.6
apache/airflow:3.3.1-python3.12
chrislusf/seaweedfs:4.45
metabase/metabase:v0.63.16.1
```

Docker Image에는 `latest` Tag를 사용하지 않는다.

Phase 9의 Metabase는 프로젝트 구현 시점과 차이가 클 수 있으므로 **Phase 9 착수 직전에 최신 보안 Patch를 다시 확인**한다. 버전을 변경하면 ADR 또는 변경 기록에 남긴다.

### 4.4 Python Version Policy

프로젝트 Python Minor는 `3.12`로 고정한다.

```toml
[project]
requires-python = ">=3.12,<3.13"
```

Repository에는 다음 파일을 둔다.

```text
.python-version
```

내용:

```text
3.12
```

2026-09-03 기준 Python 3.12 계열의 최신 Security Release는 3.12.14이지만, 프로젝트 계약은 Patch가 아닌 **3.12 Minor Line**으로 고정한다.

실제 개발 환경에서 사용한 정확한 Patch는 README의 Environment 또는 Benchmark Metadata에 기록한다.

### 4.5 Dependency Pinning Policy

직접 Dependency는 `pyproject.toml`에서 Baseline Version을 명시하고 전체 Transitive Dependency는 `uv.lock`으로 재현한다.

원칙:

```text
pyproject.toml
     ↓
직접 Dependency Baseline

uv.lock
     ↓
전체 Dependency Resolution
```

Version Upgrade는 자동으로 수행하지 않는다.

업데이트 조건:

1. 보안 Patch 필요
2. 현재 Version의 Bug로 프로젝트 구현이 차단됨
3. 필요한 기능이 신규 Version에만 존재
4. 전체 테스트와 E2E Pipeline이 통과함

Docker Image 역시 Version Tag를 고정한다. 최종 Portfolio Release 또는 Benchmark 결과를 확정할 때는 필요하면 Image Digest까지 기록한다.

### 4.6 dbt / DuckDB Compatibility Policy

`dbt-duckdb 1.11.0`은 dbt-core 1.8 이상과 DuckDB 1.0 이상을 지원 범위로 두므로 다음 조합을 V1.1 Baseline으로 사용한다.

```text
dbt-core     1.12.3
dbt-duckdb   1.11.0
DuckDB       1.5.5
```

단, `uv lock` 후 다음 Smoke Test를 통과한 조합을 최종 Lock으로 간주한다.

```text
dbt debug
dbt parse
dbt build
dbt test
DuckDB S3-compatible Parquet read
incremental merge model
```

### 4.7 Airflow Dependency Policy

Airflow Base Image:

```text
apache/airflow:3.3.1-python3.12
```

Airflow 공식 Image에 프로젝트 실행 Dependency를 추가해야 하는 경우 `airflow/Dockerfile`에서 확장한다.

Airflow 자체와 Provider Dependency를 추가/변경할 때는 Airflow 3.3.1 Constraints와의 호환성을 확인한다.

프로젝트 로컬 Python 환경의 `uv.lock`을 Airflow 이미지에 무조건 그대로 적용하지 않고, Airflow Runtime과 충돌 가능성을 검증한다.

---

## 5. 데이터 전략

### 5.1 Seed Dataset

초기 Seed는 **Olist Brazilian E-Commerce Dataset**을 사용한다.

주요 원본:

- customers
- orders
- order_items
- products
- payments
- sellers

후순위:

- reviews
- geolocation

### 5.2 Olist 사용 원칙

Olist 원본 스키마를 PostgreSQL에 그대로 복제하지 않는다.

```text
Olist CSV
    ↓
Seed Transformation
    ↓
Project OLTP Source Schema
```

Olist의 값과 관계는 최대한 보존하되 다음 기능을 지원하기 위해 프로젝트용 Source Schema를 구성한다.

- 증분 추출
- 상태 변경
- Synthetic 데이터 추가
- SCD2
- Late Arrival
- Pipeline 재실행

프로젝트 확장 필드는 문서에서 Olist 원본 필드와 명확히 구분한다.

### 5.3 프로젝트 확장 필드

대표적인 확장 필드:

- `created_at`
- `updated_at`
- `membership_level`
- `payment_status`

`membership_level`과 `payment_status`는 Olist 원본 필드가 아니라 Synthetic Service Environment를 위해 추가한다.

### 5.4 Raw Dataset 다운로드

Dataset Identifier:

```text
olistbr/brazilian-ecommerce
```

다운로드는 Kaggle CLI shell command가 아니라 Python 기반 `kagglehub`를 사용한다.

파일:

```text
scripts/download_dataset.py
```

기본 구현:

```python
from pathlib import Path

import kagglehub


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "data" / "raw" / "olist"
    output_dir.mkdir(parents=True, exist_ok=True)

    path = kagglehub.dataset_download(
        "olistbr/brazilian-ecommerce",
        output_dir=str(output_dir),
    )

    print(f"Dataset downloaded to: {path}")


if __name__ == "__main__":
    main()
```

실행:

```bash
uv run python scripts/download_dataset.py
```

Raw Dataset은 Git에 Commit하지 않는다.

```text
data/raw/olist/
```

Seed Loader는 KaggleHub Cache Location에 직접 의존하지 않고 항상 프로젝트 내부 `data/raw/olist/`를 입력으로 사용한다.

---

## 6. PostgreSQL 구성

### 6.1 Container 정책

WSL 리소스를 고려하여 PostgreSQL Container는 V1에서 **1개**만 사용한다.

단, 데이터베이스는 역할별로 논리 분리한다.

```text
PostgreSQL Container
│
├── commerce_source
│   └── OLTP Source Data
│
├── airflow_metadata
│   └── Airflow Metadata
│
└── pipeline_metadata
    └── Watermark / Pipeline Run Metadata
```

세 Database의 Schema와 계정은 가능한 한 역할별로 분리한다.

---

## 7. Source Data Contract

### 7.1 기본 원칙

Source DB는 분석용 Star Schema가 아닌 **서비스형 OLTP 모델**로 유지한다.

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

삭제 이벤트와 Tombstone 처리는 V1 범위에서 제외한다.

---

### 7.2 customers

Grain:

> 고객 레코드당 1행

| Column             | Type      | Constraint | 설명                          |
| ------------------ | --------- | ---------- | ----------------------------- |
| customer_id        | VARCHAR   | PK         | 주문에서 사용하는 고객 식별자 |
| customer_unique_id | VARCHAR   | NOT NULL   | 동일 고객 식별용 Natural Key  |
| zip_code_prefix    | VARCHAR   |            | 우편번호 Prefix               |
| city               | VARCHAR   |            | 도시                          |
| state              | VARCHAR   |            | 주                            |
| membership_level   | VARCHAR   | NOT NULL   | BRONZE / SILVER / GOLD        |
| created_at         | TIMESTAMP | NOT NULL   | Source 생성 시각              |
| updated_at         | TIMESTAMP | NOT NULL   | 마지막 변경 시각              |

#### Membership Rule

Synthetic 환경의 회원 등급은 **완료 주문 수**를 기준으로 변경한다.

```text
BRONZE : 완료 주문 0~4
SILVER : 완료 주문 5~14
GOLD   : 완료 주문 15+
```

등급 변경은 Source의 `membership_level`, `updated_at`을 갱신하고 SCD2 실험 대상으로 사용한다.

---

### 7.3 products

Grain:

> 상품당 1행

| Column        | Type      | Constraint | 설명             |
| ------------- | --------- | ---------- | ---------------- |
| product_id    | VARCHAR   | PK         | 상품 식별자      |
| category_name | VARCHAR   |            | 상품 카테고리    |
| weight_g      | INTEGER   |            | 무게             |
| length_cm     | INTEGER   |            | 길이             |
| height_cm     | INTEGER   |            | 높이             |
| width_cm      | INTEGER   |            | 너비             |
| created_at    | TIMESTAMP | NOT NULL   | 생성 시각        |
| updated_at    | TIMESTAMP | NOT NULL   | 마지막 변경 시각 |

---

### 7.4 sellers

Grain:

> 판매자당 1행

| Column          | Type      | Constraint | 설명             |
| --------------- | --------- | ---------- | ---------------- |
| seller_id       | VARCHAR   | PK         | 판매자 식별자    |
| zip_code_prefix | VARCHAR   |            | 우편번호 Prefix  |
| city            | VARCHAR   |            | 도시             |
| state           | VARCHAR   |            | 주               |
| created_at      | TIMESTAMP | NOT NULL   | 생성 시각        |
| updated_at      | TIMESTAMP | NOT NULL   | 마지막 변경 시각 |

---

### 7.5 orders

Grain:

> 주문당 1행

| Column                | Type      | Constraint | 설명                   |
| --------------------- | --------- | ---------- | ---------------------- |
| order_id              | VARCHAR   | PK         | 주문 식별자            |
| customer_id           | VARCHAR   | FK         | customers.customer_id  |
| order_status          | VARCHAR   | NOT NULL   | Canonical Order Status |
| purchase_at           | TIMESTAMP | NOT NULL   | 주문 발생 시각         |
| approved_at           | TIMESTAMP |            | 승인 시각              |
| carrier_at            | TIMESTAMP |            | 배송사 전달 시각       |
| delivered_at          | TIMESTAMP |            | 배송 완료 시각         |
| estimated_delivery_at | TIMESTAMP |            | 예상 배송일            |
| created_at            | TIMESTAMP | NOT NULL   | Source 생성 시각       |
| updated_at            | TIMESTAMP | NOT NULL   | 마지막 변경 시각       |

---

### 7.6 order_items

Grain:

> 주문 내 상품 라인당 1행

Composite PK:

```text
(order_id, order_item_id)
```

| Column            | Type      | Constraint | 설명                |
| ----------------- | --------- | ---------- | ------------------- |
| order_id          | VARCHAR   | PK/FK      | orders.order_id     |
| order_item_id     | INTEGER   | PK         | 주문 내부 상품 순번 |
| product_id        | VARCHAR   | FK         | products.product_id |
| seller_id         | VARCHAR   | FK         | sellers.seller_id   |
| shipping_limit_at | TIMESTAMP |            | 배송 제한 시각      |
| price             | DECIMAL   | NOT NULL   | 상품 가격           |
| freight_value     | DECIMAL   | NOT NULL   | 배송비              |
| created_at        | TIMESTAMP | NOT NULL   | Source 생성 시각    |

V1에서는 `order_items`를 append-only 성격의 테이블로 취급한다.

---

### 7.7 payments

Grain:

> 주문별 결제 Sequence당 1행

Composite PK:

```text
(order_id, payment_sequence)
```

| Column           | Type      | Constraint | 설명                     |
| ---------------- | --------- | ---------- | ------------------------ |
| order_id         | VARCHAR   | PK/FK      | orders.order_id          |
| payment_sequence | INTEGER   | PK         | 주문 내 결제 순번        |
| payment_type     | VARCHAR   | NOT NULL   | 결제 방식                |
| installments     | INTEGER   |            | 할부 개월                |
| payment_value    | DECIMAL   | NOT NULL   | 결제 금액                |
| payment_status   | VARCHAR   | NOT NULL   | Canonical Payment Status |
| created_at       | TIMESTAMP | NOT NULL   | 생성 시각                |
| updated_at       | TIMESTAMP | NOT NULL   | 마지막 변경 시각         |

---

## 8. 상태 모델

### 8.1 Order State Machine

Canonical 상태:

```text
CREATED
APPROVED
SHIPPED
DELIVERED
CANCELED
```

허용 전이:

```text
CREATED
 ├── APPROVED
 └── CANCELED

APPROVED
 ├── SHIPPED
 └── CANCELED

SHIPPED
 └── DELIVERED
```

다음과 같은 역방향 전이는 정상 서비스 이벤트로 생성하지 않는다.

```text
DELIVERED → CREATED
SHIPPED   → APPROVED
```

### 8.2 Payment State Machine

Canonical 상태:

```text
PENDING
COMPLETED
FAILED
REFUNDED
```

허용 전이:

```text
PENDING
 ├── COMPLETED
 └── FAILED

COMPLETED
 └── REFUNDED
```

---

## 9. Synthetic Data Generator

### 9.1 목적

정적 Olist Seed만으로 재현하기 어려운 다음 상황을 생성한다.

- 신규 고객
- 신규 주문
- 신규 주문 상품
- 신규 결제
- 주문 상태 변경
- 결제 상태 변경
- 고객 등급 변경
- Late Arrival
- 지연 결제

### 9.2 결정적 실행

성능 테스트와 장애 시나리오 재현을 위해 Generator는 동일 입력에 대해 동일한 결과를 생성할 수 있어야 한다.

필수 입력:

```text
random_seed
logical_date
order_count
anomaly_profile
```

예:

```bash
python -m src.generator \
  --seed 42 \
  --logical-date 2026-09-03 \
  --orders 1000 \
  --anomaly-profile default
```

### 9.3 데이터 생성 규모

초기 개발 예시:

```text
신규 고객       10~50
신규 주문      100~500
Order Item    주문당 분포 기반
Payment       주문당 분포 기반
```

Benchmark 단계에서는 **Orders 수를 Scale 기준**으로 사용한다.

```text
S : 100K Orders
M : 1M Orders
L : 5M Orders
```

필요 시 10M Orders까지 확장한다.

---

## 10. Anomaly Injection

### 10.1 원칙

서비스형 OLTP Source의 기본 무결성을 유지하기 위해 anomaly를 두 종류로 나눈다.

### 10.2 Service-level Scenario

Source DB에 정상적으로 존재할 수 있지만 파이프라인 처리가 어려운 상황:

- Late Arriving Order
- Delayed Payment
- 오래된 Business Event의 뒤늦은 Update
- Customer Membership 변경

### 10.3 Pipeline-level Corruption Injection

DB PK/FK/NOT NULL 제약과 충돌하는 오류는 Source DB 자체를 망가뜨리지 않고 **Extract 이후 Validation 이전**에 테스트 목적으로 주입한다.

예:

- Duplicate Row
- NULL Business Key
- Broken Product FK
- Invalid Value

이를 통해 서비스 DB의 무결성을 유지하면서 Data Quality와 Quarantine을 검증한다.

---

## 11. 증분 추출 전략

### 11.1 원칙

모든 테이블에 동일한 Incremental 전략을 강제하지 않는다.

| Source Table | 변경 특성   | Incremental Key                            |
| ------------ | ----------- | ------------------------------------------ |
| customers    | Mutable     | `(updated_at, customer_id)`                |
| products     | Mutable     | `(updated_at, product_id)`                 |
| sellers      | Mutable     | `(updated_at, seller_id)`                  |
| orders       | Mutable     | `(updated_at, order_id)`                   |
| order_items  | Append-only | `(created_at, order_id, order_item_id)`    |
| payments     | Mutable     | `(updated_at, order_id, payment_sequence)` |

### 11.2 Composite Watermark

Mutable Table은 timestamp 하나만 사용하지 않고 Timestamp + Business Key를 함께 사용한다.

개념 예:

```sql
WHERE updated_at > :last_updated_at
   OR (
        updated_at = :last_updated_at
        AND primary_key > :last_primary_key
   )
```

Composite PK 테이블은 안정적인 정렬 순서로 Key 전체를 Tie-breaker로 사용한다.

### 11.3 Watermark 저장 위치

Watermark는 Airflow 내부 상태에만 의존하지 않는다.

`pipeline_metadata` Database에 저장한다.

예:

```text
watermarks

pipeline_name
source_table
watermark_timestamp
watermark_key
updated_at
```

### 11.4 Watermark Commit Rule

Watermark는 Extract 시작 시 갱신하지 않는다.

```text
Extract
   ↓
Ingestion Validation
   ↓
Parquet 생성
   ↓
SeaweedFS 적재
   ↓
Object/Row 검증
   ↓
Batch Commit
   ↓
Watermark Update
```

SeaweedFS 적재 또는 검증 단계가 실패하면 Watermark를 갱신하지 않는다.

dbt Transformation 실패는 이미 Bronze가 정상 Commit된 이후의 실패이므로 동일 Bronze Batch를 재사용하여 재처리한다.

---

## 12. Batch Identity 및 Idempotency

### 12.1 Batch ID

Batch ID는 Airflow의 Logical Date를 기반으로 결정적으로 생성한다.

개념:

```text
{dag_id}_{logical_date_utc}
```

예:

```text
warehouse_pipeline_20260903T000000Z
```

동일 Logical Batch를 재실행하면 동일 `batch_id`를 사용한다.

### 12.2 Idempotency 정책

동일 Batch를 여러 번 실행해도 최종 결과는 동일해야 한다.

잘못된 결과:

```text
첫 실행 10,000 rows
재실행 10,000 rows

Final = 20,000 rows
```

기대 결과:

```text
첫 실행 10,000 rows
재실행 10,000 rows

Final = 10,000 rows
```

---

## 13. SeaweedFS S3-compatible Object Storage / Bronze 설계

### 13.1 Bucket

```text
commerce-lake
```

SeaweedFS는 V1에서 `weed mini` 기반 단일 로컬 S3-compatible 개발 환경으로 시작한다.

기본 S3 Endpoint:

```text
http://seaweedfs:8333
```

Python Upload/Read Client는 `boto3`를 사용한다. Storage 전용 SDK에 결합하지 않아 추후 AWS S3 전환 시 데이터 처리 코드 변경 범위를 줄인다.

### 13.2 Object Layout

```text
commerce-lake/
└── bronze/
    ├── customers/
    ├── products/
    ├── sellers/
    ├── orders/
    ├── order_items/
    └── payments/
```

세부 Object Key:

```text
bronze/
└── {table}/
    └── ingestion_date=YYYY-MM-DD/
        └── batch_id={batch_id}/
            ├── data.parquet
            └── manifest.json
```

### 13.3 Manifest

`manifest.json`에는 최소 다음 정보를 기록한다.

```text
batch_id
source_table
watermark_before
watermark_after
row_count
created_at
status
```

### 13.4 Re-run 정책

동일 `batch_id` 재실행 시 새로운 임의 파일을 추가하지 않는다.

- 동일 Batch Prefix 사용
- 기존 Batch Output을 결정적으로 Replace
- 성공 Batch는 Manifest로 Commit 여부 확인

이를 통해 Bronze Layer 자체에서 중복 Batch를 방지한다.

### 13.5 Layer 정책

V1에서는 SeaweedFS에 Bronze만 둔다.

```text
Bronze → SeaweedFS / Parquet
Silver/Gold → DuckDB/dbt
```

Object Storage에 Silver/Gold를 불필요하게 중복 저장하지 않는다.

---

## 14. Pipeline Metadata

### 14.1 P0 요구사항

Pipeline Metadata는 선택 기능이 아니라 V1 핵심 요구사항으로 취급한다.

### 14.2 pipeline_runs

예:

```text
pipeline_runs

run_id
batch_id
dag_id
source_table
logical_date
started_at
finished_at
watermark_before
watermark_after
rows_extracted
rows_valid
rows_rejected
rows_loaded
status
error_message
```

상태 예:

```text
RUNNING
SUCCESS
FAILED
```

### 14.3 목적

다음을 추적할 수 있어야 한다.

- 어디까지 읽었는가
- 몇 건을 읽었는가
- 몇 건이 Reject 되었는가
- 어느 Batch가 실패했는가
- 재실행 전후 결과가 동일한가
- Watermark가 어디까지 Commit 되었는가

---

## 15. Ingestion Validation 및 Quarantine

### 15.1 역할 분리

Data Quality를 두 계층으로 분리한다.

```text
Source
   ↓
Extract
   ↓
Ingestion Validation
   │
   ├── Valid
   │     ↓
   │   SeaweedFS Bronze
   │
   └── Invalid
         ↓
      Quarantine

SeaweedFS Bronze
   ↓
dbt + DuckDB
   ↓
Warehouse Quality Tests
```

### 15.2 Ingestion Validation

검사 대상:

- 필수 컬럼 존재
- PK/Business Key NULL
- 기본 Type
- Duplicate within Batch
- 명백한 Broken Reference
- 지원하지 않는 Canonical Value

### 15.3 Quarantine

예시 구조:

```text
quarantine_records

record_id
batch_id
source_table
error_type
error_message
detected_at
raw_payload
```

또는 SeaweedFS의 별도 Quarantine Prefix로 저장할 수 있다.

최종 저장 방식은 Phase 6 구현 시 선택하되 동일 Batch와 원본 Row를 추적 가능해야 한다.

---

## 16. dbt + DuckDB 설계

### 16.1 관계

개념 구조는 다음과 같다.

```text
SeaweedFS / Parquet
       │
       ▼
dbt + DuckDB Adapter
       │
       ▼
data/warehouse/warehouse.duckdb
       │
       ├── staging
       ├── intermediate
       └── marts
```

DuckDB가 S3-compatible Endpoint를 통해 SeaweedFS의 Parquet을 직접 읽는 방식을 사용한다.

Local SeaweedFS 접근에서는 S3 Endpoint, Access Key, Secret Key, Path-style URL 설정을 환경변수/Secret으로 주입하며 SQL 또는 dbt Model에 Credential을 하드코딩하지 않는다.

### 16.2 Materialization 기본 정책

| Layer        | Materialization  |
| ------------ | ---------------- |
| staging      | view             |
| intermediate | view 우선        |
| dimensions   | table            |
| facts        | incremental 우선 |

실제 성능 측정 결과에 따라 Intermediate의 일부를 table로 변경할 수 있다.

### 16.3 dbt Layer

#### Staging

- `stg_customers`
- `stg_products`
- `stg_sellers`
- `stg_orders`
- `stg_order_items`
- `stg_payments`

책임:

- 컬럼명 표준화
- 타입 정규화
- Timestamp 정규화
- Canonical Value 정리
- 분석에 불필요한 기술 컬럼 최소화

#### Intermediate

- `int_orders_enriched`
- `int_order_items_enriched`
- `int_payment_summary`
- `int_customer_history`

#### Mart

Dimensions:

- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`

Facts:

- `fact_orders`
- `fact_order_items`
- `fact_payments`

---

## 17. Dimensional Model Grain

Grain은 모델 구현 전에 고정한다.

| Model            | Grain                    |
| ---------------- | ------------------------ |
| dim_customer     | 고객 Version당 1행       |
| dim_product      | 상품당 1행               |
| dim_seller       | 판매자당 1행             |
| dim_date         | Calendar Date당 1행      |
| fact_orders      | 주문당 1행               |
| fact_order_items | 주문 상품 Line당 1행     |
| fact_payments    | 주문 결제 Sequence당 1행 |

### 17.1 Grain 규칙

Fact Join으로 인해 Measure가 중복 집계되지 않도록 한다.

예:

```text
fact_orders 1행
   +
order_items N행

잘못된 Join 후 order_total을 SUM
→ 매출 중복
```

각 Mart Model의 문서에는 반드시 다음을 기록한다.

- Grain
- Business Key
- Foreign Key
- Measure
- 주요 Data Quality Test

---

## 18. SCD Type 2

### 18.1 대상

V1 SCD2 대상:

```text
dim_customer
```

추적 속성:

- `membership_level`
- `city`
- `state`

### 18.2 Dimension Schema

```text
customer_key
customer_id
customer_unique_id
membership_level
city
state
valid_from
valid_to
is_current
```

`customer_key`는 Surrogate Key이다.

### 18.3 Temporal Join

Fact가 현재 고객 버전을 무조건 참조하면 안 된다.

주문 발생 시점에 유효했던 Customer Dimension Version을 참조한다.

개념:

```text
order.purchase_at >= dim_customer.valid_from
AND
order.purchase_at < COALESCE(dim_customer.valid_to, infinity)
```

예:

```text
2026-01-01 SILVER 시작
2026-06-01 GOLD 시작

2026-03-01 주문
→ SILVER customer_key 참조
```

---

## 19. Late Arriving Data

### 19.1 시간 개념 구분

Late Arrival은 다음 세 시간을 구분한다.

```text
Business Event Time
Update Time
Ingestion Time
```

예:

```text
purchase_at  = 2026-09-01 10:00
updated_at   = 2026-09-03 09:00
ingested_at  = 2026-09-03 10:00
```

### 19.2 처리 목표

`updated_at` 기반 증분 추출을 통해 데이터 자체를 놓치지 않는다.

문제는 이미 생성된 과거 Business Date의 Mart가 변경될 수 있다는 점이다.

```text
9/1 Mart 생성 완료
     ↓
9/3에 9/1 Business Event 도착
     ↓
영향받는 Business Date 식별
     ↓
해당 Mart 재계산 / Merge
```

### 19.3 Acceptance

Late Arrival 데이터가 이후 Batch에 들어오더라도 최종 Mart 집계에서 누락되지 않아야 한다.

---

## 20. Airflow 설계

### 20.1 Executor

V1에서는:

```text
LocalExecutor
```

를 사용한다.

CeleryExecutor, Redis Worker Cluster 등은 범위에서 제외한다.

### 20.2 DAG 책임

DAG 파일은 오케스트레이션에 집중한다.

```text
airflow/dags/
```

실제 데이터 처리 로직:

```text
src/
```

DAG 내부에 대규모 SQL·ETL 로직을 직접 작성하지 않는다.

### 20.3 DAG 구성

```text
source_simulation_dag
warehouse_pipeline_dag
```

#### source_simulation_dag

책임:

- Synthetic Source Event 생성
- logical date 전달
- Generator 실행

개발 기본값:

```text
manual
```

#### warehouse_pipeline_dag

책임:

```text
extract
  ↓
validate
  ↓
load_bronze
  ↓
commit_watermark
  ↓
dbt_build
  ↓
dbt_test
```

기본 Scheduling:

```text
@daily
```

로 정의하되 개발 시 환경 설정으로 Schedule을 비활성화할 수 있게 한다.

### 20.4 Retry 기본값

초기 기본값:

```text
retries = 2
retry_delay = 60 seconds
```

환경변수 또는 Config로 변경 가능하게 한다.

### 20.5 XCom 원칙

대용량 DataFrame이나 Parquet 데이터를 XCom으로 전달하지 않는다.

XCom에는 다음과 같은 Metadata만 전달한다.

- batch_id
- object path
- row count
- watermark

---

## 21. Backfill 및 Re-run

### 21.1 Backfill

특정 Logical Date 또는 기간을 명시하여 다시 처리할 수 있어야 한다.

예:

```text
2026-09-01 SUCCESS
2026-09-02 FAILED
2026-09-03 SUCCESS
```

9월 2일 Batch만 재실행 가능해야 한다.

### 21.2 원칙

Backfill은 Airflow UI 기능에만 의존하지 않는다.

데이터 처리 함수 자체가 다음 입력을 받을 수 있어야 한다.

```text
logical_date
batch_id
watermark range
```

### 21.3 검증

Backfill 결과는 동일 범위를 Full Rebuild한 결과와 정합해야 한다.

---

## 22. Warehouse Data Quality

### 22.1 dbt Generic Tests

최소 적용:

- `unique`
- `not_null`
- `relationships`
- `accepted_values`

### 22.2 Custom Tests

최소 검증:

```text
payment_value >= 0
price >= 0
freight_value >= 0
delivered_at >= purchase_at
현재 customer version은 customer당 정확히 1개
Fact Foreign Key가 Dimension에 존재
```

### 22.3 Order Status

허용값:

```text
CREATED
APPROVED
SHIPPED
DELIVERED
CANCELED
```

### 22.4 Payment Status

허용값:

```text
PENDING
COMPLETED
FAILED
REFUNDED
```

---

## 23. BI

BI는 핵심 프로젝트 범위가 아니라 Data Mart 사용 가능성 검증 계층이다.

### 23.1 우선 도구

```text
Metabase
```

다만 DuckDB 연결 방식은 Phase 9 전에 실제 환경에서 검증한다.

직접 연결이 안정적이지 않을 경우 Serving DB를 별도로 두는 방안을 검토하며, 이는 아키텍처 변경 사항으로 ADR에 기록한다.

### 23.2 Dashboard

#### Sales Overview

- 일별 매출
- 주문 건수
- 평균 주문 금액

#### Product

- 카테고리별 매출
- Top Product
- 판매량

#### Customer

- 신규 고객
- 재구매 고객
- 지역별 고객
- Membership별 매출

대시보드 디자인 자체에는 과도한 시간을 사용하지 않는다.

---

## 24. 비기능 요구사항

### 24.1 재현성

기본 인프라는 다음 흐름으로 재현 가능해야 한다.

```bash
git clone <repository>
cd commerce-data-platform
docker compose up -d
```

데이터 다운로드 및 초기화는 별도 Script로 자동화한다.

### 24.2 설정 분리

코드에 직접 작성하지 않는다.

- Password
- Access Key
- Secret Key
- Database URL
- Bucket Name
- Port
- Endpoint

구조:

```text
.env
.env.example
```

`.env`는 Git에 포함하지 않는다.

### 24.3 Observability

최소 확인 가능 정보:

- Batch ID
- Logical Date
- Batch Start / End
- Watermark Before / After
- Rows Extracted
- Rows Valid
- Rows Rejected
- Rows Loaded
- Execution Time
- Pipeline Status
- Error Message

---

## 25. Benchmark

### 25.1 Scale 기준

데이터 규모 단위는 **Orders 수**로 통일한다.

```text
100K Orders
1M Orders
5M Orders
```

### 25.2 실험 항목

#### Full Refresh vs Incremental

측정:

- 처리 시간
- 읽은 Row 수
- 실제 변경 Row 수
- 최종 결과 정합성

#### CSV vs Parquet

측정:

- 파일 크기
- 읽기 시간
- Query 실행 시간

#### Full Scan vs Partition Filtering

측정:

- Scan 대상
- 실행 시간

### 25.3 실험 조건

성능 비교는 다음을 고정한다.

- 동일 Host
- 동일 WSL Resource
- 동일 Dataset Scale
- 동일 Random Seed
- 동일 Query
- 동일 Pipeline 설정

각 주요 Benchmark는 여러 번 실행하고 **Median**을 대표값으로 사용한다.

기본 반복 횟수:

```text
5회
```

필요한 경우 Cold Run과 Warm Run을 구분하여 기록한다.

---

## 26. 핵심 기능 요구사항

| ID    | 요구사항                                | 우선순위 |
| ----- | --------------------------------------- | -------- |
| FR-01 | Olist → Project Source Schema Seed 적재 | P0       |
| FR-02 | Deterministic Synthetic Generator       | P0       |
| FR-03 | Table별 Incremental Extract             | P0       |
| FR-04 | Composite Watermark 관리                | P0       |
| FR-05 | Pipeline Metadata 저장                  | P0       |
| FR-06 | SeaweedFS Parquet Bronze 적재           | P0       |
| FR-07 | Batch Idempotency                       | P0       |
| FR-08 | Airflow Pipeline                        | P0       |
| FR-09 | dbt Staging / Intermediate / Mart       | P0       |
| FR-10 | Star Schema 및 Fact Grain               | P0       |
| FR-11 | Data Quality                            | P0       |
| FR-12 | Backfill / Re-run                       | P0       |
| FR-13 | Late Arriving Data 처리                 | P0       |
| FR-14 | SCD Type 2                              | P0       |
| FR-15 | Quarantine                              | P1       |
| FR-16 | Metabase Dashboard                      | P1       |
| FR-17 | 1M+ Synthetic Scale                     | P1       |
| FR-18 | Benchmark                               | P1       |
| FR-19 | Cloud Migration PoC                     | P2       |
| FR-20 | CDC                                     | P2       |

---

## 27. Acceptance Criteria

### AC-01 E2E

Olist Seed + Synthetic 데이터가 다음 전체 경로를 통해 Mart까지 도달한다.

```text
PostgreSQL
→ Airflow
→ SeaweedFS
→ dbt + DuckDB
→ Data Mart
```

### AC-02 Incremental Load

신규/변경 데이터가 있을 때 전체 Source Table을 매번 다시 처리하지 않는다.

### AC-03 Idempotency

동일 Batch를 3회 실행해도 Business Key 기준 중복 Row가 0건이어야 한다.

### AC-04 Watermark

SeaweedFS Load 또는 Validation 실패 시 Watermark가 전진하지 않아야 한다.

성공 Batch에서만 Watermark가 Commit되어야 한다.

### AC-05 Backfill

지정 Logical Date를 재처리할 수 있어야 하고 결과가 Full Rebuild의 동일 범위 결과와 정합해야 한다.

### AC-06 Data Quality

정의된 Pipeline-level Corruption을 Validation 또는 dbt Test가 탐지해야 한다.

### AC-07 SCD2

고객 속성이 변경되면:

```text
기존 Version 종료
+
신규 Version 생성
```

이 발생해야 한다.

각 `customer_id`에 대해 `is_current = true`인 Dimension Row는 정확히 1개여야 한다.

### AC-08 Temporal Join

속성 변경 전 발생한 주문은 당시 유효했던 Customer Version을 참조해야 한다.

### AC-09 Late Arrival

과거 Business Event가 늦게 도착해도 최종 Mart 집계에 반영되어야 한다.

### AC-10 Referential Integrity

모든 Fact FK는 대응 Dimension과 관계 검증을 통과해야 한다.

### AC-11 Observability

각 Pipeline Run에 대해 최소 다음을 조회할 수 있어야 한다.

- Batch ID
- Status
- Watermark
- Rows Extracted
- Rows Loaded
- Rows Rejected
- Duration

### AC-12 Reproducibility

Repository 문서의 실행 절차만으로 로컬 개발 환경을 재현할 수 있어야 한다.

---

## 28. Repository Structure

```text
commerce-data-platform/
│
├── README.md
├── PRD.md
├── AGENTS.md
├── compose.yaml
├── pyproject.toml
├── uv.lock
├── .python-version
├── .env.example
├── .gitignore
│
├── airflow/
│   ├── Dockerfile              # 공식 Airflow 3.3.1-python3.12 확장 시 사용
│   ├── dags/
│   │   ├── source_simulation_dag.py
│   │   └── warehouse_pipeline_dag.py
│   └── logs/                   # Git 제외
│
├── src/
│   ├── seed/
│   ├── generator/
│   ├── ingestion/
│   └── common/
│
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   │       ├── dimensions/
│   │       └── facts/
│   ├── snapshots/
│   ├── tests/
│   └── macros/
│
├── sql/
│   ├── source/
│   ├── metadata/
│   └── validation/
│
├── data/
│   ├── raw/
│   │   └── olist/              # Git 제외
│   ├── generated/              # Git 제외
│   ├── warehouse/              # DuckDB, Git 제외
│   └── samples/
│
├── scripts/
│   ├── init.sh
│   ├── download_dataset.py
│   ├── seed.sh
│   └── reset.sh
│
├── tests/
│   ├── generator/
│   ├── ingestion/
│   └── integration/
│
└── docs/
    ├── architecture/
    ├── adr/
    ├── benchmarks/
    └── troubleshooting/
```

### 책임 분리

- 프로젝트 전체 실행/설정 → Root
- Python 데이터 처리 → `src/`
- Airflow Orchestration → `airflow/`
- SQL Transformation / Modeling → `dbt/`
- Source/Metadata DDL → `sql/`
- 원본 및 로컬 Warehouse → `data/`
- 반복 작업 자동화 → `scripts/`
- 코드/통합 테스트 → `tests/`
- 설계 판단 및 성능 기록 → `docs/`

---

## 29. 개발 단계 및 Definition of Done

### Phase 0. Project Bootstrap

#### 목표

실제 데이터 파이프라인 구현에 들어가기 전에 **개발환경, Repository, 버전, 기본 프로젝트 구조를 재현 가능한 상태로 고정**한다.

#### 구현

- WSL 내부 프로젝트 경로 생성
  - `~/projects/commerce-data-platform`
- Git Repository 초기화
- Python `3.12` 설치 및 프로젝트 Pin
  - `uv python install 3.12`
  - `uv python pin 3.12`
- `pyproject.toml`, `uv.lock`, `.python-version` 구성
- Repository 기본 디렉터리 구조 생성
  - `src/`
  - `airflow/`
  - `dbt/`
  - `sql/`
  - `data/`
  - `scripts/`
  - `tests/`
  - `docs/`
- Root 문서 배치
  - `README.md`
  - `PRD.md`
  - `AGENTS.md`
- 환경/보안 기본 파일 생성
  - `.env.example`
  - `.gitignore`
- `compose.yaml` Skeleton 생성
- `scripts/download_dataset.py` 생성
- Initial Git Commit 생성

#### Phase 0 범위 제한

Phase 0에서는 다음 구현에 들어가지 않는다.

- PostgreSQL Table 생성
- Olist 실제 Seed 적재
- Synthetic Generator 구현
- SeaweedFS 실행 구성
- Airflow DAG 구현
- dbt Model 구현

`compose.yaml`은 Phase 0에서 유효한 Skeleton까지 만들고, 서비스별 실제 구성은 해당 Phase에서 추가한다.

#### Definition of Done

- `uv run python --version`이 Python `3.12.x`를 출력한다.
- `uv sync`가 정상 수행된다.
- `docker compose config`가 오류 없이 통과한다.
- Repository 기본 디렉터리 구조가 생성되어 있다.
- `.env`와 `data/raw/`, `data/generated/`, `data/warehouse/`, `airflow/logs/`가 Git 추적 대상에서 제외된다.
- `README.md`, `PRD.md`, `AGENTS.md`가 Repository Root에 존재한다.
- `scripts/download_dataset.py`가 존재한다.
- Initial Git Commit이 생성되어 있다.

---

### Phase 1. Source Environment

#### 목표

PostgreSQL에 프로젝트용 OLTP Source 환경을 구성하고 Olist Raw Dataset을 실제 Source Schema로 Seed 적재한다.

#### 구현

- `compose.yaml`에 PostgreSQL `18.6` Service 구성
- PostgreSQL 기동 및 Health Check
- 역할별 Database 초기화
  - `commerce_source`
  - `airflow_metadata`
  - `pipeline_metadata`
- Project Source Schema DDL 생성
- `scripts/download_dataset.py` 실행
- Olist 주요 Raw CSV 다운로드
- Olist → Project Source Schema Seed Transformation
- Seed Loader 실행
- Seed 재실행 정책 구현

#### Definition of Done

- PostgreSQL `18.6` Container가 정상 기동한다.
- `commerce_source`, `airflow_metadata`, `pipeline_metadata` Database가 분리되어 존재한다.
- Olist 주요 6개 Raw Dataset을 `data/raw/olist/`에서 확인할 수 있다.
- Olist → PostgreSQL Seed 적재가 성공한다.
- Source PK/FK 관계 검증을 통과한다.
- 프로젝트 확장 필드가 정상 생성된다.
- Source Table별 Row Count를 기록할 수 있다.
- 동일 Seed Loader 재실행 시 정의된 정책에 따라 중복이 발생하지 않는다.

---

### Phase 2. Synthetic Data Generator

#### 구현

- Customer Generator
- Order Generator
- Order Item Generator
- Payment Generator
- State Transition
- Membership Update
- `random_seed`
- `logical_date`

#### Definition of Done

- 동일 Seed + Logical Date로 동일 데이터 생성
- Canonical State Machine 준수
- Source에 신규/변경 데이터 발생
- Membership 등급 변경 시나리오 재현

---

### Phase 3. Incremental Ingestion + SeaweedFS S3

#### 구현

- SeaweedFS `4.45` (`weed mini` / S3 API)
- boto3 S3 Client
- Parquet
- Table별 Incremental Extract
- Composite Watermark
- Batch ID
- Manifest
- Ingestion Validation
- Pipeline Metadata

#### Definition of Done

- 6개 Source Table 증분 추출 성공
- `bronze/sellers` 포함 전체 Bronze 적재
- 동일 Batch 재실행 시 중복 Object 없음
- 실패 Batch에서 Watermark 미갱신
- 성공 Batch에서 Watermark 정상 Commit
- Pipeline Run Metadata 조회 가능

---

### Phase 4. Airflow

#### 구현

- LocalExecutor
- `source_simulation_dag`
- `warehouse_pipeline_dag`
- Retry
- Dependency
- Scheduling
- Metadata-only XCom

#### Definition of Done

- Warehouse DAG E2E 성공
- 의도적 Task Failure 후 Retry 검증
- 실패 후 동일 Batch Re-run 성공
- 대용량 Payload가 XCom에 저장되지 않음

---

### Phase 5. dbt + DuckDB + Modeling

#### 구현

- SeaweedFS S3 Endpoint의 Parquet Read
- DuckDB Warehouse
- Staging
- Intermediate
- Mart
- Fact/Dimension
- Incremental Fact
- SCD2 Customer

#### Definition of Done

- dbt build 성공
- 모든 Model Grain 문서화
- Fact/Dimension 관계 정상
- SCD2 Version 생성 정상
- Temporal Join 검증 성공

---

### Phase 6. Data Quality

#### 구현

- Pipeline-level Corruption Injection
- Quarantine
- dbt Generic Tests
- Custom Tests

#### Definition of Done

- Duplicate 탐지
- NULL Business Key 탐지
- Broken Reference 탐지
- Invalid Status 탐지
- 정상 데이터는 오탐 없이 통과
- Reject Row와 Batch ID 추적 가능

---

### Phase 7. Reliability Scenarios

#### 시나리오

- Duplicate Batch
- Pipeline Failure
- Late Arrival
- Missing Batch
- Backfill
- Re-run
- Customer Attribute Change

#### Definition of Done

각 시나리오에 대해 다음을 문서화한다.

```text
문제
→ 재현 방법
→ 관측 결과
→ 원인
→ 해결
→ 재검증
```

---

### Phase 8. Benchmark

#### Scale

```text
100K Orders
1M Orders
5M Orders
```

#### Definition of Done

- 동일 Random Seed 사용
- 동일 WSL Resource 사용
- 주요 테스트 5회 반복
- Median 기록
- Full vs Incremental 비교
- CSV vs Parquet 비교
- 결과를 `docs/benchmarks/`에 기록

---

### Phase 9. BI

#### 구현

- Metabase 연결 방식 검증
- Sales Overview
- Product
- Customer

#### Definition of Done

- Data Mart 기반 Dashboard 조회 가능
- Raw Source 직접 참조 없음
- BI 연결 방식과 Trade-off 문서화

---

## 30. ADR 대상

최소 다음 의사결정은 `docs/adr/`에 기록한다.

```text
001-use-minio-instead-of-s3.md
002-use-duckdb-as-local-warehouse.md
003-use-parquet-for-bronze.md
004-use-table-specific-incremental-strategy.md
005-use-composite-watermark.md
006-use-single-postgres-container.md
007-version-pinning-policy.md
```

ADR 기본 형식:

```text
Context
Decision
Alternatives
Consequences
```

---

## 31. 최종 포트폴리오에서 보여줄 내용

기술 스택 목록보다 다음을 중심으로 설명한다.

1. Olist를 그대로 적재하지 않고 서비스형 Source Schema를 구성한 이유
2. 테이블 특성별 증분 전략을 나눈 기준
3. Timestamp 단독이 아닌 Composite Watermark를 사용한 이유
4. Watermark Commit 시점을 어떻게 결정했는가
5. 동일 Batch 재실행에서 중복을 어떻게 방지했는가
6. Fact Grain을 어떻게 정의했고 중복 집계를 어떻게 방지했는가
7. SCD2와 Temporal Join을 어떻게 구현했는가
8. Late Arrival이 과거 Mart에 미치는 영향을 어떻게 처리했는가
9. Ingestion Validation과 Warehouse Quality를 왜 분리했는가
10. 실패·Backfill·Re-run을 어떻게 검증했는가
11. 100K → 1M → 5M에서 병목이 어떻게 변했는가
12. Baseline 측정 후 어떤 개선을 적용했는가

---

## 32. 최종 확정 문장

> **Python 3.12 + WSL2 Ubuntu + Docker 기반 로컬 환경에서 Olist를 Seed로 프로젝트용 PostgreSQL OLTP Source를 구성하고, 결정적 Synthetic Data Generator로 지속적인 데이터 변경을 발생시킨다. Airflow를 통해 테이블 특성별 증분 데이터를 수집하고 Composite Watermark와 Batch Metadata로 상태를 관리하며, SeaweedFS의 S3-compatible Object Storage에 Parquet Bronze를 적재한다. 이후 dbt + DuckDB로 Star Schema와 SCD Type 2 Data Mart를 구축하고, Data Quality·Idempotency·Backfill·Late Arrival·Failure Recovery를 재현 및 검증한다. 직접 Dependency와 Container Image는 검증된 Baseline Version으로 고정하고, `uv.lock`과 Docker Tag를 통해 재현성을 확보한다.**
