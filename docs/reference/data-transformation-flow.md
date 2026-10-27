# 데이터 흐름: Source → Data Lake → Data Warehouse → Serving

> 상태: Reference
> 기준 문서: [PRD v1.28](../../PRD_v1.28.md), [Phase 3](../phases/phase-03-incremental-ingestion.md), [Phase 4](../phases/phase-04-airflow-orchestration.md), [Phase 5](../phases/phase-05-bronze-catalog-and-staging.md), [Phase 7](../phases/phase-07-data-quality-publish.md), [Phase 10](../phases/phase-10-bi.md)

이 문서는 하나의 주문 레코드가 PostgreSQL 원천에서 Metabase가 읽는 Serving 파일에 도달할 때까지의 전체 경로를 정리한다. 3-8절은 이름, 타입, 값, Grain이 어느 지점에서 왜 바뀌는지와, 그 변환을 다른 지점에서 했을 때 무엇이 깨지는지를 기록한다. 9-13절은 Mart가 만들어진 뒤 Build, Publish, Serving Export, Metabase 재지정, 운영 기록으로 이어지는 구간을 단계별 동작과 실패 시 동작 중심으로 정리한다. 이 구간에서는 데이터 값이 바뀌지 않는다. 바뀌는 것은 파일 위치와 가시성이다.

## 1. 전체 흐름

```text
PostgreSQL commerce_source               OLTP 원천
        ↓ ingest_table()                 이름 보존, 기술 컬럼만 추가 (Table별 extract_validate_load)
SeaweedFS bronze/*.parquet               Data Lake (불변 Object)
        ↓ sync_bronze_catalog()          prepare_warehouse_build가 Build 파일에 실행
DuckDB control.bronze_files              Commit된 Object 목록 (Build 파일 안)
        ↓ dbt staging                    이름·값 변환이 일어나는 유일한 지점
DuckDB staging.*                         분석 Naming (View)
        ↓ dbt intermediate               Join과 사전 집계
DuckDB intermediate.*                    (View)
        ↓ dbt marts                      Grain 확정
DuckDB dimensions·facts·metrics.*        Mart (Build 파일 build/{publish_run_id}.duckdb 안)
        ↓ publish_mart                   검증된 Build 파일을 원자 교체
data/warehouse/warehouse.duckdb          Published Warehouse
↓ export_serving_mart                    Mart Schema만 복사, serving_manifest 기록
data/serving/exports/{export_id}.duckdb  Serving 파일 (mart.duckdb와 같은 inode)
        ↓ repoint_metabase_serving       Metabase 연결 경로 교체·검증·Schema 동기화
Metabase                                 BI 조회
        ↓ publish_run_summary            DagRun마다 운영 기록 1행
pipeline_metadata.warehouse_run_reports
```

각 구간의 책임은 겹치지 않는다.

| 구간                   | 이름     | 타입       | 값         | Grain    | 근거                                             |
| ---------------------- | -------- | ---------- | ---------- | -------- | ------------------------------------------------ |
| Source → Bronze        | 보존     | 고정       | 보존       | 보존     | 원천 무손실 복제로 재수집 없는 Replay를 보장한다 |
| Bronze → Catalog       | 보존     | 보존       | 보존       | 보존     | COMMITTED Object 목록만 전달한다                 |
| Catalog → Staging      | **변환** | **정규화** | **표준화** | 보존     | 분석 Naming으로 바꾸는 단일 지점이다             |
| Staging → Intermediate | 보존     | 보존       | 보존       | **변경** | Join과 사전 집계로 Grain을 맞춘다                |
| Intermediate → Mart    | 보존     | 보존       | 보존       | 확정     | 문서화된 Grain과 Unique Key로 고정한다           |
| Build → Published      | 보존     | 보존       | 보존       | 보존     | 검증을 통과한 파일만 원자 교체로 노출한다        |
| Published → Serving    | 보존     | 보존       | 보존       | 보존     | Mart Schema만 복사해 BI 읽기를 Warehouse와 분리한다 |

이름 변환이 Staging 한 곳에만 있는 것이 변환 구간 설계의 핵심이다. 근거는 6절에서 다룬다. Mart 이후 구간은 값을 바꾸지 않고, 실패한 결과가 BI에 보이지 않게 하는 데 책임이 있다.

---

## 2. 계층별 저장 형태

| 계층      | 저장소                       | 물리 형태      | 변경 가능성             | 조회 주체                          |
| --------- | ---------------------------- | -------------- | ----------------------- | ---------------------------------- |
| Source    | PostgreSQL `commerce_source` | Row 지향 Table | Mutable (UPDATE)        | Generator, Ingestion Extract       |
| Data Lake | SeaweedFS (S3 호환)          | Parquet Object | Immutable (Append Only) | Ingestion Commit, dbt Source Macro |
| Catalog   | DuckDB `control` (Build 파일 안) | Table      | 매 DagRun 재동기화      | dbt Source Macro                   |
| Build     | `data/warehouse/build/{publish_run_id}.duckdb` | DuckDB 파일 | DagRun마다 Published 복사본에서 새로 만든다 | dbt build |
| Warehouse | `data/warehouse/warehouse.duckdb` — `staging`·`intermediate`·`dimensions`·`facts`·`metrics` | View / Table | Publish 때 파일 전체를 원자 교체 | 분석가, Serving Export |
| Serving   | `data/serving/exports/{export_id}.duckdb` (`mart.duckdb`와 같은 inode) | DuckDB 파일 | Export마다 새 파일, 최신 3개 보존 | Metabase |
| 운영 기록 | PostgreSQL `pipeline_metadata` | Table | Publish Run 상태 전이, DagRun별 기록 Upsert | 운영자 |

Source가 Mutable이고 Lake가 Immutable인 것이 계층을 나누는 근본 이유다. 원천의 `UPDATE`는 이전 값을 지우지만, Lake는 Batch마다 새 Object를 쌓으므로 과거 상태가 남는다. 이 차이가 시점별 이력 복원을 가능하게 한다.

---

## 3. Source → Bronze: 이름을 바꾸지 않는 구간

Phase 3 `ingest_table()`(`src/ingestion/service.py`)이 담당하고, `warehouse_pipeline_dag`의 `extract_validate_load` Task가 Source Table 9개에 Mapping되어 Table마다 한 번 호출한다. 추출 전에 `acquire_source_snapshot_lease`가 원천 변경 Lease를 잡고, 모든 Table Task가 끝나면 `release_source_snapshot_lease`가 해제한다. `src/ingestion/contract/tables.py`의 `TableConfig.source_columns`가 계약이다.

### 3.1 이 구간에서 하는 일

| 작업           | 내용                                                                       |
| -------------- | -------------------------------------------------------------------------- |
| 증분 추출      | Table별 Cursor 컬럼 기준 Keyset Pagination (`customers`는 `created_at`, `customer_subscriptions`/`customer_membership_tiers`는 `updated_at` 등 Table마다 다르다) |
| Type 고정      | PostgreSQL 타입을 Arrow 타입으로 명시 변환                                 |
| 기술 컬럼 추가 | `_batch_id`, `_run_id`, `_ingested_at`, `_source_table`, `_schema_version` |
| 검증과 격리    | 계약 위반 Row를 Quarantine으로 분리                                        |
| Commit         | Manifest와 Checksum 기록 후 `bronze_objects`에 COMMITTED 기록              |

### 3.2 Type 변환 표

이름은 그대로지만 타입은 Arrow로 고정한다.

| PostgreSQL 타입 | Arrow 타입                  | 근거                                                                              |
| --------------- | --------------------------- | --------------------------------------------------------------------------------- |
| `VARCHAR(n)`    | `string`                    | Parquet에 길이 제약이 없다. 길이 검증은 Source CHECK 제약이 이미 수행했다         |
| `CHAR(2)`       | `string`                    | 고정 길이 Padding을 제거해 비교를 단순하게 만든다                                 |
| `INTEGER`       | `int32`                     | 원천 범위를 그대로 유지한다                                                       |
| `NUMERIC(14,2)` | `decimal128(14,2)`          | 금액이다. `float`로 바꾸면 합계에 오차가 생겨 금액 Measure 검증이 실패한다 |
| `TIMESTAMPTZ`   | `timestamp("us", tz="UTC")` | 모든 시각을 UTC microsecond로 통일한다                                            |

`NUMERIC` → `decimal128` 유지가 중요하다. `price`, `freight_value`, `payment_value`를 `float64`로 저장하면 `SUM()` 결과가 Full Refresh와 Incremental 사이에서 미세하게 달라지고, Logical Hash 비교(`P5-31`)가 재현 불가능해진다.

### 3.3 추가되는 기술 컬럼

| 컬럼              | 타입                 | 값                             | 용도                                                        |
| ----------------- | -------------------- | ------------------------------ | ----------------------------------------------------------- |
| `_batch_id`       | `string`             | `{dag_id}__{YYYYMMDDTHHMMSSZ}` | 같은 Batch의 9개 Table을 묶는다. Current 선택 Tie-breaker다 |
| `_run_id`         | `string`             | Pipeline Run UUID              | 실행 추적용이다                                             |
| `_ingested_at`    | `timestamp[us, UTC]` | Bronze 기록 시각               | Current 선택 Tie-breaker다                                  |
| `_source_table`   | `string`             | 원천 Table 이름                | Object 자체로 출처를 식별한다                               |
| `_schema_version` | `int32`              | `3` (`BRONZE_SCHEMA_VERSION`)  | 지원하지 않는 Version을 dbt 이전에 차단한다                 |

지원 Version 목록은 `src/ingestion/contract/schema.py`의 `SUPPORTED_BRONZE_SCHEMA_VERSIONS`이고, 현재 `{3}`이다. Version 증가 규칙은 [ADR 015](../adr/015-bronze-schema-evolution-policy.md)를 따른다.

`_batch_id`와 `_ingested_at`은 Staging에서 **보존해야 한다**. 같은 `customer_id`가 여러 Batch에 걸쳐 여러 Version으로 존재할 때, 어느 Row가 최신인지 판정하는 유일한 근거이기 때문이다.

### 3.4 Object 배치

```text
bronze/{source_table}/ingestion_date={YYYY-MM-DD}/batch_id={batch_id}/data.parquet
bronze/{source_table}/ingestion_date={YYYY-MM-DD}/batch_id={batch_id}/manifest.json
```

`ingestion_date`는 수집일이지 비즈니스 이벤트일이 아니다. Late Arrival 주문은 과거 `order_purchase_timestamp`를 가지지만 오늘 `ingestion_date` 아래에 저장된다. 이 분리가 Late Arrival 처리의 전제다.

### 3.5 왜 여기서 이름을 바꾸지 않는가

Bronze가 원천의 무손실 복제여야 하는 이유는 세 가지다.

1. **Replay**: 장애 복구 시 Source를 다시 읽지 않고 Commit된 Bronze만 재적용한다. 이름이 바뀌어 있으면 원천과 대조 검증이 불가능하다.
2. **Schema 진화**: 원천에 컬럼이 추가되면 Bronze는 그대로 받고, Staging Mapping만 고치면 된다. Bronze에서 이름을 바꾸면 과거 Object와 신규 Object의 Schema가 갈라진다.
3. **감사**: 분석 결과가 이상할 때 Bronze Parquet를 열어 원천 값과 1:1로 비교할 수 있어야 한다.

---

## 4. Bronze → Catalog: 목록만 전달하는 구간

`sync_bronze_catalog()`(`src/ingestion/bronze/catalog.py`)가 담당한다. 별도 Task는 없다. `warehouse_pipeline_dag`의 `prepare_warehouse_build` Task가 Published 파일을 Build 파일로 복사한 직후 그 Build 파일에 호출한다(`src/warehouse/publish.py`의 `prepare_warehouse_build`). 따라서 Catalog는 dbt가 실행될 Build 파일 안에 있고, Publish 때 Warehouse와 함께 교체된다.

```text
pipeline_metadata.bronze_objects (status = 'COMMITTED')
        ↓
DuckDB control.bronze_files (data/warehouse/build/{publish_run_id}.duckdb 안)
```

| 동작 | 내용                                                                       |
| ---- | -------------------------------------------------------------------------- |
| 필터 | `status = 'COMMITTED'`인 Object만                                          |
| 방식 | `DELETE` 후 전체 `INSERT`를 한 Transaction으로                             |
| 검증 | 각 Entry의 `schema_version`을 `assert_supported_schema_version()`으로 확인 |

### 4.1 왜 S3 Prefix Glob을 쓰지 않는가

dbt가 `read_parquet('s3://bronze/orders/**/*.parquet')`로 읽으면 다음이 섞여 들어온다.

| 위험                  | 결과                                           |
| --------------------- | ---------------------------------------------- |
| 쓰다 만 Object        | Commit 실패로 남은 부분 파일을 분석에 포함한다 |
| Orphan Object         | Metadata에 없는 Object를 읽는다                |
| 미지원 Schema Version | 차단 없이 dbt Model이 깨진다                   |

Metadata를 Source of Truth로 두면 Commit Protocol이 보장한 Object만 dbt에 노출된다. 이것이 Phase 5 `P5-02` Macro가 명시적 Object 목록을 받는 이유다.

---

## 5. Catalog → Staging: 이름과 값이 바뀌는 유일한 구간

여기서 4종류의 변환이 일어난다. 각각의 근거는 아래에 나눠 정리한다.

| 변환 종류             | 대상                                                 | 근거 요약                                  |
| --------------------- | ---------------------------------------------------- | ------------------------------------------ |
| Table Prefix 제거     | 9개 컬럼                                             | 소속이 이미 Table 이름에 있다              |
| Timestamp 접미사 통일 | 5개 컬럼                                             | 이름이 타입을 속인다                       |
| Business Key 교체     | `customer_id` / `customer_unique_id`                 | 원천 PK가 사람을 식별하지 않는다           |
| 상태값 표준화         | `order_status`, `payment_status`, `subscription_status`, `membership_tier` | 원천 상태를 분석용 표기 규칙으로 통일한다 |

### 5.1 Table Prefix 제거

| PostgreSQL                            | Bronze                  | Staging         | 근거                            |
| ------------------------------------- | ----------------------- | --------------- | ------------------------------- |
| `customers.customer_city`             | `customer_city`         | `city`          | Table 이름이 이미 소속을 말한다 |
| `customers.customer_state`            | `customer_state`        | `state`         | 위와 같다                       |
| `sellers.seller_city`                 | `seller_city`           | `city`          | 위와 같다                       |
| `sellers.seller_state`                | `seller_state`          | `state`         | 위와 같다                       |
| `products.product_category_name`      | `product_category_name` | `category_name` | 위와 같다                       |
| `products.product_weight_g`           | `product_weight_g`      | `weight_g`      | 위와 같다                       |
| `products.product_length_cm`          | `product_length_cm`     | `length_cm`     | 위와 같다                       |
| `products.product_height_cm`          | `product_height_cm`     | `height_cm`     | 위와 같다                       |
| `products.product_width_cm`           | `product_width_cm`      | `width_cm`      | 위와 같다                       |
| `order_payments.payment_installments` | `payment_installments`  | `installments`  | 위와 같다                       |

Olist 원본이 CSV였기 때문에 생긴 접두사다. 여러 CSV를 한 곳에 합칠 때 이름 충돌을 막으려면 접두사가 필요하지만, Table로 분리된 이후에는 중복이다.

Join할 때의 차이가 명확하다.

```sql
-- 접두사를 남기면 alias와 접두사가 이중으로 붙는다
SELECT c.customer_city, s.seller_city FROM ... c JOIN ... s

-- 제거하면 alias만으로 구분된다
SELECT c.city, s.city FROM ... c JOIN ... s
```

### 5.2 Timestamp 접미사 통일

PostgreSQL `orders`의 실제 정의다.

| PostgreSQL 컬럼                 | PostgreSQL 타입 | 접미사       | Staging                 | 근거                          |
| ------------------------------- | --------------- | ------------ | ----------------------- | ----------------------------- |
| `order_purchase_timestamp`      | `TIMESTAMPTZ`   | `_timestamp` | `purchase_at`           | 접미사 3종이 혼재한다         |
| `order_approved_at`             | `TIMESTAMPTZ`   | `_at`        | `approved_at`           | 위와 같다                     |
| `order_delivered_carrier_date`  | `TIMESTAMPTZ`   | `_date`      | `carrier_at`            | 타입은 시각인데 이름은 날짜다 |
| `order_delivered_customer_date` | `TIMESTAMPTZ`   | `_date`      | `delivered_at`          | 위와 같다                     |
| `order_estimated_delivery_date` | `TIMESTAMPTZ`   | `_date`      | `estimated_delivery_at` | 위와 같다                     |

5개 컬럼의 타입은 모두 `TIMESTAMPTZ`인데 접미사는 `_timestamp`, `_at`, `_date` 3종이다.

실제 `orders` 데이터를 보면 `order_delivered_carrier_date`와
`order_delivered_customer_date`에는 날짜뿐 아니라 배송 인계·완료 시각도 들어 있다. 따라서
`_date` 접미사만으로 `DATE` 타입으로 축소하면 시각 성분과 같은 날 안의 이벤트 순서를 잃는다.
두 컬럼은 `TIMESTAMPTZ`로 보존하고, Staging에서 실제 타입을 드러내는 `carrier_at`,
`delivered_at`으로 이름을 바꾼다. `order_estimated_delivery_date`의 자정 값은 5.2.1에서 별도로
검토하지만, 현재 원천 계약에서는 같은 방식으로 `TIMESTAMPTZ`를 유지한다.

`order_payments.payment_initiated_at`, `payment_completed_at`, `payment_failed_at`,
`payment_refunded_at`은 Olist CSV에 없는 Generator 확장 컬럼이다. 각각 결제 생성, 완료, 실패,
환불의 비즈니스 시각을 기록한다. 원본 Olist 시드 행은 근거가 없어 네 컬럼 모두 `NULL`이다.
증분 Cursor인 `updated_at`과 달리 결제 생명주기 분석에 사용하는 사건 시각이므로 Staging에서도
이름과 타입을 그대로 보존한다.

`_date`가 특히 위험하다. 이름을 믿은 분석가는 이렇게 쓴다.

```sql
-- 시각 성분 때문에 0건이 나온다. 에러는 없다
WHERE order_delivered_carrier_date = DATE '2026-09-08'
```

`_at`으로 통일하면 "이건 시점이므로 범위 비교가 필요하다"가 이름에서 드러난다. 이 결함은 Olist 원본 CSV에서 물려받은 것이라 원천에서는 고칠 수 없다. Raw 호환 계약을 지켜야 하기 때문이다. Staging이 고치는 유일한 지점이다.

#### 5.2.1 `order_estimated_delivery_date`의 자정 값 검토

`order_estimated_delivery_date`의 시간 성분이 모두 `00:00:00`인 것은 날짜 전용 값일 가능성을
시사한다. 그러나 현재 Source 계약은 이 컬럼을 `TIMESTAMPTZ`로 정의하고 Bronze는 원천 타입을
그대로 보존한다. 따라서 Source/Bronze의 타입과 Staging 이름은 각각 `TIMESTAMPTZ`와
`estimated_delivery_at`을 유지한다.

일별 분석이 필요하면 원본 시각을 바꾸지 않고 Staging 이후에 UTC 기준 날짜를 파생한다.

```sql
CAST(estimated_delivery_at AT TIME ZONE 'UTC' AS DATE) AS estimated_delivery_date
```

향후 이 컬럼을 `DATE`로 바꾸려면 Source와 Bronze의 Breaking Change가 되므로 Schema Version 증가,
Migration/Backfill 범위, 날짜 기준 시간대를 ADR로 먼저 확정해야 한다. 현재 계약에는 이 변경을
포함하지 않는다.

### 5.3 Business Key 교체

가장 위험하고 가장 중요한 변환이다. PostgreSQL 실제 DDL은 다음과 같다.

```sql
CREATE TABLE customers (
    customer_id        VARCHAR(64) PRIMARY KEY,   -- 주문마다 새로 발급된다
    customer_unique_id VARCHAR(64) NOT NULL,      -- 사람 1명당 1개다
    ...
);
CREATE TABLE orders (
    customer_id VARCHAR(64) REFERENCES customers (customer_id)
);
```

Staging에서 두 이름을 맞바꾼다.

| PostgreSQL           | 실제 의미         | Staging              | 근거                                                  |
| -------------------- | ----------------- | -------------------- | ----------------------------------------------------- |
| `customer_id`        | 주문-고객 결합 키 | `source_customer_id` | 사람이 아니므로 `customer_id`라는 이름을 쓰면 안 된다 |
| `customer_unique_id` | 사람 식별자       | `customer_id`        | 분석에서 "고객"은 사람을 뜻한다                       |

이름을 그대로 두면 분석이 조용히 틀린다.

```sql
-- 원천 이름 그대로: 재구매 고객이 항상 0명이다
SELECT customer_id, COUNT(*) FROM orders
GROUP BY customer_id HAVING COUNT(*) > 1
```

`customer_id`가 주문마다 새로 생기므로 `COUNT(*)`는 항상 1이다. 결과는 빈 집합이고, 에러는 발생하지 않는다. 재구매율, LTV, 코호트 분석이 모두 같은 방식으로 틀린다.

교체 이후에는 의도대로 동작한다.

```sql
SELECT customer_id, COUNT(*) FROM marts.<주문 Fact>
GROUP BY customer_id HAVING COUNT(*) > 1
```

**대가**: `stg_orders`는 `customer_id`를 직접 얻지 못한다. 원천 `orders.customer_id`는 결합 키일 뿐이므로, 사람 ID를 얻으려면 `stg_customers_current`를 `source_customer_id`로 Join해야 한다.

**이력 추적과의 연결**: 사람 단위 이력을 추적하려면 Business Key가 사람을 식별해야 한다. 결합 키로 이력을 만들면 Version이 항상 1개씩 생겨 추적이 무의미해진다. Warehouse의 이력 추적 구현은 [Mart Grain 계약](mart-grain.md)이 정본이다.

### 5.4 사람 단위 집계

`customers`는 계정 불변 테이블이므로 `created_at`, `_ingested_at`, `_batch_id`로 같은 계정의
Current Bronze 행만 고른다. `stg_customers_current`는 모든 `source_customer_id`마다 한 행을
유지하며 해당 계정의 `city`·`state`를 주문 배송지 스냅샷으로 전달한다. 분석 고객 키는
`customer_unique_id`이고, 사람 단위 구독 상태·등급 이력은 별도 `customer_subscriptions`/`customer_membership_tiers` 관측에서 만든다.

### 5.5 나머지 이름 변환

| PostgreSQL           | Staging            | 근거                                                                                                    |
| -------------------- | ------------------ | ------------------------------------------------------------------------------------------------------- |
| `payment_sequential` | `payment_sequence` | 값은 1, 2, 3인 순번(명사)인데 이름은 형용사다. `WHERE payment_sequential = 1`은 "순차적인 = 1"로 읽힌다 |

### 5.6 상태값 표준화

PostgreSQL DDL의 CHECK 제약이 허용하는 값이 원천 Domain이다.

```sql
order_status IN ('created','approved','processing','invoiced',
                 'shipped','delivered','canceled','unavailable')
payment_status IN ('pending','completed','failed','refunded')
subscription_status IN ('ACTIVE','PAYMENT_FAILED','CANCEL_REQUESTED','CHURNED')
membership_tier IN ('BRONZE','SILVER','GOLD')
```

`subscription_status`와 `membership_tier`는 원천에서부터 이미 대문자다. `order_status`/`payment_status`만 소문자 원천을 Staging에서 대문자로 바꾼다.

Staging은 원천 8개 주문 상태를 축약하지 않고, 대문자 `order_status`로 표준화해 보존한다.

- order

  | 원천 값       | `order_status` | 근거                                      |
  | ------------- | -------------- | ----------------------------------------- |
  | `created`     | `CREATED`      | 주문 생성 직후다                          |
  | `approved`    | `APPROVED`     | 결제 승인이다                             |
  | `processing`  | `PROCESSING`   | 배송 전 세부 운영 상태다                  |
  | `invoiced`    | `INVOICED`     | 송장 발행 상태다                          |
  | `shipped`     | `SHIPPED`      | 발송이다                                  |
  | `delivered`   | `DELIVERED`    | 배송 완료다. 기본 GMV 기준이다            |
  | `canceled`    | `CANCELED`     | 취소다                                    |
  | `unavailable` | `UNAVAILABLE`  | 재고 부재로 주문을 이행할 수 없는 상태다  |

- payment

  | 원천 값     | Staging 값  |
  | ----------- | ----------- |
  | `pending`   | `PENDING`   |
  | `completed` | `COMPLETED` |
  | `failed`    | `FAILED`    |
  | `refunded`  | `REFUNDED`  |

  구독 결제도 같은 `standardized_payment_status`를 사용해 원천의 `completed`·`failed`를 `COMPLETED`·`FAILED`로 변환한다. 구독 결제의 허용 상태는 이 두 값으로 제한한다.

- subscription / tier

  `subscription_status`(`ACTIVE`/`PAYMENT_FAILED`/`CANCEL_REQUESTED`/`CHURNED`)와 `membership_tier`(`BRONZE`/`SILVER`/`GOLD`)는 원천 값 자체가 이미 대문자 표준 표기다. Staging은 원천 열을 그대로 노출한다. Bronze의 `status_domains`(허용 상태 목록)와 Staging의 `not_null`·`accepted_values`가 미허용 상태를 차단한다. `order_status`/`payment_status`와 달리 소문자→대문자 변환 구간이 아니다.

**왜 대문자인가**: 소문자는 원천 값, 대문자는 Staging 표준 상태다. Model에서 `= 'delivered'`를 보면 원천을 직접 참조하는 실수이고, `= 'DELIVERED'`면 Staging 이후다. 규칙이 눈에 보이므로 리뷰에서 잡을 수 있다.

**왜 세부 상태를 보존하는가**: `PROCESSING`과 `INVOICED`, `CANCELED`와 `UNAVAILABLE`은 운영 원인이 다르다. `order_status`에 8개 상태를 보존하면 송장 발행 후 배송 지연, 재고 부재 취소율 같은 분석을 Staging 이후에도 수행할 수 있다. Bronze는 감사·Replay용 원본이고, 세부 운영 분석의 유일한 경로가 되어서는 안 된다.

**분석 그룹은 언제 만드는가**: 기본 GMV처럼 `order_status = 'DELIVERED'`만 필요한 지표에는 그룹이 필요 없다. "배송 전"처럼 여러 상태를 묶는 집계가 여러 Model이나 대시보드에서 반복될 때만 dbt Macro 또는 Mapping Seed로 조건을 재사용한다. 이 규칙은 집계 시점에 적용하며, `order_status_group` 컬럼을 Staging이나 Fact에 저장하지 않는다.

### 5.7 바뀌지 않는 것

| 컬럼                                      | 유지 근거                                                             |
| ----------------------------------------- | --------------------------------------------------------------------- |
| `order_id`, `product_id`, `seller_id`     | 접두사가 곧 엔티티 이름이다. 제거하면 `id`가 되어 Join에서 모호해진다 |
| `price`, `freight_value`, `payment_value` | 접두사가 없고 이미 명사다                                             |
| `order_item_id`                           | `(order_id, order_item_id)` 복합 PK의 일부다. 원천 Grain을 유지한다   |
| `payment_type`                            | 접두사가 의미를 가진다. `type`만으로는 무엇의 종류인지 알 수 없다     |
| `_batch_id`, `_ingested_at`               | Current 선택과 이력 관측 정렬 Tie-breaker가 사용한다                  |

---

## 6. 왜 변환을 Staging에서만 하는가

이름 변환의 위치를 다르게 잡으면 각각 다른 방식으로 깨진다.

| 변환 위치                 | 깨지는 것                                                                     |
| ------------------------- | ----------------------------------------------------------------------------- |
| Source (원천 컬럼명 변경) | Olist Raw 호환 계약 위반이다. Seed 재적재가 불가능해진다                      |
| Bronze (수집 시 Rename)   | 원천 대조 검증이 불가능하다. Replay가 깨진다. 과거 Object와 Schema가 갈라진다 |
| **Staging**               | **깨지지 않는다. Bronze는 원본을 보존하고, Rename은 한 곳에만 있다**          |
| Intermediate 이후         | 같은 Rename이 Model마다 흩어진다. 한쪽만 고치면 불일치가 생긴다               |

Phase 5 문서의 "Intermediate/Mart는 Raw Source Prefix를 직접 참조하지 않는다" 계약이 이 규칙을 강제하고, AC-19가 검증한다.

---

## 7. Staging → Intermediate → Mart: Grain을 맞추고 확정하는 구간

이름은 바뀌지 않는다. 바뀌는 것은 Grain이다.

Staging은 원천 Grain을 그대로 유지한다. Intermediate가 Join과 사전 집계로 목표 Grain에
맞추고, Mart가 그 Grain을 Unique Key로 확정한다.

| 구간                   | Grain 책임                                                        |
| ---------------------- | ------------------------------------------------------------------ |
| Staging → Intermediate | Join과 사전 집계로 Grain을 목표 Grain에 맞춘다                     |
| Intermediate → Mart    | 문서화된 Grain과 Unique Key로 고정한다. Mart는 계산하지 않고 투영한다 |

### 7.1 왜 사전 집계가 필요한가

Grain이 다른 두 입력을 Fact에서 직접 Join하면 양쪽 행 수가 곱해져 Measure가 부풀려진다.

```sql
-- 잘못된 방식: Item 3행 × Payment 2행 = 6행이 되어 양쪽 합계가 부풀려진다
SELECT o.order_id, SUM(i.price), SUM(p.payment_value)
FROM orders o JOIN order_items i USING (order_id)
              JOIN order_payments p USING (order_id)
GROUP BY o.order_id
```

Item 3행, Payment 2행인 주문에서 Join 결과는 6행이 된다. `SUM(i.price)`는 실제의 2배,
`SUM(p.payment_value)`는 3배가 된다. 에러는 나지 않는다.

올바른 방식은 각각을 먼저 목표 Grain으로 접은 뒤 1:1로 Join하는 것이다. 사전 집계와 파생
계산은 Intermediate에서 끝내므로, Fact의 Grain·Key·Measure 제공 책임과 집계·계산 책임이
섞이지 않는다.

### 7.2 Mart 설계는 어디에 있는가

Dimension·Fact·Report Model의 Grain, Unique Key, Measure 계약, Model 간 관계, 이력 추적
방식은 이 문서가 아니라 [Mart Grain 계약](mart-grain.md)이 정본이다. 이 문서는 Source에서
Staging까지의 이름·타입·값 변환을 다루고, Mart 설계는 다루지 않는다.

---

## 8. 한 주문의 전체 여정

`order_purchase_timestamp = 2026-09-05T10:00:00Z`인 주문 1건이 2026-09-08 Batch에 수집되는 경우다.

| 계층         | 위치                                                                                                     | 형태                                                                                                                                   |
| ------------ | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Source       | `commerce_source.orders`                                                                                 | `order_id='o1'`, `customer_id='c1'`, `order_status='delivered'`, `order_purchase_timestamp='2026-09-05T10:00:00Z'`                     |
| Bronze       | `bronze/orders/ingestion_date=2026-09-08/batch_id=warehouse_pipeline_dag__20260908T000000Z/data.parquet` | 위와 동일한 컬럼명 + `_batch_id`, `_ingested_at`, `_schema_version=3`                                                                  |
| Catalog      | `control.bronze_files`                                                                                   | `source_table='orders'`, `object_key='bronze/orders/...'`, `schema_version=3`                                                          |
| Staging      | `staging.stg_orders`                                                                                     | `order_id='o1'`, `source_customer_id='c1'`, `customer_id='u1'`(Join), `order_status='DELIVERED'`, `purchase_at='2026-09-05T10:00:00Z'` |
| Intermediate | `intermediate.*`                                                                                          | Staging 값을 목표 Grain으로 접고 파생 값을 계산한다                                                                                    |
| Mart         | `dimensions.*`, `facts.*`, `metrics.*`                                                                    | 확정된 Grain의 행으로 투영된다. Model과 컬럼은 [Mart Grain 계약](mart-grain.md) 참조                                                  |
| Published    | `data/warehouse/warehouse.duckdb`                                                                         | 같은 행. `mart_publish_runs` Row가 `PUBLISHED`가 된 Build 파일이다                                                                     |
| Serving      | `data/serving/exports/{export_id}.duckdb`                                                                 | 같은 행. Mart 3개 Schema만 있고 `main.serving_manifest`에 `publish_run_id`가 기록된다                                                  |

`ingestion_date`는 2026-09-08이지만 `purchase_at`은 2026-09-05다. 이 분리 덕분에 Late Arrival 주문이 과거 Business Date의 Fact로 정확히 들어간다.

---

## 9. Mart 이후 실행 순서

`source_simulation_dag`가 Generator를 실행한 뒤 `trigger_warehouse_pipeline`으로 같은 Logical Date의 `warehouse_pipeline_dag`를 시작한다. `warehouse_pipeline_dag`는 `schedule=None`, `max_active_runs=1`이다. Task 순서는 다음과 같다.

```text
initialize_run → acquire_source_snapshot_lease → extract_validate_load ×9 ─┬→ release_source_snapshot_lease ─┐
                                                                            └→ verify_bronze_commit_task      │
   → prepare_warehouse_build → dbt_build → publish_mart → export_serving_mart → repoint_metabase_serving     │
   → publish_run_summary (trigger_rule=all_done, release 이후) ←─────────────────────────────────────────────┘
```

| Task | 재시도 | 하는 일 | 실패 시 |
| --- | --- | --- | --- |
| `prepare_warehouse_build` | 기본값(2회) | 활성 Publish Run을 잡고 Published 파일을 Build 파일로 복사한 뒤 Catalog를 동기화한다 | 복사·동기화 실패는 Run `FAILED`, Build 파일 격리. 다른 활성 Run이 있으면 `PublishInProgressError`. Published 파일 변화 없음 |
| `dbt_build` | 0 | Build 파일에만 `dbt build`를 실행한다 | `DBT_BUILD_ERROR`/`DBT_TEST_ERROR`로 `FAILED`, Build 파일 `failed/` 격리 |
| `publish_mart` | 0 | CHECKPOINT, Hash 기록, `os.replace`로 Published 교체 | 교체 전이면 Published 변화 없음 |
| `export_serving_mart` | 0 | Published에서 Serving 파일을 만든다 | `SERVING_EXPORT_FAILED`. Publish 결과는 `PUBLISHED`로 남는다 |
| `repoint_metabase_serving` | 기본값(2회) | Metabase 연결을 새 Export로 바꾸고 검증한다 | `METABASE_REPOINT_FAILED`. 옛 Export 정리 안 함 |
| `publish_run_summary` | 기본값(2회) | Summary와 운영 기록을 남긴다 | 운영 기록 수집·저장 실패는 DAG를 실패시키지 않는다 |

dbt·Publish·Export를 재시도하지 않는 이유는 부분 실행 결과를 덮어쓰지 않고 실패 Build를 원인 분석용으로 보존하기 위해서다. 실패 Error Type별 조치는 [Warehouse Publish 오류 유형](../troubleshooting/warehouse-publish.md)이 정본이다.

---

## 10. Build와 Publish: 검증된 파일만 교체하는 구간

[ADR 016](../adr/016-publish-mart-via-warehouse-file-swap.md)의 Build-then-swap이다. dbt는 Published 파일을 직접 고치지 않는다.

```text
data/warehouse/warehouse.duckdb ──copy──→ data/warehouse/build/{publish_run_id}.duckdb
                                                  │ sync_bronze_catalog + dbt build
                                                  ↓ 성공
data/warehouse/warehouse.duckdb ←──os.replace──── (같은 파일)
                                                  ↓ 실패
                                     data/warehouse/failed/{publish_run_id}.duckdb (최신 3개 보존)
```

### 10.1 Publish Run 상태

`pipeline_metadata.mart_publish_runs`가 Publish의 권한 기록이다.

| 상태 | 의미 | 진입 지점 |
| --- | --- | --- |
| `BUILDING` | Build 파일 준비와 dbt 실행 중 | `prepare_warehouse_build`의 `start_publish_run` |
| `PUBLISHING` | Hash·파일 Identity를 기록했고 교체 직전 | `publish_build`의 `mark_publishing` |
| `PUBLISHED` | 교체와 Directory fsync 완료 | `mark_published` |
| `FAILED` | `error_type`, `error_message`, `failed_path` 기록 | `_fail`, 회수 |

전이는 `BUILDING → PUBLISHING → PUBLISHED`, `BUILDING/PUBLISHING → FAILED`, 그리고 만료 회수 때 Published 파일 Identity가 일치하는 `PUBLISHING → PUBLISHED`(10.2절)뿐이다. `mart_publish_runs_single_active_idx`가 `BUILDING`·`PUBLISHING` Row를 전역 1개로 제한한다. 다른 활성 Run이 있으면 `PublishInProgressError`다.

### 10.2 Heartbeat Lease와 회수

| 장치 | 동작 | 판정 문서 |
| --- | --- | --- |
| Heartbeat | 준비·dbt·Publish 단계 동안 30초마다 `heartbeat_at`을 갱신한다. 갱신이 거부되면 소유권 상실로 보고 다음 상태 전이가 막힌다 | [061](../architect-review/061_publish-heartbeat-lease.md) |
| 만료 회수 | 다음 준비 단계가 마지막 Heartbeat(없으면 시작 시각) 뒤 5분(`PUBLISH_LEASE`)이 지난 활성 Run을 닫는다 | [061](../architect-review/061_publish-heartbeat-lease.md) |
| 같은 DagRun 회수 | `prepare_warehouse_build` 재시도는 같은 `(pipeline_name, dag_run_id)`의 `BUILDING` Row를 Heartbeat와 무관하게 즉시 `FAILED`로 닫는다. `PUBLISHING` Row와 `dag_run_id`가 없는 CLI Run은 대상이 아니다 | [061](../architect-review/061_publish-heartbeat-lease.md) |
| 파일 Identity | `PUBLISHING` 진입 때 Build 파일의 inode·`mtime_ns`를 기록한다. 만료된 `PUBLISHING` Run은 Published 파일 Identity가 기록과 같을 때만 `PUBLISHED`로 확정하고, 다르면 `FAILED`로 닫는다. 재Hash 없이 교체 완료 여부를 판정한다 | [058](../architect-review/058_publish-file-identity.md) |

회수로 `FAILED`가 된 Run은 `error_type=UNKNOWN_ERROR`로 남고, Build 파일은 `failed/`로 격리된다. `PUBLISHED`로 확정된 Run은 남은 Build 잔여물만 지운다.

---

## 11. Serving Export: BI 읽기를 Warehouse와 분리하는 구간

[ADR 019](../adr/019-serving-file-visibility-for-metabase.md)가 근거다. Metabase가 Published Warehouse를 직접 열면 Publish 교체와 읽기가 같은 파일에서 충돌하고, Metabase가 이미 연 파일의 옛 내용을 계속 볼 수 있다. 그래서 Mart만 담은 별도 파일을 Export마다 새 경로로 만든다.

`export_serving_mart`(`src/serving/export.py`) 순서:

1. Published 파일의 inode·`mtime_ns`가 `publish_mart` 결과와 같은지 확인한다. 다르면 거부한다.
2. `data/serving/build/{export_id}.duckdb`에 `dimensions`·`facts`·`metrics`의 모든 Table·View를 Table로 복사한다.
3. 같은 파일의 `main.serving_manifest`에 `export_id`, `publish_run_id`, `mart_result_hashes`, `row_counts`를 한 행으로 기록한다.
4. Schema 경계(Mart 3개 + `main.serving_manifest`만)를 확인하고, CHECKPOINT 뒤 WAL 부재를 확인하고, Identity를 한 번 더 확인한다.
5. `os.link`로 `data/serving/exports/{export_id}.duckdb`를 먼저 만들고, 그다음 `os.replace`로 `data/serving/mart.duckdb`를 교체한다. 두 경로는 같은 inode다. 마지막으로 두 Directory를 fsync한다.

| 실패 지점 | 결과 |
| --- | --- |
| 1~4단계 | Build 파일 삭제. `mart.duckdb`·`exports/` 변화 없음 |
| 5단계 `os.replace` 실패 | 새 `exports/` Link 삭제. `mart.duckdb`는 이전 파일 그대로 |
| 5단계 `os.replace` 뒤 fsync 실패 | 새 `exports/` Link 삭제. `mart.duckdb`는 이미 새 파일이다. Metabase는 `exports/` 경로를 읽으므로 영향이 없고, 재실행으로 회복한다 |
| DAG Task 예외 | `AirflowFailException SERVING_EXPORT_FAILED`, 재시도 없음. 재지정 Task는 `upstream_failed` |

Export는 PostgreSQL에 행을 쓰지 않는다. 어느 Publish Run을 담았는지는 파일 안 `serving_manifest`가 유일한 기록이다. CLI `python -m src.serving export`도 같은 두 함수(Export, 재지정·정리)를 같은 순서로 호출한다([059](../architect-review/059_serving-refresh-single-path.md)).

---

## 12. Metabase 재지정과 최신성

`repoint_and_prune_serving`(`src/serving/metabase_repoint.py`) 순서:

1. `METABASE_URL`, `METABASE_API_KEY`, `METABASE_SERVING_DATABASE_ID` 중 하나라도 없으면 `SKIPPED`.
2. Metabase Database의 `details.database_file`을 `/serving/exports/{export_id}.duckdb`로 바꾼다.
3. `/api/dataset`으로 `SELECT export_id FROM serving_manifest`를 실행해 새 `export_id`가 보이는지 검증한다.
4. `sync_schema`를 요청한다. 실패해도 경고만 남기고 결과는 `SUCCESS`다([060](../architect-review/060_metabase-layer-cleanup.md)).
5. 결과가 `SUCCESS`·`SKIPPED`이면 `exports/`에서 mtime 기준 최신 3개(`SERVING_EXPORT_RETENTION`)만 남긴다.

| 결과 | DAG | 옛 Export 정리 |
| --- | --- | --- |
| `SUCCESS` | Task 성공 | 함 |
| `SKIPPED` | Task 성공 | 함 |
| `FAILED` | `METABASE_REPOINT_FAILED`, 기본 재시도 | 안 함. Metabase가 아직 옛 Export를 가리킬 수 있기 때문이다([050](../architect-review/050_serving-export-retention-vs-repoint-failure.md), [051](../architect-review/051_metabase-repoint-retry-semantics.md)) |

최신성은 `check_serving_freshness`(`src/serving/freshness.py`)가 판정한다. Metabase를 거쳐 실제 연결된 파일의 `serving_manifest.publish_run_id`를 읽고, `mart_publish_runs`의 최신 `PUBLISHED` Run과 같으면 `fresh=true`다. 파일을 직접 열지 않고 Metabase를 거치므로 Metabase 내부 Connection이 옛 파일을 잡고 있는 경우도 잡힌다([059](../architect-review/059_serving-refresh-single-path.md)). 수동 확인은 `python -m src.serving status`이고, 최신이면 종료 코드 0, 아니면 1이다.

---

## 13. 운영 기록: DagRun마다 한 행

`publish_run_summary`는 `trigger_rule=all_done`이고 `release_source_snapshot_lease` 뒤에 실행되므로, 앞 Task가 실패해도 실행된다. 기존 Summary(Bronze 검증 수, Publish 상태, Export·재지정 상태)에 운영 값을 더해 두 곳에 남긴다.

- Task Log: `RUN_REPORT {JSON}` 한 줄
- `pipeline_metadata.warehouse_run_reports`: `dag_run_id` 기준 Upsert 한 행(`ok`, `failures`, `report`)

`src/observability/run_report.py`의 `collect_run_report`는 DagRun 경과 시간, Task 상태, Ingestion 결과, Lease 잔존, Bronze Object, 활성 Publish Run, 최신성, 게시 데이터 지연, 직전 게시 대비 행 수, 파일 개수·크기를 각각 독립적으로 수집한다. 한 수집이 실패하면 `PROBE_ERROR:<이름>`으로만 남고 나머지는 계속된다. `record_run_report`는 저장 실패를 `RUN_REPORT_WRITE_FAILED` 로그로만 남긴다. 운영 기록 때문에 DAG가 실패하지 않는다.

`failures`에 들어가는 코드:

| 코드 | 조건 |
| --- | --- |
| `TASK_NOT_SUCCESS:<task_id>` | 종료 Task 중 `success`가 아닌 것 |
| `PUBLISH_<status>` | Publish 상태가 `PUBLISHED`가 아님 |
| `SERVING_EXPORT_FAILED` | Export가 `SUCCESS`가 아님 |
| `REPOINT_<status>` | 재지정이 `SUCCESS`가 아님. `SKIPPED`도 실패로 본다 |
| `NOT_FRESH` | 최신성 검사가 `fresh=false` |
| `DATA_STALE` | 게시 파일의 최신 주문 구매 시각이 수집 시각보다 원천 DAG 간격의 2배 넘게 뒤처짐 |
| `ROW_COUNT_DROP:<relation>` | 이번 PUBLISHED Run의 행 수가 직전 PUBLISHED Run보다 1% 넘게 감소. Full Refresh Run은 제외. Publish는 막지 않는다 |
| `EXPORTS_COUNT_<n>` | `exports/` 파일 수가 보존 개수(3)와 다름 |
| `FAILED_BUILDS_OVER_RETENTION` | `failed/` 파일 수가 보존 개수(3) 초과 |
| `BUILD_DIR_NOT_EMPTY` | Warehouse 또는 Serving `build/`에 잔여 파일 |
| `LEASE_HELD` | Table Lease 또는 원천 변경 Lease가 남아 있음 |
| `SLOW_RUN` | DagRun 경과 600초 이상 |
| `PROBE_ERROR:<probe>` | 해당 수집 실패 |

일정 기간의 합격 여부는 `sql/validation/warehouse_run_report_window.sql`로 한 번에 본다.

```bash
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$PIPELINE_METADATA_DB" -P pager=off' \
  < sql/validation/warehouse_run_report_window.sql
```

창 길이(`window_size`)와 실행 간격(`slot_size`)은 파일 첫 View에서 정한다. Source DAG 일정을 바꾸면 `slot_size`도 함께 바꾼다. 결과는 기대 Slot 수, 기록 수, 정상·비정상 실행 수, 누락 Slot 수, 경과 시간 p95·최대값, 실패 코드별 건수와 `verdict`다. 누락 Slot 0, 비정상 실행 0, p95 300초 이하이면 `PASS`다. 이어지는 상세 결과는 비정상 실행 목록과, 누락 Slot별 `generator_runs` 상태다. Warehouse DagRun이 시작되지 않은 `source_simulation_dag` 실패는 이 누락 Slot으로 드러난다.

---

## 14. 요약

| 원칙                                       | 내용                                                  |
| ------------------------------------------ | ----------------------------------------------------- |
| Bronze는 원천을 보존한다                   | Replay, Schema 진화, 감사를 가능하게 한다             |
| 이름 변환은 Staging에서 1회만 한다         | 원본 보존과 중복 제거를 동시에 만족하는 유일한 위치다 |
| Catalog가 dbt 입력을 통제한다              | Prefix Glob은 Commit되지 않은 Object를 섞는다         |
| Grain은 Intermediate에서 맞춘다            | Fan-out 없이 Measure를 계산한다                       |
| Mutable 원천은 Bronze 누적으로 이력이 된다 | Warehouse가 시점별 유효 값을 복원할 수 있는 전제다    |
| dbt는 Build 파일에서만 실행한다            | 실패한 Build는 격리되고 Published 파일은 그대로다     |
| Publish 활성 Run은 하나뿐이다              | Heartbeat Lease와 파일 Identity로 중단된 Run을 회수한다 |
| BI는 Export마다 새 Serving 파일을 읽는다   | Publish 교체와 BI 읽기가 같은 파일에서 충돌하지 않는다 |
| 재지정 실패 시 옛 Export를 지우지 않는다   | Metabase가 가리키는 파일이 사라지지 않는다            |
| DagRun마다 운영 기록 1행을 남긴다          | 기록 실패가 DAG를 실패시키지 않고, 창 단위 SQL 하나로 합격 여부를 본다 |

---

## 관련 문서

- [Phase 3. Incremental Ingestion](../phases/phase-03-incremental-ingestion.md) — Source에서 Bronze까지의 Commit Protocol
- [Phase 4. Airflow Orchestration](../phases/phase-04-airflow-orchestration.md) — 각 구간의 실행 경계와 Lease
- [Phase 5. Bronze Catalog + Staging](../phases/phase-05-bronze-catalog-and-staging.md) — Source에서 Staging까지의 Model 구현
- [Phase 6. Dimensional Modeling](../phases/phase-06-dimensional-modeling.md) — Staging 이후의 Intermediate와 Mart
- [Phase 7. Data Quality & Publish](../phases/phase-07-data-quality-publish.md) — Build-then-swap Publish와 품질 Gate
- [Phase 10. BI](../phases/phase-10-bi.md) — Serving Export와 Metabase 연결
- [Mart Grain 계약](mart-grain.md) — Mart의 Grain, Unique Key, Measure 계약, 이력 추적 구현
- [ADR 016](../adr/016-publish-mart-via-warehouse-file-swap.md) — Warehouse File 교체로 Mart를 Publish한다
- [ADR 019](../adr/019-serving-file-visibility-for-metabase.md) — Metabase의 Serving 파일 교체 가시성
- [Warehouse Publish 오류 유형](../troubleshooting/warehouse-publish.md) — Publish Error Type별 원인과 조치
- [Metabase Serving 연결](../../metabase/README.md) — Metabase 등록, 재지정 CLI, 최신성 확인
- Architect 판정: [058 Publish 파일 Identity](../architect-review/058_publish-file-identity.md), [059 Serving 갱신 경로 단일화](../architect-review/059_serving-refresh-single-path.md), [060 Metabase 계층 정리](../architect-review/060_metabase-layer-cleanup.md), [061 Publish Heartbeat Lease](../architect-review/061_publish-heartbeat-lease.md)
- [PRD v1.28](../../PRD_v1.28.md) — Section 7.1 상태 Mapping, Section 14 dbt Model, Section 15 이력 추적, Section 17 Data Quality와 Publish, Section 18 Observability
