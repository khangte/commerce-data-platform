# 데이터 변환 흐름: Source → Data Lake → Data Warehouse

> 상태: Reference
> 기준 문서: [PRD v1.10](../../PRD_v1.10.md), [Phase 3](../phases/phase-03-incremental-ingestion.md), [Phase 4](../phases/phase-04-airflow-orchestration.md), [Phase 5](../phases/phase-05-bronze-catalog-and-staging.md)

이 문서는 하나의 주문 레코드가 PostgreSQL 원천에서 DuckDB Mart에 도달할 때까지 이름, 타입, 값, Grain이 어느 지점에서 왜 바뀌는지를 정리한다. 각 변환의 근거와, 그 변환을 다른 지점에서 했을 때 무엇이 깨지는지를 함께 기록한다.

## 1. 전체 흐름

```text
PostgreSQL commerce_source          OLTP 원천
        ↓ Phase 3 ingest_table()    이름 보존, 기술 컬럼만 추가
SeaweedFS bronze/*.parquet          Data Lake (불변 Object)
        ↓ Phase 3 sync_bronze_catalog()
DuckDB control.bronze_files         Commit된 Object 목록
        ↓ Phase 5 dbt staging       이름·값 변환이 일어나는 유일한 지점
DuckDB staging.*                    분석 Naming (View)
        ↓ Phase 5 dbt intermediate  Join과 사전 집계
DuckDB intermediate.*               (View)
        ↓ Phase 5 dbt marts         Grain 확정
DuckDB marts.*                      Data Warehouse (Table / Incremental)
```

각 구간의 책임은 겹치지 않는다.

| 구간                   | 이름     | 타입       | 값         | Grain    | 근거                                             |
| ---------------------- | -------- | ---------- | ---------- | -------- | ------------------------------------------------ |
| Source → Bronze        | 보존     | 고정       | 보존       | 보존     | 원천 무손실 복제로 재수집 없는 Replay를 보장한다 |
| Bronze → Catalog       | 보존     | 보존       | 보존       | 보존     | COMMITTED Object 목록만 전달한다                 |
| Catalog → Staging      | **변환** | **정규화** | **표준화** | 보존     | 분석 Naming으로 바꾸는 단일 지점이다             |
| Staging → Intermediate | 보존     | 보존       | 보존       | **변경** | Join과 사전 집계로 Grain을 맞춘다                |
| Intermediate → Mart    | 보존     | 보존       | 보존       | 확정     | 문서화된 Grain과 Unique Key로 고정한다           |

이름 변환이 Staging 한 곳에만 있는 것이 이 설계의 핵심이다. 근거는 6절에서 다룬다.

---

## 2. 계층별 저장 형태

| 계층      | 저장소                       | 물리 형태      | 변경 가능성             | 조회 주체                          |
| --------- | ---------------------------- | -------------- | ----------------------- | ---------------------------------- |
| Source    | PostgreSQL `commerce_source` | Row 지향 Table | Mutable (UPDATE)        | Generator, Ingestion Extract       |
| Data Lake | SeaweedFS (S3 호환)          | Parquet Object | Immutable (Append Only) | Ingestion Commit, dbt Source Macro |
| Catalog   | DuckDB `control`             | Table          | 매 DagRun 재동기화      | dbt Source Macro                   |
| Warehouse | DuckDB `staging`~`marts`     | View / Table   | Incremental 교체        | 분석가, BI                         |

Source가 Mutable이고 Lake가 Immutable인 것이 계층을 나누는 근본 이유다. 원천의 `UPDATE`는 이전 값을 지우지만, Lake는 Batch마다 새 Object를 쌓으므로 과거 상태가 남는다. 이 차이가 시점별 이력 복원을 가능하게 한다.

---

## 3. Source → Bronze: 이름을 바꾸지 않는 구간

Phase 3 `ingest_table()`이 담당한다. `src/ingestion/tables.py`의 `TableConfig.source_columns`가 계약이다.

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
| `_schema_version` | `int32`              | `1`                            | 지원하지 않는 Version을 dbt 이전에 차단한다                 |

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

Phase 3 `sync_bronze_catalog()`가 담당하고, Phase 4 `warehouse_pipeline_dag`의 `sync_bronze_catalog_task`가 호출한다.

```text
pipeline_metadata.bronze_objects (status = 'COMMITTED')
        ↓
DuckDB control.bronze_files
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
subscription_status IN ('NON_MEMBER','TRIAL','ACTIVE','PAYMENT_FAILED',
                        'CANCEL_REQUESTED','CHURNED')
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

- subscription / tier

  `subscription_status`(`NON_MEMBER`/`TRIAL`/`ACTIVE`/`PAYMENT_FAILED`/`CANCEL_REQUESTED`/`CHURNED`)와 `membership_tier`(`BRONZE`/`SILVER`/`GOLD`)는 원천 값 자체가 이미 대문자 표준 표기다. Staging Macro(`standardized_subscription_status`, `standardized_membership_tier`)는 값을 바꾸지 않고 허용 목록 검증만 수행한다. `order_status`/`payment_status`와 달리 소문자→대문자 변환 구간이 아니다.

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
| Bronze       | `bronze/orders/ingestion_date=2026-09-08/batch_id=warehouse_pipeline_dag__20260908T000000Z/data.parquet` | 위와 동일한 컬럼명 + `_batch_id`, `_ingested_at`, `_schema_version=2`                                                                  |
| Catalog      | `control.bronze_files`                                                                                   | `source_table='orders'`, `object_key='bronze/orders/...'`, `schema_version=2`                                                          |
| Staging      | `staging.stg_orders`                                                                                     | `order_id='o1'`, `source_customer_id='c1'`, `customer_id='u1'`(Join), `order_status='DELIVERED'`, `purchase_at='2026-09-05T10:00:00Z'` |
| Intermediate | `intermediate.*`                                                                                          | Staging 값을 목표 Grain으로 접고 파생 값을 계산한다                                                                                    |
| Mart         | `marts.*`                                                                                                 | 확정된 Grain의 행으로 투영된다. Model과 컬럼은 [Mart Grain 계약](mart-grain.md) 참조                                                  |

`ingestion_date`는 2026-09-08이지만 `purchase_at`은 2026-09-05다. 이 분리 덕분에 Late Arrival 주문이 과거 Business Date의 Fact로 정확히 들어간다.

---

## 9. 요약

| 원칙                                       | 내용                                                  |
| ------------------------------------------ | ----------------------------------------------------- |
| Bronze는 원천을 보존한다                   | Replay, Schema 진화, 감사를 가능하게 한다             |
| 이름 변환은 Staging에서 1회만 한다         | 원본 보존과 중복 제거를 동시에 만족하는 유일한 위치다 |
| Catalog가 dbt 입력을 통제한다              | Prefix Glob은 Commit되지 않은 Object를 섞는다         |
| Grain은 Intermediate에서 맞춘다            | Fan-out 없이 Measure를 계산한다                       |
| Mutable 원천은 Bronze 누적으로 이력이 된다 | Warehouse가 시점별 유효 값을 복원할 수 있는 전제다    |

---

## 관련 문서

- [Phase 3. Incremental Ingestion](../phases/phase-03-incremental-ingestion.md) — Source에서 Bronze까지의 Commit Protocol
- [Phase 4. Airflow Orchestration](../phases/phase-04-airflow-orchestration.md) — 각 구간의 실행 경계와 Lease
- [Phase 5. Bronze Catalog + Staging](../phases/phase-05-bronze-catalog-and-staging.md) — Source에서 Staging까지의 Model 구현
- [Phase 6. Dimensional Modeling](../phases/phase-06-dimensional-modeling.md) — Staging 이후의 Intermediate와 Mart
- [Mart Grain 계약](mart-grain.md) — Mart의 Grain, Unique Key, Measure 계약, 이력 추적 구현
- [PRD v1.10](../../PRD_v1.10.md) — Section 7.1 상태 Mapping, Section 14 dbt Model, Section 15 이력 추적 요구사항
