# Phase 5. Bronze Catalog + Staging

> 상태: Done  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 4. Airflow Orchestration](phase-04-airflow-orchestration.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.9](../../PRD_v1.9.md)
> 참고: [데이터 변환 흐름](../reference/data-transformation-flow.md) — 계층별 이름·타입·값 변환의 근거

## 목표

Metadata에서 COMMITTED인 Bronze Object만 읽어 DuckDB Staging까지 구축한다. Source Naming을 분석 Naming으로 바꾸고 상태값을 표준화하는 단일 지점을 만든다.

이 Phase는 Source Schema만 입력으로 받는다. Grain 설계를 필요로 하지 않으므로 Dimensional Modeling보다 먼저 완료할 수 있다. Staging은 원천 Grain을 그대로 보존하며, Grain을 바꾸는 작업은 [Phase 6](phase-06-dimensional-modeling.md)이 담당한다.

## 핵심 계약

- S3 Prefix Glob이 아니라 `control.bronze_files`의 COMMITTED Object 목록만 읽는다.
- Source Prefix 제거, Timestamp Rename, 상태 표준화는 Staging에서 처음 수행한다. 이 변환은 Staging에만 존재한다.
- Staging은 원천 Grain을 바꾸지 않는다. Join으로 Grain이 늘어나거나 집계로 줄어드는 작업은 하지 않는다.
- Bronze 기술 컬럼 `_batch_id`, `_ingested_at`은 Staging에서 보존한다. Current 선택과 이력 관측 정렬의 Tie-breaker가 이 컬럼을 사용한다.
- Mutable Entity는 `updated_at`, `_ingested_at`, `_batch_id` 순으로 Bronze Version 중 Current를 결정한다.

## 선행 조건

- Phase 3의 `sync_bronze_catalog`가 COMMITTED Bronze만 `control.bronze_files`에 동기화한다.
- Phase 4 Warehouse DAG가 `sync_bronze_catalog_task`를 실행한다.
- 지원 가능한 Bronze `schema_version` 목록이 정의됐다.
- Phase 1의 Source Schema가 확정됐다.

## Warehouse 구조

```text
data/warehouse/warehouse.duckdb

control       table
staging       view
```

Intermediate 이후 계층의 Schema와 Materialization은 Phase 6에서 정의한다.

## Phase 5A. Bronze File Catalog

- [x] `P5-01` DuckDB `control.bronze_files` Schema와 동기화 구현
- [x] `P5-02` Catalog 기반 `read_parquet([...])` dbt Macro 구현
- [x] `P5-03` 지원하지 않는 Bronze Schema Version 사전 차단
- [x] `P5-04` 빈 Object 목록과 신규 Object 증분 동기화 처리
- [x] S3 Prefix 전체 Glob을 사용하는 Model/Macro가 없는지 정적 검사

```text
pipeline_metadata.bronze_objects(COMMITTED)
    ↓ sync
control.bronze_files
    ↓ explicit object list
dbt source macro
```

## Phase 5B. Staging

권장 구현 순서:

1. `stg_products`
2. `stg_sellers`
3. `stg_order_items`
4. `stg_payments`
5. `stg_orders`
6. `stg_customers_current`
7. `stg_customer_subscriptions`
8. `stg_customer_membership_tiers`
9. `stg_subscription_payments`

- [x] `P5-05` Product/Seller Naming과 Type 표준화
- [x] `P5-06` Order Item/Payment Naming과 Type 표준화
- [x] `P5-07` Order Timestamp Rename과 8개 표준 상태 매핑
- [x] `P5-08` Customer Business Key 변환과 Current 선택
- [x] `P5-09` Customer Observation Deduplication (구독 축·등급 축 각각)
- [x] `P5-10` Staging Mapping 자동 검증

Customer Mapping:

| Source/Bronze           | Staging                   |
| ----------------------- | ------------------------- |
| `customer_id`           | `source_customer_id`      |
| `customer_unique_id`    | `customer_id`             |
| `customer_city`         | `city`                    |
| `customer_state`        | `state`                   |
| `customers.created_at`  | `created_at`              |

구독 축은 `stg_customer_subscriptions`, 등급 축은 `stg_customer_membership_tiers`에서 각각 사람
키를 `customer_id`로 바꾸고 값을 대문자로 표준화한다. 두 Staging 모두 Current 선택을 하지
않고 Bronze 누적 행을 `customer_id + updated_at + attribute_hash`로 중복 제거한다. 두 축을
어떻게 결합해 이력으로 만들지는 Phase 6이 결정한다.

Order Mapping:

| Source/Bronze                       | Staging                  |
| ----------------------------------- | ------------------------ |
| `customer_id`                       | `source_customer_id`     |
| customers Join `customer_unique_id` | `customer_id`            |
| `order_purchase_timestamp`          | `purchase_at`            |
| `order_approved_at`                 | `approved_at`            |
| `order_delivered_carrier_date`      | `carrier_at`             |
| `order_delivered_customer_date`     | `delivered_at`           |
| `order_estimated_delivery_date`     | `estimated_delivery_at`  |
| `order_status`                      | 대문자 8개 `order_status` |

`stg_orders`의 `customer_id`는 Source에 없다. `stg_customers_current`를 `source_customer_id`로 Join해 `customer_unique_id`를 가져와야 한다. 이 Join은 Grain을 바꾸지 않는다.

Payment Mapping:

| Source/Bronze          | Staging                 |
| ---------------------- | ----------------------- |
| `payment_sequential`   | `payment_sequence`      |
| `payment_installments` | `installments`          |
| `payment_status`       | 대문자 `payment_status` |

Product/Seller Mapping:

| Source/Bronze           | Staging         |
| ----------------------- | --------------- |
| `product_category_name` | `category_name` |
| `product_weight_g`      | `weight_g`      |
| `product_length_cm`     | `length_cm`     |
| `product_height_cm`     | `height_cm`     |
| `product_width_cm`      | `width_cm`      |
| `seller_city`           | `city`          |
| `seller_state`          | `state`         |

Order Status Mapping은 PRD Section 7.1을 단일 Macro 또는 Seed Mapping Source로 재사용한다. `tables.py`의 `ORDERS_TABLE.status_domains`가 허용하는 8개 값을 모두 `order_status`에 Mapping해야 한다.

```text
created     → CREATED
approved    → APPROVED
processing  → PROCESSING
invoiced    → INVOICED
shipped     → SHIPPED
delivered   → DELIVERED
canceled    → CANCELED
unavailable → UNAVAILABLE
```

다중 상태를 묶는 집계가 여러 Model이나 대시보드에서 반복될 때만 dbt Macro 또는 Mapping Seed로 조건을 재사용한다. 별도 분석 그룹 컬럼은 Staging에 저장하지 않는다.

Payment Status Mapping:

```text
pending   → PENDING
completed → COMPLETED
failed    → FAILED
refunded  → REFUNDED
```

`subscription_status`와 `membership_tier`는 원천 값 자체가 이미 대문자 표준 표기다. Staging Macro는 값을 바꾸지 않고 허용 목록 검증만 수행한다.

`stg_customers_current`의 출력 Grain은 주문 결합을 위한 `source_customer_id` 1행이다. 계정은
불변이므로 `created_at`, `_ingested_at`, `_batch_id`로 같은 계정의 Current Bronze 행만 고르고,
해당 계정의 `city`·`state`를 그대로 보존한다. 사람 단위 등급은 이 모델에 복사하지 않는다.

## 범위 밖

- Intermediate와 Mart Model 구현
- Grain 변경, 집계, 이력 구간 생성
- Incremental 전략과 Late Arrival 재계산
- 최종 Publish Swap과 전체 품질 Gate
- BI Dashboard

## 테스트와 Gate

| AC    | 시나리오       | 합격 증거                             |
| ----- | -------------- | ------------------------------------- |
| AC-19 | Staging Naming | Alias/값 보존, Raw Prefix 직접 참조 0 |
| AC-22 | 표준화 상태값  | 정의 Mapping 100% 일치                |

추가 검증:

- Bronze Version Current 선택 Tie-breaker Test
- `stg_customers_current` 대표 Row 선택 Tie-breaker Test
- Order Status 8개 표준 상태 전체 Mapping Test
- Staging 복합 Key 중복 0
- 동일 Timestamp/다른 Attribute Hash Contract Error
- Catalog 기반 Source Macro의 빈 목록·미지원 Version 처리

## 요구사항 추적

| 구분 | 연결 항목                                |
| ---- | ---------------------------------------- |
| PRD  | Section 10.4 Commit File Catalog         |
| PRD  | Section 14.1 Staging                     |
| ADR  | ADR-002 DuckDB Local Warehouse           |
| ADR  | ADR-008 Staging Naming 표준화            |
| ADR  | ADR-010 메타데이터 기반 Bronze 파일 목록 |
| FR   | FR-10 dbt Staging                        |

## 산출물

- dbt Project/Profile Template와 DuckDB Warehouse
- Catalog-based Bronze Source Macro
- Staging Model 9개
- Source→Staging Mapping 자동 검증 Test

## 파일·폴더별 변경 요약

| 경로                                          | 변경 내용                                                                                                                                                                                                                                                                                                           |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dbt/dbt_project.yml`                          | DuckDB dbt 프로젝트의 모델 경로와 계층별 Materialization·Schema를 정의했다.                                                                                                                           |
| `dbt/profiles.yml`                             | 로컬 DuckDB Warehouse와 SeaweedFS Path-style S3 연결을 환경 변수 기반으로 구성했다. Credential은 파일에 저장하지 않는다.                                                                     |
| `dbt/macros/bronze_source.sql`                 | COMMITTED File Catalog만 명시적 `read_parquet([...])` 목록으로 변환한다. 지원 Schema Version을 다시 확인하고, 빈 Catalog에는 Source 계약과 동일한 빈 Relation을 반환한다. 9개 Source를 모두 등록했다. |
| `dbt/macros/generate_schema_name.sql`          | dbt 기본 Schema 접두어를 제거해 Warehouse 계약과 Schema 이름을 일치시킨다.                                                                  |
| `dbt/macros/current_bronze_records.sql`        | Mutable Entity의 최신 Bronze Version을 `updated_at`, `_ingested_at`, `_batch_id` 순서로 하나만 선택하는 공통 Macro를 추가했다.                                                                  |
| `dbt/macros/status_standardization.sql`        | 주문 8개·결제 4개 원천 상태를 대문자 표준값으로 바꾸고, 구독 상태·등급은 허용 목록을 검증하는 Macro를 추가했다.                                                                                                      |
| `dbt/models/staging/*.sql`                     | Product, Seller, Order Item, Payment, Order와 고객 Current·관측, 구독 결제를 Staging View로 구현했다. 주문은 모든 `source_customer_id` 매핑을 유지해 분석 고객 Business Key 누락을 테스트로 차단한다.        |
| `dbt/models/staging/schema.yml`                | Staging Key, 상태 도메인, 필수값의 dbt 자동 테스트를 정의했다.                                                                                                                                    |
| `dbt/tests/stg_*_unique.sql`                   | 주문 Line, 결제 Sequence, 고객 관측의 문서화된 복합 Grain 중복을 검증한다.                                                                                                                        |
| `dbt/tests/stg_source_mapping.sql`             | Bronze Current 행과 Staging의 Prefix 제거, Timestamp Rename, 상태 표준화, 고객 키·기간 경계를 행 단위로 대조한다.                                                                                |
| `dbt/README.md`                                | 루트 기준 dbt 실행 명령과 Catalog Macro의 입력 경계를 기록했다.                                                                                                                               |
| `tests/test_dbt_catalog_macro.py`               | 빈 Catalog 처리, Commit된 명시적 Parquet 목록 생성, 미지원 Schema Version의 dbt 사전 차단을 독립 DuckDB로 검증한다.                                                                            |

## Definition of Done

- [x] 모든 `P5-*` Task가 완료됐다.
- [x] dbt가 COMMITTED Catalog Object만 읽는다. (`bronze_source` Macro는 `control.bronze_files`만 읽고, 이 Catalog는 `sync_bronze_catalog`가 COMMITTED Object만 동기화한다)
- [x] Staging Naming/상태 Mapping이 100% 일치한다. (`stg_source_mapping` 계약 테스트 PASS)
- [x] Staging이 원천 Grain을 바꾸지 않는다.
- [x] AC-19, AC-22가 통과한다.

## Portfolio Evidence

- Source→Staging Naming Mapping 표와 자동 검증
- Catalog 기반 입력 경계와 Prefix Glob 부재 정적 검사

## 권장 Commit

```text
feat: build duckdb staging from bronze catalog
```

## 다음 Phase 인계

Phase 6은 이 Phase의 Staging을 입력으로 받아 Grain을 바꾸는 Intermediate와 Mart를 만든다. Phase 6 시작 전에 [Mart Grain 계약](../reference/mart-grain.md)이 확정돼 있어야 한다.
