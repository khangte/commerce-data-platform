# Phase 5. dbt + DuckDB Modeling

> 상태: Planned  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 4. Airflow Orchestration](phase-04-airflow-orchestration.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.5](../../PRD_v1.5.md)  
> 참고: [데이터 변환 흐름](../architecture/data-transformation-flow.md) — 계층별 이름·타입·Grain 변환의 근거

## 목표

Metadata에서 COMMITTED인 Bronze Object만 읽어 DuckDB에 Staging, Intermediate, Dimension, Fact를 구축한다. Source Naming은 Staging에서 분석 Naming으로 변환하고, 고객 SCD2와 주문 시점 Temporal Join을 정확히 구현한다.

## 핵심 계약

- S3 Prefix Glob이 아니라 `control.bronze_files`의 COMMITTED Object 목록만 읽는다.
- Source Prefix 제거, Timestamp Rename, 상태 표준화는 Staging에서 처음 수행한다.
- Intermediate/Mart는 Raw Source Prefix를 직접 참조하지 않는다.
- Bronze 기술 컬럼 `_batch_id`, `_ingested_at`은 Staging에서 보존한다. Current 선택과 SCD2 정렬의 Tie-breaker가 이 컬럼을 사용한다.
- Mutable Entity는 `updated_at`, `_ingested_at`, `_batch_id` 순으로 Bronze Version 중 Current를 결정한다.
- 모든 Mart는 문서화된 Grain과 `unique_key`를 가진다.
- Item과 Payment를 각각 주문 Grain으로 집계한 뒤 `fact_orders`에 Join한다.
- SCD2 구간은 `[valid_from, valid_to)`이고 고객별 Current Version은 정확히 하나다.
- Incremental 결과는 동일 입력의 Full Refresh와 Logical Hash가 같아야 한다.

## 선행 조건

- Phase 3의 `sync_bronze_catalog`가 COMMITTED Bronze만 `control.bronze_files`에 동기화한다.
- Phase 4 Warehouse DAG가 `sync_bronze_catalog_task`를 실행한다. `P4-11` dbt Build 호출 경계는 아직 비어 있고, 이 Phase가 dbt Project와 CLI를 완성한 뒤 활성화한다.
- 지원 가능한 Bronze `schema_version` 목록이 정의됐다.
- Phase 2의 Membership/Address 변경 Fixture가 존재한다.

## Warehouse 구조

```text
data/warehouse/warehouse.duckdb

control       table
staging       view
intermediate  view
dimensions    table 또는 incremental
facts         incremental
```

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
7. `stg_customer_observations`

- [x] `P5-05` Product/Seller Naming과 Type 표준화
- [x] `P5-06` Order Item/Payment Naming과 Type 표준화
- [x] `P5-07` Order Timestamp Rename과 8개 표준 상태 매핑
- [x] `P5-08` Customer Business Key 변환과 Current 선택
- [x] `P5-09` Customer Observation Deduplication
- [x] `P5-10` Staging Mapping 자동 검증

Customer Mapping:

| Source/Bronze           | Staging                   |
| ----------------------- | ------------------------- |
| `customer_id`           | `source_customer_id`      |
| `customer_unique_id`    | `customer_id`             |
| `customer_city`         | `city`                    |
| `customer_state`        | `state`                   |
| `customers.created_at`  | `created_at`              |

`customer_memberships`는 별도 `stg_customer_observations`에서 사람 키를 `customer_id`로 바꾸고
`membership_level`을 대문자로 표준화한다. 이력은 Current 선택을 하지 않고 Bronze 누적 행을
`customer_id + updated_at + attribute_hash`로 중복 제거한다.

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

`stg_orders`의 `customer_id`는 Source에 없다. `stg_customers_current`를 `source_customer_id`로 Join해 `customer_unique_id`를 가져와야 한다.

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

다중 상태를 묶는 집계가 여러 Model이나 대시보드에서 반복될 때만 dbt Macro 또는 Mapping Seed로 조건을 재사용한다. 별도 분석 그룹 컬럼은 Staging·Intermediate·Fact에 저장하지 않는다.

Payment Status Mapping:

```text
pending   → PENDING
completed → COMPLETED
failed    → FAILED
refunded  → REFUNDED
```

`stg_customers_current`의 출력 Grain은 주문 결합을 위한 `source_customer_id` 1행이다. 계정은
불변이므로 `created_at`, `_ingested_at`, `_batch_id`로 같은 계정의 Current Bronze 행만 고르고,
해당 계정의 `city`·`state`를 그대로 보존한다. 사람 단위 등급은 이 모델에 복사하지 않는다.

## Phase 5C. Intermediate

- [x] `P5-11` `int_orders_enriched`
- [x] `P5-12` `int_order_items_enriched`
- [x] `P5-13` `int_payment_summary`
- [x] `P5-14` `int_customer_history`
- [x] `P5-15` `int_affected_business_dates`
- [x] `P5-16` `control.affected_keys` 기록과 Invocation ID 연결

Late Arrival 영향 범위는 주문 구매일, 연결 주문 구매일, 고객 변경 구간, Product/Seller 사용 주문일을 기준으로 계산한다.

## Phase 5D. Dimension

- [x] `P5-17` `dim_product`
- [x] `P5-18` `dim_seller`
- [x] `P5-19` UTC 기준 `dim_date`
- [x] `P5-20` SCD Type 2 `dim_customer`

| Model          | Grain            | Unique Key     |
| -------------- | ---------------- | -------------- |
| `dim_customer` | 고객 Version 1행 | `customer_key` |
| `dim_product`  | 상품 1행         | `product_id`   |
| `dim_seller`   | 판매자 1행       | `seller_id`    |
| `dim_date`     | UTC Date 1행     | `date_key`     |

SCD2 추적 속성:

```text
membership_level
```

SCD2 규칙:

- `(customer_id, updated_at, _ingested_at, _batch_id)` 순으로 관측을 정렬한다.
- `customer_id + updated_at + tracked_attribute_hash` 관측을 Deduplicate한다.
- 속성 Hash가 같으면 새 Version을 만들지 않는다.
- 최초 Version의 `valid_from`은 `created_at`이다. Olist Seed에 과거 속성 이력이 없으므로 최초 Version을 Baseline Snapshot으로 간주한다.
- 이후 변경 Version의 `valid_from`은 `updated_at`이다.
- 다음 Version의 `valid_from`이 현재 Version의 `valid_to`다.
- `customer_key = Hash(customer_id, valid_from, attribute_hash)`로 만든다.
- 마지막 Version만 `valid_to=NULL`, `is_current=true`다.
- 동일 고객/동일 `updated_at`의 서로 다른 Hash는 Contract Error다.

최초 Version의 `valid_from`을 `updated_at`으로 잡으면 그 이전 구매 주문이 유효한 Customer Version을 찾지 못해 Unknown Customer Key가 발생한다. AC-12를 위반하므로 `created_at` 규칙은 필수다.

`dim_customer` Schema:

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

## Phase 5E. Fact와 Measure

권장 구현 순서:

- [ ] `P5-21` 주문 Line Grain의 `fact_order_items`
- [ ] `P5-22` 결제 Sequence Grain의 `fact_payments`
- [ ] `P5-23` 주문 Grain의 `fact_orders`
- [ ] `P5-24` 모든 Fact의 `unique_key`와 Incremental 교체 구현

| Model              | Grain             | Unique Key                     |
| ------------------ | ----------------- | ------------------------------ |
| `fact_order_items` | 주문 Line 1행     | `(order_id, order_item_id)`    |
| `fact_payments`    | 결제 Sequence 1행 | `(order_id, payment_sequence)` |
| `fact_orders`      | 주문 1행          | `order_id`                     |

`fact_order_items` Measure:

```text
item_price       = price
freight_value    = freight_value
line_gross_value = item_price + freight_value
```

`fact_orders` Measure:

```text
item_subtotal     = SUM(item.price)
freight_total     = SUM(item.freight_value)
gross_order_value = item_subtotal + freight_total
payment_total     = SUM(payment.payment_value)
order_count       = 1
```

Item/Payment Raw Grain을 직접 다대다 Join한 뒤 합산하지 않는다. 기본 Sales/GMV는 `order_status='DELIVERED'`의 `gross_order_value`이며, `payment_total`을 Revenue와 동일시하지 않는다.

## Phase 5F. SCD2와 Temporal Join

- [ ] `P5-25` Customer Observation에서 Version 구간 생성
- [ ] `P5-26` 구간 중첩/공백/Current Version 검증
- [ ] `P5-27` 주문 `purchase_at` 기준 Temporal Join
- [ ] `P5-28` 정상 E2E Unknown Customer Key 0 검증

```sql
order.purchase_at >= dim_customer.valid_from
AND order.purchase_at < COALESCE(dim_customer.valid_to, TIMESTAMPTZ 'infinity')
```

## Phase 5G. Incremental과 Late Arrival

- [ ] `P5-29` 변경 Key 기반 Transactional `DELETE + INSERT` 또는 검증된 `MERGE`
- [ ] `P5-30` 영향 Key/Business Date 재계산
- [ ] `P5-31` Incremental과 Full Refresh Logical Hash 비교
- [ ] `P5-32` Bronze Replay와 Re-extract 입력 경계 제공
- [ ] `P5-33` Phase 4 Warehouse DAG의 `P4-11` dbt Build 호출 경계 활성화

## 범위 밖

- 최종 Publish Swap과 전체 품질 Gate
- 장애 Runbook 작성
- BI Dashboard

## 테스트와 Gate

| AC    | 시나리오                      | 합격 증거                                     |
| ----- | ----------------------------- | --------------------------------------------- |
| AC-01 | E2E Fact 도달                 | 고정 주문의 Source→Bronze→Fact Count/Key 추적 |
| AC-09 | BRONZE→SILVER→GOLD            | 3 Version, Overlap 0, Current 1               |
| AC-10 | 구간별 주문                   | 주문 시점에 유효한 Customer Key 참조          |
| AC-11 | 3일 전 Late Order의 모델링 측 | 과거 Business Date Fact 재계산                |
| AC-12 | Referential Integrity         | Fact FK/Unique 통과, 정상 Unknown 0           |
| AC-19 | Staging Naming                | Alias/값 보존, Raw Prefix 직접 참조 0         |
| AC-22 | 표준화 상태값                 | 정의 Mapping 100% 일치                        |

추가 검증:

- Model별 Grain/Unique Key 중복 0
- Item/Payment Fan-out 방지 SQL Test
- `gross_order_value`와 `payment_total` 계산 Test
- Bronze Version Current 선택 Tie-breaker Test
- `stg_customers_current` 대표 Row 선택 Tie-breaker Test
- Order Status 8개 표준 상태 전체 Mapping Test
- SCD2 최초 Version `valid_from = created_at` Test
- 동일 Timestamp/다른 Attribute Hash Contract Error
- Incremental/Full Refresh Key별 값과 Logical Hash 일치

## 요구사항 추적

| 구분 | 연결 항목                                 |
| ---- | ----------------------------------------- |
| PRD  | Section 10.4 Commit File Catalog          |
| PRD  | Section 14 dbt + DuckDB 모델              |
| PRD  | Section 15 SCD Type 2와 Temporal Join     |
| PRD  | Section 16 Late Arrival, Backfill, Re-run |
| ADR  | ADR-002 DuckDB Local Warehouse            |
| ADR  | ADR-008 Staging Naming 표준화             |
| ADR  | ADR-009 관측 기반 고객 SCD2               |
| ADR  | ADR-010 메타데이터 기반 Bronze 파일 목록  |
| ADR  | ADR-011 Late Arrival 재처리 전략          |
| FR   | FR-10 dbt Staging/Intermediate/Mart       |
| FR   | FR-11 Star Schema/Fact Grain              |
| FR   | FR-14 Late Arrival 재처리                 |
| FR   | FR-15 SCD2/Temporal Join                  |

## 산출물

- dbt Project/Profile Template와 DuckDB Warehouse
- Catalog-based Bronze Source Macro
- Staging/Intermediate Model
- 4개 Dimension과 3개 Fact
- SCD2와 Temporal Join
- Affected Key/Date 기반 Incremental 재계산
- Mapping, Grain, Measure, SCD2, Full Refresh 비교 Test

## 파일·폴더별 변경 요약

| 경로                                          | 변경 내용                                                                                                                                                                                                                                                                                                           |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dbt/dbt_project.yml`                          | DuckDB dbt 프로젝트의 모델 경로와 계층별 Materialization·Schema를 정의했다.                                                                                                                           |
| `dbt/profiles.yml`                             | 로컬 DuckDB Warehouse와 SeaweedFS Path-style S3 연결을 환경 변수 기반으로 구성했다. Credential은 파일에 저장하지 않는다.                                                                     |
| `dbt/macros/bronze_source.sql`                 | COMMITTED File Catalog만 명시적 `read_parquet([...])` 목록으로 변환한다. 지원 Schema Version을 다시 확인하고, 빈 Catalog에는 Source 계약과 동일한 빈 Relation을 반환한다.                       |
| `dbt/macros/generate_schema_name.sql`          | dbt 기본 Schema 접두어를 제거해 `staging`, `intermediate`, `dimensions`, `facts` Schema 이름을 Warehouse 계약과 일치시킨다.                                                                  |
| `dbt/macros/current_bronze_records.sql`        | Mutable Entity의 최신 Bronze Version을 `updated_at`, `_ingested_at`, `_batch_id` 순서로 하나만 선택하는 공통 Macro를 추가했다.                                                                  |
| `dbt/macros/status_standardization.sql`        | 주문 8개·결제 4개·고객 등급 3개 원천 상태를 명시적 대문자 표준값으로 바꾸는 Macro를 추가했다.                                                                                                      |
| `dbt/models/staging/*.sql`                     | Product, Seller, Order Item, Payment, Order와 고객 Current·관측을 Staging View로 구현했다. 주문은 모든 `source_customer_id` 매핑을 유지해 분석 고객 Business Key 누락을 테스트로 차단한다.        |
| `dbt/models/staging/schema.yml`                | Staging Key, 상태 도메인, 필수값의 dbt 자동 테스트를 정의했다.                                                                                                                                    |
| `dbt/tests/stg_*_unique.sql`                   | 주문 Line, 결제 Sequence, 고객 관측의 문서화된 복합 Grain 중복을 검증한다.                                                                                                                        |
| `dbt/tests/stg_source_mapping.sql`             | Bronze Current 행과 Staging의 Prefix 제거, Timestamp Rename, 상태 표준화, 고객 키·기간 경계를 행 단위로 대조한다.                                                                                |
| `dbt/README.md`                                | 루트 기준 dbt 실행 명령과 Catalog Macro의 입력 경계를 기록했다.                                                                                                                               |
| `tests/test_dbt_catalog_macro.py`               | 빈 Catalog 처리, Commit된 명시적 Parquet 목록 생성, 미지원 Schema Version의 dbt 사전 차단을 독립 DuckDB로 검증한다.                                                                            |
| `dbt/macros/bronze_source.sql` | 수정 | 7번째 Source인 `customer_memberships`와 분리된 계정/멤버십 Schema를 Bronze Macro에 등록했다. |
| `dbt/models/staging/stg_customers_current.sql` | 수정 | 불변 계정 주소와 `created_at`만 주문 결합 Grain으로 노출하도록 축소했다. |
| `dbt/models/staging/stg_customer_observations.sql` | 수정 | 사람 단위 Membership Bronze 관측만으로 SCD2 입력과 단일 속성 Hash를 만들도록 변경했다. |
| `dbt/models/intermediate/int_customer_history.sql` | 수정 | 주소 추적을 제거하고 Membership Version 구간만 계산하도록 변경했다. |
| `dbt/models/intermediate/int_orders_enriched.sql` | 수정 | 계정 주소를 `source_customer_id`로 직접 결합해 주문 스냅샷으로 보존하도록 변경했다. |
| `dbt/models/marts/dimensions/dim_customer.sql` | 수정 | 사람 Membership Version Dimension에서 `city`·`state`를 제거했다. |
| `dbt/models/staging/schema.yml`, `dbt/tests/stg_source_mapping.sql` | 수정 | 분리된 계정/멤버십 Mapping과 검증 계약을 반영했다. |
| `docs/phases/phase-05-dbt-duckdb-modeling.md` | 수정 | 계정 주소 스냅샷과 사람 단위 Membership SCD2의 Grain 분리를 기록했다. |

## Definition of Done

- [ ] 모든 `P5-*` Task가 완료됐다.
- [ ] dbt가 COMMITTED Catalog Object만 읽는다.
- [ ] Staging Naming/상태 Mapping이 100% 일치한다.
- [ ] 모든 Mart의 Grain과 Unique Key가 검증된다. Dimension 4개와 Fact 3개를 모두 포함한다.
- [ ] Phase 4 Warehouse DAG의 `dbt_build` 호출 경계가 활성화된다.
- [ ] Customer SCD2 구간 중첩이 0이고 Current가 정확히 1개다.
- [ ] 주문이 구매 시점에 유효한 Customer Version을 참조한다.
- [ ] Incremental과 Full Refresh의 Logical Hash가 같다.
- [ ] AC-01, 09, 10, 11, 12, 19, 22가 통과한다.

## Portfolio Evidence

- Source→Staging Naming Mapping 표와 자동 검증
- Fact Grain/Measure Contract 및 Fan-out 방지 Test
- Customer SCD2 Timeline
- Temporal Join 결과
- Late Arrival 전후 Affected Date와 Fact Diff
- Incremental/Full Refresh Hash 비교

## 권장 Commit

```text
feat: build duckdb marts with dbt
```

## 다음 Phase 인계

Phase 6은 이 Phase의 Model Test를 통합 품질 Gate로 묶고, 실패한 Build가 마지막 성공 Mart를 훼손하지 않는 Publish 절차를 완성한다. 이 Phase가 `P5-33`으로 Phase 4의 `P4-11`을 채우므로, Phase 6 시작 시점에는 모든 `P4-*` Task가 완료된 상태여야 한다.
