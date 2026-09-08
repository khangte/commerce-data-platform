# Phase 5. dbt + DuckDB Modeling

> 상태: Planned  
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 4. Airflow Orchestration](phase-04-airflow-orchestration.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.4](../../PRD_v1.4.md)

## 목표

Metadata에서 COMMITTED인 Bronze Object만 읽어 DuckDB에 Staging, Intermediate, Dimension, Fact를 구축한다. Source Naming은 Staging에서 분석 Naming으로 변환하고, 고객 SCD2와 주문 시점 Temporal Join을 정확히 구현한다.

## 핵심 계약

- S3 Prefix Glob이 아니라 `control.bronze_files`의 COMMITTED Object 목록만 읽는다.
- Source Prefix 제거, Timestamp Rename, 상태 표준화는 Staging에서 처음 수행한다.
- Intermediate/Mart는 Raw Source Prefix를 직접 참조하지 않는다.
- Mutable Entity는 `updated_at`, `_ingested_at`, `_batch_id` 순으로 Current를 결정한다.
- 모든 Mart는 문서화된 Grain과 `unique_key`를 가진다.
- Item과 Payment를 각각 주문 Grain으로 집계한 뒤 `fact_orders`에 Join한다.
- SCD2 구간은 `[valid_from, valid_to)`이고 고객별 Current Version은 정확히 하나다.
- Incremental 결과는 동일 입력의 Full Refresh와 Logical Hash가 같아야 한다.

## 선행 조건

- Phase 3의 Catalog에 COMMITTED Bronze만 동기화된다.
- Phase 4 Warehouse DAG가 `sync_bronze_catalog`와 dbt 실행 경계를 제공한다.
- 지원 가능한 Bronze `schema_version` 목록이 정의됐다.
- Phase 2의 Membership/Address 변경 Fixture가 존재한다.

## Warehouse 구조

```text
data/warehouse/warehouse.duckdb

control       table
staging       view
intermediate  view
marts         table 또는 incremental
```

## Phase 5A. Bronze File Catalog

- [ ] `P5-01` DuckDB `control.bronze_files` Schema와 동기화 구현
- [ ] `P5-02` Catalog 기반 `read_parquet([...])` dbt Macro 구현
- [ ] `P5-03` 지원하지 않는 Bronze Schema Version 사전 차단
- [ ] `P5-04` 빈 Object 목록과 신규 Object 증분 동기화 처리
- [ ] S3 Prefix 전체 Glob을 사용하는 Model/Macro가 없는지 정적 검사

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

- [ ] `P5-05` Product/Seller Naming과 Type 표준화
- [ ] `P5-06` Order Item/Payment Naming과 Type 표준화
- [ ] `P5-07` Order Timestamp Rename과 표준화 상태값 매핑
- [ ] `P5-08` Customer Business Key 변환과 Current 선택
- [ ] `P5-09` Customer Observation Deduplication
- [ ] `P5-10` Staging Mapping 자동 검증

대표 Mapping:

| Source/Bronze                     | Staging              |
| --------------------------------- | -------------------- |
| `customers.customer_id`           | `source_customer_id` |
| `customers.customer_unique_id`    | `customer_id`        |
| `customer_city`, `customer_state` | `city`, `state`      |
| `order_purchase_timestamp`        | `purchase_at`        |
| `order_approved_at`               | `approved_at`        |
| `payment_sequential`              | `payment_sequence`   |
| `product_category_name`           | `category_name`      |

Order Status Mapping은 단일 Macro 또는 Seed Mapping Source로 관리한다.

```text
approved / processing / invoiced → APPROVED
shipped                         → SHIPPED
delivered                       → DELIVERED
canceled / unavailable          → CANCELLED
```

## Phase 5C. Intermediate

- [ ] `P5-11` `int_orders_enriched`
- [ ] `P5-12` `int_order_items_enriched`
- [ ] `P5-13` `int_payment_summary`
- [ ] `P5-14` `int_customer_history`
- [ ] `P5-15` `int_affected_business_dates`
- [ ] `P5-16` `control.affected_keys` 기록과 Invocation ID 연결

Late Arrival 영향 범위는 주문 구매일, 연결 주문 구매일, 고객 변경 구간, Product/Seller 사용 주문일을 기준으로 계산한다.

## Phase 5D. Dimension

- [ ] `P5-17` `dim_product`
- [ ] `P5-18` `dim_seller`
- [ ] `P5-19` UTC 기준 `dim_date`
- [ ] `P5-20` SCD Type 2 `dim_customer`

SCD2 추적 속성:

```text
membership_level
city
state
```

SCD2 규칙:

- `customer_id + updated_at + tracked_attribute_hash` 관측을 Deduplicate한다.
- 속성 Hash가 같으면 새 Version을 만들지 않는다.
- `customer_key = Hash(customer_id, valid_from, attribute_hash)`로 만든다.
- 마지막 Version만 `valid_to=NULL`, `is_current=true`다.
- 동일 고객/동일 `updated_at`의 서로 다른 Hash는 Contract Error다.

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
- Current 선택 Tie-breaker Test
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

| 경로                                          | 변경 내용                                                                                       |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `docs/phases/phase-05-dbt-duckdb-modeling.md` | 프로젝트 내부 용어를 한국어 중심으로 정리하고, 코드·DB 식별자와 `Logical Hash` 표기는 유지했다. |

## Definition of Done

- [ ] 모든 `P5-*` Task가 완료됐다.
- [ ] dbt가 COMMITTED Catalog Object만 읽는다.
- [ ] Staging Naming/상태 Mapping이 100% 일치한다.
- [ ] 모든 Mart의 Grain과 Unique Key가 검증된다.
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

Phase 6은 이 Phase의 Model Test를 통합 품질 Gate로 묶고, 실패한 Build가 마지막 성공 Mart를 훼손하지 않는 Publish 절차를 완성한다.
