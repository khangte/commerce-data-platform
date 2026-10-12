# 056. `fct_order_payment` Dimension FK 추가 여부 (Mart Grain 계약 §3.3)

- 날짜: 2026-10-12
- 판정: **`purchase_date_key` 추가는 승인을 권고한다(계약 개정). `customer_key`는 추가하지 않는다(보류).** 계약 개정이므로 착수 여부는 사용자가 정한다.
- 결정: 2026-10-12 사용자 승인. `purchase_date_key`만 추가하고 `customer_key`와 환불·실패 기간 Card는 만들지 않는다(9절).
- 계기: 사용자 검토 요청. `fct_order_payment`에 Dimension 참조가 없어 결제를 고객·일자로 자르려면 `fct_order`와 Join해야 한다. 이 Join은 계약 §6을 어긴다.
- 관련: [045](045_fct-order-item-purchase-date-key.md)(같은 유형의 선례), [046](046_mart-build-session-timezone.md)(재Publish 절차), `docs/reference/mart-grain.md` §3.1·§3.3·§6, PRD v1.21 §14.3

## 1. 현재 상태

- 계약 §3.3(`docs/reference/mart-grain.md:356-369`)의 컬럼은 `order_id`, `payment_sequence`, `payment_value`, `payment_status`뿐이다. 출처는 `stg_payments`다.
- 구현(`dbt/models/marts/facts/fct_order_payment.sql`)은 `stg_payments`를 그대로 투영한다. Dimension 참조는 없다. `schema.yml:66-80`의 참조 Test는 `order_id → fct_order.order_id` 하나다.
- 소비처: Metabase Card(`metabase/export/`, `metabase/queries/`)와 Report Model 중 `fct_order_payment`를 읽는 것은 없다. 읽는 곳은 R09 신뢰성 Test(`tests/reliability/test_r08_r10_late_and_history.py:254-346`)와 `mart_hash` 대상 목록(`src/warehouse/mart_hash.py:44`)뿐이다.
- 결제 시각: Olist Seed 결제는 `payment_initiated_at`·`payment_completed_at`이 `NULL`이다(`src/seed/loader.py:343-344`). Generator 결제만 값을 갖는다(`src/generator/orders.py:204`).

## 2. 판단

### 2.1 빈틈은 실재한다

PRD §14.3은 "Refund/Failed 분석은 `fact_payments.payment_status`"로 결제 Fact를 정본 경로로 지정한다. 기간별 환불·실패 금액은 이 경로로만 구할 수 있다.

- `fct_order.payment_total`은 상태와 무관하게 결제 금액을 모두 합한다. 따라서 상태별 금액을 대신할 수 없다.
- 결제 Fact에 날짜가 없으므로 기간 필터를 걸려면 `fct_order`와 Join해야 한다. 이는 §6("Fact끼리는 서로 직접 Join하지 않는다")에 어긋난다.

045가 `fct_order_item`에서 닫은 것과 같은 유형이다. 045는 당시 결제 Fact를 날짜로 자르는 Card가 없다는 이유로 YAGNI로 제외했다. 사용자가 이 분석 경로를 요구한 지금은 그 전제가 바뀌었다.

### 2.2 대안

| 안 | 내용 | 판정 |
| -- | ---- | ---- |
| A | `fct_order_payment`에 `purchase_date_key` 추가 | **채택 권고** |
| A+ | A에 더해 `customer_key` 추가 | 보류. 2.3 참조 |
| B | `fct_order`에 상태별 결제 금액(`refunded_value`, `failed_value` 등) 추가 | 기각. 상태 값마다 컬럼이 늘고 결제 Line 단위 분석이 사라진다. PRD가 지정한 정본 경로(결제 Fact의 `payment_status`)와도 다르다 |
| C | Metabase Question에서 `fct_order`와 Join | 기각. §6 위반이다. BI 쪽에 Grain 결합 로직이 생긴다 |
| D | 현행 유지 | 기각. PRD §14.3의 분석을 기간 축으로 수행할 수 없다 |

A는 Line Fact에 Header의 날짜 FK를 복제하는 표준 방식이다. 결제 1건은 주문 1건에 속하므로 행 수와 Grain은 바뀌지 않는다.

### 2.3 `customer_key`는 지금 넣지 않는다

- 결제 금액의 고객 축 분석은 `fct_order.payment_total`과 `fct_order.customer_key`로 이미 가능하다.
- "결제 상태 × 고객(또는 등급)" 분석은 PRD에도, 현재 Card 계획에도 없다.
- `fct_order_item`도 날짜 FK만 받고 `customer_key`는 받지 않았다(045). 결제만 고객 FK를 가지면 Line Fact 간 규칙이 갈린다.
- 추가하면 SCD2 Temporal Join 결과를 두 번째 Fact에 복제하게 된다. 그러면 두 Fact의 일치를 Test로 계속 강제해야 한다.

요구가 생기면 아래 규칙으로 추가한다.

- 의미: `fct_order.customer_key`와 같은 값이다. 즉 주문 구매 시각(`purchase_at`) 기준으로 유효한 `dim_customer` 버전이다. 결제 시각 기준 버전은 쓰지 않는다. Seed 결제에는 결제 시각이 없고(1절), 같은 주문의 결제가 다른 고객 버전을 가리키면 주문 단위 집계와 어긋난다.
- 파생: `int_orders_enriched`를 `order_id`로 Join해서 가져온다. Temporal Join 식을 다시 쓰지 않는다. 순환 의존은 없다(`int_orders_enriched`는 결제 Model에 의존하지 않는다).
- Test: `not_null`, `relationships → dim_customer.customer_key`, 그리고 같은 `order_id`의 `fct_order.customer_key`와 일치하는지 확인하는 Singular Test.
- 증분: 등급 변경으로 바뀌는 주문은 `int_affected_order_keys`의 `affected_from_customers`가 이미 잡는다. 결제 Fact도 같은 Key 집합으로 교체하므로 별도 처리는 필요 없다.

## 3. 날짜 Key의 의미

- 이름은 `purchase_date_key`다. 의미는 **결제가 속한 주문의 구매일**이다. 결제일이 아니다.
- `payment_date_key`라는 이름은 쓰지 않는다. `fct_subscription_payment.payment_date_key`는 실제 결제 시도일이다. 같은 이름이 두 의미를 가지면 BI에서 혼동한다.
- 결제 사건일을 Key로 쓰는 안은 기각한다.
  - Olist Seed 결제 약 10만 건의 결제 시각이 `NULL`이다. `NOT NULL` FK가 될 수 없다.
  - 상태마다 시각 컬럼이 다르다(`initiated`, `completed`, `failed`, `refunded`). 이 Fact는 Accumulating Snapshot이므로 상태가 바뀔 때마다 행의 날짜가 다른 일자로 이동한다.
- 식은 `fct_order.purchase_date_key`와 같다: `cast(strftime(date_trunc('day', purchase_at), '%Y%m%d') as integer)`. 기준 TimeZone은 UTC다(046).

## 4. 계약 개정 내용 (승인 시)

`docs/reference/mart-grain.md` §3.3 `fct_order_payment`:

- 출처를 `stg_payments`에서 `int_order_payments_enriched`로 바꾼다.
- `- Dimension 참조: dim_date` 줄을 추가한다.
- 컬럼 `purchase_date_key INTEGER`를 `payment_sequence` 다음에 추가한다. 종류는 Dimension FK, Null 불가다.
- 정의: 결제가 속한 주문의 구매일. `stg_orders.purchase_at`에서 `fct_order.purchase_date_key`와 같은 식으로 파생한다. 결제일이 아니다.
- Test: `not_null`, `relationships → dim_date.date_key`, 같은 `order_id`의 `fct_order.purchase_date_key`와 일치.

PRD는 고치지 않는다. PRD §14.3이 Mart 컬럼 계약을 `mart-grain.md`에 위임하고 중복 정의하지 않기 때문이다.

## 5. 구현 규칙 (승인 시)

1. 새 Intermediate `int_order_payments_enriched`를 만든다. `stg_payments`에 `stg_orders`를 `order_id`로 Left Join하고 3절의 식으로 `purchase_date_key`를 계산한다. 045의 `int_order_items_enriched`와 같은 형태다.
2. `int_order_fact_ready`와 `int_orders_enriched`는 참조하지 않는다. 날짜만 필요하므로 `dim_customer` 의존을 끌고 오지 않는다(045 구현 규칙 2와 같은 이유).
3. `stg_orders`는 주문 1건 1행이므로 Join 뒤에도 결제 행 수가 같아야 한다.
4. `fct_order_payment.sql`은 `int_order_payments_enriched`를 읽고 `purchase_date_key`를 `payment_sequence` 다음에 투영만 한다. 증분 조건(`int_affected_order_keys`)과 `unique_key='order_id'`는 그대로 둔다.
5. 기존 컬럼 순서는 바꾸지 않는다. 구현은 `payment_status`, `payment_value` 순이고 계약 표는 그 반대지만 이번 범위가 아니다.
6. `int_payment_summary`는 바꾸지 않는다.
7. 날짜 식이 세 곳(`int_order_fact_ready`, `int_order_items_enriched`, 새 Model)에 생긴다. 045처럼 Macro로 추출하지 않고 Singular Test로 일치를 강제한다.

증분 정합성은 추가 작업 없이 유지된다. 주문의 `purchase_at`이 바뀌면 `int_affected_order_keys`의 `affected_orders`가 그 주문을 잡는다. 결제 Fact도 같은 Key 집합으로 `delete+insert`한다.

## 6. 영향 범위

| 대상 | 영향 | 조치 |
| ---- | ---- | ---- |
| 계약 `mart-grain.md` §3.3 | 컬럼·출처·참조 추가 | 4절대로 개정 |
| dbt Model | 새 `int_order_payments_enriched`, `fct_order_payment.sql` 수정 | 5절 |
| `dbt/models/marts/facts/schema.yml` | `purchase_date_key`에 `not_null`, `relationships → ref('dim_date').date_key` | 추가 |
| Singular Test | 새 `dbt/tests/fct_order_payment_date_matches_order.sql`: `fct_order_payment`와 `fct_order`의 `purchase_date_key`가 다른 `order_id` 0건 | 추가 |
| `tests/test_warehouse_quality_contract.py` | 새 relationships와 Singular Test가 있는지 단언(045 때 `:34`, `:50`과 같은 형태) | 추가 |
| `src/warehouse/mart_hash.py:44` 정렬 Key | `("order_id", "payment_sequence")`는 Grain Key라 그대로다 | 변경 없음 |
| `fct_order_payment` Logical Hash 값 | `SELECT *`이므로 컬럼이 늘어 값이 바뀐다 | 개정 전 Hash와 비교하지 않는다(045 §7 규칙) |
| Phase 8 증거(`data/reliability/r09/`, `r14/`) | 기록된 Hash는 개정 전 값이다. R09 Test는 같은 실행 안의 전후 Hash만 비교한다(`test_r08_r10_late_and_history.py:327`) | 재계산하지 않는다. 다음 실행부터 새 값으로 기록된다 |
| Phase 9 Benchmark Hash 증거 | 같은 코드 버전의 Incremental과 Full Refresh를 비교하므로 비교 방식은 유지된다. 과거 기록값은 개정 전 값이다 | 재계산하지 않는다. 개정 이후 실행끼리만 비교한다 |
| `int_affected_order_keys`, `processed_batch_watermark` | 변경 없음 | 없음 |
| Metabase Card | `fct_order_payment`를 쓰는 Card가 없다 | 변경 없음. 새 컬럼은 Metabase Schema Sync로 나타난다. 환불·실패 기간 Card는 별도 요청으로 다룬다 |
| README ER 그림·Fact 표 (`README.md:152-171`) | ER에 `dim_date \|\|--o{ fct_order_payment : purchase_date_key`가 없다. Fact 표 주요 컬럼에 `purchase_date_key`가 없다 | 두 곳 갱신 |
| PRD | 변경 없음(4절) | 없음 |

## 7. 기존 데이터 반영

Full Refresh 재Publish가 필수다. dbt 설정에 `on_schema_change`가 없으므로 기본값 `ignore`가 적용된다(`dbt/dbt_project.yml:35`). 증분 Build는 기존 Table에 새 컬럼을 만들지 않는다. 그 상태에서 `purchase_date_key` Test가 돌면 Build가 실패하고 Publish Gate가 그 Build를 `failed/`로 격리한다. 따라서 코드 반영 뒤 첫 Build는 반드시 Full Refresh여야 한다.

045 §6의 규칙을 그대로 따른다. Published 파일에 dbt를 직접 실행하지 않는다. `--select`로 범위를 줄이지 않는다.

1. `source_simulation_dag`를 일시 정지한다. 이 DAG가 `warehouse_pipeline_dag`를 Trigger하므로(`airflow/dags/source_simulation_dag.py:87`) 정지하지 않으면 코드 반영 직후 정기 증분 Build가 실패한다. 실행 중인 `warehouse_pipeline_dag` Run이 없는지도 확인한다.
2. 코드를 반영한다.
3. Published 파일이 직전 PUBLISHED Run의 기록 Hash와 일치하는지 확인한다. 불일치하면 진행하지 않고 보고한다.
4. 실행: `python -m src.warehouse.publish --full-refresh --pipeline-name contract_056_full_refresh`
5. 검증:
   - `mart_publish_runs`에 새 PUBLISHED Row가 생기고 `previous_publish_run_id`가 직전 Run을 가리킨다.
   - `changed_relations`는 `facts.fct_order_payment` 하나여야 한다. 다른 Relation이 바뀌면 직전 Publish 이후 Bronze Batch 유입 여부를 먼저 확인한다. 유입이 없으면 Full Refresh와 Incremental 불일치로 보고 중단한다.
   - `fct_order_payment`의 행 수와 `sum(payment_value)`가 직전 Run과 같다.
   - `RUN_PUBLISHED_MART_CHECK=1`로 `tests/test_published_mart_queryable.py`를 실행한다(043).
6. `source_simulation_dag`를 재개한다. 수동 Publish CLI는 Serving Export와 Metabase 재지정을 하지 않는다. Serving 반영은 다음 정기 `warehouse_pipeline_dag` Run의 `export_serving_mart` → `repoint_metabase_serving`이 맡는다. 그 Run은 새 컬럼이 있는 Published 파일을 복사해 증분 Build하므로 정상 통과해야 한다. 그 Run의 성공과 Serving `export_id` 갱신까지 확인한다.

## 8. 사용자 결정 사항

1. 계약 개정(A안, `purchase_date_key` 추가)을 승인하는가.
2. `customer_key`도 함께 넣는가. 권고는 "넣지 않음"이다(2.3).
3. 환불·실패 금액의 기간 Card를 함께 만드는가. 만들지 않으면 이번 변경의 소비처는 Ad-hoc 조회뿐이다.

## 9. 사용자 결정 (2026-10-12)

사용자가 lead를 통해 8절의 세 사항을 결정했다.

| # | 사항 | 결정 | 근거 |
| - | ---- | ---- | ---- |
| 1 | 계약 개정(A안, `purchase_date_key` 추가) | **승인** | 4절대로 개정한다 |
| 2 | `customer_key` 추가 | **넣지 않음** | 결제 Fact에는 고객 이력이 필요 없다. 고객 축 분석은 `fct_order.payment_total`로 충분하다 |
| 3 | 환불·실패 금액 기간 Card | **만들지 않음** | 이번 변경의 소비처는 Ad-hoc 조회다 |

- 구현 범위는 4~6절의 `purchase_date_key` 관련 항목이다. 2.3절의 `customer_key` 추가 규칙은 적용하지 않는다.
- 6절의 Metabase 행은 "변경 없음"으로 확정한다.
- 기존 데이터 반영은 7절 절차를 따른다.
- 구현은 developer가 진행 중이다.
