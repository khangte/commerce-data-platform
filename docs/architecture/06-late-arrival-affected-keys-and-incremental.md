# 06. Late Arrival 영향 Key와 Affected 기반 Incremental 설계

> 대상 Task: `P6-10` Late Arrival 영향 범위 계산, `P6-22` 영향 Key/Business Date 재계산
> 기준 문서: [PRD v1.12](../../PRD_v1.12.md) Section 14.4 / Section 16, [Mart Grain 계약](../reference/mart-grain.md), [Phase 6](../phases/phase-06-dimensional-modeling.md)
> 브랜치: `feature/phase6-remaining`
> 상태: lead 승인 완료 (2026-09-18)

## 1. 배경

Phase 6의 Mart 구현은 끝났지만, 재계산 경로가 비어 있다. 현재 상태는 다음과 같다.

- `int_affected_business_dates`는 주문 축 영향 범위만 산출한다. v1.12에서 추가된 구독 결제 축(`fact_subscription_payments`)의 영향 범위는 계산하지 않는다.
- 고객 축 입력이 `stg_customer_subscription_observations`를 참조한다. v1.12에서 `int_customer_history`는 거래 실적 등급 축만 남겼으므로 입력과 산출의 계약이 어긋난다.
- `control.affected_keys`에 영향 Key를 기록만 하고 아무도 읽지 않는다. `dbt/models` 전체에 `is_incremental()`이 한 건도 없다. 모든 Fact가 매 Build마다 Intermediate를 전량 스캔해 전체를 교체한다.

이 문서는 위 세 가지를 해소하는 설계를 확정한다. Logical Hash 비교(`P6-23`)와 Bronze Replay 경계(`P6-24`)는 이 설계를 전제로 하는 후속 Task이며 여기서 다루지 않는다.

## 2. 설계 원칙

1. 영향 Key 모델은 Grain 문장을 하나만 가진다. 한 모델이 여러 도메인의 행을 섞지 않는다.
2. 영향 범위는 좁히기보다 **누락을 만들지 않는 쪽**으로 정한다. 과다 포함은 비용이고, 누락은 조용한 오답이다.
3. 재계산 대상은 SCD2 구간 조인으로 유도하지 않고, **변경된 Business Key와 그 최소 변경 시각** 기준으로 유도한다. 근거는 4.3절이다.
4. `control.affected_keys`는 감사 기록이다. Fact의 실행 입력이 아니다. 근거는 5.1절이다.

---

## 3. D-1. 영향 Key 모델을 도메인별로 나눈다

### 결정

영향 Key Intermediate를 두 모델로 나눈다. 기존 `int_affected_business_dates`는 주문 축 모델로 이름을 바꾼다.

| 모델                                     | Grain 문장                                                                        | 컬럼                             | 소비 Fact                                          |
| ---------------------------------------- | --------------------------------------------------------------------------------- | -------------------------------- | -------------------------------------------------- |
| `int_affected_order_keys`                | 이번 재계산 경계에서 주문 축 Fact가 다시 계산해야 하는 주문 1건                    | `order_id`, `business_date_key`  | `fact_orders`, `fact_order_items`, `fact_payments` |
| `int_affected_subscription_payment_keys` | 이번 재계산 경계에서 구독 결제 Fact가 다시 계산해야 하는 결제 시도 1건            | `payment_id`, `business_date_key` | `fact_subscription_payments`                       |

`business_date_key`는 각 도메인의 Business Event Time에서 유도한다. 주문 축은 구매일, 구독 결제 축은 결제일이다.

### 기각한 대안

단일 다형 모델 `int_affected_keys(affected_domain, entity_key, business_date_key)`를 검토했다. 기각 사유는 두 가지다.

- Mart Grain 계약 1.1절은 모델마다 Grain 문장 하나를 요구한다. 다형 모델은 "한 행이 무엇인가"가 `affected_domain` 값에 따라 달라져 Grain 문장을 하나로 쓸 수 없다.
- Fact가 `where affected_domain = '...'` 필터를 빠뜨리면 전혀 다른 도메인의 Key로 재계산 범위를 잡는다. 실패가 오류가 아니라 잘못된 결과로 나타난다.

단, `control.affected_keys`는 도메인 컬럼을 가진 단일 테이블로 유지한다. PRD Section 16이 기록 테이블을 단수로 지정하고, 이 테이블의 용도는 실행이 아니라 조회이기 때문이다.

---

## 4. D-2. 구독 결제 축 영향 Key 유도 규칙

`fact_subscription_payments`의 한 행은 `payment_id` 1건이고, 그 행이 보유한 값 중 재계산으로 바뀔 수 있는 것은 세 가지다. 결제 원본 속성, `subscription_key`, `customer_key`다. 따라서 영향 원천도 세 가지다.

| 원천 | 조건                                                                                                      | 재계산이 필요한 이유                                             |
| ---- | --------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| S1   | `stg_subscription_payments`에서 `_batch_id`가 재계산 경계보다 큰 행                                        | 결제 시도가 신규 도착했거나 결과·금액이 갱신됐다                 |
| S2   | 이번 경계에서 관측이 들어온 `subscription_id`의 최소 관측 `updated_at` 이후에 발생한 그 계약의 모든 결제   | 계약 SCD2 Version 구성이 바뀌어 `subscription_key`가 달라질 수 있다 |
| S3   | 이번 경계에서 관측이 들어온 `customer_id`의 최소 관측 `updated_at` 이후에 발생한 그 고객의 모든 결제       | 등급 SCD2 Version 구성이 바뀌어 `customer_key`가 달라질 수 있다   |

S2의 관측 입력은 `stg_customer_subscription_observations`, S3의 관측 입력은 `stg_customer_tier_observations`다. 결제 행의 `customer_id`는 `stg_subscription_payments`가 이미 계약 조인으로 노출하고 있다.

### 4.3. SCD2 구간 조인을 쓰지 않는 이유

현행 `int_affected_business_dates`는 `int_customer_history`의 Version 구간에 주문을 조인해 영향 주문을 찾는다. 이 방식은 Version이 **새로 생기거나 이동할 때**는 맞지만, Version이 **사라질 때** 누락을 만든다.

관측이 재적재되어 `attribute_hash`가 바뀌면 기존 Version 하나가 사라지고 그 구간의 사건이 인접한 이전 Version으로 흡수된다. 흡수하는 Version의 `valid_from`은 이번 경계보다 과거이므로 `valid_from >= 경계` 조건에 걸리지 않는다. 그 구간의 사건은 키가 바뀌었는데도 재계산 대상에서 빠진다.

Business Key 단위로 경계를 잡으면 이 경우가 함께 잡힌다. "이번 경계에서 관측이 바뀐 고객의, 그 최소 변경 시각 이후 사건 전부"는 Version 구성이 어떻게 바뀌든 상위 집합이다. 구간 조인보다 단순하고, SCD2 재계산 결과에 의존하지 않는다.

---

## 5. D-3. 고객 축 계약 불일치 수정

### 현행 문제

`int_affected_business_dates.sql`의 `customer_axis_batch` CTE는 구독 관측과 등급 관측의 `updated_at`을 `union`한 뒤, 그 전역 최소값 이후의 `int_customer_history` Version에 주문을 조인한다. 문제는 세 가지다.

1. `int_customer_history`는 등급 축만 만든다. 구독 관측은 이제 이 모델에 아무 Version도 만들지 않으므로, 구독 관측으로 고객 축 재계산을 유발하는 것은 근거가 없다. 구독 관측이 실제로 바꾸는 것은 `dim_subscription`과 `fact_subscription_payments`이고, 그 경로는 D-2가 담당한다.
2. 경계가 전역 `min(updated_at)` 하나다. 한 고객의 3년 전 지연 관측 하나가 **모든 고객**의 그 이후 Version을 재계산 대상으로 만든다.
3. `latest_batch`를 `stg_orders`에서만 뽑는다. `_batch_id`는 `{dag_id}__{logical_date}` 형식으로 Run 안의 모든 Table이 공유하지만(`src/ingestion/batch.py:32`), 주문 Table 추출이 실패하거나 건너뛴 Run에서는 이 값이 뒤처진다.

### 결정

- 고객 축 입력을 `stg_customer_tier_observations`로 한정한다. 구독 관측 참조를 제거한다.
- 경계를 `customer_id`별 최소 변경 `updated_at`으로 좁힌다. 변경된 고객의 그 시각 이후 주문만 대상으로 한다.
- 배치 경계를 `stg_orders`의 최신 `_batch_id`가 아니라 D-4의 처리 Watermark로 바꾼다.

주문 축 영향 원천은 수정 후 다음과 같다. Product/Seller 축은 현행 규칙을 유지한다.

| 원천 | 조건                                                                                 |
| ---- | ------------------------------------------------------------------------------------ |
| O1   | `stg_orders`에서 `_batch_id`가 경계보다 큰 행                                        |
| O2   | `stg_order_items` / `stg_payments`에서 경계보다 큰 행이 연결된 주문                  |
| O3   | 등급 관측이 바뀐 `customer_id`의 최소 변경 `updated_at` 이후 주문                    |
| O4   | `stg_products` / `stg_sellers`에서 경계보다 큰 행을 사용한 주문                      |

---

## 6. D-4. 재계산 경계는 처리 Watermark로 정한다

### 현행 문제

현행 모델은 "가장 최근 `_batch_id` 하나"를 경계로 쓴다. dbt Build 사이에 Ingestion Batch가 둘 이상 들어오면 이전 Batch의 변경은 영구히 재계산되지 않는다. 실패한 Build를 재실행하는 경우, 실패 이후 새 Batch가 들어왔다면 같은 구멍이 생긴다.

### 결정

`control` Schema에 처리 Watermark를 둔다.

```text
control.dbt_processed_batch (
    processed_batch_id VARCHAR NOT NULL,
    invocation_id      VARCHAR NOT NULL,
    processed_at       TIMESTAMPTZ NOT NULL
)
```

- `on-run-start` Hook이 `control` Schema와 이 Table을 생성한다. 영향 Key 모델이 실행 시점에 Table을 읽을 수 있어야 하기 때문이다.
- 영향 Key 모델은 `_batch_id > (select coalesce(max(processed_batch_id), '') from control.dbt_processed_batch)`로 경계를 잡는다. `_batch_id`는 고정 폭 UTC 타임스탬프를 포함하므로 같은 `dag_id` 안에서 사전식 비교가 시간 순서와 일치한다. 이 가정은 Test로 고정한다.
- `on-run-end` Hook이 이번 Build가 본 최대 `_batch_id`를 기록한다. Build가 중간에 실패하면 Watermark는 전진하지 않고, 다음 Build가 같은 범위를 상위 집합으로 다시 처리한다. 재계산은 멱등이므로 안전하다.
- dbt는 Model이 실패해도 `on-run-end` Hook을 실행한다. 따라서 "전진하지 않는다"를 두 개의 Guard로 강제한다. 둘 다 통과할 때만 Watermark를 전진시킨다.
  - **Guard 1 (선택 범위)**: `selected_resources`가 네 Fact를 모두 포함한다. 부분 선택 Build가 경계를 전진시키지 못하게 한다.
  - **Guard 2 (실행 결과)**: `on-run-end` Context의 `results`에 상태가 `error`, `fail`, `runtime error`, `skipped`인 Node가 하나도 없다. Model 실패, 상류 실패로 인한 Skip, Test 실패를 모두 막는다. `warn`은 막지 않는다. Build를 중단시키지 않는 경고이기 때문이다.

---

## 7. D-5. `control.affected_keys` 스키마 변경

현행 스키마는 `order_id`를 필수 컬럼으로 가져 구독 결제 축을 담을 수 없다. 도메인 컬럼을 가진 형태로 바꾼다.

```text
control.affected_keys (
    invocation_id     VARCHAR NOT NULL,
    affected_domain   VARCHAR NOT NULL,   -- 'order' | 'subscription_payment'
    entity_key        VARCHAR NOT NULL,   -- order_id 또는 payment_id
    business_date_key INTEGER NOT NULL,
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
)
```

`record_affected_keys()` 매크로는 두 영향 Key 모델을 각각 자기 도메인 값으로 `insert`한다. 기존 테이블은 Phase 6 범위에서 보존 가치가 없으므로 재생성한다.

---

## 8. D-6. Fact는 `control.affected_keys`가 아니라 영향 Key 모델을 참조한다

### 결정

Fact의 Incremental 필터는 `ref('int_affected_order_keys')` 또는 `ref('int_affected_subscription_payment_keys')`를 직접 읽는다. `control.affected_keys`는 읽지 않는다.

### 근거

`record_affected_keys()`는 `on-run-end` Hook이다. Fact가 실행되는 시점에 이번 `invocation_id`의 행은 아직 없다. Fact가 `control.affected_keys`를 읽으면 직전 Invocation의 범위로 재계산하게 된다. 이 순서 의존은 Build가 한 번 실패하면 어긋난다.

영향 Key 모델은 `view`이므로 Fact 실행 시점에 평가된다. dbt DAG 상에서도 Fact가 영향 Key 모델의 하위가 되어 실행 순서가 강제된다. 영향 Key 모델은 Fact를 참조하지 않으므로 순환은 생기지 않는다.

`control.affected_keys`의 역할은 PRD Section 16이 요구하는 감사 기록으로 한정한다. Late Arrival 전후 Affected Date 증거(Portfolio Evidence)와 `P6-23` 비교 시 어떤 범위가 재계산됐는지 조회하는 데 쓴다.

---

## 9. D-7. Fact별 Incremental 필터와 `unique_key`

### 자식 Fact의 삭제 문제

`fact_order_items`의 dbt `unique_key`는 현재 `['order_id', 'order_item_id']`다. `delete+insert` 전략은 `delete from target where (order_id, order_item_id) in (select distinct order_id, order_item_id from source)`를 생성한다(dbt-core `default__get_delete_insert_merge_sql`). 재계산 대상 주문에서 Line이 **하나 사라진** 경우 그 Line은 `source`에 없으므로 삭제되지 않고 Fact에 남는다. Full Refresh 결과와 달라지므로 `P6-23`에서 Hash 불일치로 드러난다.

### 결정

dbt의 `unique_key`를 **재계산 단위**로 정의하고, Grain 유일성은 지금처럼 Test가 강제한다. 두 개념을 분리한다.

| Model                        | dbt `unique_key` (재계산 단위) | Grain 유일성 강제 수단                     | Incremental 필터 대상 |
| ---------------------------- | ------------------------------ | ------------------------------------------ | --------------------- |
| `fact_orders`                | `order_id`                     | `schema.yml`의 `unique`                    | `order_id`            |
| `fact_order_items`           | `order_id`                     | `tests/fact_order_items_unique.sql`        | `order_id`            |
| `fact_payments`              | `order_id`                     | `tests/fact_payments_unique.sql`           | `order_id`            |
| `fact_subscription_payments` | `payment_id`                   | `schema.yml`의 `unique`                    | `payment_id`          |

자식 Fact의 `unique_key`를 부모 키로 바꾸면 삭제 절이 `where order_id in (select distinct order_id from source)`가 되어, 영향 주문의 기존 Line을 전부 지운 뒤 새 Line 집합을 넣는다. 사라진 Line이 남지 않는다. Incremental Select가 영향 주문의 **모든** Line을 반환하는 것이 이 방식의 전제이며, 필터를 `order_id in (...)`로 거는 한 성립한다.

Mart Grain 계약의 Unique Key 문장(`(order_id, order_item_id)`)은 바꾸지 않는다. 그 문장은 Grain 선언이고, dbt `unique_key`는 교체 단위 설정이다. 혼동을 막기 위해 각 모델 상단 주석에 이 구분을 적는다.

### 필터 형태

```sql
{{ config(unique_key='order_id', incremental_strategy='delete+insert') }}

select ...
from {{ ref('int_order_fact_ready') }}
{% if is_incremental() %}
where order_id in (select order_id from {{ ref('int_affected_order_keys') }})
{% endif %}
```

`incremental_predicates`는 쓰지 않는다. 이 옵션은 삭제 절에 `and`로 붙어 범위를 **좁히기만** 하므로 위 삭제 문제를 풀지 못한다.

---

## 10. D-8. Dimension Materialization은 `table`로 정정한다

Mart Grain 계약 2.5절은 `dim_subscription`의 Materialization을 `incremental`로 적었다. 구현은 `dbt_project.yml`에서 `marts.dimensions`에 `+materialized: table`을 주고 있어 실제로는 Table이다. `dim_subscription.sql`과 `dim_customer.sql`의 `unique_key` / `incremental_strategy` 설정은 현재 아무 효과가 없다.

### 결정 (lead 승인 완료)

계약을 `table`로 정정한다. 근거는 두 가지다.

- SCD2 Version 구성은 지연 관측 하나로 과거 구간 전체가 재배열된다. Incremental로 만들면 Business Key 단위 `delete+insert`가 필요한데, 그 영향 Key를 다시 계산해야 하므로 Dimension 전량 재계산과 비용 차이가 크지 않다. 재배열이 일어나는 빈도 대비 이득이 작다.
- Dimension을 매번 전량 재계산하면 `P6-23`의 Hash 비교에서 Dimension은 정의상 일치한다. 검증 대상이 Fact로 좁혀진다.

따라서 다음을 함께 처리한다.

- `docs/reference/mart-grain.md` 2.5절의 `Materialization: incremental`을 `table`로 고친다. 다른 Dimension 절에 같은 표기가 있으면 함께 고친다.
- `dim_subscription.sql`과 `dim_customer.sql`의 무효한 `config(unique_key=..., incremental_strategy=...)`를 제거한다. 설정이 남아 있으면 Materialization이 Incremental이라고 오독된다.
- Dimension용 영향 Business Key 모델은 만들지 않는다. Phase 6 범위에 추가하지 않는다.

---

## 11. 변경 대상 파일

| 경로                                                              | 변경 내용                                                                            |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `dbt/models/intermediate/int_affected_business_dates.sql`         | `int_affected_order_keys.sql`로 이름 변경, 고객 축 수정(D-3), 경계를 Watermark로 교체 |
| `dbt/models/intermediate/int_affected_subscription_payment_keys.sql` | 신규. D-2의 S1~S3 구현                                                               |
| `dbt/models/intermediate/schema.yml`                              | 두 영향 Key 모델의 Grain·Unique Key Test 추가                                        |
| `dbt/macros/record_affected_keys.sql`                             | 도메인 컬럼 스키마로 변경, 두 모델 기록                                             |
| `dbt/macros/processed_batch_watermark.sql`                        | 신규. `on-run-start` 생성, `on-run-end` 전진                                         |
| `dbt/dbt_project.yml`                                             | `on-run-start` 추가, `on-run-end`에 Watermark 전진 추가                              |
| `dbt/models/marts/facts/*.sql`                                    | `is_incremental()` 필터와 `unique_key` 조정(D-7)                                     |
| `dbt/tests/`                                                      | 영향 Key 누락 검증 Test 추가                                                         |
| `dbt/models/marts/dimensions/dim_subscription.sql`, `dim_customer.sql` | 무효한 `unique_key` / `incremental_strategy` 설정 제거(D-8)                      |
| `docs/reference/mart-grain.md`                                     | Dimension Materialization 표기를 `table`로 정정(D-8)                                 |

## 12. 검증 방법

| 항목                        | 검증                                                                                             |
| --------------------------- | ------------------------------------------------------------------------------------------------ |
| 영향 Key Grain              | 두 모델의 Key 중복 0을 Singular Test로 강제                                                      |
| 지연 주문 재계산            | 기존 `tests/integration/test_order_e2e_and_late_order_mart_integration.py`(AC-11) 통과 유지        |
| 지연 구독 결제 재계산       | 신규 통합 Test. 과거 `payment_at` 결제 도착 시 해당 `payment_date_key` Fact가 갱신되는지 확인     |
| 계약 Version 변경 재계산    | 신규 통합 Test. 과거 시점 계약 관측 도착 시 그 이후 결제의 `subscription_key`가 갱신되는지 확인    |
| 다중 Batch 경계             | dbt Build 없이 Batch 2회 적재 후 1회 Build 시 두 Batch 변경이 모두 반영되는지 확인                |
| 재계산 정확성 전체          | `P6-23` Logical Hash 비교가 최종 판정. 영향 Key 누락은 Hash 불일치로 드러난다                    |

## 13. 구현 순서

1. D-4 Watermark 매크로와 Hook. 이후 모든 변경의 전제다.
2. D-3 주문 축 수정과 모델 이름 변경.
3. D-2 구독 결제 축 모델 신규 작성.
4. D-5 `control.affected_keys` 스키마 변경.
5. D-7 Fact 필터 적용. 여기까지가 `P6-22`다.
6. 12절의 Test 추가.

구현 계획(`writing-plans`)은 lead 승인 뒤 `docs/superpowers/plans/`에 작성한다.
