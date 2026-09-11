# Fact 계층 책임 분리 정비 계획

> 상태: In progress — Fact 책임 분리는 완료했고, Metrics 기준선 대조와 Publish 보존 검증이 남아 있다.
> 작성일: 2026-09-11
> 관련 문서:
> [데이터 변환 흐름](00-data-transformation-flow.md),
> [Phase 5. dbt DuckDB Modeling](../phases/phase-05-dbt-duckdb-modeling.md),
> [Phase 6. Data Quality & Publish](../phases/phase-06-data-quality-publish.md),
> [PRD v1.8](../../PRD_v1.8.md)

## 1. 목적과 결정 범위

현재 Mart는 Model별 Grain과 Unique Key를 정의하고, 서로 다른 Item·Payment Grain을 직접
다대다 결합하지 않도록 방지한다. 다만 `fact_orders` SQL 안에 주문 Item 집계와 주문 단위
파생값 계산이 함께 있다. 이 문서는 이를 Intermediate 계층으로 옮겨 Fact가 확정된 Grain의
행을 제공하는 역할에 집중하도록 정비하는 계획이다.

이번 결정은 Fact에서 값을 전혀 계산하지 않는 원자 Fact만을 강제하지 않는다. 주문 1행에서
공통으로 쓰는 `gross_order_value`, 배송 소요일 같은 안정적인 주문 단위 측정값은 계속
`fact_orders`의 컬럼으로 제공한다. 단, 집계·계산 SQL의 책임은 Intermediate에 두고 Fact는
그 결과를 투영한다.

```text
Staging       원본 Naming·Type·상태 표준화
Intermediate  조인, Grain 정렬, 사전 집계, 파생 측정값 계산
Fact          문서화된 Grain의 키·차원키·준비된 측정값 투영
Metrics       기간·상태·등급별 최종 집계와 비율 계산
```

## 2. 현재 상태

### 2.1 Fact 내부 책임 혼재

`fact_orders`는 주문 1건 Grain을 지키지만 아래 작업을 같은 SQL에서 수행한다.

| 작업 | 현재 위치 | 영향 |
| --- | --- | --- |
| 주문별 Item 가격·배송비 합계 | `fact_orders`의 `item_totals` CTE | 하위 주문 Line Grain의 `SUM()`이 Fact 정의와 섞인다. |
| 주문 총액 | `fact_orders` | `item_subtotal + freight_total` 계산식의 소유 위치가 Fact가 된다. |
| 구매일 키·배송 소요일·지연 여부 | `fact_orders` | 날짜 파생 규칙과 Fact Grain 검증을 별도로 시험하기 어렵다. |
| 주문별 결제 합계 | `int_payment_summary` | 이미 Intermediate로 분리되어 있어 목표 구조의 선례다. |

`fact_order_items`는 `int_order_items_enriched`의 결과를 투영하고, `fact_payments`는
Staging을 투영한다. `fact_subscription_payments`는 결제 시점의 고객 SCD2 키를 찾는
Temporal Join을 수행하므로, 같은 원칙을 적용하면 이 결합도 Intermediate로 이동할 대상이다.

### 2.2 넓은 Fact와 많은 Dimension 행의 원인

“Fact는 좁고 길며 Dimension은 넓고 짧다”는 흔한 경향이지 모델의 정의는 아니다. 현재는
다음 이유로 그 모양이 약해진다.

| 관찰 | 원인 | 판단 |
| --- | --- | --- |
| `fact_orders`가 넓다 | 금액·배송 측정값 외에 `customer_city`, `customer_state` 주문 스냅샷을 반복 보관한다. | 현재는 주문 시점 속성으로 유지한다. 별도 위치 Dimension이 필요한지는 사용 사례·중복 비용을 기준으로 후속 검토한다. |
| `dim_customer` 행이 많다 | 고객은 본래 고카디널리티이며, `dim_customer`는 Type 2 이력이라 고객의 상태·등급 변경마다 Version 행이 추가된다. | 정상이다. 고객 수가 주문 수와 비슷한 Olist 특성에서는 Order Fact와 행 수가 크게 차이 나지 않는다. |
| `dim_customer`가 넓다 | 구독 상태·등급·기간·재가입 측정값과 SCD2 기술 컬럼을 함께 보관한다. | 분석 시점에 필요한 속성만 SCD2로 관리하는 것이 중요하다. `next_billing_at`을 Hash에서 제외한 현재 결정은 불필요한 Version 증가를 막는다. |
| `fact_order_items`, `fact_payments`가 길다 | 각각 주문 Line·결제 Sequence라는 하위 이벤트 Grain이다. | 일반적인 Event Fact 형태이며 정상이다. |

기준 재생성 시 Source 기준으로 주문은 99,441행, 사람 단위 구독·등급은 각각 96,096행이다.
따라서 주문 Fact와 고객 Dimension의 행 수가 비슷한 것은 고객 한 명당 주문 수가 낮은 원천
분포와 고객 고카디널리티의 결과다. 향후 구독 상태·등급 변경이 누적되면 `dim_customer`의
SCD2 Version 수는 고객 수보다 커질 수 있다.

### 2.3 `dim_membership` 분리 사전 검토

`dim_membership`은 두 가지 서로 다른 모델을 뜻할 수 있으므로 구분해야 한다.

| 후보 | 현재 적합성 | 판단 |
| --- | --- | --- |
| `dim_membership_tier` — `BRONZE`·`SILVER`·`GOLD` 3개 코드의 설명·정책 참조 차원 | 낮음 | 현재는 등급 이름 외에 설명·혜택·정책 속성이 없고, Fact가 별도 FK를 참조할 이유도 없다. 향후 등급별 혜택·표시명·유효 정책이 생길 때 추가한다. |
| 고객별 등급 이력 `dim_membership` — 고객·등급·유효 기간을 보관하는 Type 2 차원 | 낮음 | 주문 Fact와 구독 결제 Fact가 고객의 구독 상태와 등급을 같은 시점에 분석해야 한다. 분리하면 두 개의 시간 구간 Join 또는 두 FK가 필요해지고, 고객 SCD2의 결합 Version과 서로 어긋날 위험이 생긴다. |

현재 `dim_customer`는 구독 상태와 거래 실적 등급을 결합한 고객 Version 1행이다. 이는 Source를
둘로 나눈 B안과 별개로 Warehouse에서 하나의 시점별 고객 분석 키를 제공하려는 결정이다.
등급을 별도 고객 차원으로 옮겨도 전체 이력 행 수가 크게 줄지 않으며, Fact별 Temporal Join이
두 번으로 늘어난다. 따라서 현재는 `membership_tier`를 `dim_customer`에 유지한다.

재검토 조건은 다음과 같다.

- 등급별 혜택·가격·정책·표시명이 다수 생겨 3개 코드 이상의 독립 참조 속성이 필요할 때
- 등급 이력만을 별도 보안·보존·소유권 경계로 관리해야 할 때
- 구독 상태와 무관하게 등급 이력만을 주로 분석하는 Fact가 새로 생길 때

이 경우에도 먼저 작은 코드 참조 차원 `dim_membership_tier`를 추가하고, 고객별 Type 2 차원
분리는 Fact의 두 시점 키와 중복 없는 결합 계약을 설계한 뒤에만 수행한다.

## 3. 발생 가능한 문제

| 위험 | 현재 원인 | 결과 |
| --- | --- | --- |
| Grain 책임 불명확 | Fact SQL에 하위 Grain 집계 CTE가 존재한다. | Model 리뷰 때 주문 Fact의 키·측정값 계약과 집계 안전성을 함께 추적해야 한다. |
| 계산식 중복 | 주문 총액·배송 파생값을 다른 Model이나 Dashboard에서 다시 만들 가능성이 있다. | 같은 지표가 서로 다른 값으로 표시될 수 있다. |
| Fan-out 회귀 | Item·Payment·고객 이력을 Fact에서 추가 Join할 때 Grain 검토가 누락될 수 있다. | 금액·건수 중복 집계 위험이 커진다. |
| Incremental 영향 범위 불명확 | Late Arrival 또는 고객 SCD2 변경의 재계산 대상이 Fact SQL의 CTE에 숨는다. | 변경된 주문·고객의 재처리 규칙을 검증하기 어렵다. |
| Fact 폭 증가 | 반복 속성이나 비공통 파생값이 Fact에 계속 추가될 수 있다. | 저장 중복, 이해 비용, BI 모델의 선택 오류가 증가한다. |
| SCD2 행 증가 | 분석 가치가 낮은 속성까지 Hash에 넣거나, Current와 이력을 구분하지 않는다. | 고객 Dimension의 Version 수와 Temporal Join 비용이 불필요하게 증가한다. |

## 4. 목표 계층 규칙

### 4.1 허용 작업

| 계층 | 허용 작업 |
| --- | --- |
| Staging | 원본 이름·Type·상태값 정규화, 최신 Bronze 행 선택 |
| Intermediate | 한 Grain으로의 사전 집계, Dimension/SCD2 시점 결합, 날짜·금액·상태 파생, 영향 범위 계산 |
| Fact | 문서화된 Grain의 Key·Foreign Key·Intermediate에서 준비된 측정값 투영, Materialization과 Unique Key 강제 |
| Metrics | `SUM`, `COUNT`, `AVG`, 비율, 기간·세그먼트별 `GROUP BY` |

### 4.2 금지 또는 검토 대상

- Fact SQL에 하위 Grain을 `GROUP BY`해 만드는 CTE를 새로 추가하지 않는다.
- Fact SQL에 새 비즈니스 `CASE`, 날짜 차이, 금액 산식을 직접 추가하지 않는다.
- 서로 다른 Fact Grain을 직접 Join한 뒤 측정값을 집계하지 않는다.
- 고객·상품·지역 속성을 Fact에 추가할 때는 주문 시점 스냅샷이 필요한지와 별도 Dimension이
  더 적합한지를 먼저 결정한다.
- SCD2 Hash에는 분석적으로 의미 있는 상태 변화만 넣고, 주기적으로 변하지만 분석 의미가
  없는 운영 속성은 제외한다.

이 규칙은 이미 존재하는 Fact 컬럼을 즉시 제거한다는 뜻이 아니다. 최종 Fact가 제공하는
측정값의 의미·Grain은 유지하고, 계산 위치와 검증 경계를 옮기는 변경이다.

## 5. 수정 계획

### Step 1. 계약 기준선 고정

- [x] 각 Fact의 Grain, Unique Key, 허용 Measure, 입력 Intermediate를 데이터 변환 흐름의
  Model·컬럼 사전으로 확정했다.
- [x] 기존 `fact_orders` 결과의 행 수와 주문 키·금액·배송 Measure Logical Hash를 기록했다.
  기준선과 정비 후 Incremental·Full Refresh 결과는 모두 `99,441`행,
  `9e16b41a2f5ecd8d7379876eb3e838a1`이다.
- [x] `customer_city`, `customer_state`는 주문 시점 스냅샷으로 유지하기로 결정했다. 별도 위치
  Dimension 분리는 후속 검토 범위다.

### Step 2. 주문 사전 집계·파생 Intermediate 분리

- [x] `int_order_item_totals`를 만들었다. 주문 Item Line에서 `item_subtotal`, `freight_total`을
  주문 1건 Grain으로 집계한다.
- [x] `int_order_fact_ready`를 만들었다. `int_orders_enriched`, Item 합계,
  `int_payment_summary`를 주문 1건 Grain으로 결합하고, 주문 총액·구매일 키·배송 측정값을 계산한다.
- [x] 이 Model의 Grain·NULL 정책·금액 산식·Late Arrival 영향 범위는 기존 Fact 결과 비교와
  `dbt build`로 검증한다. 현재 `customer_city`, `customer_state`는 주문 시점 스냅샷으로 유지한다.

### Step 3. 구독 결제 시점 결합 분리

- [x] `int_subscription_payments_enriched`를 만들었다. 구독 결제와 `dim_customer`의
  `[valid_from, valid_to)` 구간 결합을 이곳에서 수행한다.
- [x] 결제 행이 있는 고정 Seed Generator Fixture에서 Temporal Join을 검증했다. Fixture는
  `NON_MEMBER/BRONZE` 초기 관측, `ACTIVE` 전이, 결제를 서로 다른 Bronze Batch로 수집하고,
  임시 Warehouse에서 결제 시각의 `ACTIVE/BRONZE` 고객 Version에 정확히 한 번 결합됨을 확인한다.

### Step 4. Fact를 투영 계층으로 축소

- [x] `fact_orders`가 `int_order_fact_ready`의 주문 1건 결과만 선택하도록 바꿨다.
- [x] `fact_subscription_payments`가 `int_subscription_payments_enriched`를 선택하도록 바꿨다.
- [x] `fact_order_items`, `fact_payments`의 입력 Model·Grain·Unique Key도 데이터 변환 흐름의
  Model·컬럼 사전에 같은 계약 형식으로 기록했다.

### Step 5. 회귀 방지와 검증

- [x] Fact SQL에 새 `GROUP BY`, 집계 함수, 비즈니스 파생식이 들어가지 않는 정적 계약 검사를 추가했다.
- [x] Fact별 Unique Key, 행 수, 금액·배송 Measure, 구독 결제 고객 키의 변경 전후 결과를 대조했다.
  주문 Fact의 행 수·키·Measure는 기준선으로 대조했고, 구독 결제는 고정 Seed Fixture로 비어 있지
  않은 결제의 고객 키·상태·등급 시점 결합을 대조한다.
- [x] `dbt build`와 `dbt build --full-refresh`가 각각 108개 항목을 통과했고, 주문 Fact Logical Hash가 같다.
- [ ] Metrics View의 합계가 정비 전 기준선과 일치하는지 검증한다.

### Step 6. 문서와 Publish 연결

- [x] 데이터 변환 흐름과 Phase 5 파일·폴더 요약에 새 Intermediate와 책임 경계를 반영했다.
- [ ] Phase 6 Publish 과정에서 Build/Test 실패 시 기존 Published Mart를 보존하는 검증에 새
  Intermediate·Fact 계약을 포함한다.

## 6. 완료 기준

- `fact_orders`와 `fact_subscription_payments`에 집계·비즈니스 파생 SQL이 없다.
- 주문·결제·구독 결제 Fact의 Grain과 Unique Key가 변하지 않는다.
- 정비 전후 주문 키·금액·배송 Measure·구독 결제 고객 키 결과가 동일하다.
- Metrics 집계 결과와 dbt Full Refresh Logical Hash가 동일하다.
- 고객 SCD2의 구간 중복은 0이고, 고객별 Current Version은 정확히 1개다.
- 새 책임 경계와 운영·재처리 방법이 Phase 5·6 문서에 기록된다.

## 7. 범위 밖

- 고객 SCD2를 Type 1로 바꾸거나 과거 구독·등급 이력을 삭제하는 작업
- 현재 기준으로 필요한 주문·배송 Measure의 의미 변경
- Metabase Dashboard 구현과 새 비즈니스 지표 추가
- Source·Bronze Schema 재기준화
