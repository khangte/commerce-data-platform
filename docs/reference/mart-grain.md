# Mart Grain 계약

> 상태: Reference
> 기준 문서: [PRD v1.11](../../PRD_v1.11.md), [데이터 변환 흐름](data-transformation-flow.md), [Phase 6](../phases/phase-06-dimensional-modeling.md)

이 문서는 Warehouse Mart의 Grain, Unique Key, 컬럼, Measure 계약을 정의하는 단일 정본이다. `dbt/models/marts/*/schema.yml`은 이 문서에서 생성한다. 컬럼을 추가하거나 바꿀 때는 이 문서를 먼저 고치고 `schema.yml`을 다시 만든다.

## 1. 작성 규칙

### 1.1 Grain

Grain은 "한 행이 무엇 하나를 나타내는가"의 선언이다. Model마다 Grain 문장을 정의로 삼고, Unique Key는 그 문장을 데이터로 강제하는 수단이다.

컬럼 목록만으로는 중복의 의미를 판정할 수 없다. `(order_id, order_item_id)`가 Unique하다는 사실만으로는 그 Table이 주문 Line을 나타내는지 배송 이벤트를 나타내는지 알 수 없다. 문장이 있어야 새 컬럼이 그 문장을 깨는지 판단할 수 있다.

### 1.2 컬럼 종류

모든 컬럼은 아래 다섯 종류 중 하나다. 종류가 그 컬럼에 적용할 규칙을 정한다.

| 종류         | 의미                            | Test                                                | 비고                                        |
| ------------ | ------------------------------- | --------------------------------------------------- | ------------------------------------------- |
| `PK`         | Grain을 강제하는 Key            | `not_null`, `unique`(단일) 또는 Singular Test(복합) | Grain 문장과 1:1 대응                       |
| `FK`         | Dimension 참조 Key              | `not_null`, `relationships`                         | 대상 Model을 함께 적는다                    |
| `Degenerate` | Dimension 없이 Fact에 남는 속성 | `accepted_values` 등                                | 자체 Dimension을 만들 만큼 속성이 없을 때만 |
| `Measure`    | 집계 대상 수치                  | 범위·부호 Test                                      | 5절 규칙 적용                               |
| `Attribute`  | Dimension의 서술 속성           | 필요 시                                             | Dimension 전용                              |

### 1.3 새 컬럼을 추가할 때

해당 Model의 Grain 문장이 여전히 참인지 먼저 확인한다.

| 증상                          | 의미                                | 조치                                                                    |
| ----------------------------- | ----------------------------------- | ----------------------------------------------------------------------- |
| 한 행에 값이 여러 개 필요하다 | 추가하려는 값의 Grain이 더 세밀하다 | 별도 Model로 분리하거나, Intermediate에서 이 Grain으로 집계한 뒤 넣는다 |
| 여러 행에 같은 값이 반복된다  | 추가하려는 값의 Grain이 더 거칠다   | Dimension으로 분리하고 FK로 연결한다                                    |
| 행 수가 늘어난다              | Grain 문장 자체가 바뀐다            | 새 Model을 만든다. 기존 Model의 Grain을 바꾸지 않는다                   |

### 1.4 1:1 Fact를 따로 만들지 않는다

Grain이 같은 두 Fact는 한 Table이어야 한다. 별도 Model로 쪼개면 같은 Key를 가진 1:1 Fact가 되어 Join 비용만 늘어난다.

### 1.5 Model 작성 양식

각 Model은 아래 양식으로 쓴다.

```markdown
### N.M `<model_name>`

- Grain: 한 행은 <무엇> 하나를 나타낸다.
- Unique Key: `<컬럼>` 또는 `(<컬럼>, <컬럼>)`
- Materialization: `table` | `incremental` | `view`
- 출처: `<상류 Model>`

| 컬럼 | 타입 | 종류 | Null | 정의 / Test |
| ---- | ---- | ---- | ---- | ----------- |
|      |      |      |      |             |
```

- 타입은 DuckDB 타입으로 적는다. 금액은 `decimal(14,2)`를 쓰고 `double`을 쓰지 않는다.
- `Null` 열은 `Y`(허용) 또는 `N`(불가)로 적는다.
- `정의 / Test` 열에는 Measure의 계산식, FK의 참조 대상, 값 목록을 적는다.

## 2. Dimension

| Model          | Grain              | Unique Key     | Materialization |
| -------------- | ------------------ | -------------- | --------------- |
| `dim_customer` | 고객 상태 버전 1행 | `customer_key` | incremental     |
| `dim_date`     | 날짜 1일 1행       | `date_key`     | table           |
| `dim_product`  | 상품 1개 1행       | `product_key`  | table           |
| `dim_seller`   | 판매자 1명 1행     | `seller_key`   | table           |

### 2.1 `dim_customer`

- Grain: 고객 상태 버전 1행
- Primary Key: `customer_key`
- Business Key: `customer_id`
- Version Unique Key: (`customer_id`, `valid_from`)
- SCD Type: Type 2
- Materialization: incremental
- 출처: `customers`, `customer_membership_tiers`

| 컬럼              | 타입        | 종류                | Null | 정의 / Test                                                                                     |
| ----------------- | ----------- | ------------------- | ---- | ----------------------------------------------------------------------------------------------- |
| `customer_key`    | BIGINT      | Surrogate Key / PK  | N    | 고객 상태 버전을 식별하는 DW 내부 키 / `unique`, `not_null`                                     |
| `customer_id`     | VARCHAR     | Business Key        | N    | 실제 고객 식별자. Olist `customer_unique_id` 매핑 / `not_null`                                  |
| `membership_tier` | VARCHAR     | Dimension Attribute | N    | 고객의 거래 실적 등급. SCD2로 이력을 관리 / `not_null`, `accepted_values: BRONZE, SILVER, GOLD` |
| `valid_from`      | TIMESTAMPTZ | SCD2 Metadata       | N    | 해당 고객 버전의 유효 시작 시각 / `not_null`                                                    |
| `valid_to`        | TIMESTAMPTZ | SCD2 Metadata       | Y    | 해당 고객 버전의 유효 종료 시각. 현재 버전은 `NULL`                                             |
| `is_current`      | BOOLEAN     | SCD2 Metadata       | N    | 현재 유효한 고객 버전 여부 / `not_null`                                                         |

### 2.2 `dim_date`

- Grain: 날짜 1일 1행
- Primary Key: `date_key`
- Business Key: `full_date`
- Materialization: table
- 생성 방식: 지정 기간의 날짜를 연속 생성

| 컬럼           | 타입     | 종류                | Null | 정의 / Test                                          |
| -------------- | -------- | ------------------- | ---- | ---------------------------------------------------- |
| `date_key`     | INTEGER  | Surrogate Key / PK  | N    | 날짜 식별 키. `YYYYMMDD` 형식 / `unique`, `not_null` |
| `full_date`    | DATE     | Business Key        | N    | 실제 날짜 값 / `unique`, `not_null`                  |
| `year`         | SMALLINT | Dimension Attribute | N    | 연도                                                 |
| `quarter`      | TINYINT  | Dimension Attribute | N    | 분기 번호 `1~4` / `accepted_values: 1,2,3,4`         |
| `month`        | TINYINT  | Dimension Attribute | N    | 월 번호 `1~12`                                       |
| `month_name`   | VARCHAR  | Dimension Attribute | N    | 월 표시명. 예: `January`, `September`                |
| `day`          | TINYINT  | Dimension Attribute | N    | 월 기준 일자 `1~31`                                  |
| `day_of_week`  | TINYINT  | Dimension Attribute | N    | 요일 번호. `1=Monday ~ 7=Sunday`                     |
| `day_name`     | VARCHAR  | Dimension Attribute | N    | 요일 표시명. 예: `Monday`, `Tuesday`                 |
| `week_of_year` | TINYINT  | Dimension Attribute | N    | 연도 기준 주차                                       |
| `is_weekend`   | BOOLEAN  | Dimension Attribute | N    | 토요일 또는 일요일 여부                              |

### 2.3 `dim_product`

- Grain: 상품 1개 1행
- Primary Key: `product_key`
- Business Key: `product_id`
- Materialization: table
- 출처: `products`

| 컬럼                    | 타입    | 종류                | Null | 정의 / Test                                   |
| ----------------------- | ------- | ------------------- | ---- | --------------------------------------------- |
| `product_key`           | BIGINT  | Surrogate Key / PK  | N    | DW 내부 상품 식별 키 / `unique`, `not_null`   |
| `product_id`            | VARCHAR | Business Key        | N    | Olist 원본 상품 식별자 / `unique`, `not_null` |
| `product_category_name` | VARCHAR | Dimension Attribute | Y    | 상품 카테고리명                               |
| `product_weight_g`      | INTEGER | Dimension Attribute | Y    | 상품 무게(g)                                  |
| `product_length_cm`     | INTEGER | Dimension Attribute | Y    | 상품 길이(cm)                                 |
| `product_height_cm`     | INTEGER | Dimension Attribute | Y    | 상품 높이(cm)                                 |
| `product_width_cm`      | INTEGER | Dimension Attribute | Y    | 상품 너비(cm)                                 |

### 2.4 `dim_seller`

- Grain: 판매자 1명 1행
- Primary Key: `seller_key`
- Business Key: `seller_id`
- Materialization: table
- 출처: `sellers`

| 컬럼           | 타입    | 종류                | Null | 정의 / Test                                     |
| -------------- | ------- | ------------------- | ---- | ----------------------------------------------- |
| `seller_key`   | BIGINT  | Surrogate Key / PK  | N    | DW 내부 판매자 식별 키 / `unique`, `not_null`   |
| `seller_id`    | VARCHAR | Business Key        | N    | Olist 원본 판매자 식별자 / `unique`, `not_null` |
| `seller_city`  | VARCHAR | Dimension Attribute | N    | 판매자가 위치한 도시                            |
| `seller_state` | VARCHAR | Dimension Attribute | N    | 판매자가 위치한 브라질 주(State) 코드           |

## 3. Fact

| Model               | Grain                   | Unique Key                         | Materialization |
| ------------------- | ----------------------- | ---------------------------------- | --------------- |
| `fct_order`         | 주문 1건                | `order_id`                         | incremental     |
| `fct_order_item`    | 주문 상품 항목 1건      | (`order_id`, `order_item_id`)      | incremental     |
| `fct_order_payment` | 주문 내 결제 레코드 1건 | (`order_id`, `payment_sequential`) | incremental     |

### 3.1 `fct_order`

- Grain: 주문 1건
- Unique Key: `order_id`
- Fact Type: Accumulating Snapshot
- Materialization: incremental
- 출처: `orders`
- Dimension 참조: `dim_customer`, `dim_date`

| 컬럼                    | 타입        | 종류                              | Null | 정의 / Test                                                                                                                          |
| ----------------------- | ----------- | --------------------------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `order_id`              | VARCHAR     | Degenerate Dimension / Unique Key | N    | 주문 식별자. Olist 원본 `order_id` / `unique`, `not_null`                                                                            |
| `customer_key`          | BIGINT      | Dimension FK                      | N    | 주문 당시 유효한 고객 버전의 `dim_customer.customer_key` / `not_null`, `relationships → dim_customer.customer_key`                   |
| `purchase_date_key`     | INTEGER     | Dimension FK                      | N    | 주문 발생일에 해당하는 `dim_date.date_key` / `not_null`, `relationships → dim_date.date_key`                                         |
| `source_customer_id`    | VARCHAR     | Degenerate Dimension              | N    | 원본 `customers.customer_id`, 주문-원본고객 추적용 / `not_null`                                                                      |
| `customer_city`         | VARCHAR     | Fact Attribute                    | N    | 주문 당시 고객 도시 스냅샷 / `not_null`                                                                                              |
| `customer_state`        | VARCHAR     | Fact Attribute                    | N    | 주문 당시 고객 주(State) 스냅샷 / `not_null`                                                                                         |
| `order_status`          | VARCHAR     | Fact Attribute                    | N    | 주문의 표준 상태 / `not_null`, `accepted_values: CREATED, APPROVED, PROCESSING, INVOICED, SHIPPED, DELIVERED, CANCELED, UNAVAILABLE` |
| `purchased_at`          | TIMESTAMPTZ | Event Timestamp                   | N    | 고객이 주문을 생성한 시각. 원본 `order_purchase_timestamp` / `not_null`                                                              |
| `carrier_handoff_at`    | TIMESTAMPTZ | Event Timestamp                   | Y    | 주문이 물류사에 전달된 시각. 원본 `order_delivered_carrier_date`                                                                     |
| `estimated_delivery_at` | TIMESTAMPTZ | Event Timestamp                   | N    | 주문 당시 예상 배송 완료 시각. 원본 `order_estimated_delivery_date` / `not_null`                                                     |
| `delivered_at`          | TIMESTAMPTZ | Event Timestamp                   | Y    | 고객에게 실제 배송 완료된 시각. 원본 `order_delivered_customer_date`                                                                 |
| `delivery_days`         | INTEGER     | Derived Measure                   | Y    | 주문 생성부터 실제 배송 완료까지 걸린 일수. `delivered_at - purchased_at`. 미배송 주문은 `NULL`                                      |
| `order_count`           | SMALLINT    | Additive Measure                  | N    | 주문 건수 집계를 위한 상수 값 `1` / `not_null`, `accepted_values: 1`                                                                 |

### 3.2 `fct_order_item`

- Grain: 주문 상품 항목 1건
- Unique Key: (`order_id`, `order_item_id`)
- Fact Type: Transaction Fact
- Materialization: incremental
- 출처: `order_items`, `orders`
- Dimension 참조: `dim_product`, `dim_seller`, `dim_date`

| 컬럼                      | 타입          | 종류                             | Null | 정의 / Test                                                                                          |
| ------------------------- | ------------- | -------------------------------- | ---- | ---------------------------------------------------------------------------------------------------- |
| `order_id`                | VARCHAR       | Degenerate Dimension / Grain Key | N    | Olist 주문 식별자 / `not_null`                                                                       |
| `order_item_id`           | INTEGER       | Degenerate Dimension / Grain Key | N    | 주문 내부 상품 항목 순번 / `not_null`                                                                |
| `product_key`             | BIGINT        | Dimension FK                     | N    | 상품 Dimension 참조 키 / `not_null`, `relationships → dim_product.product_key`                       |
| `seller_key`              | BIGINT        | Dimension FK                     | N    | 판매자 Dimension 참조 키 / `not_null`, `relationships → dim_seller.seller_key`                       |
| `purchase_date_key`       | INTEGER       | Dimension FK                     | N    | 주문이 발생한 날짜 / `not_null`, `relationships → dim_date.date_key`                                 |
| `shipping_limit_date_key` | INTEGER       | Dimension FK                     | N    | 판매자가 물류사에 상품을 전달해야 하는 기한의 날짜 / `not_null`, `relationships → dim_date.date_key` |
| `shipping_limit_at`       | TIMESTAMPTZ   | Event Timestamp                  | N    | 원본 `shipping_limit_date`. 정확한 배송 준비 마감 시각                                               |
| `price`                   | DECIMAL(14,2) | Additive Measure                 | N    | 해당 주문 상품의 판매 가격 / `not_null`, `>= 0`                                                      |
| `freight_value`           | DECIMAL(14,2) | Additive Measure                 | N    | 해당 주문 상품에 배분된 배송비 / `not_null`, `>= 0`                                                  |
| `item_count`              | SMALLINT      | Additive Measure                 | N    | 주문 상품 항목 수 집계를 위한 상수 `1` / `not_null`, `accepted_values: 1`                            |

### 3.3 `fct_order_payment`

- Grain: 주문 내 결제 레코드 1건
- Unique Key: (`order_id`, `payment_sequential`)
- Fact Type: Accumulating Snapshot
- Materialization: incremental
- 출처: `order_payments`, `orders`
- Dimension 참조: `dim_customer`, `dim_date`

| 컬럼                   | 타입          | 종류                             | Null | 정의 / Test                                                                                                      |
| ---------------------- | ------------- | -------------------------------- | ---- | ---------------------------------------------------------------------------------------------------------------- |
| `order_id`             | VARCHAR       | Degenerate Dimension / Grain Key | N    | 결제가 속한 주문 식별자 / `not_null`                                                                             |
| `payment_sequential`   | INTEGER       | Grain Key                        | N    | 동일 주문 내 결제 레코드 순번 / `not_null`, `>= 1`                                                               |
| `customer_key`         | BIGINT        | Dimension FK                     | N    | `order_payments.created_at` 시점에 유효한 고객 버전 키 / `not_null`, `relationships → dim_customer.customer_key` |
| `initiated_date_key`   | INTEGER       | Dimension FK                     | Y    | `payment_initiated_at`의 날짜. 시각이 `NULL`이면 `NULL` / `relationships → dim_date.date_key`                    |
| `completed_date_key`   | INTEGER       | Dimension FK                     | Y    | `payment_completed_at`의 날짜. 시각이 `NULL`이면 `NULL` / `relationships → dim_date.date_key`                    |
| `failed_date_key`      | INTEGER       | Dimension FK                     | Y    | `payment_failed_at`의 날짜. 시각이 `NULL`이면 `NULL` / `relationships → dim_date.date_key`                       |
| `refunded_date_key`    | INTEGER       | Dimension FK                     | Y    | `payment_refunded_at`의 날짜. 시각이 `NULL`이면 `NULL` / `relationships → dim_date.date_key`                     |
| `payment_type`         | VARCHAR       | Fact Attribute                   | N    | 결제수단. 예: `credit_card`, `boleto`, `voucher`, `debit_card` / `not_null`                                      |
| `payment_installments` | INTEGER       | Fact Attribute                   | Y    | 결제 할부 개월 수 / `>= 0`                                                                                       |
| `payment_value`        | DECIMAL(14,2) | Additive Measure                 | N    | 해당 결제 레코드의 결제 금액 / `not_null`, `>= 0`                                                                |
| `payment_status`       | VARCHAR       | Fact Attribute                   | N    | 프로젝트 정의 결제 상태 / `not_null`, `accepted_values: pending, completed, failed, refunded`                    |
| `payment_initiated_at` | TIMESTAMPTZ   | Business Event Timestamp         | Y    | 결제 시도가 시작된 시각                                                                                          |
| `payment_completed_at` | TIMESTAMPTZ   | Business Event Timestamp         | Y    | 결제가 성공적으로 완료된 시각                                                                                    |
| `payment_failed_at`    | TIMESTAMPTZ   | Business Event Timestamp         | Y    | 결제가 실패한 시각                                                                                               |
| `payment_refunded_at`  | TIMESTAMPTZ   | Business Event Timestamp         | Y    | 환불이 발생한 시각                                                                                               |

## 4. Report

`rpt_*`는 Intermediate가 아닌 Mart 위에서 파생되는 Metrics Model이다. BI가 Source·Bronze·Staging을 직접 조회하지 않도록 Mart만 읽는 소비 계층을 제공한다.

| Model | Grain | Unique Key | Materialization |
| ----- | ----- | ---------- | --------------- |
|       |       |            |                 |

### 4.1 `<rpt_name>`

- Grain:
- Unique Key:
- Materialization:
- 출처:

| 컬럼 | 타입 | 종류 | Null | 정의 / Test |
| ---- | ---- | ---- | ---- | ----------- |
|      |      |      |      |             |

### 4.2 Report Model을 만들 때

- 날짜 축은 필요한 가장 낮은 Grain으로 유지한다. 접힌 Grain은 되돌릴 수 없지만, 펼쳐진 Grain은 BI가 roll-up할 수 있다.
- 단일 Fact 한 개를 단순 `group by`하면 되는 지표는 Report Model로 만들지 않는다. BI가 직접 집계한다.
- Grain이 다른 Fact를 결합하는 Model은 `count(distinct ...)`로 접은 컬럼임을 이름에 드러낸다.

### 4.3 알려진 제약

Grain을 접어서 잃은 분석 축, 이름이 실제 의미보다 넓게 읽히는 컬럼을 여기에 적는다.

## 5. Measure 규칙

Measure는 자신이 속한 Grain에서만 유효하다. 계산식은 각 Model의 컬럼 표에 적고, 이 절에는 Grain을 넘어 적용되는 규칙만 둔다.

### 5.1 계산 위치

Fact는 계산하지 않고 투영한다. 집계와 파생 계산은 Intermediate에서 끝낸다. Fact의 Grain·Key 제공 책임과 집계 책임을 분리하기 위해서다.

### 5.2 Additive 구분

| 구분          | 의미                             | 허용 집계          |
| ------------- | -------------------------------- | ------------------ |
| Additive      | 모든 축으로 합산 가능            | `SUM`              |
| Semi-additive | 일부 축(주로 시간)에서 합산 불가 | 축별로 명시        |
| Non-additive  | 합산 불가                        | 평균·분위수·비율만 |

기간 차이, 비율, Flag는 Non-additive다. 컬럼 표의 `정의` 열에 구분을 함께 적는다.

### 5.3 금액 Measure

- Grain이 다른 Raw 입력을 직접 다대다 Join한 뒤 SUM하지 않는다. 6절 참조.
- 주문 금액과 실제 결제 금액을 같은 컬럼으로 합치지 않는다. 할부·환불·실패로 값이 달라진다.
- 금액 타입은 `decimal(14,2)`를 유지한다. `double`은 Full Refresh와 Incremental 사이에서 합계가 달라진다.

### 5.4 NULL 처리

- 원천 Timestamp가 `NULL`이면 그 Timestamp로 계산한 Measure도 `NULL`이다.
- `NULL`인 행은 평균과 비율의 분모에서 제외한다. `0`으로 바꾸지 않는다.
- Flag Measure는 근거 컬럼이 `NULL`이면 `NULL`이다. `false`로 바꾸지 않는다.

## 6. Fan-out 방지

Grain이 다른 두 Fact를 직접 Join하면 양쪽 행 수가 곱해져 Measure가 부풀려진다. 에러는 나지 않는다.

```sql
-- 잘못된 방식: 왼쪽 3행 × 오른쪽 2행 = 6행이 되어 양쪽 합계가 부풀려진다
SELECT a.key, SUM(b.value), SUM(c.value)
FROM a JOIN b USING (key)
       JOIN c USING (key)
GROUP BY a.key
```

올바른 방식은 각각을 먼저 목표 Grain으로 접은 뒤 1:1로 Join하는 것이다.

```text
<상류 A> → <A 집계 Intermediate> ┐
                                 ├→ <결합 Intermediate> → <Fact>
<상류 B> → <B 집계 Intermediate> ┘
```

Fact끼리는 서로 직접 Join하지 않는다.

## 7. 검증

Grain 계약은 문서가 아니라 Test로 강제한다. 아래 표는 컬럼 표의 `정의 / Test` 열이 어떤 수단으로 구현되는지를 정한다.

| 검증 대상            | 수단                                                                                                          |
| -------------------- | ------------------------------------------------------------------------------------------------------------- |
| 단일 컬럼 Unique Key | `schema.yml`의 `unique` Data Test                                                                             |
| 복합 Unique Key      | `dbt/tests/<model>_unique.sql` Singular Test                                                                  |
| 참조 무결성          | `schema.yml`의 `relationships` 또는 Singular Test                                                             |
| 값 목록              | `schema.yml`의 `accepted_values`                                                                              |
| Null 불가            | `schema.yml`의 `not_null`                                                                                     |
| SCD2 복합 Unique Key | `(customer_id, valid_from)`을 검증하는 `dbt/tests/dim_customer_version_unique.sql` Singular Test              |
| SCD2 유효 기간       | `dim_customer_scd2_no_overlapping_ranges.sql` 및 `dim_customer_exactly_one_current_version.sql` Singular Test |
| 계층 책임 분리       | `tests/test_fact_layer_contract.py`                                                                           |

`dbt build`가 전부 통과해야 Publish된다. Grain/Measure Test 실패는 Phase 7 품질 Gate에서 Publish를 차단한다.

## 8. schema.yml 생성 규칙

이 문서의 컬럼 표에서 `dbt/models/marts/*/schema.yml`을 만든다. 생성물에는 Grain 문장을 중복해 적지 않고 이 문서를 가리킨다.

```yaml
models:
  - name: <model_name>
    description: "Grain: docs/reference/mart-grain.md#<anchor>"
    columns:
      - name: <컬럼>
        data_tests: [...]
```

| 문서                  | schema.yml         |
| --------------------- | ------------------ |
| 컬럼 이름             | `columns[].name`   |
| `Null` = `N`          | `not_null`         |
| 종류 = `PK`, 단일 Key | `unique`           |
| 종류 = `PK`, 복합 Key | Singular Test 파일 |
| 종류 = `FK`           | `relationships`    |
| `정의` 열의 값 목록   | `accepted_values`  |

타입은 `schema.yml`에 적지 않는다. Model SQL의 `CAST`가 타입의 정본이다.

## 관련 문서

- [데이터 변환 흐름](data-transformation-flow.md) — Source에서 Staging까지의 이름·타입·값 변환
- [PRD v1.11](../../PRD_v1.11.md) — Section 14 dbt Model, Section 15 SCD2와 Temporal Join
- [Phase 6. Dimensional Modeling](../phases/phase-06-dimensional-modeling.md) — Model 구현 순서와 Task
- [Phase 7. Data Quality & Publish](../phases/phase-07-data-quality-publish.md) — Grain/Measure Test의 Publish Gate
- [Phase 10. BI](../phases/phase-10-bi.md) — Report Model 소비와 Dashboard Metric 정의
