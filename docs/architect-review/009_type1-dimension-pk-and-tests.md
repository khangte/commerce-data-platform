# 009. Type 1 Dimension의 Primary Key 계약과 PK Test

- 판정: **갈래 1 채택. 계약 문서를 구현에 맞춘다. Business Key를 Primary Key로 인정한다.**
- 대상: `docs/reference/mart-grain.md`, `dbt/models/marts/dimensions/schema.yml`
- 선행 문서: `docs/architect-review/002_mart-grain-contract-drift.md` (보류 건 중 일부 처리)
- 기록일: 2026-09-18

## 1. 범위

002 보류 목록에서 아래 항목만 처리한다.

- `dim_product` / `dim_seller` / `dim_date`에 `schema.yml` 항목이 없고 PK `not_null`·`unique` Test가 없는 문제
- 위 Test를 걸기 위해 먼저 정해야 하는 PK 이름: `product_key` vs `product_id`, `seller_key` vs `seller_id`, `full_date` vs `calendar_date`

아래 항목은 이번 범위가 아니다. 건드리지 않는다.

- Fact Model 이름 (`fct_*` vs `fact_*`)
- `fct_order_payment` Unique Key (`payment_sequential` vs `payment_sequence`)
- `docs/architecture/07-incremental-full-refresh-logical-hash.md`의 기록 (작성 시점의 결정 기록이므로 고치지 않는다)

## 2. 판단

### 2.1 `dim_product` / `dim_seller`: Business Key를 PK로 둔다

계약 문서 1.8절은 비-SCD2 Dimension의 Surrogate Key를 `md5(<Business Key>)`로 정의한다. 이 Key는 Business Key의 순수 함수다.

- 두 Dimension은 Type 1이다. 한 Entity가 정확히 한 행이다. `md5(product_id)`는 `product_id`와 1:1이라 행 식별에 정보를 더하지 않는다.
- Surrogate Key가 제 역할을 하는 경우는 한 Business Key가 여러 행을 가질 때(SCD2 Version)나 원천 Key가 불안정할 때다. 둘 다 해당하지 않는다. `stg_products.product_id`와 `stg_sellers.seller_id`는 이미 `not_null`·`unique` Test로 보장된다.
- 구현을 계약에 맞추면(갈래 2) `dim_product`, `dim_seller`, `int_order_items_enriched`, `fact_order_items`의 컬럼과 Join 경로가 바뀐다. `P6-23` Mart Hash 대상의 정렬 Key도 바뀐다. 얻는 것은 `*_key` 명명의 일관성뿐이다.
- 나중에 두 Dimension을 Type 2로 바꾸면 그때 `*_key`를 도입한다. 지금 미리 만들지 않는다 (YAGNI).

따라서 `*_key` Surrogate Key는 SCD Type 2 Dimension(`dim_customer`, `dim_subscription`)에만 둔다는 규칙으로 계약을 고친다. `dim_date.date_key`는 기존대로 예외다.

### 2.2 `dim_date`: Business Key 이름을 `calendar_date`로 고친다

PK `date_key`는 계약과 구현이 같다. 불일치는 Business Key 이름뿐이다. 이름만 다르고 의미는 같다. 같은 원칙으로 계약을 구현에 맞춘다. 컬럼 이름을 바꾸면 Mart Hash 결과와 기존 Test가 흔들리는데, 얻는 것이 없다.

### 2.3 Fact FK 이름도 함께 고친다

계약 3장의 주문 상품 Fact는 `product_key` / `seller_key` FK를 적는다. PK 이름을 바꾸면 이 FK 이름도 같이 바뀌어야 계약이 스스로 모순되지 않는다. Fact Model 이름(3장 절 제목의 `fct_order_item`)은 이번 범위가 아니므로 그대로 둔다. 컬럼 행만 고친다.

## 3. 변경 지시

### 3.1 `dbt/models/marts/dimensions/schema.yml`

기존 두 항목 뒤에 아래 세 항목을 추가한다. 다른 컬럼 Test는 추가하지 않는다.

| Model | 컬럼 | Test |
| ----- | ---- | ---- |
| `dim_date` | `date_key` | `not_null`, `unique` |
| `dim_date` | `calendar_date` | `not_null`, `unique` |
| `dim_product` | `product_id` | `not_null`, `unique` |
| `dim_seller` | `seller_id` | `not_null`, `unique` |

각 Model에 한국어 `description`을 단다. 기존 두 항목과 같은 형식을 따른다.

### 3.2 `docs/reference/mart-grain.md`

1. 1.8절
   - 첫 문장의 주어를 "SCD Type 2 Dimension의 Surrogate Key"로 좁힌다.
   - 생성식 표에서 `비-SCD2` 행을 뺀다.
   - Type 1 Dimension(`dim_product`, `dim_seller`)은 Surrogate Key를 두지 않고 Business Key를 PK로 쓴다는 문단을 추가한다. 근거는 2.1의 두 줄(한 Entity 한 행, `md5(<Business Key>)`는 Business Key와 1:1)로 충분하다. Type 2로 바뀌면 그때 `*_key`를 도입한다는 문장을 붙인다.
   - "Fact의 Dimension FK는 참조 대상 Dimension의 Surrogate Key와 같은 타입으로 적는다"를 "참조 대상 Dimension의 Primary Key와 같은 이름과 타입으로 적는다"로 바꾼다.
2. 2장 Dimension 요약 표: `dim_product` Unique Key를 `product_id`, `dim_seller` Unique Key를 `seller_id`로 바꾼다.
3. 2.2절 `dim_date`: `Business Key: full_date`를 `calendar_date`로 바꾼다. 컬럼 표의 `full_date` 행 이름도 `calendar_date`로 바꾼다.
4. 2.3절 `dim_product`: `Primary Key`를 `product_id`로 바꾼다. `Business Key` 줄은 두되 값이 같음을 드러낸다 (`Business Key: product_id (PK와 같음)`). 컬럼 표에서 `product_key` 행을 지운다. `product_id` 행의 종류를 `Business Key / PK`로 바꾼다.
5. 2.4절 `dim_seller`: 2.3절과 같은 방식으로 `seller_id` 기준으로 고친다.
6. 3장 주문 상품 Fact 컬럼 표: `product_key` 행을 `product_id`로, `seller_key` 행을 `seller_id`로 바꾼다. 정의 칸의 `relationships` 대상도 `dim_product.product_id`, `dim_seller.seller_id`로 바꾼다. 절 제목과 Model 이름은 고치지 않는다.

위 여섯 곳 외의 불일치(4장 참고)는 고치지 않는다.

### 3.3 검증

- `dim_date`, `dim_product`, `dim_seller`의 dbt Test가 모두 통과해야 한다.
- 단위 Test(`tests/`, integration 제외)가 모두 통과해야 한다. `tests/test_dimension_materialization_contract.py`가 `mart-grain.md`를 읽는다.
- `grep -n "product_key\|seller_key\|full_date" docs/reference/mart-grain.md` 결과가 비어야 한다.

## 4. 이번에 새로 확인했지만 처리하지 않는 불일치

계약 문서를 읽다 확인했다. 002 목록에도 없던 항목이다. 이번 범위가 아니므로 기록만 한다.

| 항목 | 계약 문서 | 구현 |
| ---- | --------- | ---- |
| `dim_product` Attribute 이름 | `product_category_name`, `product_weight_g`, `product_*_cm` | `category_name`, `weight_g`, `*_cm` |
| `dim_seller` Attribute 이름 | `seller_city`, `seller_state` | `city`, `state` |
| `dim_date` 누락 Attribute | `month_name`, `day_name`, `week_of_year`, `is_weekend` | 없음 |
| `dim_date.day_of_week` 규칙 | `1=Monday ~ 7=Sunday` | `extract(dow)`, `0=Sunday ~ 6=Saturday` |
| 주문 상품 Fact의 Dimension FK Test | `relationships → dim_product`, `dim_seller` | `fact_order_items`에 `relationships` Test 없음 |

마지막 항목은 Test 부재이므로 품질 위험이 있다. `int_order_items_enriched`가 `left join`이라 Dimension에 없는 `product_id`가 Fact에 조용히 들어갈 수 있다. 다음 정리 때 우선 다룬다.
