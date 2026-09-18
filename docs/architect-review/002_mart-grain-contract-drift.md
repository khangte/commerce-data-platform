# 002. Mart Grain 계약과 구현의 명명 불일치 (보류)

- 판정: **보류 — 이번 브랜치 범위 밖 (lead 결정)**
- 후속: Dimension PK 이름(`dim_product` / `dim_seller` / `dim_date`)과 PK Test 부재는 `009_type1-dimension-pk-and-tests.md`에서 처리했다. Fact 이름과 `payment_sequential` 항목은 여전히 보류다.
- 대상: `docs/reference/mart-grain.md` vs `dbt/models/marts/`
- 발견 경위: `P6-23` Hash 정렬 Key 확정 중 확인
- 기록일: 2026-09-18
- 관련 설계 문서: `docs/architecture/07-incremental-full-refresh-logical-hash.md` 7장

## 1. 결정

`feature/phase6-remaining`에서 다루지 않는다. Phase 6의 완료 조건에 포함하지 않고 별도 이슈로만 남긴다.

`P6-23`의 Hash 정렬 Key는 계약 문서가 아니라 **Model이 실제로 내보내는 컬럼 이름**을 쓴다. 이 결정은 불일치를 해소하지 않고 우회한다. 우회했다는 사실을 여기 남긴다.

## 2. 불일치 목록

| 항목 | 계약 문서 | 구현 |
| ---- | --------- | ---- |
| `dim_product` Primary Key | `product_key` | `product_id` 컬럼만 존재 |
| `dim_seller` Primary Key | `seller_key` | `seller_id` 컬럼만 존재 |
| `dim_date` Business Key | `full_date` | `calendar_date` |
| Fact Model 이름 | `fct_order`, `fct_order_item`, `fct_order_payment`, `fct_subscription_payment` | `fact_orders`, `fact_order_items`, `fact_payments`, `fact_subscription_payments` |
| `fct_order_payment` Unique Key | `payment_sequential` | `payment_sequence` |

추가로 `dbt/models/marts/dimensions/schema.yml`에 `dim_product`, `dim_seller`, `dim_date` 항목이 없다. 세 Dimension의 Primary Key에 `not_null`·`unique` Test가 걸려 있지 않다.

## 3. 왜 나중에라도 다뤄야 하는가

- 계약 문서는 Mart Grain의 단일 정본이다. 정본이 틀리면 다음 Model 작성자가 틀린 이름을 따라 쓴다.
- `dim_product` / `dim_seller` / `dim_date`의 PK 중복은 지금 아무 Test도 잡지 못한다. Dimension PK 중복은 Fact Join에서 Fan-out으로 번진다.
- Surrogate Key 명명(`*_key`)을 계약대로 맞출지, 구현대로 Business Key를 PK로 둘지는 설계 판단이 필요하다. 문서만 고치는 작업이 아니다.

## 4. 다시 다룰 때의 시작점

두 갈래 중 하나를 먼저 정해야 한다.

1. 계약을 구현에 맞춘다. `dim_product` / `dim_seller`는 Type 1 Dimension이므로 Business Key를 PK로 두는 것이 타당하다는 논거가 있다. 문서 수정과 Test 추가로 끝난다.
2. 구현을 계약에 맞춘다. 모든 Dimension이 Surrogate Key를 갖는 일관성을 얻지만, Fact의 FK 컬럼과 Join 경로가 함께 바뀐다. `P6-23` Hash 대상의 정렬 Key도 따라 바뀐다.

어느 쪽이든 Fact 이름(`fct_*` vs `fact_*`)은 문서 쪽을 구현에 맞추는 것이 맞다. 이미 Model, Test, 통합 Test가 전부 `fact_*`를 쓴다.
