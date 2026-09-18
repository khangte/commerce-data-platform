# 005 AC-12 판정과 `fact_orders` Unknown Key

- 판정일: 2026-09-18
- 대상: `ff061b2` (fact_orders FK Test 추가), `791c350`
- 판정: **AC-12 미통과. Phase 6 마감 불가.** 추가한 Test는 옳고, 그 Test가 실제 결함을 잡았다. 결함은 `fact_orders` Model이 아니라 AC-11 통합 Test의 Fixture에 있다.

## 1. 추가된 Test

`ff061b2`가 `dbt/models/marts/facts/schema.yml`의 `fact_orders`에 요청한 네 Test를 정확히 추가했다.

- `customer_key`: `not_null`, `relationships` → `dim_customer.customer_key`
- `purchase_date_key`: `not_null`, `relationships` → `dim_date.date_key`

형식은 `fact_subscription_payments` 블록과 같다. 요청 범위와 일치하고 이탈은 없다.

## 2. 그 Test가 잡은 실패

`tests/integration/test_order_e2e_and_late_order_mart_integration.py`를 실행하면 AC-11 Test가 실패한다.

```
Done. PASS=139 WARN=0 ERROR=1 SKIP=3 NO-OP=0 TOTAL=143
```

오류 Node는 `not_null_fact_orders_customer_key`이고 `Got 1 result`다. 즉 주문 Fact 한 행의 `customer_key`가 NULL이다. 이것이 AC-12가 말하는 Unknown Key다. Test를 넣기 전에는 같은 상태가 조용히 통과했다.

AC-01 Test(`test_fixed_order_is_traceable_from_source_to_fact`)와 Incremental/Full Refresh Hash Test는 계속 통과한다.

## 3. 원인

`customer_key`는 시점 결합으로 붙는다(`dbt/models/intermediate/int_orders_enriched.sql`).

```sql
left join {{ ref('dim_customer') }}
    on stg_orders.customer_id = dim_customer.customer_id
    and stg_orders.purchase_at >= dim_customer.valid_from
    and stg_orders.purchase_at < coalesce(dim_customer.valid_to, timestamptz 'infinity')
```

`dim_customer` 최초 Version의 `valid_from`은 `created_at`이다(`int_customer_history.sql`의 `case when version_rank = 1 then created_at else updated_at end`). 이 규칙은 Phase 6 문서가 명시한 Unknown Key 방지 규칙이며 그대로 유지해야 한다.

실패한 Fixture는 이 규칙이 감당할 수 없는 상태를 만든다.

- `new_customer_record()`는 `created_at = config.logical_date`로 Customer를 만든다(`src/generator/customers.py:533`). Fixture의 `config`는 `mutation_time = FIXTURE_START`다.
- 같은 Fixture의 `late_order_bundle(..., business_event_time=mutation_time - timedelta(days=3))`은 주문 구매 시각을 3일 전으로 잡는다.

결과적으로 **주문이 자기 고객보다 3일 먼저 존재한다**. 어떤 Version도 `purchase_at`을 덮지 못하므로 결합이 NULL을 낸다.

## 4. 조치 방향

Model을 고치지 않는다. 고칠 대상은 Fixture다.

주문은 고객보다 먼저 발생할 수 없다. AC-11이 검증하려는 것은 "3일 전 Business Time의 주문이 늦게 수집되어 과거 Business Date Mart를 갱신한다"이지 "고객보다 먼저 발생한 주문"이 아니다. 따라서 Fixture는 Customer와 그 Membership Tier 관측을 `business_event_time` 이전 시각으로 만들어야 한다. 주문의 `updated_at`(수집 계기)은 지금처럼 `mutation_time`으로 둔다. 그래야 Late Arrival 성격이 유지된다.

Model 쪽 대안 두 가지는 채택하지 않는다.

- 최초 Version `valid_from`을 사건 시각까지 소급 확장: 이력 Dimension이 관측하지 않은 구간을 지어내게 된다.
- Unknown 회원 행을 만들어 `coalesce`: 지금은 Unknown이 정상 상태가 아니라 Fixture 결함의 신호다. Unknown 회원을 먼저 만들면 이 신호가 영구히 가려진다.

## 5. 마감 조건

- AC-01, AC-09, AC-10, AC-11, AC-12 중 AC-11과 AC-12만 열려 있다. 두 항목은 같은 Fixture 수정 하나로 함께 닫힌다.
- 수정 뒤 `tests/integration/test_order_e2e_and_late_order_mart_integration.py`가 2건 모두 통과하면 `docs/phases/phase-06-dimensional-modeling.md`의 마지막 DoD를 켜고 Phase 6을 마감한다.

## 추가 (2026-09-18)

이 문서의 4절 "Fixture를 고쳐라" 판정은 [006](006_early-arriving-fact-effective-from.md)이 철회했다. 같은 형태를 운영 Generator의 `late-arrival` Profile이 직접 만들기 때문이다. 조치는 Fixture 수정이 아니라 Model의 결합 하한 변경이다.
