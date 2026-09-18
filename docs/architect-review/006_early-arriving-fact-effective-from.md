# 006 Early-Arriving Fact와 `effective_from` 결합 하한

- 판정일: 2026-09-18
- 대상: reviewer의 `not_null_fact_orders_customer_key` 위반 재현 보고
- 판정: **Model을 고친다.** [005](005_ac12-fact-orders-unknown-key.md)의 "Fixture 결함" 판정을 철회한다.

## 1. 005를 철회하는 이유

005는 "주문이 자기 고객보다 먼저 존재하는 것은 Fixture가 만든 비현실적 상태"라고 판정했다. 이 판정은 틀렸다. 같은 형태를 **운영 Generator가 직접 만든다**.

```python
# src/generator/service.py:112
customer = new_customer_record(config, order_ordinal)
bundle = _bundle_for_profile(config, customer, catalog, order_ordinal)

# src/generator/service.py:214
if config.anomaly_profile == "late-arrival":
    business_event_time = config.logical_date - timedelta(days=3 + order_ordinal % 3)
```

`new_customer_record()`는 `created_at = config.logical_date`인 새 고객을 만들고(`src/generator/customers.py:533`), `late-arrival` Profile은 그 고객의 주문 구매 시각을 3~5일 전으로 잡는다. Membership Tier 관측도 `created_at = customer.created_at`이다(`src/generator/customers.py:250`). 따라서 `late-arrival` Profile로 Generator를 돌릴 때마다 같은 위반이 재현된다. Fixture만의 문제가 아니다.

즉 이 상태는 Early-Arriving Fact다. 사건이 그 Dimension의 첫 관측보다 앞선다. 현실에서도 정상이다. 등급 관측이 첫 구매 이후에 생기는 경우가 그렇다.

## 2. 무엇이 깨지는가

`int_orders_enriched.sql:22`의 결합 하한이 `dim_customer.valid_from`이고, 최초 Version의 `valid_from`은 첫 관측의 `created_at`이다(`int_customer_history.sql`). 사건이 그보다 앞서면 어떤 Version도 덮지 못해 `customer_key`가 NULL이 되고, `ff061b2`가 추가한 `not_null_fact_orders_customer_key`가 `dbt build`를 ERROR로 끝낸다.

같은 구조가 `int_subscription_payments_enriched.sql`에도 두 번 있다(구독 Version, 고객 Version). 지금은 Fixture가 해당 조합을 만들지 않아 드러나지 않을 뿐이다.

## 3. 채택하는 설계: 결합 전용 하한 Column

최초 Version에만 결합 하한을 과거로 여는 별도 Column을 만든다.

- `int_customer_history`와 `int_subscription_history`에 `effective_from`을 추가한다. 값은 `case when version_rank = 1 then timestamptz '-infinity' else valid_from end`이다.
- `dim_customer`와 `dim_subscription`이 `effective_from`을 그대로 노출한다.
- 시점 결합 3곳의 하한만 `valid_from`에서 `effective_from`으로 바꾼다. 상한(`valid_to`) 조건은 그대로 둔다.
- `valid_from`은 그대로 둔다. 의미는 "관측된 유효 시작"이다.

### `valid_from`을 직접 `-infinity`로 바꾸지 않는 이유

1. Surrogate Key가 `md5(customer_id | valid_from | attribute_hash)`다(`dim_customer.sql:3`). `valid_from`을 바꾸면 모든 최초 Version의 Key 값이 바뀐다.
2. `rpt_subscription_funnel_daily.sql:15`이 `strftime(date_trunc('day', valid_from), '%Y%m%d')`로 Date Key를 만든다. `-infinity`가 들어가면 이 Report가 깨진다.
3. 관측 시작 시각이라는 사실 자체를 잃는다.

`effective_from`은 이 셋을 모두 피한다. 결합 하한만 바뀌고 Key·Report·이력 의미는 그대로다.

### 대안을 버린 이유

- **Generator가 고객 `created_at`을 사건 시각 이전으로 backdate한다**: Source를 고쳐도 Warehouse는 여전히 Early-Arriving Fact를 못 견딘다. 다른 Source나 다른 Profile에서 같은 형태가 오면 또 깨진다. 근본 처리가 아니다.
- **Unknown 회원 행 + `coalesce`**: AC-12의 "정상 Unknown 0"에 어긋난다. Dimension이 아예 없는 진짜 결손까지 Unknown으로 덮어 신호를 잃는다.

`not_null`과 `relationships` Test는 유지한다. Dimension 자체가 없는 결손은 `effective_from`으로도 NULL이 남아 그대로 잡힌다.

## 4. 불변 조건 확인

- 이력 구간 중첩 0: `effective_from`은 최초 Version의 하한만 내린다. 최초 Version의 상한은 둘째 Version의 `valid_from`이므로 중첩이 생기지 않는다.
- Current Version 1개: `is_current` 계산은 건드리지 않는다.
- Incremental·Full Refresh Hash 동일: `-infinity`는 상수이고 재계산해도 같다.
- P6-22 영향 Key 재계산: `int_affected_*` Model은 `valid_from`을 쓰지 않는다. 영향 없다.

## 5. 검증

1. `test_late_order_updates_the_past_business_date_mart`가 통과한다. Fixture는 고치지 않는다. 이 Fixture가 운영 `late-arrival` Profile과 같은 형태이기 때문이다.
2. `dbt/tests/`에 최초 Version의 `effective_from`이 `-infinity`인지 확인하는 Singular Test를 `dim_customer`와 `dim_subscription` 각각에 추가한다.
3. 구독 결제가 구독 첫 관측보다 앞서는 통합 Test를 1건 추가해 `fact_subscription_payments.subscription_key`가 NULL이 되지 않음을 고정한다.
4. 기존 SCD2 중첩·Current Version Test와 `test_incremental_full_refresh_hash_integration.py`가 계속 통과한다.

이 네 가지가 통과하면 AC-11과 AC-12가 함께 닫히고 Phase 6을 마감한다.
