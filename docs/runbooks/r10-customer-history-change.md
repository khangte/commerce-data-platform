# R-10 Customer 이력 변경

## 문제

고객 거래 실적 등급 또는 구독 계약 상태가 바뀌었을 때, `dim_customer`/`dim_subscription`이 새
SCD2 Version을 정확히 하나 열고 이전 Version을 겹침 없이 닫는지, 그리고 변경 시점 전후의
주문·구독 결제가 각각 해당 시점에 유효했던 Version Key에 결합하는지 확인한다. 결합이 틀리면
변경 이전 사건이 새 등급/상태로 잘못 집계되거나, 변경 이후 사건이 과거 등급/상태에 묶여 남는다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability/test_r08_r10_late_and_history.py::test_r10_a_subscription_or_tier_change_opens_a_new_version_and_rebinds_events -v`를
실행한다. 기준 주문·구독 결제를 정상 수집·Build한 뒤(`sequence=1`), `membership_change_scenario`로
등급을 BRONZE→SILVER로, `subscription_transition_scenario`로 구독 상태를 ACTIVE→PAYMENT_FAILED로
전이시키고, 그 이후 시각의 두 번째 주문·구독 결제를 더해 `customer_membership_tiers`/
`customer_subscriptions`/`orders`/`order_items`/`order_payments`/`subscription_payments`를
`sequence=2`로 재수집해 두 번째 `dbt build`를 실행한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| 두 번째 Build 직후 | `dim_customer`/`dim_subscription`에 이 고객·구독으로 정확히 2개 Version이 존재 | 일치 |
| 두 번째 Build 직후 | 1번 Version만 `effective_from = -infinity`이고 2번 Version만 `is_current = true` | 일치 |
| 두 번째 Build 직후 | 1번 Version의 `valid_to`가 2번 Version의 `valid_from`과 같아 구간이 겹치거나 비지 않음 | 일치 |
| 두 번째 Build 직후 | 변경 이전 주문/구독 결제는 1번 Version의 Key에, 변경 이후 주문/구독 결제는 2번 Version의 Key에 결합 | 일치 |
| 첫/두 번째 Build 직후 각각 | `dim_customer_*`·`dim_subscription_*` Singular Test(AC-09/AC-10) 전부 통과 | 일치 |

## 원인과 불변 조건

`int_customer_history.sql`/`int_subscription_history.sql`은 `lag(attribute_hash)`로 직전 관측과
해시가 다른 지점만 새 Version으로 잘라내고, `row_number()`로 매긴 `version_rank`가 1이면
`effective_from`을 `timestamptz '-infinity'`로, 그 외에는 Mutation 시각(`updated_at`)으로 둔다.
`valid_to`는 `lead()`로 다음 Version의 `valid_from`을 그대로 가져오므로 구간 사이에 겹침도
빈틈도 생기지 않는다. `int_orders_enriched.sql`/`int_subscription_payments_enriched.sql`은
각각 `purchase_at`/`payment_at`이 `[effective_from, valid_to)` 구간에 드는 `dim_customer`/
`dim_subscription` 행과 조인하므로, 사건 시각이 변경 시각보다 앞이면 구 Version에, 뒤면 새
Version에 자동으로 묶인다. `subscription_transition_records`의 `PAYMENT_FAILED` 전이는
`current_period_started_at`을 바꾸지 않으므로, 변경 후 두 번째 구독 결제도 첫 결제와 같은
`billing_period_start_at`을 그대로 재사용할 수 있다.

## 복구 절차

이 시나리오는 정상 동작이라 복구 대상이 아니다. Version이 둘로 갈라지지 않거나 사건이 잘못된
Key에 묶였다면 원인은 보통 두 가지다: (1) `membership_change_scenario`/
`subscription_transition_scenario` 호출 시 `config.logical_date`가 기존 `updated_at`보다
앞서 `ValueError`로 막혔거나 애초에 반영이 안 됐거나, (2) 재수집 대상에서 변경된 축
(`customer_membership_tiers`/`customer_subscriptions`) 또는 새 사건(`orders`/
`subscription_payments`) 중 일부를 빠뜨려 두 번째 `dbt build`가 예전 관측만 보고 있는 경우다.
원인 제거 후 `dbt build`를 다시 실행하면 된다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r10", ...)` 증적에서
`customer_version_count == 2`, `subscription_version_count == 2`,
`membership_tier_before == "BRONZE"`, `membership_tier_after == "SILVER"`,
`subscription_status_before == "ACTIVE"`, `subscription_status_after == "PAYMENT_FAILED"`,
`before_order_customer_key`/`before_payment_subscription_key`가 1번 Version Key와,
`after_order_customer_key`/`after_payment_subscription_key`가 2번 Version Key와 같은지 확인한다.
