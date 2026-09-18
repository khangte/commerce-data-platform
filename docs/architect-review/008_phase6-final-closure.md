# 008 Phase 6 최종 마감 판정

- 판정일: 2026-09-18
- 대상: `b6ddb0e` (006 `effective_from` 결합 하한), `a891b7a` (007 Mart Hash Sentinel), 선행 `ff061b2`·`791c350`
- 판정: **AC-12 통과. Phase 6 마감 가능.** [003](003_phase6-closure-gate.md)의 조건부 미마감을 해제한다.

## 1. 구현 검수

`b6ddb0e`는 [006](006_early-arriving-fact-effective-from.md)의 설계와 일치한다.

- `int_customer_history`·`int_subscription_history`에 `case when version_rank = 1 then timestamptz '-infinity' else updated_at end as effective_from`을 추가했다.
- `dim_customer`·`dim_subscription`이 `effective_from`을 노출하고, `schema.yml`이 두 Column에 `not_null`을 건다.
- 시점 결합 3곳(`int_orders_enriched.sql`, `int_subscription_payments_enriched.sql` 2곳)의 하한만 `valid_from`에서 `effective_from`으로 바꿨다. 상한 `coalesce(valid_to, timestamptz 'infinity')` 조건은 그대로다.
- `valid_from`은 변경하지 않았다. 따라서 Surrogate Key와 `rpt_subscription_funnel_daily`의 Date Key가 그대로 유지된다.
- 최초 Version의 `effective_from`이 `-infinity`인지 확인하는 Singular Test 2건과 구독 결제 Early-Arriving 통합 Test 1건을 추가했다.

`a891b7a`는 [007](007_mart-hash-infinity-sentinel.md)의 결정과 일치한다. `_json_default()`의 naive 분기에서 `datetime.min`·`datetime.max`만 Sentinel 문자열로 정규화하고, 그 밖의 naive datetime은 `ValueError`로 계속 거부한다. Offset 유실 Guard가 그대로 남았다.

두 커밋 모두 지시 범위를 벗어난 변경이 없다.

## 2. 검증 결과

전체 Test: **203 passed, 2 failed, 5 skipped** (529초). 실패 2건은 [004](004_seed-guard-order-dependent-tests.md)에서 lead가 보류한 Seed Guard 순서 의존 Test다. Phase 6 범위 밖이다. 003 시점의 199 passed에서 4건이 늘었고 새 실패는 없다.

AC 증거 Test는 별도로 재실행해 9건 전부 통과를 확인했다.

| AC | 증거 | 상태 |
| --- | --- | --- |
| AC-01 | `test_fixed_order_is_traceable_from_source_to_fact` | 통과 |
| AC-09 | SCD2 중첩·Current Version Singular Test. 위 통합 Test의 `dbt build`에서 함께 실행된다 | 통과 |
| AC-10 | `test_subscription_payment_temporal_join_integration.py` 6건 | 통과 |
| AC-11 | `test_late_order_updates_the_past_business_date_mart`, `test_late_subscription_payment_updates_the_past_payment_date_fact` | 통과 |
| AC-12 | `fact_orders`의 `customer_key`·`purchase_date_key` `not_null`+`relationships` Test가 위 통합 Test의 `dbt build`에서 ERROR 없이 통과 | 통과 |

AC-12가 닫힌 근거는 두 가지다. 첫째, 003이 지적한 `fact_orders`의 FK·Unknown 검증 공백이 `ff061b2`로 메워졌다. 둘째, 그 Test가 잡아낸 실제 위반(`not_null_fact_orders_customer_key` 1건)이 Fixture 조작이 아니라 Model의 결합 하한 수정으로 해소됐다. AC-11의 Late Order Fixture는 고치지 않았고, 그 상태 그대로 Unknown Key 0을 만족한다.

## 3. 마감

`docs/phases/phase-06-dimensional-modeling.md`의 마지막 DoD `AC-01, 09, 10, 11, 12가 통과한다`를 켠다. DoD 8건이 모두 충족되므로 Phase 6을 마감한다.

## 4. 이월

- Seed Guard 순서 의존 Test 2건([004](004_seed-guard-order-dependent-tests.md)). Phase 7 착수 전에 별도 Task로 처리하기를 권한다. Phase 7도 같은 공유 Database 위에서 통합 Test를 돌리므로 Noise가 이어진다.
