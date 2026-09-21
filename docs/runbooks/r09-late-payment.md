# R-09 Late Payment

## 문제

이미 오래전에 구매된 주문에 뒤늦게 결제 완료 Mutation이 도착했을 때, 그 결제가 연결된 주문의
과거 구매일을 영향 범위(`int_affected_order_keys`)에 다시 포함시키는지 확인한다. 포함되지
않으면 결제 상태가 바뀌었는데도 `fct_order_payment`가 과거 상태로 멈춰 있는다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r08_r10_late_and_history.py::test_r09_a_late_payment_pulls_the_linked_order_purchase_date_into_the_affected_range -v`를 실행한다.
구매일이 10일 전인 주문을 결제 대기(`pending`) 상태로 수집·Build한 뒤, `delayed_payment_transition`으로
그 결제를 완료(`completed`)로 전이시키고 `order_payments`만 `sequence=2`로 재수집해 두 번째
`dbt build`를 실행한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| 첫 Build 직후 | `fct_order_payment.payment_status == "PENDING"` | 일치 |
| 결제 완료 전이 후 재수집 | Source 결제 행이 1건 갱신되어 재수집 Row Count가 1 | 일치 |
| 두 번째 Build 직후 | `fct_order_payment.payment_status == "COMPLETED"`로 바뀌고 `fct_order.purchase_date_key`는 원래 구매일 그대로 | 일치 |
| 두 번째 Build 직후 | `control.affected_keys`에 이 주문의 `entity_key`로 새 행이 추가되고, 그 `business_date_key`가 원래 구매일과 같다 | 일치 |
| 첫/두 번째 Build 직후 각각 | `fct_order.payment_total`이 그 시점 `fct_order_payment.payment_value` 합계와 같다(측정값 정합) | 일치 |
| 두 번째 Build 직후 | `facts.fct_order`/`facts.fct_order_payment` Row Count는 첫 Build와 불변, `facts.fct_order_payment` Logical Hash는 변경(중복 없이 제자리 갱신) | 일치 |

## 원인과 불변 조건

`int_affected_order_keys`의 O2b 경로(`affected_from_payments`,
`dbt/models/intermediate/int_affected_order_keys.sql`)는 `stg_payments._batch_id > lower_bound`인
행을 `stg_orders`와 `order_id`로 Join해 그 주문의 `purchase_at`(과거 날짜 그대로)을 가져온다.
즉 결제가 최근에 바뀌었어도 영향 범위의 `business_date_key`는 결제 시각이 아니라 **연결된 주문의
구매일**이다 — 이것이 "구매일이 영향 범위에 포함된다"는 요구의 정확한 의미다. `record_affected_keys()`는
매 Build마다 그 시점 `int_affected_order_keys` 스냅샷을 무조건 append하므로, 두 번째 Build에서
같은 `entity_key`로 새 행이 하나 더 늘어난다. `delayed_payment_transition`은
`plan_payment_transition`을 거쳐 `PAYMENT_TRANSITIONS`가 허용하는 `pending → completed` 전이만
적용하므로 상태 기계를 벗어난 결제 상태는 만들어지지 않는다.

## 복구 절차

이 시나리오는 정상 동작이라 복구 대상이 아니다. 결제 상태가 Mart에 반영되지 않았다면 (1)
`order_payments` 재수집이 실제로 일어났는지(`row_count`), (2) 두 번째 Build가
`is_incremental()`로 실행됐는지(첫 Build가 `--full-refresh`가 아니었는지) 확인한다. 원인 제거 후
`dbt build`를 다시 실행하면 된다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r09", ...)` 증적에서
`payment_status_before_late_payment == "PENDING"`, `payment_status_after_late_payment ==
"COMPLETED"`, `affected_key_records_after == affected_key_records_before + 1`,
`fct_order_payment_total_before`/`fct_order_payment_total_after`가 각각 그 시점 결제 합계와 같은지,
`pre_build_fct_order_row_count == post_build_fct_order_row_count`,
`pre_build_fct_order_payment_row_count == post_build_fct_order_payment_row_count`,
`pre_build_fct_order_payment_hash != post_build_fct_order_payment_hash`를 확인한다.
