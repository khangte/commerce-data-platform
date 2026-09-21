# R-08 Late Order

## 문제

Business Time이 현재 처리 창보다 며칠 앞선(과거) 주문이 새 Mutation Cursor로 뒤늦게 도착했을 때,
`int_affected_order_keys`가 이 주문을 정확히 한 번만 집어내고 `fct_order`의 과거 Business Date
행을 새로 추가하는지 확인한다. 두 번 집히면 중복 Fact 행이 생기고, 아예 못 집으면 과거 Mart가
영영 갱신되지 않는다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r08_r10_late_and_history.py::test_r08_a_late_order_updates_the_past_business_date_mart_once -v`를 실행한다.
기준 주문 하나를 정상 수집·Build한 뒤(`sequence=1`), 구매일이 3일 전인 Late Order를
`late_order_bundle`로 만들어 `sequence=2`로 `orders`/`order_items`/`order_payments`만 재수집하고
두 번째 `dbt build`(`--full-refresh` 없이)를 실행한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| 두 번째 Build 직후 | Late Order의 `purchase_date_key`가 3일 전 날짜로 `fct_order`에 정확히 1행 존재 | 일치 |
| 두 번째 Build 직후 | 기준 주문의 `purchase_date_key`는 변경 없음 | 일치 |
| 두 번째 Build 직후 | `facts.fct_order` Row Count가 정확히 1 증가하고 Logical Hash가 바뀐다 | 일치 |
| 두 번째 Build 이후 재수집 | 같은 `orders` Table을 다시 Idle 수집하면 새 Row가 0건 (Late Order가 한 번만 집혔음을 증명) | 일치 |

## 원인과 불변 조건

`late_order_bundle`은 `order_purchase_timestamp`/`order_estimated_delivery_date`만 과거로
돌리고 `updated_at`/`created_at`은 Mutation 시각 그대로 둔다(`src/generator/scenarios.py`).
따라서 `stg_orders._batch_id`는 기준 주문보다 나중에 정렬되고, 두 번째 Build 시점의
`processed_batch_lower_bound()`가 첫 Build의 Watermark이므로 `int_affected_order_keys`의 O1
경로(`affected_orders`, `dbt/models/intermediate/int_affected_order_keys.sql`)가 이 주문만
골라낸다. `fct_order.sql`은 `is_incremental()`일 때만 이 목록으로 `delete+insert`하므로,
Late Order는 새 행으로 추가되고 기존 행은 건드리지 않는다. Mutation Cursor(`_batch_id`)가 한 번
전진하면 그 Batch는 다음 `record_affected_keys()` 호출에서 다시 잡히지 않는다 — 이것이 "정확히
한 번" 수집의 근거다(AC-20).

## 복구 절차

이 시나리오는 정상 동작이라 복구 대상이 아니다. Late Order가 Mart에 반영되지 않았다면 원인은
보통 두 가지다: (1) 두 번째 `dbt build`가 `--full-refresh`로 실행돼 `is_incremental()`이 거짓이
됐거나, (2) `advance_processed_batch_watermark`의 Guard(4개 Fact 모두 선택, 실패 노드 없음)가
깨져 Watermark가 전진하지 않았다. 두 경우 모두 원인 제거 후 `dbt build`를 다시 실행하면 된다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r08", ...)` 증적에서
`post_build_fct_order_row_count == pre_build_fct_order_row_count + 1`,
`late_order_fact_row_count == 1`, `idle_reingest_row_count == 0`을 확인한다.
