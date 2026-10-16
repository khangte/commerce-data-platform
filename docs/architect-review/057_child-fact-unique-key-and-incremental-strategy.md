# 057. 자식 Fact 복합 `unique_key`와 `merge` 전환 반려

- 날짜: 2026-10-16
- 판정: **반려.** 두 제안 모두 반려한다.
  - `fct_order_item`·`fct_order_payment`의 `unique_key='order_id'`를 유지한다.
  - 현재 상태 Fact 4개의 `incremental_strategy='delete+insert'`를 유지한다.
- 결정: 2026-10-16 사용자 승인. 같은 날 복합 Key가 다시 제안됐을 때(`fct_order_item.order_status` 추가 검토와 함께) 사용자가 반려를 재확인했다.
- 계기:
  - lead brief로 복합 `unique_key` 전환이 제안됐다.
  - 이어서 `delete+insert` 대신 `merge`를 쓸지 질문이 왔다.
- 기록 사유: 이 판정의 근거는 그동안 git이 추적하지 않는 계획 문서(`docs/superpowers/plans/2026-10-16-order-event-history-facts.md` 결정 8·10)에만 있었다. 그 계획은 일부 파기됐다. 이 문서가 판정의 정본이다.
- 관련:
  - Phase 6 계획 `docs/superpowers/plans/2026-09-18-phase6-affected-keys-incremental.md` Task의 `fact_order_items` `unique_key` 변경 Step
  - 커밋 `adb3434`(`stg_order_items` 중복 제거), `91c62dc`(`delete+insert` 계약 테스트)

## 1. 제안

| 제안 | 내용 |
| --- | --- |
| A. 복합 `unique_key` | `fct_order_item`: `unique_key=['order_id','order_item_id']`<br>`fct_order_payment`: `unique_key=['order_id','payment_sequence']`<br>`delete+insert`와 `int_affected_order_keys` 필터는 유지한다. |
| B. `merge` 전환 | 현재 상태 Fact의 `incremental_strategy`를 `merge`로 바꾼다. |

A의 목적(사용자 요구)은 "delete 때문에 Fact에서 행이 사라지면 안 된다"였다.

## 2. 제안 근거 검증 (2026-10-16, 실제 Bronze 조회)

| 항목 | 제안 근거 | 확인 결과 |
| --- | --- | --- |
| Bronze 삭제 표시 | 없음 | 맞음. 두 Table 원천 컬럼에 삭제 Flag가 없다 |
| `order_items` | 113,215 Key, 다중 Version 0 | 맞음. 113,215행 = 113,215 Key |
| `order_payments` | 104,174 Key, 다중 Version 274 | 맞음. 104,449행 / 104,174 Key. 뒤 Batch에서 새 Sequence가 생긴 주문은 없다 |
| 복합 Grain Test | 이미 있음 | 맞음. `dbt/tests/fct_order_item_unique.sql`, `dbt/tests/fct_order_payment_unique.sql` |
| 현재 Fact | — | `fct_order_item` 113,215행, `fct_order_payment` 104,174행. Bronze Key 수와 같고 중복 0 |

근거는 사실이다. 그러나 이 근거로 변경이 필요하다는 결론은 나오지 않는다.

## 3. A 복합 `unique_key` 반려 근거

1. **현재 설계에서도 행은 사라지지 않는다.** 영향받은 주문 X에 대해 `delete+insert`는 X의 행을 모두 지운다. 그다음 Staging 전체에서 X의 행을 다시 넣는다. 넣는 행이 지운 행을 항상 포함한다.
   - `stg_order_items`는 Bronze 전체에서 Key마다 한 행을 남긴다(`current_bronze_records`, `adb3434`).
   - `stg_payments`는 Key별 최신 Version을 남긴다. Key를 버리지 않는다.
   - `int_order_items_enriched`, `int_order_payments_enriched`는 모두 `left join`이다.
   - 증분 필터는 주문 단위다.
   - Bronze에는 삭제 경로가 없다.
   - 그래서 현재 입력에서는 두 설정의 결과가 행 단위로 같다. 제안은 목적에 더하는 것이 없다.
2. **두 설정이 갈리는 유일한 경우에 복합 Key가 틀린 결과를 낸다.** Staging에서 주문 X의 Key 하나가 빠지는 경우다. 원천 삭제, Line·결제 축소, 상류 Model 변경이 그 원인이다.
   - `order_id` 설정은 그 행을 지운다. Full Refresh 결과와 같다.
   - 복합 Key 설정은 Stale 행을 남긴다. Full Refresh 결과와 어긋난다. 그러면 `tests/integration/test_incremental_full_refresh_hash_integration.py`의 Logical Hash 동치가 깨진다.
   - Stale 결제 행이 남으면 `fct_order.payment_total`과 `fct_order_payment` 합계도 어긋난다.
3. **이미 내린 결정을 되돌린다.** Phase 6 계획은 처음에 복합 Key를 썼다. "복합 Key로 두면 원본에서 사라진 Line이 Fact에 남는다"는 이유로 `order_id`로 바꿨다.
4. **회귀 방지 테스트를 지워야 한다.** `tests/test_fact_incremental_contract.py::test_order_axis_facts_replace_whole_orders`가 `unique_key='order_id'`를 요구한다. `order_item_id',`도 금지한다. 제안은 이 테스트가 막는 바로 그 회귀다.
5. **재추출 중복은 복합 Key로 해결되지 않는다.** 같은 Key의 두 행이 같은 Insert에 함께 들어가기 때문이다. 이 문제는 Staging에서 중복을 없애 해결했다(`adb3434`).

검토했지만 채택하지 않은 대안: Staging Key가 Fact에 모두 있는지 보는 완전성 Singular Test. 사용자 결정(2026-10-16)으로 제외했다.

## 4. B `merge` 전환 반려 근거

환경은 DuckDB 1.5.5, dbt-duckdb 1.11.0이다. `MERGE INTO`, `WHEN NOT MATCHED BY SOURCE`, `merge_clauses`를 모두 지원한다. 그래도 바꾸지 않는다. 이유는 지원 여부가 아니라 결과의 의미가 달라지는 데 있다. 인메모리 DuckDB로 확인했다.

| `merge` 구성 | 결과 |
| --- | --- |
| 자식 Fact, `ON order_id` | 오류 없이 주문의 모든 Line이 Source 한 행의 값으로 덮어써진다 |
| 복합 Key | 사라진 Line이 남는다(3절 2항의 결함) |
| 복합 Key + 무조건 `NOT MATCHED BY SOURCE DELETE` | Source는 영향 주문뿐이므로 나머지 주문이 모두 지워진다 |
| 복합 Key + 영향 주문으로 한정한 `BY SOURCE DELETE` | 현재 `delete+insert`와 결과가 같다. 조건 문자열 안에서 `int_affected_order_keys`를 참조해야 한다 |
| Source Key 중복 | DuckDB `MERGE`도 오류를 내지 않는다 |

- `fct_order`, `fct_subscription_payment`는 Key당 한 행이다. `merge`와 결과가 같아 얻는 것이 없다.
- dbt-duckdb는 delete와 insert를 한 트랜잭션에서 실행하고 한 번 commit한다. `merge`로 원자성이 나아지지 않는다.
- 성능 이점을 기대할 근거도 없다. 매번 영향 Key만 처리한다. 벤치마크는 하지 않았다.

**공백과 조치:** 기존 계약 테스트는 `unique_key`만 고정했다. 누가 `merge`로 바꿔도 테스트가 통과한 채 자식 Fact 데이터가 손상될 수 있었다. `91c62dc`가 `tests/test_fact_incremental_contract.py::test_current_state_facts_use_delete_insert`를 추가했다. 이 테스트는 현재 상태 Fact 4개의 `incremental_strategy='delete+insert'`를 고정한다.

## 5. 재제안 검토 (2026-10-16, `fct_order_item.order_status` 추가와 함께)

다른 Session이 "영향 주문을 통째로 다시 고르므로 Stale 행이 없다"며 복합 Key를 다시 제안했다.

- 이 주장은 3절 1항과 같다. 현재 입력에서는 두 설정의 결과가 같다는 뜻이므로 바꿀 이유가 되지 않는다. 결과가 갈리는 경우에는 3절 2항대로 복합 Key가 틀린다.
- `order_status` 추가는 판정을 바꾸지 않는다.
  - 상태는 주문 Grain 값이다.
  - 상태만 바뀌어도 새 `stg_orders` Version이 `int_affected_order_keys`에 잡힌다. 그러면 그 주문의 항목 행이 모두 교체된다.
  - 교체 단위로는 `order_id`가 맞다.

## 6. 유지되는 계약

- `fct_order_item`, `fct_order_payment`: `unique_key='order_id'`.
- `fct_order`, `fct_order_item`, `fct_order_payment`, `fct_subscription_payment`: `incremental_strategy='delete+insert'`.
- Grain 유일성은 Singular Test(`fct_order_item_unique.sql`, `fct_order_payment_unique.sql`)가 강제한다. dbt `unique_key`는 교체 단위다.
- 고정 테스트: `test_order_axis_facts_replace_whole_orders`, `test_current_state_facts_use_delete_insert`.

## 7. 재검토 조건

- Bronze에 삭제 표시나 삭제 경로가 생기는 경우.
- 자식 Fact를 주문이 아닌 단위로 증분 교체해야 할 성능 근거가 측정으로 확인되는 경우.

두 경우 모두 Full Refresh와 Logical Hash 동치를 먼저 보장해야 한다.
