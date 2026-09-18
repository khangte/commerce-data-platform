# 003 Phase 6 마감 판정

- 판정일: 2026-09-18
- 대상: Phase 6 Dimensional Modeling 전체 (`docs/phases/phase-06-dimensional-modeling.md`)
- 판정: **조건부 미마감**. Task는 모두 끝났고 DoD 8건 중 7건이 충족됐다. AC-12 Referential Integrity 증거만 부족하다.

## 1. 확인한 상태

`P6-01`부터 `P6-25`까지 모든 Task가 완료됐다. 마지막 두 건은 이번 검증에서 다음을 확인했다.

- `P6-24` Bronze Replay·Re-extract 입력 경계: 커밋 `3b2f4a4`~`9eb0e9b`. 계획 `docs/superpowers/plans/2026-09-18-phase6-replay-boundary-and-reextract.md`의 Task 6개와 변경 파일이 일치한다.
- `P6-25` Warehouse DAG `dbt_build` 호출 경계: `airflow/dags/warehouse_pipeline_dag.py`의 `dbt_build_task`가 `trigger_rule="all_success"`로 Catalog 동기화 뒤에 실행되고, 실패를 `AirflowFailException`으로 전파한다. `tests/test_airflow_dags.py::test_warehouse_pipeline_defines_the_dbt_build_boundary`가 이 경계를 고정한다. 이 Test는 Airflow Compose Smoke Flag 없이도 실행된다.

전체 Test 실행 결과는 199 passed, 2 failed, 5 skipped다. 실패 2건은 Phase 1 Seed Test이며 Phase 6 범위가 아니다(아래 3절).

## 2. AC별 증거

| AC | 증거 | 상태 |
| --- | --- | --- |
| AC-01 | `tests/integration/test_order_e2e_and_late_order_mart_integration.py::test_fixed_order_is_traceable_from_source_to_fact` | 통과 |
| AC-09 | `dbt/tests/` Singular Test `dim_customer_scd2_no_overlapping_ranges.sql`, `dim_customer_exactly_one_current_version.sql` 및 구독 이력 대응 Test. 통합 Build에서 함께 실행된다 | 통과 |
| AC-10 | `tests/integration/test_subscription_payment_temporal_join_integration.py` 5건 | 통과 |
| AC-11 | `test_late_order_updates_the_past_business_date_mart`, `test_late_subscription_payment_updates_the_past_payment_date_fact` | 통과 |
| AC-12 | `dbt/models/marts/facts/schema.yml` | **부족** |

### AC-12가 부족한 이유

`fact_subscription_payments`는 `subscription_key`, `customer_key`, `payment_date_key` 세 Column에 `not_null`과 `relationships` Test를 모두 걸고 있다.

반면 `fact_orders`는 `customer_key`와 `purchase_date_key`를 Select하면서(`dbt/models/marts/facts/fact_orders.sql:12`) 두 Column에 아무 Test도 걸지 않았다. `schema.yml`에 선언된 것은 `order_id`의 `not_null`, `unique`뿐이다. 따라서 주문 Fact의 Dimension 참조가 깨져도, 또는 Unknown Key로 떨어져도 `dbt build`가 통과한다.

AC-12의 합격 증거는 "Fact FK/Unique 통과, 정상 Unknown 0"이다. Unique는 충족한다. FK와 Unknown 0은 주문 계열 Fact에서 검증되지 않는다.

`fact_order_items`와 `fact_payments`는 Dimension Key를 갖지 않고 `order_id`만 실으므로 별도 FK Test 대상이 아니다.

## 3. Phase 6 범위 밖으로 분류한 실패

`tests/integration/test_seed_integration.py`의 2건은 `run_seed()`를 직접 호출한다. Generator가 한 번이라도 성공한 Database에서는 Seed Guard가 `Seed is blocked because a successful generator run already exists`로 막는다. 즉 이 Test들은 Generator Test보다 먼저 실행되는 순서에서만 통과하는 순서 의존 Test다. Phase 1 Test 설계 문제이며 Phase 6 Model·Mart 로직과 무관하다.

## 4. 마감 조건

Phase 6을 닫으려면 다음 한 가지가 필요하다.

- `dbt/models/marts/facts/schema.yml`의 `fact_orders`에 `customer_key`와 `purchase_date_key`의 `not_null` Test와 `dim_customer`, `dim_date`를 향한 `relationships` Test를 추가한다. 기존 통합 Test가 매번 `dbt build`를 돌리므로 별도 통합 Test는 필요하지 않다.

이 작업이 끝나면 `docs/phases/phase-06-dimensional-modeling.md`의 DoD 마지막 항목을 켜고 Phase 6을 마감한다.
