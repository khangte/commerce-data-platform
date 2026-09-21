# 021. Task 11 (R-08 / R-09) 검수 — R-08 승인, R-09 보강 2건

> 판정: architect, 2026-09-21 (Phase 8 Task 11 developer 구현 검수)
> 관련: AC-11, AC-20, R-08, R-09

## 판정

**R-08은 승인한다. R-09는 계획이 요구한 두 항목이 빠졌다. 보강 뒤 Task 11을 닫는다.**

### R-08 — 승인

| 계획 요구 | 구현 | 확인 |
| --------- | ---- | ---- |
| Late Order 전후 Mart Hash·Row Count 포착 | `pre_hashes`/`post_hashes`, `pre_counts`/`post_counts` | 통과 |
| 과거 Business Date가 바뀐다 | `late_rows == [(expected_date_key,)]`, Mutation 시각보다 3일 앞선 Date Key | 통과 |
| Mutation Cursor로 **1회만** 수집 | 두 겹으로 증명 | 통과 |
| 중복 Fact Row 없음 | `post_counts == pre_counts + 1`, `late_rows` 길이 1 | 통과 |
| 증적에 Before/After Diff | Row Count·Hash 양쪽 기록 | 통과 |

"1회만 수집"을 두 겹으로 증명한 구성이 좋다. `late_rows == [(expected_date_key,)]`가 Fact 측 중복 부재를 보이고, 뒤이은 Idle 재수집이 `SUCCESS_NO_DATA` / `row_count == 0`을 돌려주는 것이 Cursor가 Late Row를 이미 지나쳤음을 보인다. 둘 중 하나만으로는 부족하다 — 앞의 것만 있으면 중복 제거가 가린 것인지 구분되지 않고, 뒤의 것만 있으면 애초에 수집됐는지 알 수 없다.

Baseline Order의 `purchase_date_key`가 Mutation 시각 날짜 그대로인 것도 함께 단언해, Late Order만 과거 날짜에 꽂혔음을 보인 점도 맞다. 대조군 없이 "과거 날짜에 들어갔다"만 단언하면 전부 과거로 밀렸을 가능성을 배제하지 못한다.

`_assert_relationship_tests_passed`를 두 Build 모두에 건 것도 확인했다.

### R-09 — 보강 2건

증명된 부분은 정확하다. 10일 전 구매일 Order의 결제가 늦게 완료될 때 `control.affected_keys`에 그 구매일(`business_date_key == expected_date_key`)이 새로 들어가고, `payment_status`가 `PENDING`에서 `COMPLETED`로 바뀌며, `purchase_date_key`는 움직이지 않는다. R-09의 핵심인 "영향 범위가 과거 구매일까지 당겨진다"는 증명됐다.

`post_affected_rows[-1]`을 `recorded_at` 정렬 기준 마지막으로 본 것, `len(...) == pre_affected_count + 1`로 정확히 한 건만 늘었음을 본 것 모두 맞다.

빠진 것이 둘이다.

#### 보강 1 — Fact 측정값 정합 단언이 없다

계획 Step은 "the fact measures reconcile after the rebuild"다. 현재 Test는 `payment_status`라는 **상태**만 단언하고 **측정값**을 단언하지 않는다. `dbt/models/marts/facts/fct_order.sql`에 `payment_total`이 있으므로 검증 가능하다.

지연 결제가 완료로 바뀌면 `fct_order.payment_total`과 `fct_order_payment`의 해당 Order 합계가 Rebuild 뒤에도 서로 맞아야 한다. 상태만 맞고 금액이 어긋나는 것이 이 시나리오에서 가장 아픈 실패이고, 지금 그 실패를 잡을 단언이 없다.

**조치**: 두 번째 Build 뒤 같은 DuckDB 연결에서 `fct_order.payment_total`과 `SELECT sum(payment_value) FROM facts.fct_order_payment WHERE order_id = ?`를 비교해 단언한다. 첫 Build 뒤에도 같은 비교를 넣어 Before/After 양쪽에서 정합이 유지됨을 보인다. 추가 Build는 필요 없다.

`payment_total`이 완료 결제만 집계하는지 전체를 집계하는지는 Model을 읽고 그 정의에 맞춰 단언한다. 정의를 바꾸지 마라 — 이 Test는 관측이지 설계 변경이 아니다.

#### 보강 2 — 증적에 Mart Diff가 없다

계획 Step은 "Record the before/after mart diff (relation, row count, hash prefix) in the evidence and the runbooks"다. R-08 증적에는 있고 **R-09 증적에는 Mart Hash도 Row Count도 없다.** `docs/runbooks/r09-late-payment.md`도 마찬가지다.

R-09는 Row가 늘지 않고 기존 Row가 갱신되는 시나리오다. 그래서 오히려 기록할 값이 분명하다 — `facts.fct_order_payment` Row Count는 **변하지 않고** Logical Hash는 **바뀐다.** 이 조합 자체가 "중복 없이 제자리 갱신됐다"는 증거다.

**조치**: `mart_logical_hashes`/`mart_row_counts`를 두 Build 전후로 호출한다. 증적에 `facts.fct_order`와 `facts.fct_order_payment`의 Before/After Row Count와 Hash를 넣고, Row Count 불변·Hash 변경을 단언한다. Runbook 기대/실제 표에도 같은 행을 추가한다.

## Phase 문서

`docs/phases/phase-08-reliability.md`의 `P8-13`·`P8-14`가 이미 체크됐다. 보강 2건이 끝난 뒤의 상태를 가리키도록 그대로 두되, 보강 완료 전에 Task 11을 닫지 않는다.
