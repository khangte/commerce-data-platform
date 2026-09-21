# 022. Task 12 (R-10) 검수 — 승인, 보강 1건

> 판정: architect, 2026-09-21 (Phase 8 Task 12 developer 구현 검수)
> 관련: AC-09, AC-10, R-10, [021 판정](021_task11-r08-r09-verification.md)

## 판정

**R-10을 승인한다. 한 줄짜리 보강 1건 뒤 Task 11·12를 함께 닫는다.**

### 계획 요구 대조

| 계획 요구 | 구현 | 확인 |
| --------- | ---- | ---- |
| 구독 상태 변경 1건 + 등급 변경 1건을 Generator 전이 경로로 | `subscription_transition_scenario(..., "PAYMENT_FAILED")`, `membership_change_scenario(...)` | 통과 |
| 새 SCD2 Version이 열린다 | `len(customer_versions) == 2`, `len(subscription_versions) == 2` | 통과 |
| 이전 Version이 겹침 없이 닫힌다 | `first[3] == second[2]` (`valid_to == 다음 valid_from`) | 통과 |
| Current Version이 정확히 하나 | `first[4] is False`, `second[4] is True` + 길이 2 | 통과 |
| 변경 이후 사건은 새 Version Key에, 이전 사건은 옛 Key에 결합 | `fct_order` 두 건, `fct_subscription_payment` 두 건 양방향 | 통과 |
| 같은 Publish Run에서 `dim_customer_*` Singular Test 통과 | `_assert_prefixed_tests_passed(..., "dim_customer_")` 두 Build 모두 | 통과 |

겹침 부재를 `valid_to == 다음 valid_from`이라는 **인접성**으로 단언한 것이 맞다. 반개 구간에서 인접성은 겹침과 구멍을 동시에 배제한다. 부등식 두 개로 나눠 쓰는 것보다 강하고 짧다.

결합 검증을 Order와 Subscription Payment **양쪽**에서, 각각 변경 전후 사건으로 네 번 단언한 점이 좋다. 한쪽만 보면 Dimension이 통째로 새 Key로 밀렸는지 구분되지 않는다.

`effective_from = timestamptz '-infinity'`를 첫 Version에만 참으로 단언한 것도 `dim_customer_first_version_effective_from_is_negative_infinity` Singular Test와 같은 불변 조건을 Test 쪽에서 독립적으로 확인하는 것이라 중복이 아니다.

## 보강 1건 — `dim_subscription_*` Singular Test도 함께 단언한다

이 Test는 구독 상태를 `ACTIVE`에서 `PAYMENT_FAILED`로 전이시킨다. `dbt/tests/`에는 그 전이를 직접 지키는 Singular Test가 있다.

```
dim_subscription_transition_valid.sql
dim_subscription_scd2_no_overlapping_ranges.sql
dim_subscription_exactly_one_current_version.sql
dim_subscription_first_version_effective_from_is_negative_infinity.sql
dim_subscription_one_open_contract.sql
dim_subscription_version_unique.sql
```

현재 Test는 `dim_customer_` 접두사만 확인한다. 계획에 `dim_customer_*`만 적은 것은 내가 AC-09·AC-10만 보고 쓴 탓이고, 실제 시나리오는 Subscription SCD2를 같은 무게로 건드린다.

`_assert_prefixed_tests_passed`의 값어치는 "Test가 실패하지 않았다"가 아니라 **"Test가 실제로 돌았다"**를 확인하는 데 있다. 선택에서 빠지거나 Warn으로 떨어지면 Build는 성공으로 끝난다. Subscription 쪽에도 같은 보호가 필요하다.

**조치**: 두 Build 뒤 `_assert_prefixed_tests_passed(tmp_path / "dbt-target", "dim_subscription_")`를 추가한다. 추가 Build는 없다. `docs/runbooks/r10-customer-history-change.md` 기대/실제 표에도 해당 행을 추가한다.

계획 Task 12의 Step 표현도 `dim_customer_*`에서 `dim_customer_*`·`dim_subscription_*`로 고친다. 내가 갱신했다.

## Task 11 보강 확인

021이 요구한 R-09 보강 2건을 확인했다.

| 021 요구 | 구현 | 확인 |
| -------- | ---- | ---- |
| Fact 측정값 정합 (`payment_total` == `sum(payment_value)`) | 첫·두 번째 Build 양쪽에서 단언 | 통과 |
| 증적·Runbook에 Before/After Mart Diff | `mart_logical_hashes`/`mart_row_counts` 전후, Runbook 표 2행 추가 | 통과 |

Row Count 불변과 Hash 변경을 한 쌍으로 기록해 "중복 없이 제자리 갱신"을 보이게 한 것까지 요구대로다.

## 재검수 (2026-09-21)

보강 1건 반영 확인. Task 11·12를 닫는다.

| 항목 | 확인 |
| ---- | ---- |
| `_assert_prefixed_tests_passed(..., "dim_subscription_")` 두 Build 각각 | `tests/reliability/test_r08_r10_late_and_history.py:410`, `:446` |
| Runbook 기대/실제 표 행 확장 | `docs/runbooks/r10-customer-history-change.md:27` |
| 추가 Build 없음 | 기존 두 Build 뒤에만 단언 |

이로써 R-08·R-09·R-10이 모두 닫혔다. Phase 8 남은 항목은 Task 13(R-14), Task 13A(014·019 결함 수정), Task 14(Troubleshooting 색인), Task 15(전체 스위트 마감)다.
