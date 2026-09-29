# 054. 구독 결제 기간 불변식과 결제 경로 정합화 (053 별건)

> 설계: architect, 2026-09-29 (lead 설계 요청, 사용자 수정 승인됨)
> 관련: [053](053_subscription-reactivation-payment-row.md) 별건, [048](048_generator-logical-date-regression-silent-loss.md) 보충 수정·Cursor Guard, PRD v1.20 Section 6.1

## 사용자 규칙

> 구독이 활성 상태이면 추가 결제를 받지 않는다. 결제는 결제 예정일이 되었을 때, 또는 비활성 상태에서 다시 활성화할 때만 한다.

## 기한

**2026-10-09 00:00 UTC 전에 예약 실행 환경에 반영해야 한다.** 구독 `d3fc3b15…`의 `next_payment_attempt_at`이 이 시각이다. 이전 코드가 이 시각 이후 한 번이라도 돌면 겹치는 회차 2 `completed` 행이 생긴다. 결제 행은 불변이라 되돌릴 수 없다.

## 1. 불변식

같은 구독(`subscription_id`) 기준이다.

| ID | 불변식 |
| -- | ------ |
| I1 | `completed` 결제 행끼리 청구 기간(`billing_period_start_at` ~ `billing_period_end_at`, 반열린 구간)이 겹치지 않는다. |
| I2 | 한 회차에 `completed`는 최대 1건이다. `completed` 뒤에는 같은 회차의 시도가 없다. |
| I3 | 결제 시도는 청구 기간 시작 이후에만 한다. `payment_at >= billing_period_start_at`. (결제 예정일 이전 청구 금지) |
| I4 | `ACTIVE` 구독의 현재 기간은 가장 최근 `completed` 결제 행의 청구 기간과 같다. `completed` 행이 없으면 계약 시작 기간(`subscription_started_at` ~ +30일)이다. `billing_due_at = next_payment_attempt_at = current_period_ends_at`이다. |
| I5 | `PAYMENT_FAILED` 구독의 `next_payment_attempt_at`은 `updated_at`보다 뒤다. 재시도 예정 시각이 지난 채로 남지 않는다. |

I1·I4를 합치면 "다음 청구는 현재 결제 기간이 끝난 뒤에만 한다"가 된다. I4의 다음 청구 시각이 현재 결제 기간의 끝이기 때문이다.

감사 SQL(Source)은 부록 A에 둔다.

## 2. 결제 경로 점검

| 경로 | 현재 동작 | 판정 |
| ---- | --------- | ---- |
| (a) `subscription-active` 새 계약 시작 | 결제 행 없음. 기간 = 시작 ~ +30일, 첫 청구 = +30일 | 준수. 추가 결제가 아니다. I4의 "completed 없음" 경우다 |
| (b) 정기 청구 성공 (`_advance_active_billing`) | 회차 N+1, 기간 = `billing_due_at` ~ +30일. 구독 기간을 결제 기간으로 갱신 | 준수 |
| (c) 정기 청구 실패 | 실패 행, 상태 `PAYMENT_FAILED`, 재시도 +2일, 유예 종료 +7일 | 준수 |
| (d) 재시도 실패 | 실패 행만 쓴다. 구독 행을 갱신하지 않는다. `next_payment_attempt_at`이 과거에 남는다 | **위반 (I5).** 다음 예약 실행마다 다시 청구한다. `@hourly`면 유예 7일 동안 최대 약 168회 시도한다 |
| (e) 재시도 성공 복귀 (`PAYMENT_FAILED` → `ACTIVE`) | 성공 행 기간 = `billing_due_at` ~ +30일. 구독 기간은 복귀 시각 ~ +30일로 다시 잡는다 | **위반 (I4, 결과적으로 I1).** 053 별건의 원인 |
| (f) `subscription-payment-failed` Profile (048 보충 수정) | 선택한 `ACTIVE` 구독에 결제 예정일 전이라도 즉시 실패 행을 쓴다. 기간 시작 = 미래 `billing_due_at` | **위반 (I3).** 예정일 전 청구다. 이어지는 재시도 성공도 예정일 전 결제가 된다. `d3fc3b15…`가 이 경우다 |
| (g) 해지 신청·이탈 (스캔, Profile) | 결제 행 없음 | 준수 |
| (h) `CANCEL_REQUESTED` → `ACTIVE` | 전이 표는 허용하지만 호출 경로가 없다 | 해당 없음. 이번 범위에서 다루지 않는다 |

**(a)에 대한 판단.** 규칙의 "비활성 상태에서 다시 활성화할 때"는 결제 가능 시점을 제한하는 문구로 읽는다. 계약 시작 시 결제를 의무로 두지 않는다. 현재 첫 30일 무결제 동작을 유지한다. 해지 뒤 재가입도 새 계약 시작이므로 같다. 사용자가 "재활성화 시 결제"를 의무로 뜻했다면 계약 시작 결제(회차 1 `completed` 강제)를 따로 설계한다. 그 경우 Mart 구독 매출이 계약당 29.90 BRL 늘고 기존 계약 4건의 처리가 필요하다. 이번 구현에는 넣지 않는다.

## 3. 설계

모두 `src/generator/service.py` 안의 변경이다. 새 Table·컬럼·Error Type은 없다.

### 3.1 재시도 성공 복귀 — (e) 수정

`_advance_active_billing`을 일반화한다. 결제 성공이면 이전 상태와 무관하게 같은 함수로 구독을 갱신한다.

- `current_period_started_at` = `payment.billing_period_start_at`
- `current_period_ends_at` = `billing_due_at` = `next_payment_attempt_at` = `payment.billing_period_end_at`
- `subscription_status` = `ACTIVE`, `auto_renew_enabled` = True
- `payment_failed_at` = `cancel_requested_at` = `ended_at` = None
- `status_changed_at`: 이전 상태가 `ACTIVE`면 유지한다. `PAYMENT_FAILED`면 `logical_date`로 바꾼다.
- `updated_at` = `logical_date`

`_bill_and_transition`의 성공 분기에서 `_persist_scan_transition(..., "ACTIVE")` 호출을 이 함수로 바꾼다. 로그 행 `_subscription_scan_row(..., "ACTIVE")`는 그대로 남긴다.

재시도 결제의 청구 기간은 지금처럼 `billing_due_at`에서 시작한다. 재시도는 원래 회차의 대금을 받는 것이다. 새 기간을 여는 것이 아니다. 이 규칙이 PRD grain "재시도는 같은 회차에서 `attempt_sequence`만 증가"와 맞는다.

`_transition_subscription_record`의 `ACTIVE` 분기는 바꾸지 않는다. 결제 경로가 더는 이 분기를 쓰지 않는다. 호출자는 Test 보조 함수뿐이다.

### 3.2 재시도 실패 — (d) 수정

`_bill_and_transition`에서 `PAYMENT_FAILED` 상태의 결제가 실패하면 구독 행을 갱신한다.

- `next_payment_attempt_at` = `logical_date` + 2일 (최초 실패와 같은 간격)
- `payment_failed_at` = `logical_date`
- `updated_at` = `logical_date`
- `current_period_ends_at`(유예 종료), `status_changed_at`, `billing_due_at`은 유지한다

상태 전이가 아니므로 `subscription_transition_records`를 쓰지 않는다. `_advance_active_billing`처럼 `replace`로 Record를 만들고 `persist_subscription_records`로 저장한다. `subscriptions_updated`에 반영한다.

재시도 예정 시각이 유예 종료보다 뒤면 스캔 1단계(만료 종료)가 먼저 이탈 처리한다. 추가 분기는 필요 없다.

### 3.3 `subscription-payment-failed` Profile — (f) 수정

Profile을 "결제 예정일이 된 정기 청구를 실패로 강제"로 바꾼다. 별도 결제 쓰기 경로를 없애고 스캔의 정기 청구 경로 하나로 합친다.

1. `run_generator`에서 스캔 **전에** 대상을 고른다. 후보는 스캔 2단계(정기 청구) 조건을 만족하는 구독이다. `ACTIVE`, `auto_renew_enabled`, `next_payment_attempt_at <= logical_date`, `updated_at < logical_date`. 3.4의 "이미 결제된 기간" 조건에 걸리는 구독은 제외한다.
2. 후보가 없으면 쓰기 없이 `ValueError("subscription-payment-failed requires an ACTIVE subscription due for billing")`로 실패한다. 기존 "eligible subscription record" 실패와 같은 계열이다.
3. 후보 중 하나를 기존 선택 Hash(`entity: "subscription-transition-selection"`)로 고른다.
4. 스캔에 대상 `subscription_id`를 넘긴다. 스캔은 그 구독의 정기 청구에서 `plan_subscription_payment(..., forced_status="failed")`를 쓴다. 나머지는 (c)와 같다.
5. 스캔 뒤 Profile 결과 행(`customer_unique_id`, `subscription_status`, `subscription_updated_at`)을 `logical_rows`에 추가한다. 구독 Count는 스캔이 이미 더했으므로 다시 더하지 않는다.
6. `_apply_subscription_transition`의 `PAYMENT_FAILED` 결제 쓰기 블록(048 보충 수정)을 지운다. 이 Profile은 더 이상 `_apply_subscription_transition`을 거치지 않는다.

대가가 있다. 이 Profile은 결제 예정일이 된 구독이 있을 때만 돈다. 048 4단계처럼 계약 직후(09-07)에 돌리는 순서는 이제 실패한다. 예약 실행은 `default`만 쓰므로 운영 영향은 없다. 기존 BI 증적은 이전 동작의 결과로 남긴다. 다시 만들지 않는다.

### 3.4 기존 구독 `d3fc3b15…` 처리 — 스캔의 재정렬 분기

현재 값: 구독 기간 09-09 ~ 10-09, 다음 청구 10-09. 회차 1 `completed` 청구 기간 10-06 ~ 11-05.

스캔 2단계 앞에 분기를 하나 둔다.

- 조건: `ACTIVE`이고 `next_payment_attempt_at <= logical_date`인데, 이 구독의 최신 `completed` 행 `billing_period_end_at`이 `logical_date`보다 뒤다.
- 동작: 결제하지 않는다. 구독 기간을 그 `completed` 행의 청구 기간으로 맞추고 `billing_due_at = next_payment_attempt_at = billing_period_end_at`으로 둔다. `updated_at = logical_date`. 논리 행을 남긴다(`scan_subscription_status: "ACTIVE_REALIGNED"` 같은 구분값).

`d3fc3b15…`는 10-09 이후 첫 예약 실행에서 기간 10-06 ~ 11-05, 다음 청구 11-05로 바뀐다. 회차 2는 11-05에 11-05 ~ 12-05로 생긴다. I1·I4를 만족한다.

수정 뒤 코드에서는 3.1이 I4를 지키므로 이 분기에 들어오는 구독이 생기지 않는다. 그래도 불변식 방어로 남긴다. I1 위반 결제를 막는 마지막 지점이다.

대안을 기각한 이유:

- **SQL로 Source 직접 수정.** Lease, `generator_runs` 증적, 결정성 Hash를 우회한다. `updated_at`을 다음 예약 실행 `logical_date`와 충돌하지 않게 고르기도 어렵다(현재 `*/10` 스케줄).
- **048식 개발 데이터 재구성.** 구독 1건을 위해 Source·Bronze·Warehouse를 다시 만들고 BI 증적도 무효가 된다. 비용이 너무 크다.

`d3fc3b15…`의 회차 1 두 행(09-07 `failed`, 09-09 `completed`)은 I3을 영구 위반한다. 결제 행은 불변이므로 고치지 않는다. 감사 SQL에서 이 두 `payment_id`를 알려진 예외로 둔다(부록 A).

### 3.5 죽은 contract 항목 삭제

`_subscription_profile_contract`의 `"subscription-active"` 항목(현재 675행)을 지운다. 3.3 뒤에는 `"subscription-payment-failed"` 항목도 호출되지 않는다. 함께 지운다. 남는 항목은 `subscription-cancel-requested`, `subscription-churned` 두 개다.

## 4. 계약 점검

| 항목 | 판단 |
| ---- | ---- |
| 048 Cursor Guard | 모든 새 쓰기의 `updated_at`·`created_at`은 `logical_date`다. 재정렬·재시도 실패 갱신도 같다. Guard 변경 없음 |
| 결정적 ID | `payment_id`·`provider_payment_id` = (구독, 회차, 시도) 규칙 그대로. Profile 실패도 스캔 경로의 같은 회차·시도 계산을 쓴다 |
| `GENERATOR_VERSION` | `1.13.0` → `1.14.0`. 같은 입력의 출력이 바뀐다(3.1~3.4). `SUPPORTED_GENERATOR_VERSIONS`는 현재 관례대로 새 값 하나다 |
| 결정성 | 재정렬 분기와 Profile 대상 선택은 Source 상태와 `deterministic_inputs`만 쓴다. 같은 입력 재실행은 같은 결과다 |

## 5. Mart 영향

dbt 모델 변경은 없다. 값만 바뀐다.

- `dim_subscription`: 재시도 성공 복귀 버전의 기간 컬럼이 결제 기간과 같아진다. 재시도 실패 때마다 같은 상태(`PAYMENT_FAILED`)의 새 SCD2 버전이 생긴다. 관측 해시에 `next_payment_attempt_at`·`payment_failed_at`이 들어가기 때문이다.
- `rpt_subscription_funnel_daily`: 상태가 바뀐 버전만 센다(`previous != current`). 같은 상태 버전이 늘어도 건수는 변하지 않는다. `d3fc3b15…` 재정렬도 `ACTIVE` → `ACTIVE`이므로 세지 않는다.
- `fct_subscription_payment`·`rpt_subscription_payment_outcomes_daily`: grain 변화 없음. 재시도 폭주가 없어지므로 앞으로 실패 시도 건수가 줄어든다. `d3fc3b15…`의 겹치는 회차 2가 생기지 않으므로 10-09 구독 매출 29.90 BRL 과대 계상이 없다.

## 6. PRD 반영 범위 (lead)

PRD 6.1에 다음을 반영할지 lead가 정한다.

- `subscription_payments`: I1~I3 한 줄씩. "재시도 결제의 청구 기간은 원래 회차 기간이다."
- `customer_subscriptions`: I4·I5 한 줄씩. "결제 성공 시 현재 기간은 그 결제의 청구 기간이다."
- Profile 설명이 있는 곳에: "`subscription-payment-failed`는 결제 예정일이 된 정기 청구를 실패로 기록한다."

## 7. 구현 태스크 (developer)

1. 3.1 재시도 성공 복귀. `_advance_active_billing` 일반화.
2. 3.2 재시도 실패 갱신.
3. 3.4 재정렬 분기. 최신 `completed` 행 조회 함수를 `subscription_payments.py`에 둔다.
4. 3.3 Profile 재구성. 048 보충 수정의 결제 쓰기 블록 삭제.
5. 3.5 죽은 contract 항목 삭제.
6. `GENERATOR_VERSION` `1.14.0`.
7. 테스트(아래 8).

## 8. 테스트

단위 테스트(`tests/generator/`, 기존 `test_subscription_profile_payment.py`의 메모리 Fixture 방식):

- (T1) 정기 청구 실패(예정일 D) 뒤 D+2 재시도 성공: 구독 기간 = D ~ D+30, `billing_due_at = next_payment_attempt_at` = D+30, `payment_failed_at` None, `status_changed_at` = D+2. 결제 행은 회차 N, 시도 2.
- (T2) 재시도 실패: `next_payment_attempt_at` = 실행 시각 + 2일, `payment_failed_at`·`updated_at` = 실행 시각, 유예 종료 불변. 1시간 뒤 실행은 결제 행을 쓰지 않는다.
- (T3) 정기 청구 성공 회귀: 기존 동작 그대로.
- (T4) `subscription-payment-failed`: 예정일 된 구독이 없으면 `ValueError`, 쓰기 0건. 있으면 그 구독에 실패 행(회차 N, 시도 1) **1건만** 생긴다. `payment_at >= billing_period_start_at`. 같은 입력 재실행 시 같은 대상이다.
- (T5) 재정렬: `ACTIVE`, 예정일 도래, 최신 `completed` 기간이 아직 안 끝남 → 결제 행 0건, 구독 기간 = 그 결제 기간, `updated_at = logical_date`.
- (T6) `subscription-active`: 결제 행 0건, 기간 = 시작 ~ +30일.
- 기존 048 보충 테스트 (e)·(f)는 새 Profile 의미에 맞게 고친다. 계약 시작 → +30일 이후 Profile 실행 → +2일 재시도 순서로 바꾼다.

통합 테스트(`tests/integration/`, 실제 PostgreSQL, 기존 `test_generator_service_integration.py` 방식):

- (T7) **불변식 위반 0건.** `run_generator`를 엄격히 증가하는 `logical_date`로 연속 실행한다. 구성: `subscription-active` 여러 번 → `default`를 1일 간격으로 100일 이상 → 중간에 `subscription-payment-failed`·`subscription-cancel-requested` 삽입. 실패 경로가 실제로 나오도록 seed를 고르거나 `_billing_outcome`을 고정 패턴으로 바꾼다. 마지막에 부록 A의 I1~I5 쿼리가 모두 0건이다. 재시도 성공 복귀·재시도 실패·Profile 실패가 각각 1회 이상 일어났음을 단언한다.

Gate:

- `uv run pytest tests/generator` 통과.
- 관련 통합 테스트 통과.
- `uv run ruff check src tests` 통과.
- 실제 `commerce_source`에 부록 A 실행. 기대값: I1 0, I2 0, I3 2(알려진 예외만), I4 1(`d3fc3b15…`, 10-09 재정렬 전), I5 0. architect가 설계 시점(2026-09-29)에 실측한 값과 같다.

## 부록 A. 감사 SQL (`commerce_source`)

```sql
-- I1: completed 청구 기간 겹침
SELECT a.subscription_id, a.payment_id, b.payment_id
FROM subscription_payments a
JOIN subscription_payments b
  ON a.subscription_id = b.subscription_id
 AND a.payment_id < b.payment_id
 AND a.payment_status = 'completed' AND b.payment_status = 'completed'
 AND a.billing_period_start_at < b.billing_period_end_at
 AND b.billing_period_start_at < a.billing_period_end_at;

-- I2: 회차당 completed 2건 이상 또는 completed 뒤 시도
SELECT subscription_id, billing_cycle_sequence
FROM subscription_payments
GROUP BY subscription_id, billing_cycle_sequence
HAVING count(*) FILTER (WHERE payment_status = 'completed') > 1
    OR max(attempt_sequence) > max(attempt_sequence) FILTER (WHERE payment_status = 'completed');

-- I3: 예정일 전 청구
-- 알려진 예외: 99c7b88b-94b8-55c9-bcde-f6f3ecb80e42, a8e592b1-2a86-5dad-b3bd-40862a1b0009 (d3fc3b15… 회차 1)
SELECT payment_id, subscription_id, payment_at, billing_period_start_at
FROM subscription_payments
WHERE payment_at < billing_period_start_at;

-- I4: ACTIVE 기간과 최신 completed 기간 불일치
WITH latest AS (
    SELECT DISTINCT ON (subscription_id)
           subscription_id, billing_period_start_at AS s, billing_period_end_at AS e
    FROM subscription_payments
    WHERE payment_status = 'completed'
    ORDER BY subscription_id, billing_cycle_sequence DESC
)
SELECT c.subscription_id
FROM customer_subscriptions c
LEFT JOIN latest l USING (subscription_id)
WHERE c.subscription_status = 'ACTIVE'
  AND (
        (l.subscription_id IS NULL
         AND (c.current_period_started_at <> c.subscription_started_at
              OR c.current_period_ends_at <> c.subscription_started_at + interval '30 days'))
     OR (l.subscription_id IS NOT NULL
         AND (c.current_period_started_at <> l.s OR c.current_period_ends_at <> l.e))
     OR c.billing_due_at <> c.current_period_ends_at
     OR c.next_payment_attempt_at <> c.current_period_ends_at
  );

-- I5: 지난 재시도 예정 시각
SELECT subscription_id
FROM customer_subscriptions
WHERE subscription_status = 'PAYMENT_FAILED'
  AND next_payment_attempt_at <= updated_at;
```
