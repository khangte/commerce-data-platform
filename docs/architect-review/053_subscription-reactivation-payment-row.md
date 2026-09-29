# 053. `PAYMENT_FAILED` → `ACTIVE` 복귀 시 결제 행 누락 (048 범위 밖 기록 건)

> 판정: architect, 2026-09-29 (lead 분석 요청, 읽기 전용)
> 관련: [048](048_generator-logical-date-regression-silent-loss.md) 보충 판정 "범위 밖 (기록만)", PRD v1.20 Section 6.1 `subscription_payments`

## 판정

**고치지 않는다. 048에 적은 경로는 현재 코드에 없다.** 048의 "범위 밖" 기록은 architect의 오독이다. `subscription-active` Profile은 기존 구독을 `ACTIVE`로 되돌리지 않는다. 새 구독 계약을 시작한다. 현재 코드에서 `PAYMENT_FAILED` → `ACTIVE` 전이는 만료 스캔 재결제 성공 경로 하나뿐이다. 이 경로는 성공 결제 행을 먼저 쓴다. 실측으로도 해당 구독은 0건이다.

별도로, 복귀 경로를 조사하다 **구독 기간과 결제 청구 기간이 어긋나는 문제**를 찾았다. 이 문제는 053 질문과 다르다. 아래 "별건"에 기록한다.

## (1) 사실 확인

### 코드 경로

`src/generator/service.py`의 `_apply_subscription_transition`은 Profile별로 분기한다.

- `subscription-active`: 568행에서 `_start_subscription_contract`로 바로 반환한다. 이 함수는 유효 계약이 없는 고객 한 명에게 새 `ACTIVE` 계약을 만든다. 기존 구독을 읽거나 바꾸지 않는다.
- 그 외 Profile: `_subscription_profile_contract`의 목표 상태로 전이한다.

`_subscription_profile_contract`에는 `"subscription-active": ("ACTIVE", {PAYMENT_FAILED, CANCEL_REQUESTED})` 항목이 남아 있다(675행). 568행의 조기 반환 때문에 이 항목은 실행되지 않는다. 048 작성 시 이 죽은 항목을 실제 동작으로 읽었다. 조기 반환은 commit `583b990`(2026-09-17)부터 있었다. 048(2026-09-29)보다 먼저다.

`ACTIVE`로 가는 경로 전체는 다음과 같다.

| 경로 | 시작 상태 | 결제 행 |
| ---- | --------- | ------- |
| `subscription-active` Profile (`_start_subscription_contract`) | 없음(새 계약) | 없음. 계약 시작은 청구가 아니다 |
| 만료 스캔 정기 결제 성공 (`_bill_and_transition` → `_advance_active_billing`) | `ACTIVE` | 새 회차 N, 시도 1 `completed` |
| 만료 스캔 재결제 성공 (`_bill_and_transition` → `_persist_scan_transition`) | `PAYMENT_FAILED` | 같은 회차 N, 시도 k+1 `completed`를 **먼저** 쓴다 |

`CANCEL_REQUESTED` → `ACTIVE`는 `_allowed_subscription_targets`가 허용한다. 그러나 호출하는 Profile·스캔 경로가 없다. Test 보조 함수 `subscription_transition_scenario`만 임의 전이를 만들 수 있다. 운영 경로가 아니다.

### Source 실측 (`commerce_source`, 2026-09-29)

- 구독 4건: `ACTIVE` 3, `CANCEL_REQUESTED` 1.
- 결제 행 2건. 모두 구독 `d3fc3b15…`의 회차 1이다. 09-07 시도 1 `failed`, 09-09 시도 2 `completed`.
- 실패 행만 있는 회차: **0건**.
- `PAYMENT_FAILED`를 거쳐 결제 성공 없이 `ACTIVE`가 된 구독: **0건**. 유일한 복귀 구독 `d3fc3b15…`는 09-09 재결제 성공 행이 있다.

### Mart 실측 (`data/serving/mart.duckdb`, `data/warehouse/warehouse.duckdb`)

`facts.fct_subscription_payment`는 Source와 같은 2행이다. 실패만 있는 회차는 없다.

### 현재 스케줄

`source_simulation_dag`에는 Profile 순환이 없다. `anomaly_profile` Param 기본값은 `default`이다. 예약 실행은 모두 `default`로 돈다. `generator_runs` 실측도 같다. `default` 48건(09-09~09-29), 나머지 Profile 6건은 전부 048 4단계의 CLI 실행이다.

`default` 실행에서 구독 상태는 만료 스캔으로만 바뀐다. 스캔의 `PAYMENT_FAILED` → `ACTIVE`는 재결제 성공 행을 동반한다. 따라서 **현재 스케줄에서 이 결함 전이는 일어나지 않는다.** 수동으로 어떤 Profile을 넣어도 일어나지 않는다.

## (2) 영향

없다. 결함 경로가 없으므로 틀어지는 지표가 없다.

가정으로만 정리한다. 만약 결제 행 없이 `ACTIVE`로 복귀하는 경로가 있다면 다음이 틀어진다.

- `fct_subscription_payment`·`rpt_subscription_payment_outcomes_daily`: 복귀 1건마다 `completed` 1건과 29.90 BRL이 빠진다. 구독 매출 과소, 결제 성공률 과소.
- `rpt_subscription_funnel_daily.activated_count`: 복귀가 활성화로 잡힌다. 그런데 같은 날 성공 결제가 없다. 퍼널과 결제 카드가 서로 맞지 않는다.
- 회차 grain: 해당 회차는 실패만 남은 채 닫힌다. 다음 청구가 `max + 1` 회차로 간다.

## (3) PRD 정합성

PRD 6.1 grain은 "실패 후 재시도는 같은 청구 회차에서 `attempt_sequence`만 증가한다"이다. 현재 유일한 복귀 경로는 이 규칙대로다. `_bill_and_transition`은 `PAYMENT_FAILED`에서 회차를 `next_billing_cycle_sequence - 1`로, 시도를 `next_attempt_sequence`로 계산한다. 실측 `d3fc3b15…`(회차 1, 시도 1 → 2)도 규칙과 같다. **정합한다.**

048 보충 수정(Profile 실패 전이의 실패 행 강제 기록)과의 대칭도 이미 성립한다. 실패 진입은 두 경로 모두 실패 행을 쓴다. 성공 복귀는 스캔 경로 하나뿐이고 성공 행을 쓴다.

## (4) 수정 여부

053 질문에 대한 코드 수정은 필요 없다. 048 Cursor Guard, 결정적 ID, 백필 검토도 필요 없다. 새로 쓸 행이 없고 기존 Source에 오염된 구독이 없다.

## (5) 문서화

1. 048 "범위 밖"·"잔여"의 `subscription-active` 복귀 기록을 053으로 정정한다. 048 끝에 정정 한 줄을 추가했다.
2. (선택, developer) `_subscription_profile_contract`의 `"subscription-active"` 항목을 지운다. 이 항목은 실행되지 않는다. 남겨 두면 같은 오독이 다시 생긴다. 동작 변화는 없다. 한 줄 정리이므로 다음 Generator 작업에 끼워 넣어도 된다.

## 별건 — 구독 기간과 결제 청구 기간 불일치 (기록, lead 결정)

복귀 경로를 실측하다 발견했다. 053 질문과 원인이 다르다.

### 현상

구독 `d3fc3b15…`의 현재 값:

| 항목 | 값 |
| ---- | -- |
| 계약 시작 | 09-06 (`billing_due_at` 10-06) |
| 회차 1 시도 1 `failed` (09-07, Profile 전이) | 청구 기간 **10-06 ~ 11-05** |
| 회차 1 시도 2 `completed` (09-09, 스캔 재결제) | 청구 기간 **10-06 ~ 11-05** |
| 복귀 후 구독 | `current_period` **09-09 ~ 10-09**, 다음 청구 10-09 |

실제 시각 2026-10-09 이후 만료 스캔이 회차 2를 청구한다. 청구 기간은 10-09 ~ 11-08이다. 이미 결제한 회차 1 기간(10-06 ~ 11-05)과 27일 겹친다. 같은 혜택 기간을 두 번 청구한다.

### 원인

두 규칙이 서로 다른 기간을 쓴다.

- 결제 행의 청구 기간은 `billing_due_at`에서 시작한다(`_bill_and_transition`의 `period_start`, 048 보충 수정도 같은 규칙).
- 재결제 성공 뒤 `ACTIVE` 전이(`_transition_subscription_record`)는 기간을 전이 시각부터 다시 잡는다. 방금 쓴 결제 행의 기간을 보지 않는다.

정기 결제 성공 경로(`_advance_active_billing`)는 결제 행의 기간을 그대로 구독에 반영한다. 재결제 성공 경로만 다르다.

스캔 경로에서 실패가 시작되면 `billing_due_at`이 청구 시각과 거의 같다. 이때는 2일 공백만 생기고 겹침은 없다. 048 보충 수정 뒤 Profile 실패 전이는 미래 `billing_due_at`으로 실패 행을 쓴다. 그래서 겹침이 커졌다.

### 권고

재결제 성공 복귀도 `_advance_active_billing`처럼 결제 행의 `billing_period_start_at`·`billing_period_end_at`으로 구독 기간과 다음 청구 시각을 정한다. 상태만 `ACTIVE`로 바꾼다. 영향 범위는 Generator 한 함수와 Test 1~2건이다.

기존 데이터 `d3fc3b15…`는 10-09 전에 고치면 겹치는 회차 2가 생기지 않는다. 현재 Source 행은 수정 대상이 아니다. 다만 이 구독의 `current_period`는 옛 규칙 값으로 남는다. 코드 수정 뒤 이 구독을 어떻게 다룰지(그대로 두기, 개발 데이터 재구성)는 착수 시 정한다.

착수 여부는 사용자가 정한다. 착수하면 054로 설계한다.

## 요약

| 질문 | 답 |
| ---- | -- |
| 결함 경로가 있는가 | 없다. `subscription-active`는 새 계약을 만든다. 복귀는 스캔 재결제뿐이고 성공 행을 먼저 쓴다 |
| 실측 건수 | Source·Mart 모두 0건 |
| 현재 스케줄에서 발생하는가 | 아니다. 예약 실행은 `default`만 돈다 |
| Mart 영향 | 없다 |
| PRD grain 정합 | 정합한다 |
| 수정 | 불필요. 048 기록 정정, 죽은 계약 항목 삭제(선택) |
| 별건 | 재결제 성공 복귀가 구독 기간을 결제 기간과 다르게 잡는다. 10-09 이후 청구 기간이 27일 겹친다. 수정 권고, 사용자 결정 |
