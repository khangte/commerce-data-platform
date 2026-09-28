# ADR 009. 고객 SCD2는 Bronze 누적 관측 이력으로 만든다

## Status

Accepted (PRD v1.14)

## Context

사람 단위 속성은 현재 상태만 보관하는 Source에서 `UPDATE`로 덮어쓴다. 과거 사실을 현재 속성으로
다시 해석하지 않으려면 수집 Batch마다 쌓인 Bronze 관측에서 시점별 유효 값을 복원해야 한다.

## Decision

- 고객 이력은 Bronze 누적 관측을 입력으로 SCD Type 2 유효 구간을 생성한다.
- 분석 Business Key는 Staging의 `customer_id`(Source `customer_unique_id`)를 사용한다.
- Seed의 최초 관측은 Baseline Snapshot으로 취급한다.
- 같은 사람·같은 관측 시각에 서로 다른 추적 속성 값이 오면 Source Contract 위반으로 실패한다.
- 상태 변화가 아닌 운영 속성은 Version을 늘리지 않는다. `billing_due_at`과
  `next_payment_attempt_at`은 최신 Version의 현재값으로만 조회한다.
- Fact는 사건 발생 시점에 유효한 Version을 Temporal Join한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Source의 현재 Row만 사용 | 과거 주문을 현재 고객 속성으로 잘못 해석한다. |
| Source Trigger로 History Table 유지 | V1 Raw-compatible Source의 변경 책임을 늘리고 외부 Source 일반화가 어렵다. |
| 모든 운영 속성 변경마다 SCD2 Version 생성 | 월별 일정 같은 비분석 속성이 이력을 불필요하게 폭증시킨다. |

## Consequences

성공 수집 사이의 중간 변경은 복원할 수 없다. V1 Generator는 SCD2 추적 속성을 고객당 수집 구간 내
최대 한 번만 변경한다. 정상 E2E에서 유효 Version을 찾지 못한 Unknown Fact Count는 0이어야 한다.

## Validation

- 관측 Deduplication, 유효 구간 중첩 0, Current Version 정확히 1개를 dbt Test로 확인한다.
- 동일 관측 시각의 서로 다른 추적 값이 Contract 오류가 되는지 확인한다.
- 사건 시점 Temporal Join과 Unknown Fact Count 0을 통합 테스트한다.
