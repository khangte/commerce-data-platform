# ADR 011. Late Arrival는 영향 범위 재계산과 Replay 우선으로 처리한다

## Status

Accepted (PRD v1.14)

## Context

Late Arrival는 비즈니스 사건 시각보다 늦은 원천 변경 시각으로 Source에 반영된다. `updated_at`
Cursor는 새 Row를 수집하지만, 이미 만들어진 Fact·집계의 어느 Key와 날짜를 다시 계산할지 별도
판정이 필요하다.

## Decision

- 추출 여부는 Business Event Time이 아니라 `updated_at` Cursor로 판단한다.
- `control.affected_keys`에 영향 Key/Date와 dbt Invocation ID를 기록한다.
- orders는 구매일, item/payment는 연결 주문 구매일, customers는 변경 SCD2 구간과 겹치는 주문일,
  products/sellers는 해당 Entity를 사용한 주문일을 영향 범위로 한다.
- Fact/Aggregate는 영향 Key/Date만 Transactional `DELETE + INSERT` 또는 검증된 `MERGE`로 교체한다.
- 기본 Backfill은 Commit Bronze를 다시 적용하는 Replay이며, Source Read가 필요한 경우에만
  명시 Cursor 범위와 새 `reprocess_id`를 둔 Re-extract를 사용한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Late Arrival마다 Full Refresh | 비용이 크고 영향 범위·증분 정합을 검증할 수 없다. |
| Business Event Time을 수집 Cursor로 사용 | 늦게 반영된 Source 변경을 놓친다. |
| 항상 Source Re-extract | 이미 Commit된 입력의 재현성을 버리고 Source 상태 변화에 의존한다. |

## Consequences

Business Event Time과 원천 변경 시각은 반드시 분리해 기록해야 한다. 영향 범위 계산은
Intermediate의 책임이며, Incremental 결과는 Full Refresh Logical Hash와 비교해야 한다.

## Validation

- Late Order, Delayed Payment, 과거 Event의 늦은 Update가 다음 증분 수집에 포함되는지 확인한다.
- 영향 Key/Date만 재계산하고 Incremental/Full Refresh Logical Hash가 일치하는지 확인한다.
- Replay가 Source Read 없이 Commit Bronze만 사용하고 Re-extract가 별도 `reprocess_id`를 쓰는지 확인한다.
