# ADR 004. 테이블별 Cursor 증분 전략을 사용한다

## Status

Accepted (PRD v1.14)

## Context

Source Table은 생성 전용과 변경 가능한 Table이 섞여 있고, Key 구조도 단일 Key와 복합 Key가 다르다.
하나의 전역 시각이나 Batch 기준 증분은 늦은 변경과 같은 시각의 Key 경계를 안전하게 표현하지 못한다.

## Decision

- Watermark와 수집 잠금은 `pipeline_name + source_table` 단위로 관리한다.
- Table별 Cursor Tuple은 Source 계약의 Index·정렬 순서와 같게 둔다. 예를 들어 `orders`는
  `(updated_at, order_id)`, `order_items`는 `(created_at, order_id, order_item_id)`를 쓴다.
- 각 Table Snapshot에서 Upper Bound를 고정하고, Page는 직전 Page Cursor를 Lower Bound로 하여
  Keyset Pagination으로 읽는다.
- 빈 범위는 Object 없이 `SUCCESS_NO_DATA`로 끝내고 Watermark는 유지한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| 모든 Table에 하나의 전역 Watermark | 서로 다른 변경 주기와 Key 구조 때문에 누락 또는 중복 범위를 만든다. |
| Offset Pagination | 대용량에서 비효율적이고 Snapshot 내 행 경계 변화에 취약하다. |
| 매번 Full Extract | 비용이 크고 증분·재실행·Watermark 계약을 검증할 수 없다. |

## Consequences

새 Source Table은 Cursor Tuple, 최소 Key, Index, Arrow Schema와 Validation 계약을 함께 정의해야 한다.
동일 Table의 경쟁 실행은 ADR-005의 고정 Upper Bound와 테이블별 Lease·CAS로 보호한다.

## Validation

- 각 Table Cursor와 PostgreSQL Index 정렬 순서가 일치하는지 확인한다.
- 같은 Timestamp의 복합 Key Page 경계, Empty Range, Mutable Table 변경을 통합 테스트한다.
- 실패한 Table은 Watermark가 전진하지 않고 성공한 다른 Table은 재실행에서 재사용되는지 확인한다.
