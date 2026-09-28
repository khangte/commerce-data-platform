# ADR 005. Composite Watermark와 고정 Upper Bound를 사용한다

## Status

Accepted (PRD v1.14)

## Context

Timestamp만으로는 같은 시각에 여러 Row가 바뀔 때 안정적인 경계를 만들 수 없다. Extract 중 원천이
변경되면 Page마다 읽는 범위가 달라져 누락·중복 또는 Watermark의 잘못된 전진이 생길 수 있다.

## Decision

- Watermark는 Timestamp와 전체 PK Tie-breaker를 포함한 Cursor Tuple로 저장한다.
- 수집 시작 시 REPEATABLE READ Read-only Snapshot에서 `MAX(cursor_tuple)`을
  `extract_upper_bound`로 고정한다.
- Query는 `watermark_before < cursor_tuple <= extract_upper_bound` 조건과 Cursor 정렬을 사용한다.
- 최종 Commit에서만 기존 Watermark Version을 확인하는 CAS로 Watermark를 갱신한다.
- 초기 Watermark는 논리적 `-infinity`와 Table별 최소 Key로 해석한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Timestamp만 Watermark로 사용 | 동일 Timestamp Row를 누락하거나 다음 실행에서 중복 처리할 수 있다. |
| Page마다 현재 최대값을 다시 조회 | 실행 중 범위가 이동해 재현 가능한 Batch 경계가 사라진다. |
| Lease만 사용하고 CAS를 생략 | 만료 뒤 경쟁 실행이나 비정상 호출이 Watermark를 덮어쓸 수 있다. |

## Consequences

Manifest와 Metadata에는 `watermark_before`, `extract_upper_bound`, `watermark_after`를 남긴다.
Range가 다른 동일 Table Batch는 `BATCH_IDENTITY_CONFLICT`이며, 새 범위의 강제 처리는
`reprocess_id`가 있는 Backfill로 분리한다.

## Validation

- 같은 Timestamp의 마지막 Key가 다음 Page 또는 Batch에 빠지지 않는지 확인한다.
- 고정 Upper Bound 이후에 생긴 Row가 현재 실행이 아니라 다음 실행에 포함되는지 확인한다.
- CAS 충돌, 실패 Upload/Validation, 동일 Batch 재실행에서 Watermark·Object 상태를 검증한다.
