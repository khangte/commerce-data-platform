# ADR 015. Bronze Schema 변경은 Version 증가와 재기준화 정책으로 관리한다

## Status

Accepted (PRD v1.14)

## Context

Bronze Parquet은 Batch별 누적 Object이므로 Source/Bronze Column 계약이 바뀌면 이전 Object와 새
Reader가 조용히 섞일 수 있다. 예를 들어 `order_items.shipping_limit_date` 추가는 v1 Reader의
입력 의미를 바꾸며, 단순 Alias로는 구조적 변경을 숨길 수 없다.

## Decision

- Source/Bronze Column 추가·삭제·Type·Nullability 변경은 `schema_version`을 증가시킨다.
- Manifest 형식 변경은 별도의 `manifest_version`을 증가시킨다.
- Commit된 Object의 Schema Version은 수정하지 않는다.
- dbt Staging은 지원하는 Schema Version을 명시하고, 미지원 Version은
  `SOURCE_CONTRACT_ERROR`로 실행 전에 차단한다.
- Additive Nullable Column만 Contract Test 뒤 `union_by_name=true`를 허용할 수 있다.
- Column Rename은 Bronze가 아닌 ADR-008의 Staging Alias로 처리한다.
- Breaking Change는 ADR과 Migration/Backfill 범위를 정의한다. v1→v2 변경처럼 Reader 입력이
  호환되지 않으면 재기준화로 Object·Manifest·Catalog를 새 Version으로 다시 만든다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Schema Version 없이 Parquet을 혼합 Read | 누락 Column·Type 해석 차이가 분석 결과에 조용히 섞인다. |
| 기존 Commit Object를 제자리 수정 | Checksum, Manifest, 재실행 및 Audit 증적이 깨진다. |
| 모든 Rename을 새 Bronze Column으로 처리 | 원천 보존과 분석 표준화의 책임 경계가 흐려진다. |

## Consequences

Schema Version은 Batch Identity와 Catalog의 입력 계약이다. Version 변경은 Source DDL, Arrow Schema,
Manifest, Catalog, dbt 지원 목록, Contract Test와 재기준화 계획을 같은 작업 단위로 갱신해야 한다.

## Validation

- 지원 Version만 Catalog Macro가 읽고 미지원 Version은 `SOURCE_CONTRACT_ERROR`로 차단하는지 확인한다.
- Additive Nullable Column의 `union_by_name=true` 허용은 Contract Test로 확인한다.
- Breaking Change에서 기존 Object·Manifest·Catalog를 새 Version으로 재생성한 뒤 전체 dbt 검증을 수행한다.
