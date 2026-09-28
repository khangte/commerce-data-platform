# ADR 014. Bronze Commit의 권한은 Metadata 상태로 판정한다

## Status

Accepted (PRD v1.14)

## Context

Object Storage Upload와 PostgreSQL Metadata Commit은 하나의 원자 Transaction이 아니다. Object와
Manifest가 정상이어도 Metadata Commit·Watermark CAS가 실패할 수 있으므로, Object 존재나 Manifest
`VERIFIED`만으로 공식 Bronze를 판정하면 미완료 데이터를 소비할 위험이 있다.

## Decision

- 공식 Bronze의 Source of Truth는 `pipeline_metadata.bronze_objects.status = COMMITTED`다.
- Upload 뒤 HEAD·Checksum·Row Count를 검증하고 Manifest를 `object_state=VERIFIED`로 기록한다.
- Metadata Transaction 하나에서 `bronze_objects → COMMITTED`, `pipeline_runs → SUCCESS`,
  Watermark CAS Update를 처리한다.
- Metadata COMMITTED가 없는 Object+VERIFIED Manifest는 Orphan 후보이며 dbt Read·Catalog 동기화
  대상이 아니다.
- Reconciliation은 Manifest, Checksum, Range, Schema Version을 검증하며 불일치 Object를 자동
  Commit하지 않는다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Object 존재를 Commit 기준으로 사용 | Upload 성공 뒤 Metadata 실패한 Object를 분석에 노출한다. |
| Manifest `VERIFIED`를 Commit 기준으로 사용 | VERIFIED는 Object 검증 완료일 뿐 Pipeline 성공·Watermark 갱신을 뜻하지 않는다. |
| Object Storage와 Metadata의 분산 Transaction | V1의 SeaweedFS/PostgreSQL 조합에서 실용적인 원자 Commit 수단이 아니다. |

## Consequences

실패 뒤 Orphan이 남을 수 있으며, 이는 오류가 아니라 검증·복구 대상이다. dbt 입력은 ADR-010의
Catalog를 통해서만 COMMITTED Object로 제한된다. 최종 Bronze Object는 덮어쓰기·삭제하지 않는다.

## Validation

- Upload, 검증, Manifest, Metadata Commit의 각 실패 지점에서 Watermark가 유지되는지 확인한다.
- VERIFIED만 있는 Object가 Catalog/dbt에 보이지 않는지 확인한다.
- 유효·불일치 Orphan의 Reconciliation과 동일 Batch 재실행을 통합 테스트한다.
