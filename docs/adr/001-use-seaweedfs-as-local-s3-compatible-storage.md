# ADR 001. 로컬 S3 호환 Object Storage로 SeaweedFS를 사용한다

## Status

Accepted (PRD v1.14)

## Context

증분 수집은 Parquet Bronze와 격리(Quarantine) 객체를 로컬 개발 환경에서 저장하고, S3 API를 쓰는
수집 코드의 Upload·HEAD·Checksum 검증과 Orphan 복구를 검증해야 한다. 단일 로컬 파일 경로만 쓰면
Object Storage의 최종 Key, 업로드 뒤 검증, Metadata와 Object 사이의 분산 Commit 실패를 재현할 수 없다.

## Decision

- SeaweedFS 4.45를 로컬 S3 호환 Object Storage로 사용한다.
- Endpoint는 `http://seaweedfs:8333`, Bucket은 `commerce-lake`이며 boto3는 Path-style 연결을 쓴다.
- Credential은 환경 변수에서만 읽고, Bronze·Quarantine의 최종 Key와 Manifest는 SeaweedFS에 둔다.
- `_staging/{run_id}/{table}/`은 실제 임시 Object Upload가 필요한 경우에만 쓰며, 기본 경로는 Local
  Parquet 검증 후 Final Key Upload다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| 로컬 파일 시스템만 사용 | S3 Upload, HEAD, 최종 Key 충돌 및 Object-Metadata 불일치 경로를 검증할 수 없다. |
| AWS S3를 개발 기본값으로 사용 | 비용·Credential·네트워크 의존성이 로컬 재현성을 해친다. |
| 임의의 S3 Mock 사용 | 실제 S3 호환 API와 DuckDB Parquet Read 호환성의 통합 검증 범위가 좁다. |

## Consequences

개발 환경에 SeaweedFS Container와 S3 연결 설정이 필요하다. SeaweedFS S3 API가 AWS S3와 완전히
동일하지 않다는 제한은 Compatibility Smoke Test로 관리한다. Object의 존재만으로 Commit을 뜻하지
않으며, 공식 Bronze 판정은 ADR-014의 Metadata 상태를 따른다.

## Validation

- SeaweedFS Container의 Health Check와 `commerce-lake` Bucket 접근을 확인한다.
- boto3 Path-style Upload 뒤 HEAD, 크기·Checksum 검증을 수행한다.
- DuckDB가 SeaweedFS의 Bronze Parquet을 읽는 Compatibility Smoke Test를 수행한다.
- Credential, Secret, Local Absolute Path가 Manifest·로그·저장소에 남지 않는지 확인한다.
