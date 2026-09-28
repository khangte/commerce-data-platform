# ADR 010. Metadata 기반 Bronze 파일 목록만 dbt 입력으로 사용한다

## Status

Accepted (PRD v1.14)

## Context

Object Storage에는 업로드 완료·검증 완료였지만 Metadata Commit 전인 Object와 Orphan이 존재할 수 있다.
Prefix Glob을 dbt가 직접 읽으면 공식 Commit 전 데이터를 분석 결과에 섞을 수 있다.

## Decision

- `sync_bronze_catalog`가 Metadata에서 `COMMITTED`인 Object만 DuckDB
  `control.bronze_files`에 동기화한다.
- Catalog는 `source_table`, `object_key`, `schema_version`, `batch_id`, `committed_at`,
  `row_count`, `logical_hash`를 보관한다.
- dbt Macro는 Catalog의 명시 Object 목록으로 `read_parquet([...])`를 만들고 Object Prefix Glob은
  직접 읽지 않는다.
- 빈 Catalog는 Source 계약과 같은 빈 Relation을 반환한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| S3 Prefix Glob 직접 Read | VERIFIED만 되고 COMMITTED가 아닌 Object와 Orphan을 읽을 수 있다. |
| Manifest만 Catalog 정본으로 사용 | Manifest의 VERIFIED는 Pipeline Commit을 뜻하지 않는다. |
| dbt 실행마다 Metadata를 직접 조회 | 변환 실행과 Commit 판정이 강결합되고 입력 목록의 증적이 약해진다. |

## Consequences

Catalog는 Bronze Object의 복사본이지만 Commit 권한은 Metadata에 남는다. Schema Version이 지원되지
않으면 dbt 실행 전에 차단해야 하며, Orphan 복구 전에는 Catalog에 넣지 않는다.

## Validation

- COMMITTED Object만 Catalog에 들어가고 Metadata 없는 Object는 제외되는지 확인한다.
- Macro가 명시 목록을 만들며 Prefix Glob이 없는지 정적 검사한다.
- 빈 목록과 미지원 Schema Version을 독립 DuckDB Test로 검증한다.
