# ADR 002. 로컬 Warehouse로 DuckDB를 사용한다

## Status

Accepted (PRD v1.14)

## Context

로컬에서 Metadata PostgreSQL과 분리된 분석 Warehouse가 필요하다. Bronze Parquet을 읽어 dbt
Staging·Intermediate·Mart를 만들고, 전체 새 Clone에서도 동일한 파일 기반 결과를 재현해야 한다.

## Decision

- DuckDB 1.5.5를 로컬 Warehouse로 사용하고 기본 파일은 `data/warehouse/warehouse.duckdb`로 둔다.
- `control`, `staging`, `intermediate`, `marts` Schema를 사용한다.
- `control`은 table, Staging·Intermediate는 view, Dimension은 table 또는 incremental, Fact는
  incremental로 만든다.
- dbt-core/dbt-duckdb가 DuckDB를 변환 실행 경계로 사용하며, Bronze 입력은 ADR-010의 Catalog만
  통해 읽는다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| PostgreSQL을 Warehouse에도 사용 | Source·Metadata와 분석 저장소의 책임이 섞이고 로컬 파일 기반 재현성이 낮아진다. |
| Cloud Warehouse | 비용·Credential·네트워크 의존성이 V1 로컬 개발 범위를 벗어난다. |
| Parquet을 BI가 직접 조회 | Staging 표준화, 이력, Grain·Measure 계약을 제공할 Warehouse 계층이 없다. |

## Consequences

DuckDB는 single-writer이므로 Airflow `max_active_runs=1`과 dbt 단일 Process를 유지해야 한다.
Published Warehouse에서 직접 build하지 않고 파일 교체로 Publish하는 방식은 ADR-016에서 정한다.

## Validation

- `dbt debug`, `dbt parse`, `dbt build`, `dbt test`가 DuckDB Profile에서 통과한다.
- `control.bronze_files` Catalog 기반 입력으로 Staging부터 Mart까지 생성되는지 확인한다.
- Incremental 결과와 Full Refresh 결과의 Key 정렬 Logical Hash를 비교한다.
