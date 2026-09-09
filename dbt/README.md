# dbt DuckDB 프로젝트

실행은 저장소 루트에서 수행한다. `profiles.yml`은 `data/warehouse/warehouse.duckdb`를
기본 Warehouse로 사용하며, 다른 경로가 필요하면 `WAREHOUSE_PATH` 환경 변수로 바꾼다.

```bash
.venv/bin/dbt debug --project-dir dbt --profiles-dir dbt
.venv/bin/dbt parse --project-dir dbt --profiles-dir dbt
.venv/bin/dbt run-operation validate_bronze_catalog --project-dir dbt --profiles-dir dbt
.venv/bin/dbt run-operation validate_bronze_catalog --args '{verify_read: true}' --project-dir dbt --profiles-dir dbt
```

`bronze_source(source_table)` Macro만 Bronze Parquet을 읽는다. Macro는
`control.bronze_files`의 COMMITTED Object Key를 명시적 `read_parquet([...])` 목록으로
변환하며, Prefix Glob을 사용하지 않는다. Object가 없을 때에도 Source 계약과 같은 빈
Relation을 반환하므로 Staging Model은 안전하게 빈 결과를 만든다.

SeaweedFS 접속 정보는 실행 환경 변수에서 읽으며, Credential은 이 디렉터리에 기록하지
않는다. 로컬 `.env`를 쓸 때에는 dbt 실행 전에 셸 또는 Orchestration 환경에 그 값을
전달한다. `verify_read: true`는 Catalog 목록을 실제로 읽어 접근 가능한지까지 검사한다.
