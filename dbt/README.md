# dbt DuckDB 프로젝트

## 디렉터리 구성

dbt는 SQL `SELECT` 문(Model)을 짜면 대상 DB(DuckDB)에 `CREATE TABLE/VIEW AS SELECT ...`로
실행해 주는 도구다. 데이터를 직접 저장하지 않는다 — 저장은 DuckDB가 한다.

```
dbt/
├── profiles.yml       DuckDB 접속 설정(Warehouse 파일 경로)
├── dbt_project.yml    프로젝트 전역 설정(Model 폴더별 Materialization/Schema 매핑)
├── models/            SELECT 문. dbt의 핵심. 아래 4개 Layer로 나뉜다
│   ├── staging/       Bronze Parquet을 그대로 다듬어 1:1 View로 노출
│   ├── intermediate/  Staging을 조합·집계하는 중간 단계(최종 산출물 아님)
│   └── marts/
│       ├── dimensions/  분석 대상 개체(고객/상품/판매자/날짜) 서술 속성 Table
│       └── facts/       측정값(주문/주문상품/결제) Table. Grain=이벤트 1개
├── macros/            여러 Model이 재사용하는 SQL 함수(Jinja). 예: bronze_source()
├── tests/             Model 결과가 지켜야 할 조건을 SQL로 표현(단일 테스트 파일)
│   + models/staging/schema.yml  컬럼 단위 not_null/unique 같은 선언적 Test
└── target/            dbt가 실행 시 생성하는 컴파일된 SQL·실행 결과(Git 추적 안 함)
```

`dbt_project.yml`의 `models:` 블록이 폴더 → Materialization(View/Table/Incremental)과
Schema 이름을 매핑한다. 예를 들어 `models/marts/facts/`는 `+materialized: incremental`,
`+schema: facts`라서 이 폴더의 Model은 전부 DuckDB의 `facts` Schema에 Incremental Table로
만들어진다. 개별 Model 파일에 있는 `{{ config(...) }}` 블록은 이 기본값을 파일 단위로
덮어쓴다.

Model 간 참조는 `{{ ref('model_name') }}`으로 한다. 파일 경로나 실제 Schema를 SQL에
직접 쓰지 않는다 — dbt가 의존성 그래프(DAG)를 만들어 실행 순서를 정하고, 참조된 Model이
먼저 실행되게 보장한다. 이 프로젝트에서는 `staging → intermediate → marts` 순서로
흐른다.

## 실행

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
