# 012. Metabase Serving 전략

## Status

Accepted — DuckDB Serving Connection Gate 통과 (2026-09-22)

## Context

Published Warehouse에는 `control`·`staging`·`intermediate`와 Mart가 함께 있고 DuckDB는 Schema 단위 권한을 제공하지 않는다. BI가 Published 파일을 쓰기 가능하게 열면 WAL이 생겨 다음 Publish가 거부되며, `os.replace` 뒤 열린 Handle은 교체 전 inode를 유지한다. 기존 `marts.metrics` View는 Build 파일 이름을 Published 이름으로 바꾼 뒤 생성 당시 Catalog를 찾지 못해 실패했다.

## Decision

`dimensions`·`facts`·`metrics`만 복사한 `data/serving/mart.duckdb`를 기본 소비 경로로 둔다. Export는 `STORAGE_VERSION 'v1.0.0'`으로 만들고, Metabase에는 `/serving`만 읽기 전용 마운트한다. `marts.metrics`는 table로 실체화한다.

## Alternatives

| 대안 | 판단 |
| --- | --- |
| Published Warehouse 직접 연결 | Schema 노출, WAL Publish 차단, inode 교체 뒤 오래된 Handle 때문에 기각 |
| Build 파일 이름을 고정 | ADR-016의 동시 실행·실패 격리를 깨므로 기각 |
| Export에서 View를 재작성 | dbt 밖에 두 번째 Model 정본을 만들므로 기각 |
| PostgreSQL Serving DB | DuckDB Driver Gate가 하나라도 실패하면 선택할 대안 |

## Consequences

Publish마다 Mart 복사 비용과 Serving 파일의 저장 중복이 생긴다. 열린 Connection은 재연결 전까지 이전 Export를 볼 수 있다.

## Validation

Connection Gate 자동 검증은 로컬 `.env`의 `METABASE_API_KEY`를 `X-API-Key` 요청 헤더에만 사용한다. 키는 `.env.example`에 빈 값과 용도만 안내하며, 저장소·문서·로그에 실제 값을 남기지 않는다.

 DuckDB Driver 열기, WAL 부재, Mart 전 객체 조회, 교체 뒤 재연결 가시성, 컨테이너 재기동 지속성을 검사한다. 2026-09-22에 Metabase 플러그인 디렉터리를 UID/GID `2000`이 쓰도록 고쳐 DuckDB 드라이버가 등록되는 것까지 확인했다. release `1.5.5.0` jar의 플러그인 표기 버전은 `1.4.1.0`이다.

2026-09-23 검증에서 Run `5d74a326-954b-4850-a3d6-56d1556fec8d`의 `PUBLISHED` Mart 9개에 대해 외부 접근을 끈 회귀 Test가 통과했고, Mart 12개를 담은 `data/serving/mart.duckdb`를 원자 Export했다. DuckDB JDBC native 라이브러리가 공식 Alpine 이미지의 musl과 호환되지 않아 연결 초기화 시 실패 또는 JVM SIGSEGV가 발생했다. `metabase/Dockerfile`에서 공식 Metabase 앱을 glibc 기반 Temurin Java 런타임으로 옮기고 health check에 필요한 `wget`을 포함해 해결했다.

`METABASE_API_KEY`로 등록한 `Commerce Mart Serving` Connection(id 2)은 `/serving/mart.duckdb`를 `read_only: true`, `memory_limit: 1GB`로 연다. Metabase API에서 Mart Relation 12개와 `dimensions`·`facts`·`metrics` Schema만 반환했고, `metrics.rpt_membership_tier_performance` 행 수는 3, DuckDB `access_mode`는 `read_only`였다. 컨테이너 안의 `/serving` Mount 쓰기 시도도 거부됐다. `docker compose --profile bi restart metabase` 뒤 health check·DuckDB Driver 등록·Connection(id 2)·같은 Mart 행 수 조회가 모두 유지돼 Connection Gate를 통과했다.
