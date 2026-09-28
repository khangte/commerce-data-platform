# ADR 006. 단일 PostgreSQL Container에 Source와 Metadata Database를 둔다

## Status

Accepted (PRD v1.14)

## Context

V1 로컬 환경은 Source, Pipeline Metadata, Airflow Metadata를 실제 PostgreSQL 계약으로 검증해야
하지만, 서비스마다 별도 DB Server를 운영할 만큼의 분리·배포 복잡도는 필요하지 않다.

## Decision

- PostgreSQL 18.6 Container 하나에 `commerce_source`, `pipeline_metadata`, `airflow_metadata`
  Database를 생성한다.
- Source·Metadata·Airflow는 역할별 계정과 Credential을 사용한다.
- Source Table DDL, Metadata DDL, 초기화 SQL은 멱등적으로 적용하고 Credential 원본은 `.env`에만 둔다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Database마다 별도 PostgreSQL Container | 로컬 기동·연결·백업 복잡도가 늘고 V1 검증 이점이 작다. |
| 모든 Table을 한 Database·한 계정에 둠 | Source, Pipeline Metadata, Airflow의 권한·책임 경계가 사라진다. |
| SQLite 또는 Mock Metadata | PostgreSQL Transaction, `TIMESTAMPTZ`, Lease, Row Lock과 CAS 계약을 검증할 수 없다. |

## Consequences

Container 장애와 자원은 세 Database가 공유한다. 이는 로컬 개발용 선택이며 운영 환경의 물리적
격리 설계가 아니다. Schema/Value는 Source에서 보존하고 분석용 이름 표준화는 ADR-008의 Staging에서 한다.

## Validation

- Compose Health Check 뒤 세 Database와 역할별 연결이 가능한지 확인한다.
- Source PK/FK/CHECK와 증분 Index, Metadata Transaction·Lease를 PostgreSQL 통합 테스트한다.
- `.env.example`에는 Key와 용도만 있고 Secret 값이 없는지 확인한다.
