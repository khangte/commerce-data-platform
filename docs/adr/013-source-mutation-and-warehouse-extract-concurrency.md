# ADR 013. 원천 변경과 Warehouse 수집을 원천 데이터 동시성 잠금으로 조율한다

## Status

Accepted (PRD v1.14)

## Context

V1 Source Writer는 Seed Loader와 Synthetic Generator다. Seed는 Generator 시작 전에만 허용하지만,
Generator가 Source를 변경하는 동안 Warehouse가 Table별로 독립 Snapshot을 읽으면 Table 사이의
관측 시점이 달라질 수 있다. Table별 수집 잠금은 같은 Watermark의 경쟁만 막으므로 Writer와
Warehouse 사이의 문제를 해결하지 않는다.

## Decision

- `pipeline_metadata.source_mutation_leases`의 `resource_name='commerce_source'` 한 Row로
  전역 원천 데이터 동시성 잠금을 관리한다.
- Generator는 Source Transaction 전에 `GENERATOR` Lease를, Warehouse는 모든 Table Upper Bound
  계산 전에 `WAREHOUSE` Lease를 획득한다.
- 두 Lease는 상호 배타적이며 기본 TTL은 30분, 5분마다 연장한다.
- Warehouse는 모든 Table Extract·Validation이 끝날 때까지 Lease를 유지하고, 종료 경로 Release
  Task는 `all_done`으로 실행한다.
- 만료 Lease 인수는 Version/CAS를 검증하며, Lease 획득 실패 Run은 Source Read와 Object 생성을
  시작하지 않는다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| Table별 수집 잠금만 사용 | Warehouse Run끼리의 Watermark 경합만 막고 Generator의 Source 변경은 막지 못한다. |
| 모든 Table이 하나의 DB Snapshot을 공유 | 병렬 Airflow Task와 별도 Transaction 구조에 맞지 않고 장기 Transaction 부담이 크다. |
| Generator와 Warehouse를 시간대별로만 분리 | 재실행·수동 실행에서 상호 배타성을 강제할 수 없다. |

## Consequences

이 선택은 Synthetic Source를 쓰는 V1의 한정된 계약이며 외부 운영 DB의 무중단 일관 Snapshot을
일반화하지 않는다. 원천 데이터 동시성 잠금과 테이블별 수집 잠금은 목적이 다르므로 둘 다 유지한다.

## Validation

- Generator·Warehouse가 동시에 Lease를 획득하지 못하는지 PostgreSQL 통합 테스트한다.
- Warehouse Lease 중 Generator 변경과 Source Read/Object 생성이 차단되는지 확인한다.
- TTL 갱신, 만료 인수 Fencing, 실패·Timeout 뒤 `all_done` 해제를 확인한다.
