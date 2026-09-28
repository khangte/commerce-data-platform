# ADR 003. Bronze 저장 형식으로 Parquet을 사용한다

## Status

Accepted (PRD v1.14)

## Context

Bronze는 원천 Column과 수집 기술 Column을 보존하면서 Page 단위로 기록되고, DuckDB/dbt가 반복해서
읽을 수 있어야 한다. 대용량 Table을 전부 메모리에 올리지 않고도 Type·시간대·Decimal 계약을 유지해야
한다.

## Decision

- Bronze와 Quarantine Record는 Parquet으로 저장한다.
- PyArrow 25.0.1을 Writer로 사용하고 Zstandard 압축, UTC microsecond Timestamp,
  `decimal128(14,2)`, Row Group 목표 128K행을 적용한다.
- 기본 파일 수는 Table Batch당 1개 `data.parquet`이며, 512MB 초과가 관측되면
  `part-00000.parquet` 방식으로 전환하고 ADR을 갱신한다.
- `content_sha256`은 Parquet Byte, `logical_hash`는 PK 정렬 Business Column의 Canonical JSON으로
  기록한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| CSV | Decimal·UTC Timestamp Type 계약과 효율적인 DuckDB Read를 보장하기 어렵다. |
| JSON Lines | 스키마·압축·분석 Query 효율이 Parquet보다 불리하다. |
| Database Table에 Bronze 누적 | Object 단위 재실행, Manifest, Orphan 복구의 독립된 증적 경계를 만들기 어렵다. |

## Consequences

Parquet Schema 변경은 ADR-015의 Version 정책을 따라야 한다. Manifest에는 Object 크기, Checksum,
Logical Hash, 행 수를 기록하지만 Raw Payload·Credential·Local Absolute Path는 기록하지 않는다.

## Validation

- Page별 Arrow Table을 순차 기록해 전체 Table 메모리 적재가 없는지 확인한다.
- SeaweedFS에 올린 Parquet을 DuckDB가 읽고 Decimal·UTC Timestamp 계약을 보존하는지 확인한다.
- 동일 Batch 재실행에서 `row_count + logical_hash`가 일치하는지 확인한다.
