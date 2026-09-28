# ADR 007. 재현 가능한 Version 고정 정책을 사용한다

## Status

Accepted (PRD v1.14)

## Context

로컬 Compose, Python Package, dbt, DuckDB와 Object Storage의 조합은 작은 Version 차이에도
동작이 달라질 수 있다. 새 Clone과 Benchmark가 같은 결과를 재현하려면 의존성 Resolution과
Container Image의 기준을 명시해야 한다.

## Decision

- Python은 `>=3.12,<3.13`, `.python-version`은 `3.12`로 고정한다.
- 직접 Python Dependency는 `pyproject.toml`, 전체 Resolution은 `uv.lock`으로 고정한다.
- Docker Image에는 `latest` Tag를 쓰지 않고 PRD Version Baseline의 명시 Tag를 사용한다.
- 실제 Patch, Docker/WSL Version은 README와 Benchmark Metadata에 기록한다.
- Version 변경은 `uv sync --frozen`, pytest, Ruff, dbt, SeaweedFS Parquet Read, Incremental,
  동일 Batch 재실행, 원천 데이터 동시성 잠금 Gate를 통과해야 한다.

## Alternatives

| 대안 | 기각 사유 |
| --- | --- |
| `latest` Image와 범위만 있는 Dependency | 같은 소스가 시점에 따라 다른 환경·결과를 만든다. |
| Lockfile 없이 설치 | transitive Dependency Resolution을 재현할 수 없다. |
| 모든 Patch Version까지 README에 하드코딩 | 보안 Patch 적용을 막고, 실제 실행 환경 증적을 남기는 목적과 맞지 않는다. |

## Consequences

의존성·Image Upgrade는 단순 설정 변경이 아니라 검증 Gate가 있는 변경이다. 개발자는 Lockfile과
고정 Image Version을 함께 갱신해야 하며, 호환성 결과를 문서화한다.

## Validation

- 새 Clone에서 `uv sync --frozen`과 기본 pytest·Ruff를 실행한다.
- Compose Image Tag와 `pyproject.toml`/`uv.lock`의 Version Baseline을 대조한다.
- Version 변경 시 지정된 dbt·Object Storage·증분·동시성 Gate 결과를 남긴다.
