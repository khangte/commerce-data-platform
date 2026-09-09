# Commerce Analytics Data Platform

Olist 공개 데이터를 Seed로 사용해 신뢰성 있는 로컬 Batch Data Platform을 구축하는 프로젝트입니다.

현재 기준 문서는 [PRD v1.6](PRD_v1.6.md)입니다.

## Phase 0 시작하기

필수 도구:

- Python 3.12.x
- uv 0.12.9 이상
- Docker Desktop 및 WSL Integration

초기화:

```bash
./scripts/init.sh
```

이 스크립트는 `.env`가 없을 때만 `.env.example`을 복사하고, lockfile 동기화와 Compose 문법을 검증합니다. 기존 `.env`는 덮어쓰지 않습니다.

수동 실행:

```bash
cp .env.example .env
uv sync --frozen
docker compose config
```

Phase 0 검증:

```bash
uv run python --version
uv sync --frozen
uv run ruff check .
uv run pytest
docker compose config
```

`docker compose config`가 Docker Desktop의 WSL Integration 오류로 실패하면 Docker Desktop 설정에서 현재 WSL 배포판을 활성화한 뒤 다시 실행합니다.

## Phase 경계

- Phase 0: 개발 환경, Dependency Lock, Repository 구조, 설정 Template
- Phase 1: PostgreSQL Source와 Olist Seed 적재

따라서 Dataset 실제 다운로드와 Container 기동은 Phase 1에서 수행합니다. Download CLI는 다음과 같습니다.

```bash
uv run python scripts/download_dataset.py --help
uv run python scripts/download_dataset.py
```

## Phase 1 실행하기

기본 PostgreSQL 호스트 포트는 `5433`입니다. 실제 Credential은 `.env`에서만 관리합니다.

```bash
docker compose up -d postgres
docker compose ps
uv run python -m src.seed --seeded-at 2026-09-03T00:00:00Z
```

아래 명령은 PostgreSQL Container와 Raw CSV가 준비된 경우에만 Seed 재실행 동일성을 확인합니다.

```bash
RUN_POSTGRES_INTEGRATION=1 uv run pytest tests/integration/test_seed_integration.py
```

## 로컬 데이터와 Secret

`.env`, Raw/Generated Data, DuckDB Warehouse, Airflow Log는 Git에 포함하지 않습니다. `.env.example`의 예시 Secret은 실제 값으로 사용하지 않습니다.
