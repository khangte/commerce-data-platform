# Commerce Analytics Data Platform

Olist 공개 데이터를 Seed로 사용해 신뢰성 있는 로컬 Batch Data Platform을 구축하는 프로젝트입니다.

현재 기준 문서는 [PRD v1.20](PRD_v1.20.md)입니다.

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

## Phase 0~7 전체 검증

새 Clone에서 Seed·Generator·Ingestion·Publish·Integration Test를 순서대로 실행합니다. 메인 Compose Stack을 먼저 내립니다.

```bash
docker compose down
./scripts/verify_clean_clone.sh
```

로그 경로는 첫 줄에 출력됩니다. `VERIFY_LOG=/path/to/log`로 바꿀 수 있습니다.

Warehouse Publish만 수동으로 실행하려면:

```bash
uv run python -m src.warehouse.publish
uv run python -m src.warehouse.publish --recover-only
```

Publish는 `data/warehouse/build/`에서 dbt build와 Test를 마친 뒤에만 `data/warehouse/warehouse.duckdb`를 교체합니다. 실패한 Build는 `data/warehouse/failed/`에 최근 3개만 남습니다.

실제 dbt로 Publish Gate를 검증하는 테스트는 별도 Flag가 필요합니다.

```bash
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 \
  uv run pytest tests/integration/test_publish_gate_dbt_integration.py
```

## Reliability Suite 실행하기

Phase 8 Reliability 시나리오(R-01~R-15)는 PostgreSQL·SeaweedFS Compose 서비스가 필요합니다. dbt를 실행하는
시나리오(R-14)는 `RUN_DBT_PUBLISH_INTEGRATION=1`도 함께 설정합니다.

```bash
docker compose up -d
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 \
  uv run pytest tests/reliability -v
```

특정 시나리오만 실행하려면 `-k`로 필터링합니다(예: `-k "r02 or r03 or r15"`). 증적 JSON은
`data/reliability/`에 생성되며 Git으로 관리하지 않습니다. 시나리오별 원인·조치는
[Runbook 색인](docs/runbooks/README.md)과 [Troubleshooting 색인](docs/troubleshooting/README.md)을 참고합니다.

## Benchmark 실행하기

Phase 9 Benchmark는 S(100K 주문)·M(1M)·L(5M) Scale을 `--scale`로 선택해
실행합니다. PostgreSQL·SeaweedFS Compose 서비스를 먼저 기동하고, 결과는
`data/benchmarks/{benchmark_id}/runs.jsonl`에 Raw Run별로 저장됩니다. 실행 뒤
출력된 Benchmark ID로 `report`를 호출하면 Raw 값·Median·Result Hash를 확인할 수
있습니다.

```bash
docker compose up -d

# A: Extract는 S Scale 결과만 정본으로 채택했다.
uv run python -m src.benchmark run --scenario extract --scale S

# B: 파일 형식 비교는 S/M/L 모두 같은 방식으로 생성·측정한다.
uv run python -m src.benchmark run --scenario file_format --scale S
uv run python -m src.benchmark run --scenario file_format --scale M
uv run python -m src.benchmark run --scenario file_format --scale L

# C/D: Fixture Row 수를 명시해 M/L Scale Fixture를 만든다.
uv run python -m src.benchmark run --scenario scan --scale M \
  --fixture-old-rows 5000000 --fixture-current-rows 1000000
uv run python -m src.benchmark run --scenario cache_effect --scale M --cold \
  --fixture-old-rows 5000000 --fixture-current-rows 1000000
uv run python -m src.benchmark run --scenario cache_effect --scale M --warm \
  --fixture-old-rows 5000000 --fixture-current-rows 1000000

uv run python -m src.benchmark report --benchmark-id <benchmark_id>
```

Cold Run은 Linux Page Cache 초기화 권한이 필요하며, 권한이 없으면 실제 사용한
대체 방법이 Run Metadata에 기록됩니다. Cold와 Warm 결과는 같은 모집단으로
합치지 않습니다. Scale별 Fixture·Raw 결과·해석은
[`docs/benchmarks/`](docs/benchmarks/)에 보존합니다.

## Benchmark Cache 초기화

Phase 9 Benchmark의 Cold Run은 `src/benchmark/cache.py`의 `reset_caches()`로 OS Page Cache를 지웁니다.

- 정상 경로: `sync` 실행 후 `/proc/sys/vm/drop_caches`에 `3`을 씁니다.
- WSL2 주의: WSL2 커널은 기본적으로 일반 사용자의 `drop_caches` 쓰기를 거부합니다(Permission denied).
  이 경우 `reset_caches()`는 실패를 숨기지 않고 `cache_reset_method="process_restart_only"`로
  내려가며, `services`를 넘기면 그 Compose Service들을 재시작해 대신 Cache 효과를 낮춥니다.
- Cold Run과 Warm Run은 절대 같은 집계에 섞지 않습니다.

## Dashboard 실행하기

Phase 10은 Metabase로 Sales·Product·Customer 3개 Dashboard를 제공합니다.
`Commerce Mart Serving`(database ID `2`) 하나만 읽고, Dashboard ID는 Sales `2`,
Product `3`, Customer `4`입니다.

빈 Metabase 앱 DB에서 Connection을 만든 뒤 `METABASE_API_KEY`를 설정하고
`scripts/metabase_snapshot.sh`로 카드·Dashboard를 재생성합니다. 재현 절차와
Source Model 매핑은 [`docs/bi/dashboards.md`](docs/bi/dashboards.md), Dashboard별
Dataset/Run/Export 식별자와 Screenshot 증적 현황은
[`docs/bi/evidence.md`](docs/bi/evidence.md)를 참고합니다.

## 로컬 데이터와 Secret

`.env`, Raw/Generated Data, DuckDB Warehouse, Airflow Log는 Git에 포함하지 않습니다. `.env.example`의 예시 Secret은 실제 값으로 사용하지 않습니다.
