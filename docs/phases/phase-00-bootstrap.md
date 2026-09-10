# Phase 0. Bootstrap

> 상태: Done  
> Milestone: 1 — Source Foundation  
> 선행 Phase: 없음  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.8](../../PRD_v1.8.md)

## 목표

데이터 파이프라인 구현 전에 개발 환경과 Repository 구조를 새 로컬 환경에서도 동일하게 재현할 수 있는 상태로 만든다. 이 Phase에서는 이후 구현을 담을 골격과 검증 도구만 준비한다.

## 완료 시 확보되는 상태

- Python 3.12와 `uv.lock`에 기반한 재현 가능한 Python 환경
- Ruff와 pytest를 실행할 수 있는 최소 프로젝트 구성
- 후속 Phase가 사용할 Repository 디렉터리 구조
- Secret을 제외한 환경 변수 계약
- 문법 검증이 가능한 Compose Skeleton
- Olist 원본 데이터를 내려받을 수 있는 진입점
- README만으로 수행 가능한 Bootstrap 절차

## 범위

### Task 체크리스트

- [x] `P0-01` Repository 초기화 상태와 기본 브랜치 확인
- [x] `P0-02` Python `>=3.12,<3.13` 및 `uv` 설정
- [x] `P0-03` `pyproject.toml`의 Runtime/Development Dependency와 도구 설정 구성
- [x] `P0-04` `.python-version`을 `3.12`로 고정
- [x] `P0-05` PRD Section 23의 기본 디렉터리 구조 생성
- [x] `P0-06` Python, IDE, 환경 변수, Raw/Generated Data를 고려한 `.gitignore` 작성
- [x] `P0-07` Secret 값 없이 필요한 Key만 포함한 `.env.example` 작성
- [x] `P0-08` 고정 Image Version을 사용하는 `compose.yaml` Skeleton 작성
- [x] `P0-09` `scripts/download_dataset.py`의 CLI와 다운로드 경로 구성
- [x] `P0-10` README에 사전 조건, 설치, 검증 명령 작성
- [x] `P0-11` Repository 작업 규칙을 담은 `AGENTS.md` 작성
- [x] `P0-12` 최소 pytest Smoke Test와 Ruff 설정 구성

### Repository 기준 구조

```text
airflow/dags/
src/{seed,generator,ingestion,common}/
dbt/{models,tests,macros}/
sql/{source,metadata,validation}/
data/{raw/olist,generated,warehouse,samples}/
scripts/
tests/{seed,generator,ingestion,integration}/
docs/{architecture,adr,benchmarks,phases,runbooks,troubleshooting}/
```

빈 디렉터리를 Git에서 유지해야 한다면 `.gitkeep`만 사용한다. Runtime Data와 Credential은 추적하지 않는다.

## 범위 밖

- PostgreSQL Table 또는 Database 생성
- Olist Seed 적재
- Synthetic Generator 구현
- SeaweedFS Bucket과 Prefix 구성
- Airflow DAG 구현
- dbt Model 구현

후속 Phase의 요구사항을 수용할 파일 구조와 설정 계약은 정의할 수 있지만, 기능 구현은 앞당기지 않는다.

## 검증

```bash
uv run python --version
uv sync --frozen
uv run ruff check .
uv run pytest
docker compose config
```

추가 수동 확인:

- `.env`, Raw CSV, Warehouse 파일이 Git 추적 대상이 아닌지 확인한다.
- Docker Image에 `latest` Tag가 없는지 확인한다.
- 새 Clone에서 README의 순서만으로 동일 검증을 수행할 수 있는지 확인한다.

## 요구사항 추적

| 구분 | 연결 항목                              | 이 Phase의 증거               |
| ---- | -------------------------------------- | ----------------------------- |
| PRD  | Section 3 개발 환경과 Version Baseline | Version 출력과 Lockfile       |
| PRD  | Section 23 Repository Structure        | 실제 디렉터리 구조            |
| PRD  | Section 24 Phase 0 DoD                 | 검증 명령 결과                |
| ADR  | ADR-007 Version Pinning Policy         | Lockfile과 고정 Image Version |
| AC   | AC-16 새 Clone의 Bootstrap 부분        | 새 Clone 재현 기록            |

AC-16의 전체 E2E/dbt 검증은 Phase 6 이후 완료하며, 이 Phase에서는 환경 재현성만 검증한다.

## 산출물

| 산출물               | 완료 판단                    |
| -------------------- | ---------------------------- |
| Python/uv 구성       | `uv sync --frozen` 성공      |
| Compose Skeleton     | `docker compose config` 성공 |
| Test/Lint Skeleton   | Ruff와 pytest 성공           |
| 환경 변수 Template   | Secret 값 없이 필수 Key 설명 |
| Dataset Download CLI | Help/Argument 검증 가능      |
| Bootstrap README     | 새 Clone 재현 가능           |

## 파일·폴더별 변경 요약

| 경로                                                           | 변경              | 요약                                                                           |
| -------------------------------------------------------------- | ----------------- | ------------------------------------------------------------------------------ |
| `.python-version`                                              | 생성              | Python 실행 버전을 `3.12`로 고정했다.                                          |
| `pyproject.toml`, `uv.lock`                                    | 생성              | Python 의존성, Ruff, pytest 설정과 재현 가능한 Lockfile을 추가했다.            |
| `.gitignore`                                                   | 생성·수정         | Secret, Runtime Data, IDE 파일과 로컬 작업 지침 파일을 추적 대상에서 제외했다. |
| `.env.example`                                                 | 생성              | Secret 없이 필요한 환경 변수 Key와 기본값 계약을 추가했다.                     |
| `compose.yaml`                                                 | 생성              | 후속 서비스가 확장할 수 있는 Compose Skeleton을 추가했다.                      |
| `airflow/`, `dbt/`, `data/`, `sql/`, `src/`, `tests/`, `docs/` | 생성              | 후속 Phase의 기본 디렉터리 구조와 필요한 `.gitkeep` 파일을 추가했다.           |
| `src/common/__init__.py`, `src/generator/__init__.py`, `src/ingestion/__init__.py`, `src/seed/__init__.py` | 생성 | 후속 Phase 모듈을 import할 수 있는 초기 Python Package 경계를 추가했다. |
| `scripts/download_dataset.py`                                  | 생성              | Olist Raw Dataset 다운로드 CLI를 추가했다.                                     |
| `scripts/init.sh`                                              | 생성              | 환경 초기화와 기본 검증을 수행하는 Bootstrap 스크립트를 추가했다.              |
| `tests/test_bootstrap.py`                                      | 생성              | 프로젝트 골격과 Bootstrap 계약을 확인하는 Smoke Test를 추가했다.               |
| `README.md`                                                    | 생성·수정         | 사전 조건, 설치, 검증, Dataset 다운로드 방법을 추가했다.                       |
| `AGENTS.md`                                                    | 생성 후 추적 제외 | 로컬 작업 규칙과 구현·문서 동기화 규칙을 추가했으며 현재 Git에는 포함하지 않는다. |

## Definition of Done

- [x] 모든 `P0-*` Task가 완료됐다.
- [x] Python Version이 3.12.x다.
- [x] 다섯 개 기본 검증 명령이 모두 성공한다.
- [x] Git Ignore 정책이 Runtime Data와 Secret을 차단한다.
- [x] README만으로 Bootstrap 절차를 재현했다.
- [x] 실제 구현과 문서가 다르면 관련 문서 또는 ADR을 갱신했다.

## 검증 증적

2026-09-07에 아래 명령을 실행했다.

```bash
uv sync --frozen
uv run python --version
uv run ruff check .
uv run pytest
docker compose config
./scripts/init.sh
```

- Python `3.12.3`
- Ruff 오류 없음
- pytest `5 passed`
- Compose Skeleton 문법 검증 성공
- `init.sh`가 기존 `.env`를 보존한 채 전체 Bootstrap Gate를 통과

## 권장 Commit

```text
chore: bootstrap commerce data platform
```

## 다음 Phase 인계

Phase 1은 이 Phase의 Compose, 환경 변수 계약, Dataset Download 경로를 기반으로 PostgreSQL Source와 Seed Loader를 구현한다.
