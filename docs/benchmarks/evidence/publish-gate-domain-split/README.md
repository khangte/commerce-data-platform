# 도메인별 Publish Gate 분리: 실제 Airflow 검증 증적

- 수집 시각: 2026-11-11T07:41Z (UTC), 실행 구간 06:10–07:35Z
- 환경: branch `feat/publish-gate-split` (HEAD 1d687ac), `docker compose --profile airflow up -d` 로컬 실행, Airflow 3.3.1 LocalExecutor, dbt-core 1.12.3, dbt-duckdb 1.11.0
- 관련 문서: [08-publish-gate-domain-split.md](../../08-publish-gate-domain-split.md) (DuckDB 재현 실험 수치)
- 범위: 이 실환경 run에서 강제한 Canary는 `orders_publish_gate_canary` 하나다. `subscriptions`·`dim_subscription` Canary 실패(하위 28개 Node 건너뜀 포함)는 재현 실험 `tests/reliability/test_domain_gate_experiment.py` A·E에서 확인했고, 여기서는 다시 돌리지 않았다.

## 시나리오

| 파일 | 시나리오 | 대표 run | 결과 |
|---|---|---|---|
| [01-dag-param.md](01-dag-param.md) | DAG import, `partial_publish` Param 노출, trigger conf 반영 | - | PASS |
| [02-partial-on-orders-held.md](02-partial-on-orders-held.md) | partial_publish=true + orders Canary 실패 → orders만 보류하고 게시 | `manual__2026-11-11T06:10:00+00:00`, `verify_partial_on` | PASS: `PUBLISHED`, `held_domains=["orders"]`, `PUBLISH_PARTIAL:orders` |
| [03-partial-off-full-block.md](03-partial-off-full-block.md) | partial_publish=false + 같은 실패 → v1처럼 전체 차단 | `verify_partial_off` | PASS: `FAILED` / `DBT_TEST_ERROR`, 게시본 inode·mtime 불변 |
| [04-recovery-after-canary-restore.md](04-recovery-after-canary-restore.md) | Canary 복원 뒤 보류됐던 orders 변경분 게시 | `verify_partial_recover` | PASS: `held_domains=[]`, `fct_order` 99829 → 99849 |

## 수집 방법

- Canary 강제: `dbt/tests/orders_publish_gate_canary.sql`의 var 기본값을 `true`로 임시 변경한 뒤 `git checkout`으로 복원했다. DAG에는 dbt vars를 넘기는 경로가 없다.
- REST: `docker compose exec -T airflow-api-server python /tmp/af.py <METHOD> <path> [json]`. 컨테이너 안의 simple auth 비밀번호 파일로 토큰을 받는 일회용 헬퍼이고, 토큰과 비밀번호는 출력하지 않는다.
- 메타데이터: `postgres` 컨테이너에서 `psql -x`로 `mart_publish_runs`, `warehouse_run_reports`를 조회했다. 접속 정보는 컨테이너 환경변수를 썼다.
- 로그: 호스트의 `airflow/logs/dag_id=warehouse_pipeline_dag/run_id=<run>/task_id=<task>/attempt=*.log`(JSON lines)에서 판정 관련 이벤트만 ANSI 코드를 빼고 발췌했다.
- 비밀값(.env, 비밀번호, API Key, 토큰)은 포함하지 않았다.

## 판정과 무관한 실행 환경 이슈

- WSL 메모리 5.9GB에 Metabase까지 띄우자 메모리가 고갈됐다. 그 결과 `dbt_build`가 강제 종료 후 재큐잉됐고, 결국 Docker Desktop 데몬도 재시작됐다. 데몬 재시작 뒤에는 bind mount가 비어 컨테이너를 다시 만들었다. 자세한 내용은 03, 04 문서의 "실행 환경 관찰"에 있다.
- 강제 종료 뒤 같은 `publish_run_id`로 재진입한 attempt도 정상 판정으로 끝났고, 결과 행은 run마다 1개만 남았다.
