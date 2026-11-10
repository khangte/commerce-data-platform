# 063. `tests_warned` 컬럼과 마이그레이션 전 Reader 호환

- 일자: 2026-11-10
- 대상: Publish Gate 분리 Phase 1 (`docs/superpowers/plans/2026-11-10-publish-gate-split.md`)
- 요청: reviewer — 설계 판단 1건
- 판정: **지적 타당, 수정 필요**

## 지적

`src/warehouse/publish_metadata.py`의 공용 `_RECORD_COLUMNS`에 `tests_warned`를 넣어 모든 Record 조회가 새 컬럼을 읽는다. 그런데 `src/serving/freshness.py`(`check_serving_freshness`)와 `src/serving/cli.py`(`_export`)는 `ensure_publish_metadata` 없이 `get_latest_publish_run`만 호출한다. 변경 전 DB에서는 첫 Publish가 `ALTER`를 적용하기 전까지 `serving status`/`export`가 `UndefinedColumn`으로 실패한다.

## 확인

- `_RECORD_COLUMNS`는 `get_publish_run`·`get_latest_publish_run` 등 4개 조회가 공유한다(`publish_metadata.py:80,160,171,188`).
- `ensure_publish_metadata` 호출부는 Publish 경로뿐이다(`publish.py:152,361`, `publish_metadata.py:249`). Serving Reader와 run report의 `_probe_freshness`는 마이그레이션을 적용하지 않는다.
- 원인은 계획서 Task 2가 "`PublishRecord`·`get_publish_run`에 `tests_warned` 추가"라고 지시한 것이다. 구현 이탈이 아니라 **계획 결함**이다.

## 판정

`tests_warned`를 공용 Record에서 뺀다. 이 값을 읽는 곳은 DAG `publish_run_summary` 하나뿐이고, 그 시점에는 같은 DagRun의 `prepare_warehouse_build`가 이미 `ensure_publish_metadata`를 실행했다(`build`가 있을 때만 읽는다).

| 대안 | 판정 | 사유 |
| --- | --- | --- |
| **공용 Record에서 제외 + 전용 조회 1개** | 채택 | 기존 Reader SQL이 변경 전과 같아져 업그레이드 구간 자체가 없어진다. 배포 순서 전제가 필요 없다. |
| Reader에서 `ensure_publish_metadata` 호출 | 기각 | 읽기 전용 명령(`serving status`)이 DDL을 실행하게 되고 DDL 권한을 요구한다. |
| 배포 전 마이그레이션 보장(Rollout 전제) | 기각 | 이 프로젝트는 별도 마이그레이션 단계 없이 Publish가 스키마를 준비한다. 컬럼 하나를 위해 운영 절차를 추가하지 않는다. |

## 수정 지시 (backend)

1. `_RECORD_COLUMNS`, `PublishRecord`, `_record`에서 `tests_warned`를 되돌린다(변경 전과 동일).
2. `publish_metadata.py`에 `get_tests_warned(settings, publish_run_id) -> int` 추가 — `SELECT tests_warned FROM mart_publish_runs WHERE publish_run_id = %s`, 행이 없으면 `0`.
3. `airflow/dags/warehouse_pipeline_dag.py` `publish_run_summary`는 `if build:` 안에서 `get_tests_warned`를 호출한다. `build`가 없으면 `0`.
4. `record_dbt_result(..., tests_warned=)`와 `005` SQL의 `ALTER`는 그대로 둔다(쓰기는 `prepare_warehouse_build` 뒤라 안전).
5. 테스트
   - 단위(회귀 가드): `"tests_warned" not in _RECORD_COLUMNS` — 사유를 한 줄 주석으로 남긴다.
   - 통합: 기존 Round Trip 단언을 `get_tests_warned(...) == 2`로 바꾼다.

## 업그레이드 회귀 테스트 범위

컬럼을 실제로 지운 DB에서 Reader를 돌리는 테스트는 요구하지 않는다. 채택안에서는 Reader 조회문이 변경 전과 같으므로 위 단위 가드가 같은 회귀를 막는다. 공유 테스트 DB에서 컬럼을 지웠다 되돌리는 테스트는 다른 통합 테스트를 깨뜨릴 위험이 더 크다.

## 미검증

reviewer 보고대로 PostgreSQL Round Trip은 `localhost:5433` 연결 거부로 실행되지 않았다. 수정 후 재리뷰 때 `RUN_POSTGRES_INTEGRATION=1`로 확인해야 Phase 1 완료조건 2를 충족한다.
