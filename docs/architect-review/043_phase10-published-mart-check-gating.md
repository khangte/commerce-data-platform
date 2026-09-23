# 043. Phase 10 Batch 1·2 리뷰 판정 — `test_published_mart_queryable` 실행 조건

- 일자: 2026-09-22
- 대상: `tests/test_published_mart_queryable.py`, `docs/superpowers/plans/2026-09-22-phase10-bi.md` Task 1·2·16
- 리뷰 결과: 수정요청 1건(판단 필요). 코드·문서 정합 이상 없음, ruff 통과, `dbt parse` 통과, `pytest` 236 passed / 1 failed / 101 skipped
- 판정: **(b) 조건부 skip 채택. 단 Phase 10은 이 Test가 실제로 그린이 될 때까지 미완료로 유지한다**

## 1. 실패 사실

`tests/test_published_mart_queryable.py`가 실패한다. 현재 `data/warehouse/warehouse.duckdb`는 `marts.metrics`를 `view`로 만든 시점의 산출물이고, `metrics.rpt_*` 3개가 `Binder Error: Catalog "..." does not exist!`를 그대로 낸다. developer는 SeaweedFS·PostgreSQL을 재기동할 수 없어 full publish를 실행하지 못했고 이를 사전 고지했다.

즉 실패 원인은 코드 결함이 아니라 **Published 산출물이 `dbt_project.yml` 변경 이전 상태**라는 점이다. Test가 검사하는 대상은 저장소의 코드가 아니라 생성물이다.

## 2. 판정 근거

### 2.1 (c) 실패 Test를 트리에 남기는 선택을 기각한다

상시 빨간 Test는 다음 세션의 `pytest` 결과를 무의미하게 만든다. 이후 진짜 회귀가 생겨도 "원래 1건 실패"로 읽힌다. 미달 사실은 Test 실패 상태가 아니라 Phase 문서의 미체크 항목으로 남겨야 한다. [041](041_phase9-task17-closeout-review.md) §1과 같은 원칙이다.

### 2.2 (b)가 저장소 관례와 같다

인프라 의존 Test는 이미 환경 변수 Opt-in으로 분리되어 있다. `tests/test_airflow_dags.py:19`가 `RUN_AIRFLOW_SMOKE_TEST`를, SeaweedFS 통합 Test가 `RUN_SEAWEEDFS_INTEGRATION`을 쓴다. 현재 skip 101건이 그 결과다. 이 Test도 같은 부류다 — Docker와 full publish가 없으면 검사 대상 자체가 없다.

### 2.3 skip을 채택해도 회귀 방어가 남는다

Task 2의 Serving Export는 `CREATE TABLE ... AS SELECT`로 `dimensions`·`facts`·`metrics`를 복사한다. `metrics`가 다시 깨진 View가 되면 이 복사가 실패하고 DAG의 `export_serving_mart` Task가 `SERVING_EXPORT_FAILED`로 끝난다. 즉 운영 경로가 매 Publish마다 같은 불변식을 강제한다. Opt-in Test는 그 불변식의 문서화이고 유일한 방어선이 아니다. 이 사실이 (b)를 허용하는 결정적 근거다.

### 2.4 (a) 단독으로는 부족하다

Phase 10을 미완료로 유지하는 것은 (b)와 배타가 아니다. (a)만 택하면 트리에 빨간 Test가 남아 2.1 문제가 그대로다. 두 조치를 함께 적용한다.

## 3. developer 수정 지시

1. `tests/test_published_mart_queryable.py`에 `pytest.mark.skipif`를 module 수준으로 단다. 조건은 `os.environ.get("RUN_PUBLISHED_MART_CHECK") != "1"`. `reason`은 실행 방법을 담는다 — full publish 이후 `RUN_PUBLISHED_MART_CHECK=1 uv run pytest tests/test_published_mart_queryable.py -q`.
2. `xfail`을 쓰지 않는다. 실패는 기대값이 아니고, `strict` xfail은 Publish 후 그린이 되는 순간 다시 실패한다.
3. Test 본문은 바꾸지 않는다. `enable_external_access: false`와 3개 Schema 전수 검사는 그대로 둔다.
4. `.env.example`의 Test opt-in 주석 블록에 `# RUN_PUBLISHED_MART_CHECK=1  # tests/test_published_mart_queryable.py: full publish 이후 Published Mart 조회 검증` 한 줄을 추가한다.
5. `docs/phases/phase-10-bi.md`의 `P10-*` 어느 항목도 체크하지 않는다. Phase 10 상태 줄은 `Planned`을 유지한다.
6. `pyproject.toml`에 새 marker를 등록하지 않는다. `skipif`만 쓴다.

## 4. Phase 10 완료 조건 추가

Phase 10 계획의 Task 1 Verify는 다음을 모두 요구한다. 어느 하나라도 없으면 Task 1은 미체크다.

- full publish 실행 기록 (`publish_run_id`)
- `RUN_PUBLISHED_MART_CHECK=1`으로 실행한 그린 결과
- `select count(*) from metrics.rpt_membership_tier_performance`가 `Binder Error` 대신 행 수를 반환한 실측

Phase 10 마감 Task는 위 증거 없이 DoD를 채우지 않는다. Docker 가용 세션에서 재검증하기 전까지 Phase 10은 미완료다.

## 5. 범위 확인

developer가 4개 제약(dbt 컬럼·키·모델명 불변, `mart_hash` 정렬 Key 불변, `P6-01`~`P6-25` 체크 불변, 통과 Test 삭제 금지)을 모두 지켰음을 reviewer가 확인했다. 이 판정은 제약을 바꾸지 않는다. Test를 삭제하지 않고 실행 조건만 분리한다.
