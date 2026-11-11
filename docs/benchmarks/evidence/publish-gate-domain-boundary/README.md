# 도메인 경계 미검증 3건 재현 증적 (F·G·H)

- 수집 시각: 2026-11-11T09:58Z (UTC). 실행 구간: 재현 실험 09:29–09:41Z, Airflow 09:40–09:55Z
- 환경: branch `feat/publish-gate-split` (HEAD 93f3c7a)를 로컬 Docker로 실행했다. 버전은 Airflow 3.3.1 LocalExecutor, dbt-core 1.12.3, dbt-duckdb 1.11.0이다.
- 관련 문서: [08-publish-gate-domain-split.md §6](../../08-publish-gate-domain-split.md#6-미검증-3건-재현-2026-11-11), 계획 `docs/superpowers/plans/2026-11-11-publish-gate-domain-boundary.md` Phase 2
- 이전 증적: [publish-gate-domain-split/](../publish-gate-domain-split/README.md)은 orders 보류만 Airflow에서 확인했다.

## 시나리오

| 파일 | 건 | 결과 |
|---|---|---|
| [01-repro-c-e-f-g.md](01-repro-c-e-f-g.md) | C·E Run report 판정 기록 | PASS: C는 `PUBLISH_FAILED`, E는 `PUBLISH_PARTIAL:subscriptions` |
| 〃 | F. 두 도메인 동시 실패 | PASS: 스위치 켬에서도 `FAILED`·`held_domains=[]`·Hash 불변·`PUBLISH_FAILED` |
| 〃 | G. 실데이터 경계 실패 | PASS: 경계 Test `relationships_fct_subscription_payment_customer_key…` 실패. 결과는 전체 차단, `failed/`에 되돌리기 전 파일, 오류에 `partial publish fallback` |
| [02-airflow-subscriptions-held.md](02-airflow-subscriptions-held.md) | H. Airflow에서 `subscriptions`만 보류 | PASS: `dbt_build` 성공, `held_domains=["subscriptions"]`, `PUBLISH_PARTIAL:subscriptions`, `ok=false`, `fct_order` 99849 → 99869 |

## 수집 방법

- 재현 실험: `tests/reliability/test_domain_gate_experiment.py`(integration 마커)를 1회 실행했다. 실험은 결과 JSON(`data/reliability/gate-domain-split/20261111T094058794989Z.json`)을 남기는데, 이 파일은 gitignore 대상이다. 그래서 판정 키를 01 문서에 옮겨 적었다.
- G 주입 범위:
  - `tests/reliability/faults.py::backdated_tier_observation`은 SeaweedFS `reliability-faults/` 아래에 parquet 1개를 올린다.
  - 같은 주입은 실험 Build 파일의 `control.bronze_files`에 행 1개를 넣는다.
  - 운영 Postgres Bronze Catalog와 게시본은 건드리지 않는다.
  - 종료 시 업로드한 객체를 지운다. 이후 `reliability-faults/` 목록은 비어 있었다.
- H Canary 강제:
  - `dbt/tests/subscriptions_publish_gate_canary.sql`의 var 기본값을 `true`로 잠시 바꿨다가 `git checkout`으로 되돌렸다.
  - 이렇게 한 이유는 DAG에 dbt var를 넘기는 Param이 없기 때문이다.
- REST·메타데이터·로그 수집 방식은 이전 증적과 같다.
  - REST: `airflow-api-server` 안의 일회용 `af.py`
  - 메타데이터: `postgres` 컨테이너의 `psql -x`
  - 로그: `airflow/logs/.../attempt=*.log` 발췌
- 비밀값(.env, 비밀번호, API Key, 토큰)은 넣지 않았다.
