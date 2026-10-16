# 059. Serving 갱신 경로 단일화와 지연 감지

- 날짜: 2026-10-16
- 상태: **채택·구현 완료.** 사용자 지시(2026-10-16)로 별도 승인 없이 진행, reviewer 승인, 실환경 CLI export `a3b6b240-e08c-4fcf-9c60-3e41c1247747`로 검증(재연결 SUCCESS, serving status fresh=true).
- 판정: **채택.**
  - CLI `python -m src.serving export`가 DAG와 같은 순서로 Export → Metabase 재지정 → 정리를 수행한다.
  - Metabase가 실제로 읽는 `serving_manifest.publish_run_id`를 최신 `PUBLISHED` Run과 비교하는 검사를 둔다.
  - DAG에 `full_refresh` param을 추가한다.
- 범위: 감사 F1–F9 전부. 049·050·`serving_manifest`는 유지한다.
- 계기: 카드 49–51 Binder Error 사고(2026-10-16)와 Serving 흐름 감사.
- 관련:
  - 감사 보고서 `serving-flow-audit.md`와 구현 계획 `2026-10-16-serving-refresh-plan.md`. 둘 다 git이 추적하지 않는 Scratchpad 문서다. 이 문서가 판정의 정본이다.
  - 019·049(Versioned Hardlink + API 재지정), 050(정리 조건), 051(재지정 실패 재시도), 058(Publish 파일 Identity). PRD v1.24 §17.1. 변경 계약은 PRD v1.25에 반영한다.

## 1. 사고와 근본 원인

1. a5f0507이 `fct_order_item.order_status`를 추가했다. 469224d가 카드 49–51을 그 컬럼으로 거르도록 바꿨다.
2. 수동 `uv run python -m src.warehouse.publish --full-refresh`가 Run 513b3202를 Publish했다. Serving은 갱신되지 않았다.
3. Metabase는 10-14 DAG Export 6f8e7549를 계속 읽었다. 카드 쿼리가 `Binder Error: Table "fct_order_item" does not have a column named "order_status"`로 실패했다.
4. 복구는 CLI Export와 즉석 Python 코드의 `repoint_and_prune_serving` 호출로 했다. 058 검증 때도 같은 방법을 썼다.

근본 원인은 진입점마다 Serving 단계가 다르다는 것이다.

| 진입점 | publish | export | 재지정 | 정리 |
| --- | --- | --- | --- | --- |
| DAG `warehouse_pipeline_dag` | 예 | 예 | 예 | 예 |
| `python -m src.warehouse.publish` | 예 | 아니오 | 아니오 | 아니오 |
| `python -m src.serving export` (변경 전) | — | 예 | 아니오 | 아니오 |

`--full-refresh`는 CLI에만 있다. 그래서 스키마 변경 배포는 구조적으로 Serving을 갱신하지 않는 경로를 탄다. 지연을 감지하는 단계도 없어서, 카드 쿼리가 실패한 뒤에야 드러났다.

## 2. 판정

### 2.1 CLI Export가 재지정·정리까지 수행 (F1, F4, F5) — 채택

- `python -m src.serving export`는 `export_serving_mart` → `repoint_and_prune_serving`을 호출한다. DAG의 두 Task와 같은 함수, 같은 순서다.
- DAG와 CLI가 함께 쓸 결합 함수는 만들지 않는다. DAG는 Export(retries=0)와 재지정(retries=2, 051)을 재시도 정책이 다른 Task로 나눠야 한다. 공유 단위는 두 함수다.
- 재지정 결과:
  - `SUCCESS`: 정리 후 종료 코드 0.
  - `SKIPPED`(Key 또는 Database ID 없음): 050에 따라 정리하고 0으로 끝낸다. stderr에 경고를 쓰고 `status` 확인을 안내한다.
  - `FAILED`: 정리하지 않고 종료 코드 1. 050 불변식("Metabase details가 가리킬 수 있는 파일은 삭제하지 않는다")을 지킨다.
- 즉석 재지정 코드(F4)와 CLI 경로의 무한 누적(F5)이 함께 사라진다.
- `--no-repoint` 같은 옵션은 두지 않는다. 설정을 비우면 SKIPPED로 같은 효과가 난다.
- 호스트 CLI의 Metabase URL:
  - process env의 `METABASE_URL`만 읽고, 없으면 `http://localhost:3000`을 쓴다.
  - `.env`의 `METABASE_URL=http://metabase:3000`은 Airflow 컨테이너 주소이므로 호스트에서 읽지 않는다.
- 동시 실행 위험: CLI와 DAG가 동시에 재지정하면 마지막 PUT이 이긴다. 각 Export는 완결된 파일이라 손상은 없다. 운영 규칙(§4)으로 막고 잠금은 두지 않는다.
- 호스트 정리 권한을 확인했다. `exports/`는 `1000:0` `775`, DAG가 만든 파일도 소유자 uid 1000이다.

### 2.2 DAG `full_refresh` param (F2) — 채택

- `params={"full_refresh": Param(False, type="boolean")}`. True일 때만 dbt에 `--full-refresh`를 전달한다.
- 실행: `airflow dags trigger warehouse_pipeline_dag --conf '{"full_refresh": true}'`.
- 기본값은 False다. conf 없이 실행하는 `source_simulation_dag`의 `TriggerDagRunOperator`도 동작이 바뀌지 않는다.
- CLI publish는 개발·복구용으로 남긴다. Publish 성공 뒤 stderr에 "Serving은 갱신되지 않았다"는 안내와 `src.serving export` 명령을 출력한다.

### 2.3 Serving 지연 감지 (F3, F7) — 채택

- 판정 기준은 하나다. **Metabase가 실제로 읽는 `serving_manifest.publish_run_id` = 최신 `PUBLISHED` Run.**
- 파일이 아니라 Metabase `/api/dataset`을 거쳐 읽는다. 049의 instance cache 때문에 생기는 stale까지 잡기 위해서다.
- `python -m src.serving status`: 비교 결과를 JSON으로 출력한다. fresh면 0, 아니면 1을 반환한다. 질의 실패도 1이다.
- `scripts/metabase_import.py --apply`: 모든 GET 뒤, 첫 PUT 전에 카드의 database id마다 한 번 검사한다. 지연이면 PUT 없이 중단한다.
  - **변경 0건이어도 검사한다.** 이번 사고에서 `--apply`는 변경 0건이었고, 그 직후 카드가 실패했다.
- 위치 인자 `export`를 argparse 하위 명령 `export`·`status`로 실체화한다(F7). 기존 `python -m src.serving export` 형태는 그대로 유효하다.
- DAG에는 넣지 않는다. 같은 실행 안에서 Export → 재지정을 수행하고, 재지정 검증이 이미 `[[export_id]]`를 확인한다.
- 기각: 카드 SQL을 Serving 파일에 직접 `EXPLAIN`하는 컬럼 계약 검사. Metabase 템플릿 태그 치환을 다시 구현해야 한다. 이번 원인은 Run 비교로 충분히 잡힌다.

### 2.4 정리 (F6, F8, F9) — 채택

- F6: `metabase/README.md`를 019·049·050과 이 판정 기준으로 고친다. 죽은 링크 `docs/bi/serving-refresh.md`와 "Export 뒤 재연결" 문장을 지운다. `.env.example`에 `METABASE_URL`이 컨테이너 주소라는 점을 적는다.
- F8: `prune_serving_exports`를 `metabase_repoint.py`로 옮긴다. `metabase_repoint`가 `export`를 import하는 역방향 의존이 사라진다. CLI 코드는 `src/serving/cli.py`로 분리한다.
- F9: Metabase 설정 로더 `src/serving/metabase_settings.py` 하나로 통합한다. `metabase_import.py`와 `capture_metabase_dashboards.py`의 `load_api_key` 중복을 지운다.
  - 의미는 `metabase_import` 기준이다. process env 값이 비어 있지 않으면 쓰고, 아니면 프로젝트 루트 `.env`를 읽는다.
  - `capture_metabase_dashboards.py`는 cwd 기준 `.env`에서 프로젝트 루트 `.env`로 바뀐다. `.env` 값이 비어 있으면 빈 Key를 반환하던 동작도 오류로 바뀐다.
  - DAG는 compose가 주입한 환경 변수를 그대로 읽는다. 바꾸지 않는다.

## 3. 유지 항목

| 항목 | 근거 |
| --- | --- |
| Versioned Hardlink `/serving/exports/{export_id}.duckdb` + API 재지정 | 049. 같은 경로는 instance cache와 C3P0 pool 때문에 무기한 stale이다. 기각한 B·D·E·F·G를 뒤집을 근거가 없다. |
| 정리는 재지정 `SUCCESS`·`SKIPPED` 뒤에만, `keep=3` | 050. CLI 경로에도 같은 규칙을 적용한다. |
| 재지정 실패는 재시도 가능한 `AirflowException` | 051. DAG Task는 바꾸지 않는다. |
| `serving_manifest` 구조 | 재지정 검증과 지연 감지가 모두 이 테이블에 기댄다. |
| Hash 범위(9개 Relation)와 Serving 복사 범위(3개 Schema 전체)의 차이 | 058·PRD v1.24에 기록했다. 새 조치는 없다. |

## 4. 운영 규칙 (반영 후)

1. 운영 Serving 갱신은 DAG로 한다. 스키마 변경은 `--conf '{"full_refresh": true}'`로 실행한다.
2. CLI publish 뒤에는 바로 `uv run python -m src.serving export`를 실행한다.
3. DAG 실행 중에는 CLI Export를 실행하지 않는다.
4. 카드 SQL 배포(`metabase_import --apply`)는 지연 검사를 통과해야 진행된다.

## 5. 완료 조건

- 전체 Test Suite와 `ruff check` 결과(marker별 통과·실패 수)를 보고한다.
- 실환경에서 CLI Export 1회 실행:
  - `metabase_repoint == "SUCCESS"`.
  - `exports/`에 새 Export를 포함한 3개 파일이 남는다.
  - Metabase `details.database_file`이 새 Export를 가리킨다.
- `python -m src.serving status`가 종료 코드 0, `fresh: true`를 반환한다.
- 카드 49–51을 1회씩 실행해 `completed`와 결과 행을 확인한다.
- `metabase_import 49 50 51 --apply`가 지연 검사를 통과한다.
- reviewer가 승인한다.

## 6. 재검토 조건

- 동시 실행 경합이 실제로 관측되면 CLI Export에 Postgres 잠금을 검토한다.
- 카드가 Serving 외 Database를 쓰게 되면 `metabase_import` 검사 대상을 Serving Database로 한정한다.
- Metabase DuckDB Driver가 경로별 instance cache를 없애면 049 재검토와 함께 재지정 단계를 다시 본다.
