# 062. 운영 기록 DQ 판정 추가: DATA_STALE·ROW_COUNT_DROP

- 일자: 2026-10-27
- 요청: 운영 기록(`src/observability/run_report.py`)에 데이터 지연과 행 수 급감 판정 2개 추가
- 구현 계획: `docs/superpowers/plans/2026-10-27-run-report-dq-judgments.md`

## 배경

`NOT_FRESH`는 Metabase가 가리키는 Export가 최신 `PUBLISHED` Run과 같은지만 본다. Generator가 주문을 만들지 않아도 Pipeline은 변화 없는 Run을 정상 게시하므로 `fresh=true`·`ok=true`가 된다. 데이터 자체의 시각과 양을 보는 판정이 없다.

## 확인한 사실

| 항목 | 결과 |
| --- | --- |
| 신규 주문 시각 | `src/generator/orders/orders.py:187` — `order_purchase_timestamp = config.logical_date` |
| 원천 DAG 시각 | Airflow 3.3.1, `schedule="@hourly"` 기본 → Cron Trigger Timetable. `logical_date` = 예약 실행 시각 |
| 게시 Mart의 주문 시각 | 없음. `fct_order`는 `purchase_date_key`(일 단위)만 가진다 |
| Staging·Intermediate | `view`. Bronze S3 Parquet을 읽으므로 게시 파일만으로는 조회할 수 없다 |
| 행 수 기록 | `mart_publish_runs.mart_row_counts`(PUBLISHED이면 NOT NULL), `previous_publish_run_id`는 시작 시점 최신 PUBLISHED |
| 실패 코드 집계 | `sql/validation/warehouse_run_report_window.sql`은 코드를 나열하지 않고 `jsonb_array_elements_text`로 집계한다. 변경 불필요 |

## 판정

### 1. DATA_STALE — `fct_order.purchase_at` 추가 필요 (범위 변경)

- **측정값**: `data_lag_s = now − max(facts.fct_order.purchase_at)`. `now`는 운영 기록 수집 시각(`recorded_at`)이다. 사용자가 실제로 보는 게시 파일을 기준으로 한다.
- **Mart 변경**: 일 단위 `purchase_date_key`로는 시간 단위 지연을 잴 수 없고 Intermediate는 게시 파일에서 읽을 수 없다. `fct_order`에 `purchase_at`(`stg_orders.purchase_at` 그대로)을 추가한다. Olist 원본 `order_purchase_timestamp`이므로 원본 구조 유지 결정과 충돌하지 않는다.
  - 운영 반영 시 `full_refresh=true` 1회가 필요하다(056 §7과 같은 이유: `on_schema_change` 기본값 `ignore`, `not_null` Test).
  - Full Refresh 전 Incremental Run에서는 컬럼이 없어 `PROBE_ERROR:staleness`가 난다. 반영 순서로 막는다.
- **임계값**: `stale_after_s = 2 × SOURCE_DAG_SCHEDULE 간격`. 매시간 → 7,200초, 로컬 `*/10 * * * *` → 1,200초.
  - 정상 Run의 지연은 원천 실행 + Warehouse 경과(< 간격)다. 신규 주문이 `logical_date` 시각으로 찍히기 때문이다.
  - 지연 > 1간격 = 이번 생성에서 신규 주문 없음. 지연 > 2간격 = 연속 2개 Slot에 신규 주문 없음. 한 Slot은 허용하고 연속 정체만 잡는다.
  - 경계값(같음)은 통과다.
- **간격 계산**: 새 `src/common/schedule.py`가 `SOURCE_DAG_SCHEDULE` 기본값(`@hourly`)과 간격 계산을 소유한다. 원천 DAG도 이 함수로 주기를 읽는다(기본값 중복 제거). 새 의존성(croniter)은 넣지 않는다. 지원 형식은 `@hourly`, `@daily`, `*/N * * * *`(60을 나누는 N), `M * * * *`. 그 밖은 `ValueError` → `PROBE_ERROR:staleness`로 드러난다.
- **측정 불가**: 게시 파일 없음·`fct_order` 빈 Table이면 `data_lag_s=None`, 판정 생략.

### 2. ROW_COUNT_DROP — 차단하지 않고 표시만

- **비교 대상**: 이번 Run의 `mart_row_counts` vs 이 Run 행의 `previous_publish_run_id`가 가리키는 Run의 `mart_row_counts`. 이번 Run이 `PUBLISHED`가 아니면 비교하지 않는다(`PUBLISH_*`가 이미 잡는다).
- **허용 범위**: 전 Relation 공통 비율 1%(`ROW_COUNT_DROP_TOLERANCE = 0.01`). `current < previous × 0.99`이면 `ROW_COUNT_DROP:<relation>`.
  - `delete+insert`로 생기는 정상 감소는 변경된 주문의 항목·결제 수준이다(Run당 신규 주문 기본 1건, 진행 갱신 수 건). 10만 행 Mart의 1%(약 1,000행)보다 몇 자릿수 작다.
  - 실제 사고(Bronze Object 누락, Join 회귀, 빈 Table)는 이보다 훨씬 크다. Relation별 허용값은 근거 데이터가 없으므로 두지 않는다.
  - 정확히 1% 감소는 통과, 이전 0행이면 판정하지 않는다.
- **첫 Run**: `previous_publish_run_id`가 없으면 비교하지 않는다.
- **Relation 불일치**: 양쪽에 모두 있는 Relation만 비교한다. Mart 추가·삭제는 계약 변경이지 데이터 감소가 아니다.
- **Full Refresh**: 행 수는 기록하되 `ROW_COUNT_DROP`은 내지 않는다. 운영자가 요청한 재구축이며 Incremental 누적 오차를 바로잡으면서 줄어드는 것은 정상이다. 다음 Incremental Run은 이 Run을 기준으로 비교한다.
- **차단하지 않는 이유**:
  1. 게시 시점에는 dbt Test가 이미 통과했다. 차단은 Publish 상태 기계에 새 실패 경로를 더한다.
  2. 정상 감소를 차단하면 이후 Run도 같은 옛 기준과 비교해 계속 막힌다. 해제하려면 Override 장치가 따로 필요하다.
  3. 표시만 하면 다음 Run이 이번 Run을 기준으로 삼아 자동 해소되고, 운영 기록과 창 SQL에 남는다.

## 영향 받는 소비자

| 위치 | 변경 |
| --- | --- |
| `tests/observability/test_run_report.py` | 판정 Parametrize 추가, Probe 이름 목록 2개 추가 |
| `tests/test_airflow_dags.py` | 원천 DAG 주기 AST 평가 Namespace 갱신 |
| `docs/reference/data-transformation-flow.md` §13 | 수집 항목·실패 코드 표 2행 추가 |
| `docs/reference/mart-grain.md` §3.1 | `fct_order.purchase_at` 행 추가 |
| `sql/validation/warehouse_run_report_window.sql` | 변경 없음(코드 일반 집계) |
| PRD | 실패 코드 목록 없음. 변경 없음 |

## 후속 (2026-10-27): 보충 실행 시 DATA_STALE 오탐

- **현상**: 원천 DAG를 지난 `logical_date` T로 수동 실행하면 신규 주문이 T로 찍혀 `max(purchase_at) = T`가 되고, `recorded_at − T > stale_after_s`이면 데이터가 정상이어도 `DATA_STALE`이 난다.
- **범위 정정**: 이미 성공한 `logical_date`의 재실행·Warehouse Task 재실행은 Generator가 성공 결과를 재사용해 새 주문을 만들지 않으므로 해당하지 않는다. 원천 Cursor 최대값 이하 시각은 `SourceCursorRegressionError`로 거부되므로, 오탐은 마지막 생성 시각 이후의 지난 구간을 채우는 보충 실행에서만 생긴다.
- **판정: 문서만 보완, 코드 표식은 넣지 않는다.**
  1. 보충 Run을 코드로 가르려면 `recorded_at − logical_date` 기준의 새 판정 규칙과 테스트가 필요하다. 문서 한 절보다 비싸다.
  2. 판별에 필요한 값(`logical_date`, `recorded_at`, `max_purchase_at`, `stale_after_s`)은 이미 운영 기록에 있다. 조회 한 번으로 가릴 수 있다.
  3. 오탐은 일시적이다. 보충을 마친 뒤 다음 정기 Run에서 풀린다.
  4. 억제 규칙을 넣으면 보충 중 실제 정체도 함께 가려진다.
- **반영**: `docs/runbooks/r11-missing-schedule.md`에 판별 조회·기준·해소 시점 절 추가, `docs/reference/data-transformation-flow.md` §13 `DATA_STALE` 행에 R-11 링크.
