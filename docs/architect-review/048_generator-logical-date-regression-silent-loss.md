# 048. Generator `logical_date` 역행으로 매시간 신규 행이 수집되지 않는다

> 판정: architect, 2026-09-29 (lead 원인 조사 요청, 읽기 전용)
> 관련: PRD v1.16 Section 4.1 `updated_at` 안전성 계약, ADR-005 Composite Watermark, [047](047_source-dag-hourly-r11-premise.md), commit `cb87afa`

## 증상

- `source_simulation_dag`는 매시간 성공한다.
- 14:00·14:27·15:00 UTC 실행 후 `serving_row_counts`가 모두 같다. 15:00 실행의 `changed_relations`는 `[]`이다. `fct_order`는 99,511행으로 변하지 않는다.
- Mart의 주문 최대 일자는 2026-10-28이다. 현재 날짜(2026-09-29)보다 미래다.

## 판정

**버그다. 데이터가 조용히 유실되고 있다.** Generator는 새 주문을 실제로 쓴다. Ingestion은 그 행을 Watermark 아래에 있다고 보고 영원히 건너뛴다. dbt에는 원인이 없다.

### (1) Generator는 실제로 새 주문을 쓴다

`pipeline_metadata.generator_runs` 기준:

| logical_date (UTC) | seed | 결과 |
| ------------------ | ---- | ---- |
| 2026-09-28 14:27 | 0 | `orders_inserted=1` |
| 2026-09-28 14:00 | 0 | `orders_inserted=1` |
| 2026-09-28 15:00 | 0 | `orders_inserted=1` |

재사용(no-op)은 없다. `commerce_source.orders`는 99,514행이다. Mart의 99,511행보다 3행 많다. 이 3행이 매시간 실행이 만든 주문이다.

### (2) Ingestion Watermark가 변경분을 놓친다 — 근본 원인

`pipeline_runs`(orders) 기준:

| logical_date | status | rows_extracted | watermark_before |
| ------------ | ------ | -------------- | ---------------- |
| 2026-09-28 15:00 | `SUCCESS_NO_DATA` | 0 | 2026-10-28T00:00 |
| 2026-09-28 14:00 | `SUCCESS_NO_DATA` | 0 | 2026-10-28T00:00 |
| 2026-09-28 14:27 | `SUCCESS_NO_DATA` | 0 | 2026-10-28T00:00 |
| 2026-10-28 00:00 | `SUCCESS` | 70 | 2026-09-03T00:00 |

인과는 다음 순서다.

1. 2026-09-28 08:13 UTC에 BI 증적용 Generator 수동 실행 7회가 있었다(seed 42~48, commit `cb87afa`). `logical_date`는 2026-09-25부터 2026-10-28까지다. 이 중 3회(09-29, 10-27, 10-28)는 실제 시각보다 미래다.
2. Generator는 신규 행에 `created_at = updated_at = logical_date`를 쓴다(`src/generator/orders.py:187-188`, 208-209). PRD 4.1 계약 그대로다.
3. 직후 Warehouse 실행이 이 행들을 수집했다. Watermark가 `2026-10-28T00:00`까지 전진했다. `customers`, `customer_subscriptions`, `customer_membership_tiers`, `orders`, `order_items`, `order_payments`의 Watermark가 모두 10-28이다. `subscription_payments`는 10-27이다.
4. 14:00부터 예약 실행이 실제 시각으로 돌아왔다. 새 행의 `updated_at`은 2026-09-28 14:00·15:00이다. 이 값은 Watermark 10-28보다 작다.
5. Incremental Extract는 Watermark보다 큰 Cursor만 읽는다. 새 행은 추출 대상이 아니다. 결과는 `SUCCESS_NO_DATA`이고 실패로 드러나지 않는다.

이 행들은 나중에도 수집되지 않는다. Watermark는 앞으로만 간다. 이미 지나친 Cursor 아래의 행은 Rewind 없이는 다시 읽히지 않는다. 실제 시각이 10-28을 지날 때까지 매시간 실행이 만드는 행은 모두 같은 방식으로 유실된다.

14:00 실행은 미래 데이터가 없었어도 문제였다. 14:27 수동 실행 뒤에 실행됐으므로 14:00 < 14:27로 역행한다. 수동 실행과 예약 실행을 섞으면 미래 날짜가 아니어도 같은 유실이 생긴다.

### (3) dbt 증분·필터는 원인이 아니다

행이 Bronze에 들어가지 않았다. dbt가 떨어뜨릴 대상이 없다. Mart 99,511행은 초기 Seed 99,441행 + 08:13 실행 70행과 정확히 같다.

### (4) 미래 일자의 원인

Generator의 날짜 산정 규칙 결함이 아니다. 사람이 파라미터로 넣은 미래 `logical_date` 때문이다(위 (2)의 1). Generator는 입력받은 `logical_date`를 주문 시각과 `updated_at`에 그대로 쓴다. 미래 값을 거부하지 않는다. `commerce_source.orders`에서 `updated_at > now()`인 행은 30개다. 미래 실행 3회 × 10건과 같다.

### 계약상 구멍

PRD 4.1은 "`updated_at` 역행은 Source Contract 위반으로 실패한다"라고 정한다. 코드는 이 검사를 **기존 행 갱신**에만 한다(`src/generator/customers.py:459,504`). **신규 행**에는 전역 Cursor 단조성 검사가 없다. 그러나 신규 행도 Watermark 입장에서는 같은 Cursor 공간에 있다. 신규 행의 `updated_at`이 Source 최대 `updated_at`보다 작거나 같으면 증분 수집에서 사라진다. `updated_at`이 같은 경우도 위험하다. Composite Watermark의 key 비교에서 `order_id`(해시)가 더 작으면 누락된다.

## 수정 방향

### A. 재발 방지 (코드, developer)

`run_generator`에서 Lease 획득 직후, 첫 쓰기 전에 검사를 추가한다.

- 대상: Generator가 쓰는 mutable Source Table의 `max(updated_at)`.
- 조건: `config.logical_date <= max(updated_at)`이면 쓰기 없이 실패한다.
- 분류: 재시도 불가 Contract 위반. 기존 Contract 계열 Error Type을 재사용하고 새 어휘는 만들지 않는다.
- 동일 입력 재사용 경로(`_successful_result`)는 이 검사보다 먼저 둔다. 이미 성공한 실행의 재호출은 쓰기가 없으므로 막지 않는다.
- Test: 미래 `logical_date` 실행 뒤 더 이른 `logical_date` 실행이 실패하고 Source 행 수가 변하지 않음을 단언한다.

미래 `logical_date` 자체는 막지 않는다. Integration Test가 격리용으로 미래 고정 시각을 쓴다(018 보충). 막아야 할 성질은 "미래"가 아니라 "역행"이다.

Ingestion 쪽에 "Watermark 아래 신규 행" 탐지를 추가하지 않는다. 쓰는 쪽에서 막으면 충분하다. 탐지 쿼리는 매 실행 전체 Table Scan이 필요하다.

### B. 현재 오염 복구 (운영, 사용자 결정 필요)

현재 Source에는 `updated_at = 2026-10-28`인 행이 남아 있다. 이 상태에서는 A를 넣어도 예약 실행이 10-28 이전 `logical_date`로 돌 때마다 실패한다. Rewind만으로는 해결되지 않는다. 재수집이 다시 10-28까지 가져가 Watermark가 원래대로 돌아가기 때문이다.

선택지:

1. **(권고) 개발 환경 데이터 재구성.** Source·Bronze·Metadata·Warehouse를 Seed 기준으로 초기화한다. 그다음 실제 시각 이하의 `logical_date`로만 Generator를 다시 돌린다. BI 증적(`docs/bi/`, 2026-10-28 기준 캡처)은 다시 만들어야 한다. 되돌릴 수 없는 작업이므로 사용자 승인이 필요하다.
2. **실제 시각이 2026-10-28을 지날 때까지 예약 실행 중지.** 데이터는 건드리지 않는다. 그 대신 약 한 달 동안 매시간 시뮬레이션이 멈춘다.
3. **이미 유실된 3건만 회수.** `rewind_tables`로 boundary를 2026-09-28 14:00 직전으로 두고 명시 Batch로 재수집한다. 결과는 R-11·R-13과 같이 멱등이다. 다만 1이나 2를 함께 하지 않으면 다음 시간부터 다시 유실된다.

**즉시 조치:** 어느 선택지를 고르든 `source_simulation_dag`를 지금 Pause 하라. 켜져 있는 동안 매시간 주문 1건과 관련 고객·결제 행이 유실된다.

## 요약

| 질문 | 답 |
| ---- | -- |
| Generator no-op인가 | 아니다. 매 실행 주문 1건을 실제로 쓴다. |
| Watermark가 변경분을 잡는가 | 못 잡는다. Watermark 10-28 > 신규 `updated_at` 09-28. `SUCCESS_NO_DATA`로 조용히 유실된다. |
| dbt가 떨어뜨리는가 | 아니다. Bronze에 행이 없다. |
| 미래 일자 원인 | BI 증적용 수동 실행에 넣은 미래 `logical_date`(09-29·10-27·10-28). Generator는 미래·역행 입력을 거부하지 않는다. |
| 버그 여부 | 버그다. 신규 행에 대한 전역 `updated_at` 단조성 검사가 없다. |

## 실행 판정 (2026-09-29)

사용자가 승인한 사항은 두 가지다. `source_simulation_dag`를 Pause 한다. 복구는 선택지 ①(Seed 기준 개발 데이터 재구성과 BI 증적 재생성)로 한다. 아래 순서로 실행한다. 앞 단계 검증이 통과해야 다음 단계로 간다.

### 0. 전제 확인

- `source_simulation_dag`가 paused 상태다.
- 두 DAG 모두 running·queued DagRun이 없다.
- `source_mutation_leases`에 활성 소유자가 없다.

### 1. 수정 A (코드)

- `src/generator/`에 역행 전용 예외를 추가한다. 이름은 예를 들어 `SourceCursorRegressionError`로 한다.
- `run_generator`에서 Source Transaction 안, 첫 `persist_order_bundle` 전에 검사한다. Generator가 쓰는 mutable Source Table(`customers`, `customer_subscriptions`, `customer_membership_tiers`, `subscription_payments`, `orders`, `order_items`, `order_payments`)에서 **수집 Cursor 컬럼**(`TABLE_CONFIGS[table].cursor_timestamp_column`)의 최대값을 읽는다. `config.logical_date <= max(cursor)`이면 예외를 던진다. 예외 메시지에는 `logical_date`, Table, 컬럼, 최대값을 넣는다. (정정: 처음에는 `max(updated_at)`에 "컬럼이 없는 Table은 제외"라고 썼다. 그렇게 하면 Cursor가 `created_at`인 `customers`·`order_items`가 빠진다. 지금은 같은 Bundle의 `orders.updated_at` 검사가 간접적으로 이 두 Table을 막는다. 그러나 계약은 수집 Cursor를 직접 기준으로 삼아야 한다. `information_schema` 조회도 필요 없어진다.)
- 순서를 지킨다. `_successful_result` 재사용 경로가 이 검사보다 먼저다. 검사는 Lease 획득 뒤에 한다. 그래야 검사와 쓰기 사이에 다른 쓰기가 끼지 않는다.
- `src/ingestion/errors.py`의 `classify_error`에서 이 예외를 기존 `SOURCE_CONTRACT_ERROR`로 분류한다. 재시도 불가다. 새 Error Type 어휘는 만들지 않는다. ADR-018에 예외→Type 대응표가 있으면 해당 행을 추가한다.
- Test:
  - (a) 앞선 실행보다 이른 `logical_date` 실행이 실패하고 Source 행 수가 변하지 않는다.
  - (b) 같은 `logical_date`도 실패한다.
  - (c) 동일 입력 재호출은 재사용으로 성공한다.
  - (d) `classify_error`가 `SOURCE_CONTRACT_ERROR`이고 `is_retryable`이 False다.
- PRD 4.1과 코드 주석에 규칙 1줄을 반영할지는 lead가 판단한다. 문구: "신규 Row의 `updated_at`(= `logical_date`)은 Source 전체 `max(updated_at)`보다 커야 한다."

### 2. 백업 — 필요하다

재구성은 되돌릴 수 없다. 현재 상태는 048 원인 분석의 증거이기도 하다. 백업 비용도 작다(Source 약 10만 행, DuckDB 55MB). 따라서 백업한다.

위치는 `data/generated/backups/2026-09-29-pre-rebaseline/`이다. 이 경로는 `.gitignore` 대상이다. 커밋하지 않는다.

| 대상 | 방법 |
| ---- | ---- |
| `commerce_source`, `pipeline_metadata`, Metabase DB | 컨테이너 안에서 `pg_dump -Fc`로 DB별 파일 3개를 만든다 |
| Bronze | SeaweedFS `bronze/`·`quarantine/`·`_staging/` Prefix 전체를 S3 API로 내려받는다. Key 목록과 개수를 파일로 남긴다 |
| Warehouse·Serving | `data/warehouse/warehouse.duckdb`, `data/serving/mart.duckdb`를 복사한다 |
| 재구성 범위 | `uv run python -m src.rebaseline --seeded-at 2026-09-03T00:00:00Z`(dry-run) 출력 JSON을 저장한다 |

백업 후 dump 파일 크기가 0이 아닌지 확인한다. Bronze Key 개수가 dry-run inventory와 같은지 확인한다. 백업은 최종 검증 통과 뒤에도 lead가 삭제를 지시할 때까지 보존한다.

**Metabase DB는 재구성하지 않는다.** Card·Dashboard 정의가 그 안에 있다. 백업만 한다.

### 3. 재구성

`uv run python -m src.rebaseline --seeded-at 2026-09-03T00:00:00Z --confirm`

`seeded_at`은 PRD·README의 기준값 2026-09-03을 유지한다. 이 명령은 Source·pipeline Metadata·Bronze·`warehouse.duckdb`를 지운다. 그다음 Seed, 9개 Table Baseline 수집, Catalog Sync를 실행한다. `mart_publish_runs`는 지우지 않는다. 이력으로 남긴다.

검증:
- `orders` 99,441행.
- Watermark는 Table별 Cursor 컬럼의 최대값이다(아래 정정 참조).
- `generator_runs`는 0행.

> **정정 (2026-09-29, developer 질의):** 처음 적은 "모든 Watermark가 `2026-09-03T00:00:00Z`"는 architect의 오기다. Watermark는 `src/ingestion/tables.py`의 Table별 `cursor_timestamp_column` 최대값이다.
>
> - `customers`와 `order_items`는 `updated_at`이 없는 append-only Table이다. Cursor가 `created_at`이다. Seed에서 `created_at`은 Olist 사건 시각이다. 따라서 Watermark는 2018년 값이다(`customers` 2018-10-17, `order_items` 2018-09-03).
> - `updated_at` Cursor Table은 `seeded_at` 값인 09-03이다.
> - 비어 있는 `customer_subscriptions`·`subscription_payments`는 NULL이다.
>
> 이 결과를 정상 Baseline으로 인정한다. 이 결과는 1단계 검사 기준도 바로잡는다. 검사 기준은 `updated_at`이 아니라 수집 Cursor여야 한다.

### 4. BI 증적용 Generator 재실행 (CLI)

paused DAG에 수동 Run을 넣지 않는다. `python -m src.generator`로 실행한다. `logical_date`는 전부 과거이고 엄격히 증가한다. 모두 `--orders 10`이다.

| seed | logical_date (UTC) | profile |
| ---- | ------------------ | ------- |
| 42 | 2026-09-04T00:00:00Z | subscription-active |
| 43 | 2026-09-05T00:00:00Z | subscription-active |
| 44 | 2026-09-06T00:00:00Z | subscription-active |
| 45 | 2026-09-07T00:00:00Z | subscription-payment-failed |
| 46 | 2026-09-08T00:00:00Z | subscription-cancel-requested |
| 47 | 2026-09-09T00:00:00Z | default |
| 48 | 2026-09-10T00:00:00Z | subscription-active |

기존 실행과 profile 순서, 1일 간격은 같다. 다른 점은 날짜를 모두 과거로 옮긴 것 하나다. 이 변경에는 대가가 있다. 기존 10-27·10-28 실행은 30일 구독 주기(`SUBSCRIPTION_PERIOD`)를 넘겨 자동 청구 결제 1건을 만들었다. 새 배치에서는 그 결제가 지금 생기지 않는다. 약 2026-10-04 이후 매시간 실행의 만료 스캔이 실제 시각 흐름에 따라 만든다. 이 차이는 BI 증적 문서에 적는다.

검증:
- `commerce_source`에서 `updated_at > now()`인 행이 0행이다.
- 7회 모두 `SUCCESS`이고 `reused_successful_run=false`다.

### 5. Warehouse 수동 실행

`warehouse_pipeline_dag`를 수동으로 1회 트리거한다.

검증:
- 18개 Task가 모두 success다.
- orders `rows_extracted`가 70이다.
- `fct_order`가 99,511행이다.
- Mart 주문 최대 일자가 2026-09-10이다.
- `publish_status=PUBLISHED`이고 `serving_export_status=SUCCESS`다.

### 6. BI 증적 재생성

`scripts/capture_metabase_dashboards.py`로 다시 캡처한다. `docs/bi/evidence.md`·`dashboards.md`의 2026-10-28 기준 서술을 새 Run·Export ID와 2026-09-10 기준으로 바꾼다. 자동 청구 결제가 아직 없다는 점과 그 이유를 적는다. 빈 차트가 없는지 확인한다(card 54, 59 포함).

### 7. Unpause와 첫 정시 실행 확인

`source_simulation_dag`를 unpause 한다. `catchup=False`이므로 최신 구간 1건만 생성된다.

첫 예약 실행에서 확인할 항목:
- Generator `orders_inserted=1`.
- 트리거된 Warehouse의 orders `rows_extracted=1`이고 status는 `SUCCESS`다.
- `fct_order`가 99,512행이다.
- `changed_relations`에 `fct_order`가 들어 있다.
- `serving_row_counts`가 직전 실행보다 증가했다.

다음 정시 실행 1회를 더 확인한다. 99,513행이면 완료다.

### 완료 기준

1~7 검증이 모두 통과하면 048을 종결한다. 테스트 명령 출력, 백업 경로와 파일 목록, 각 Run ID를 architect에 보고한다.

## 보충 판정 — 4단계 seed47 실패 (2026-09-29)

### 증상

4단계의 09-09 seed47 `default` 실행이 `ValueError(Billing cycle and attempt sequences must start at one)`로 실패했다. 후속 실행은 중지됐다.

### 원인

구독을 `PAYMENT_FAILED`로 만드는 경로는 둘이다. 두 경로의 결과가 서로 다르다.

- **만료 스캔 청구 경로** (`service.py` `_bill_and_transition`): 실패한 결제 행(회차 N, 시도 1)을 먼저 쓴다. 그 다음 상태를 `PAYMENT_FAILED`로 바꾼다.
- **Profile 전이 경로** (`subscription-payment-failed`, `customers.py` `_transition_subscription_record`): 상태만 `PAYMENT_FAILED`로 바꾼다. 결제 행을 쓰지 않는다. `next_payment_attempt_at`은 전이 시각 + 2일이다.

재시도 경로(`service.py:353`)는 "실패한 회차가 이미 있다"고 가정한다. 회차를 `next_billing_cycle_sequence - 1`로 계산한다. 09-07 Profile 전이로 생긴 구독에는 결제 행이 없다. 따라서 09-09 재시도의 회차는 0이 된다.

이 결함은 원래 잠재해 있었다. 이전 BI 증적 순서에서는 10-27 실행이 유예 종료 뒤였다. 그래서 재시도보다 이탈(churn)이 먼저 실행됐다. `@hourly` 운영에서도 `subscription-payment-failed` Profile 실행 2일 뒤에 같은 실패가 난다. 실행 순서와 무관한 결함이다.

### 판정

**Generator를 고친다. Profile 전이 경로가 스캔 청구 경로와 같은 결제 행을 남기게 한다.**

기각 1 — "최초 재시도 회차 1 허용" 증상 보정. PRD grain은 "실패 후 재시도는 같은 청구 회차에서 `attempt_sequence`만 증가"다. 재시도는 반드시 `attempt_sequence >= 2`여야 한다. 실패 행이 없는 회차를 재시도로 기록하면 grain 의미가 깨진다. 결제 실패 시각에 해당하는 행도 Source에 없게 된다.

기각 2 — Profile 순서 변경. 결함은 순서와 무관하다. 순서를 바꾸면 이번 재구성만 피하고 `@hourly` 운영에서는 다시 실패한다.

### 조치 (developer)

1. Profile 전이가 `PAYMENT_FAILED`로 바뀔 때 같은 Source 트랜잭션에서 실패 결제 행 1건을 쓴다. 값은 다음과 같다.
   - `billing_cycle_sequence` = `next_billing_cycle_sequence(connection, subscription_id)`
   - `attempt_sequence` = 1
   - `payment_status` = `failed`, `failure_code` = `DECLINED`. 이 값은 강제한다. `_billing_outcome` Hash로 판정하지 않는다. Profile의 목적이 결제 실패이기 때문이다.
   - `billing_period_start_at` = 전이 전 Record의 `billing_due_at`, 없으면 `logical_date`. 스캔 경로와 같은 규칙이다.
   - `payment_at`, `created_at`, `updated_at` = `logical_date`. 수정 A Guard를 그대로 통과한다.
   - `result_counts["subscription_payments_inserted"]`와 논리 행 기록에 반영한다.
2. `plan_subscription_payment`에 상태 강제 인자를 추가하는 방식과 전용 함수를 두는 방식 중 하나를 고른다. 선택은 developer에 맡긴다. `payment_id`와 `provider_payment_id`의 결정적 규칙은 그대로 쓴다.
3. 재시도 경로(`service.py:353`)의 회차 계산은 바꾸지 않는다.
4. 테스트를 추가한다.
   - (e) `subscription-payment-failed` Profile 실행 뒤 해당 구독에 실패 결제 행(회차 N, 시도 1)이 1건 있다.
   - (f) 이어서 `logical_date`를 +2일 이상으로 올려 실행하면 재시도 행이 같은 회차 N, 시도 2로 생긴다. 오류가 없다.
   - 기존 Generator 테스트와 수정 A 테스트 (a)~(d)가 모두 통과한다.

### 복구 순서

1. seed47 실패 실행이 롤백됐는지 확인한다. 09-09 시각의 Source 행이 0건이어야 한다. seed47의 SUCCESS `generator_runs` 행이 없어야 한다.
2. 위 조치 1~4를 적용한다. `uv run pytest tests/generator`와 `ruff`를 통과시킨다.
3. 3단계 재구성을 다시 실행한다. 현재 Source에는 09-07 Profile로 생긴 결제 행 없는 `PAYMENT_FAILED` 구독이 남아 있다. 이 행은 새 코드로 고칠 수 없다. 백업은 이미 있으므로 다시 뜨지 않는다.
4. 4단계 seed 42~48을 처음부터 다시 실행한다. 09-07 seed45 실행 결과에 실패 결제 행 1건이 있는지 확인한다. 09-09 seed47에 재시도 행(시도 2)이 있거나, 재시도가 없더라도 실패 없이 끝나는지 확인한다.
5. 5~7단계를 원래 판정대로 진행한다. 5단계의 기대 Mart 수치는 주문 기준이므로 바뀌지 않는다.

### 범위 밖 (기록만)

`subscription-active` Profile이 `PAYMENT_FAILED`에서 `ACTIVE`로 되돌릴 때도 성공 결제 행을 쓰지 않는다. 이 경로는 실패하지 않는다. 다음 정규 청구가 `max + 1` 회차로 진행되기 때문이다. 다만 실패만 있는 회차에서 구독이 회복된 것처럼 보인다. 이번 048 범위에서는 고치지 않는다.

## 최종 검수 (2026-09-29)

**최종 판정: 승인. 048을 종결한다.** 정시 실행 검증은 사용자 지시로 16:00 UTC 1건만 확인했다. 17:00 UTC 실행은 기다리지 않는다.

| 단계 | 결과 |
| ---- | ---- |
| 1. 수정 A | `_assert_source_cursor_forward`는 재사용 경로 다음, Source 트랜잭션 안에서 첫 쓰기 전에 실행된다. Table별 `TABLE_CONFIGS` Cursor 컬럼을 쓴다. `SourceCursorRegressionError`는 `SOURCE_CONTRACT_ERROR`로 분류된다. |
| 보충 수정 | `subscription-payment-failed` Profile 전이가 실패 결제 행(회차 1, 시도 1, `failed`/`DECLINED`)을 같은 트랜잭션에서 쓴다. `plan_subscription_payment`에 `forced_status` 인자를 추가했다. |
| 테스트 | `uv run pytest tests/generator`: 52 passed. `ruff check src tests` 통과. |
| 2. 백업 | `data/generated/backups/2026-09-29-pre-rebaseline/`: DB dump 3건, SeaweedFS Manifest, DuckDB 2건, 재구성 dry-run·confirm 결과 JSON, Generator 실행 기록 JSONL. |
| 3. 재구성 | 재실행 완료. Seed 기준 orders 99,441행. 구독 0행. |
| 4. Generator | seed 42~48(09-04~09-10) 7건 성공. Source 결제 행: 09-07 회차 1 시도 1 `failed`, 09-09 회차 1 시도 2 `completed`. |
| 5. Warehouse | logical_date 09-11 실행. 9개 Table 성공. orders `rows_extracted=70`. `fct_order` 99,511행. |
| 6. BI | Publish Run `cef6b673-dc20-49bd-8f40-d615b2146796`, Export `a9ffd423-4e3d-4e20-9362-c2f4d6fb2679`로 Dashboard 2·3·4 재캡처. 합계 대조 차이 0. |
| 7. 정시 실행 | 16:00 UTC Generator Run `9053d2f2-4797-43fa-b912-37304eb287ad` 성공, `orders_inserted=1`. Warehouse `manual__2026-09-28T16:00:00+00:00` 성공. 9개 Table 모두 성공, orders `rows_extracted=1`. Publish Run `21c6334e-9ab1-484b-b1df-7106919495cf` `PUBLISHED`, dbt test 141 통과. `fct_order` 99,511 → 99,512. |

### 잔여

- `docs/bi/evidence.md`와 `docs/bi/totals-reconciliation.md`의 Git Commit 칸이 "048 복구 작업 중(미커밋)"이다. 커밋 후 해시로 채운다.
- card 54의 `payment_failed_count`는 0이다. Warehouse를 Generator 7회 뒤 한 번만 실행해 중간 `PAYMENT_FAILED` 상태가 구독 차원 이력에 없다. 실패 결제 자체는 `facts.fct_subscription_payment`에 있다. `docs/bi/evidence.md`에 기록돼 있다.
- `subscription-active` Profile의 `PAYMENT_FAILED` → `ACTIVE` 복귀는 결제 행을 쓰지 않는다. 범위 밖으로 기록만 한다.
- PRD 4.1에 "새 합성 행의 Cursor는 기존 최대값보다 커야 한다" 규칙 줄을 추가할지는 lead가 결정한다.
- 백업은 lead가 삭제를 지시할 때까지 보존한다.
- 코드와 문서 변경은 아직 커밋되지 않았다.
