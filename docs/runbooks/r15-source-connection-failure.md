# R-15 Source 연결 실패

## 문제

`open_table_snapshot`이 Snapshot을 여는 시점에 Source 연결이 끊기면, Object를 만들지 않고
Watermark를 전진시키지 않은 채 재시도 가능한 상태로 끝나야 한다. `orders` DAG는 `catchup`을 쓰지
않으므로 이 실패의 유일한 복구 경로는 다음 Schedule이나 수동 재시도지 과거 구간 자동 채움이 아니다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r11_r15_reprocess_and_source.py -k r15 -v`를 실행한다.
`tests/reliability/faults.py`의 `fail_source_connection`으로 `PostgresSettings.source_connection`을
`psycopg.OperationalError`를 던지는 함수로 바꾼 뒤 `ingest_orders`를 한 번 호출하고, 같은 `batch_id`로
결함 없이 즉시 재시도한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| 연결 실패 시점 | `OperationalError`가 그대로 전파되고 `classify_error`가 `SOURCE_CONNECTION_ERROR`로 분류 | 일치 |
| 연결 실패 직후 | `bronze_objects` 0건, `pipeline_runs` 1건(`FAILED`/`SOURCE_CONNECTION_ERROR`), `watermarks` 값 실패 전과 동일 | 일치 |
| 즉시 재시도 | 같은 `batch_id`로 `SUCCESS`, Object Key는 실패 없이 성공했을 때와 동일 | 일치 |

## 원인과 불변 조건

`open_table_snapshot`은 `with settings.source_connection() as connection: ...`에서 연결을 여는데, 이
호출은 `ingest_table`의 `PipelineRun(...)`/`record_started_run`보다 앞선다(`src/ingestion/service.py`).
`ingest_table`은 이 `with`문 전체를 감싸는 `except`에서 `run_started` 플래그로 `record_started_run`
실행 여부를 확인해, 아직 실행되지 않았으면 `_record_prestart_failure`(Lease 획득 실패와 공유하는 경로)로
`extract_upper_bound=None`인 `PipelineRun`을 시작·`FAILED` 종료 상태로 기록한다. Watermark를 전진시키는
유일한 경로인 `commit_table_run`은 호출되지 않으므로 Watermark는 실패 전 값 그대로 남는다. 지켜야 하는
불변 조건은 "Object가 없으면 Watermark도 움직이지 않는다"다 — 부분적으로 Object만 있거나 Watermark만
전진한 상태는 나타나면 안 된다.

**알려진 한계:** 수정 완료(Task 13A, [ADR-018](../adr/018-ingestion-error-type-classification.md)).
`pipeline_runs`에 `FAILED`/`SOURCE_CONNECTION_ERROR` 행이 남아 AC-13 집계에 잡힌다.

`classify_error`는 `psycopg.OperationalError`를 `SOURCE_CONNECTION_ERROR`로 분류하고, 이는
`RETRYABLE_ERROR_TYPES`에 속해 `is_retryable`이 참을 반환한다. 이 분류가 재시도 여부를 결정하는
유일한 근거이므로, 이 경로가 깨지면 재시도 가능한 실패가 영구 실패로 오분류될 수 있다.

## 복구 절차

별도 개입이 필요 없다. 같은 `batch_id`(같은 `dag_id`/`logical_date`)로 결함 없는 환경에서 재시도하면
`watermark_before`가 실패 이전 값 그대로이므로 정상적으로 이어서 수집한다. Orphan Object나 Lease
잔존이 없는지만 `bronze_objects`/`watermarks`로 확인한다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r15", ...)`로 남긴 증적에서
`classified_error == "SOURCE_CONNECTION_ERROR"`, `is_retryable == true`,
`object_count_after_failure == 0`, `pipeline_run_count_after_failure == 1`,
`pipeline_run_status_after_failure == "FAILED"`,
`pipeline_run_error_type_after_failure == "SOURCE_CONNECTION_ERROR"`,
`watermark_before_failure == watermark_after_failure`, `retry_status == "SUCCESS"`를 확인한다.
