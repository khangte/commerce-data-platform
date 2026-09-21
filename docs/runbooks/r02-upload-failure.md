# R-02 업로드 실패

## 문제

Bronze Object를 SeaweedFS S3 API에 올리는 도중 실패하면, Watermark와 `bronze_objects`가 전혀
바뀌지 않고 같은 `batch_id`로 재시도하면 정상 복구돼야 한다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r02 -v`를 실행한다.
`tests/reliability/faults.py`의 `fail_object_upload`로 첫 번째 Object 업로드 호출에서
`botocore.exceptions.ClientError`(`Error.Code=ServiceUnavailable`, `ResponseMetadata.HTTPStatusCode=503`)를
주입한 뒤, 같은 `request`(`batch_id`)로 재시도한다.

## 기대/실제 관측

| 구분 | 기대 | 실제 |
|---|---|---|
| 실패 직후 `bronze_objects` | 해당 `table_batch_id` 행 없음(0건) | 일치 |
| 실패 직후 Watermark | 업로드 전과 동일 | 일치 |
| `pipeline_runs` | `("FAILED", "OBJECT_STORAGE_ERROR")`, `is_retryable == True` | 일치 |
| 같은 `batch_id`로 재시도 | `SUCCESS` | 일치 |

## 원인과 불변 조건

업로드는 Bronze Object를 SeaweedFS에 쓰는 단계이고, `commit_table_run`(Watermark 전진·Metadata
COMMITTED 전환)은 업로드 성공 이후에만 실행된다. 업로드 단계에서 예외가 나면 `commit_table_run`
자체가 호출되지 않으므로 Metadata·Watermark는 원래 상태로 남는다. `classify_error`는
`botocore.exceptions.ClientError`/`BotoCoreError`를 `OBJECT_STORAGE_ERROR`로 분류하고,
`is_retryable`은 이 코드일 때만 예외적으로 HTTP 상태(5xx/429)를 직접 검사해 재시도 가부를
정한다(503은 재시도 가능). 근거는 [ADR-018](../adr/018-ingestion-error-type-classification.md)을
따른다.

## 복구 절차

실패 Run은 `record_failed_run`으로 이미 FAILED 처리돼 있다. `bronze_objects`에 해당
`table_batch_id` 행이 없고 Watermark가 업로드 전과 같은지 확인한 뒤, 같은 `batch_id`로 재시도한다.
HTTP 상태가 4xx(`is_retryable == False`)라면 재시도 전에 권한·설정 오류부터 고친다.

## 재검증 명령과 결과

R-02 증적의 `stored_error_type == "OBJECT_STORAGE_ERROR"`, `http_status == 503`,
`is_retryable == true`, `watermark_before == watermark_after_failure`를 확인하고 위 pytest 명령이
통과하는지 검증한다.
