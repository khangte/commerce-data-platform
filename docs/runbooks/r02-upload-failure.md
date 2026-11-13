# R-02 업로드 실패

## 문제

Bronze Object를 SeaweedFS S3 API에 올리는 도중 실패하면, Watermark와 `bronze_objects`가 전혀
바뀌지 않고 같은 `batch_id`로 재시도하면 정상 복구돼야 한다.

## 재현 조건과 명령

`COMMERCE_SOURCE_DB=commerce_source_it PIPELINE_METADATA_DB=pipeline_metadata_it SEAWEEDFS_BUCKET=commerce-lake-it RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r02 -v`를 실행한다.
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
| 일부 Object가 올라간 뒤 실패(Quarantine 뒤 Bronze, `data.parquet` 뒤 Manifest)하고 같은 `batch_id`로 재시도 | `SUCCESS` ([ADR 023](../adr/023-same-batch-retry-clears-uncommitted-objects.md)) | 일치. 격리 환경 통합 테스트에서 두 경우 모두 같은 요청 재시도 `SUCCESS`, `COMMITTED` 1행, 워터마크 전진 확인 |

## 원인과 불변 조건

업로드는 Bronze Object를 SeaweedFS에 쓰는 단계이고, `commit_table_run`(Watermark 전진·Metadata
COMMITTED 전환)은 업로드 성공 이후에만 실행된다. 업로드 단계에서 예외가 나면 `commit_table_run`
자체가 호출되지 않으므로 Metadata·Watermark는 원래 상태로 남는다. `classify_error`는
`botocore.exceptions.ClientError`/`BotoCoreError`를 `OBJECT_STORAGE_ERROR`로 분류하고,
`is_retryable`은 이 코드일 때만 예외적으로 HTTP 상태(5xx/429)를 직접 검사해 재시도 가부를
정한다(503은 재시도 가능). 근거는 [ADR-018](../adr/018-ingestion-error-type-classification.md)을
따른다.

일부 Object가 이미 올라간 경우, 재시도는 Table Lease를 쥔 상태에서 `bronze_objects`·`quarantine_batches`에
자기 `table_batch_id`·`object_key` 행이 없음을 확인하고 그 Batch 경로의 Object 네 개(Bronze `data.parquet`·
`manifest.json`, Quarantine `records.parquet`·`manifest.json`)를 지운 뒤 다시 추출·Upload한다. Commit된
Object는 지우지 않는다. 근거는 [ADR 023](../adr/023-same-batch-retry-clears-uncommitted-objects.md)이다.

## 복구 절차

실패 Run은 `record_failed_run`으로 이미 FAILED 처리돼 있다. `bronze_objects`에 해당
`table_batch_id` 행이 없고 Watermark가 업로드 전과 같은지 확인한 뒤, 같은 `batch_id`로 재시도한다.
HTTP 상태가 4xx(`is_retryable == False`)라면 재시도 전에 권한·설정 오류부터 고친다.

## 재검증 명령과 결과

R-02 증적의 `stored_error_type == "OBJECT_STORAGE_ERROR"`, `http_status == 503`,
`is_retryable == true`, `watermark_before == watermark_after_failure`를 확인하고 위 pytest 명령이
통과하는지 검증한다.


2026-11-13 격리 환경 재검증: `tests/integration` 전체 125 passed / 1 skipped(403.71초), R-01~R-07 10 passed(6.15초). 부분 업로드 재시도 두 건과 커밋 실패 재시도·커밋 객체 보존 시나리오가 통과했다. R-02 증적 `data/reliability/r02/20261113T034853349061Z.json`의 `batch_id=r02-fc3967d4c4504b7690bc23c2007aba21__20260920T000000Z`에서 `OBJECT_STORAGE_ERROR`, HTTP 503, `is_retryable=true`, 실패 전후 동일 워터마크를 확인했다.


### 실제 Airflow 재시도 관측 (2026-11-13)

기존 Airflow 스케줄러·실행기에서 임시 진단 DAG `audit_retry_probe`의 단일 태스크 `manifest_retry`를 실행했다. `manual__2026-11-13T03:50:39.214680+00:00`은 최종 `success`, 태스크는 `try_number=2`로 성공했다. 같은 `ingest_table`을 호출하되 설정을 `commerce_source_it`·`pipeline_metadata_it`·`commerce-lake-it`으로 고정해 공유 데이터를 보호했다.

첫 시도는 `data.parquet`의 HEAD 존재를 확인한 뒤 Manifest PUT에 HTTP 503을 주입해 `FAILED`·`OBJECT_STORAGE_ERROR`로 끝났다. 두 번째 실제 스케줄러 재시도는 같은 `batch_id=audit_retry_probe__20261113T035039Z`의 미확정 `data.parquet`를 삭제한 로그를 남기고 `SUCCESS`로 끝났으며, `COMMITTED`는 정확히 1행(99,881행 수집)이었다. 실행 증거 묶음의 `airflow-retry-proof.json`, `airflow-attempt-1.log`, `airflow-attempt-2.log`, 임시 DAG 원본 `audit_retry_probe.py`에 각각 메타데이터·시도별 로그·주입 코드가 남아 있다.

이는 실제 Airflow 재시도와 같은 수집 함수를 검증한 결과다. 정규 `warehouse_pipeline_dag` 전체 처리·동적 매핑·후속 dbt 단계까지 재실행한 증거는 아니다. 신뢰성·Airflow 검증 전후 공유 원천과 실제 워터마크·Bronze 메타데이터 585행·저장 객체 1,177개 스냅샷은 완전히 일치했다.
