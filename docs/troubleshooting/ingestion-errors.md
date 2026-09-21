# Ingestion 오류 유형

`src/ingestion/service.py`의 `ingest_table`이 낼 수 있는 Error Type과 예외를 원인·조치와 함께
정리한다. 모든 값은 `classify_error`(`src/ingestion/errors.py`)를 거쳐 `pipeline_runs.error_type`에
남는다 — Python 예외 클래스 이름이 그대로 저장되는 경로는 없다([ADR-018](../adr/018-ingestion-error-type-classification.md)).

| Error Type | 발생 지점 | 원인 | 조치 |
| --- | --- | --- | --- |
| `SOURCE_CONNECTION_ERROR` | `open_table_snapshot` 연결 열기(`_record_prestart_failure`) | Source DB 연결 실패(`psycopg.OperationalError`/`ConnectionError`/`TimeoutError`) | 재시도 가능(`is_retryable == True`). Object/Watermark는 전혀 바뀌지 않으므로 같은 `batch_id`로 즉시 재시도한다 |
| `LEASE_UNAVAILABLE` | Lease 획득(`_record_prestart_failure`) | `LeaseUnavailableError`(원천 동시성 잠금)·`TableLeaseUnavailableError`(테이블별 수집 잠금)·`PublishInProgressError`(Publish 잠금) — 다른 활성 실행이 잠금을 쥐고 있음 | 재시도 가능. 잠금 보유자가 끝날 때까지 기다린 뒤 재시도한다. 어느 잠금이었는지는 `error_message`로 확인한다 |
| `LEASE_OWNERSHIP_LOST` | Lease 획득 또는 `commit_table_run`(`_record_prestart_failure`/`_record_failure_without_masking`) | `LeaseOwnershipLostError`·`TableLeaseOwnershipLostError` — 처리 중 잠금 소유권을 잃음(만료 후 다른 실행이 선점) | 재시도 불가능. 이 실행은 폐기하고, 다음 Schedule 또는 수동 실행이 새 잠금으로 처음부터 다시 수집한다 |
| `OBJECT_STORAGE_ERROR` | `_upload_and_verify_bronze`(`_record_failure_without_masking`) | `botocore.exceptions.ClientError`/`BotoCoreError` — SeaweedFS S3 API 실패 | 재시도 가부는 HTTP 상태로 갈린다 — `is_retryable`이 5xx/429일 때만 `True`. Object/Watermark는 바뀌지 않으므로 재시도 가능하면 같은 `batch_id`로 즉시 재시도하고, 4xx(권한·설정 오류)면 원인부터 고친다 |
| `WATERMARK_CONFLICT` | `commit_table_run`(`_record_failure_without_masking`) | Watermark Version CAS 경쟁에서 패배 | 재시도 불가능(이 시도 기준). 승자의 Commit이 이미 반영됐는지 확인 후 다음 범위로 진행한다 |
| `SOURCE_CONTRACT_ERROR` | Schema/Batch 검증(`_record_failure_without_masking`) | Source Schema 또는 Batch 계약 위반 | 재시도 불가능. Source 쪽 계약 위반을 수정한 뒤 재실행한다 |
| `VALIDATION_THRESHOLD_EXCEEDED` | `assert_reject_rate`(`_record_failure_without_masking`) | 검증 실패(Reject) 비율이 임계값 초과 | 재시도 불가능. Quarantine된 Row를 확인해 Source 데이터 품질을 고친 뒤 재실행한다 |
| `OBJECT_VERIFICATION_ERROR` | Bronze Object 검증(`_record_failure_without_masking`) | 업로드된 Object의 Checksum/Row Count가 기대와 불일치 | 재시도 불가능. Orphan 후보로 남을 수 있으니 자동 재조정 대상인지 `find_orphan_candidates`로 확인한다 |
| `BATCH_IDENTITY_CONFLICT` | `_reuse_or_reject_committed_batch` | 동일 Batch Identity로 다른 범위를 재실행 시도 | 재시도 불가능. 의도한 범위인지 확인 후 새 `batch_id`로 실행한다 |
| `UNKNOWN_ERROR` | 분류 규칙에 없는 예외(`_record_failure_without_masking`/`_record_prestart_failure`) | 예: `RuntimeError`(Metadata Commit 직전 주입, R-03), `FileExistsError`(Object Key 중복, `storage.py`의 412 처리) | 예외 메시지로 원인을 확인한다. Object 중복(`FileExistsError`)은 별개 관심사로 이 문서에서 전용 코드를 만들지 않는다(ADR-018) |

## 관측 부재였던 R-15의 수정

이전에는 `open_table_snapshot`의 연결 열기가 `record_started_run`보다 먼저 실행되는데 그 구간을
감싸는 `except`가 없어, 연결 실패 시 `pipeline_runs`에 행이 아예 남지 않았다([019](../architect-review/019_task9-r15-r11-verification.md)).
지금은 `ingest_table`이 `run_started` 플래그로 `record_started_run` 실행 여부를 추적해, 실행되지
않았으면 `_record_prestart_failure`(Lease 획득 실패와 공유하는 경로)가 `extract_upper_bound=None`인
`PipelineRun`을 시작·`FAILED` 종료 상태로 기록한다. 자세한 내용은 ADR-018과
[R-15 Runbook](../runbooks/r15-source-connection-failure.md)을 참고한다.
