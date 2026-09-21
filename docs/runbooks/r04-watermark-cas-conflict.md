# R-04 Watermark CAS 충돌

## 문제

같은 `pipeline_name`/`source_table`의 두 Run이 같은 기준 Watermark로 동시에 Commit을 시도하면
먼저 도착한 Run만 Watermark를 전진시켜야 하고, 나중 Run은 Object와 성공 상태 없이 실패해야 한다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r04 -v`를 실행한다.
두 `run_id`가 같은 초기 Watermark(`version=0`)를 `expected_watermark`로 놓고 순서대로 `commit_table_run`을 호출한다.

## 기대/실제 관측

| 구분 | 기대 | 실제 |
|---|---|---|
| 승자 `run_id` | `bronze_objects` COMMITTED, `pipeline_runs` SUCCESS | 일치 |
| 패자 `run_id` | `WatermarkConflictError`, Object 미생성, Run은 RUNNING → 이후 FAILED | 일치 |
| 최종 Watermark | 승자의 `watermark_after`와 동일 | 일치 |

## 원인과 불변 조건

`commit_table_run`은 Object 삽입·Run 성공 전환·Watermark 전진을 한 Transaction으로 묶는다.
Watermark UPDATE는 `version = %s AND watermark_timestamp/keys IS NOT DISTINCT FROM %s` 조건의
CAS이므로, 승자가 `version`을 올린 뒤 패자의 UPDATE는 `rowcount == 0`이 되어 Transaction 전체가
롤백된다. Object insert와 Run 성공 전환도 같은 Transaction 안에 있으므로 함께 되돌아간다.

## 복구 절차

패자 `run_id`는 `record_failed_run`으로 FAILED 처리한다(이 시나리오에서는 `error_type=WATERMARK_CONFLICT`).
Watermark와 `bronze_objects`에 패자의 `table_batch_id` 흔적이 없는지 확인한 뒤, 새 `batch_id`로 재수집한다.
재수집은 승자가 남긴 최신 Watermark를 기준으로 자동 진행된다.

## 재검증 명령과 결과

R-04 증적의 `timeline`에서 승자 `run_id`가 `COMMITTED`, 패자 `run_id`가 `WATERMARK_CONFLICT`인지,
`watermark_final`이 승자 `winner_object_key`가 가리키는 Batch의 상한과 같은지 확인하고 위 pytest 명령이
통과하는지 검증한다.
