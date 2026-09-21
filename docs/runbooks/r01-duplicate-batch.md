# R-01 중복 Batch

## 문제

같은 `batch_id`를 반복 실행했을 때 Bronze Object 또는 Watermark가 중복 Commit되면 안 된다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r01 -v`를 실행한다.

## 기대/실제 관측

첫 실행은 SUCCESS, 두 번째와 세 번째 실행은 SKIPPED_ALREADY_COMMITTED이며 같은 Object Key를 재사용한다.

## 원인과 불변 조건

표준 `batch_id`의 Commit Object와 Watermark는 한 번만 생성·전진해야 한다.

## 복구 절차

실패 시 같은 `batch_id`의 Run 상태와 `table_batch_id`를 확인한 후 정상 재실행한다.

## 재검증 명령과 결과

Evidence `r01`의 `batch_id`와 세 `run_id`로 Object Key·Watermark가 동일한지 확인한다.
