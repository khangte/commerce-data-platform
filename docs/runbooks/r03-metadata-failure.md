# R-03 Metadata 실패

## 문제

검증된 Bronze Object 뒤 Metadata Commit이 중단되거나 처리된 실패로 끝날 수 있다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r03 -v`를 실행한다.

## 기대/실제 관측

Crash는 `run_id`가 RUNNING인 Orphan을 같은 Object Key로 COMMITTED 복구한다. Handled 실패는 `batch_id`의 Object를 Orphan으로 남기고 자동 복구를 거부한다.

## 원인과 불변 조건

Watermark와 `table_batch_id` Catalog는 Commit 전까지 변하지 않는다. 처리된 실패는 중단 증거가 없으므로 자동 재조정하지 않는다.

## 복구 절차

Crash는 `batch_id`, `run_id`, `table_batch_id`와 Manifest를 확인한 뒤 Orphan 재조정을 실행한다. Handled 실패는 Object·Manifest·Quarantine·Watermark를 확인하고 새 명시 Batch로 재수집한다.

## 재검증 명령과 결과

R-03 증적의 Object Key, Watermark 전후값, Orphan 목록과 오류 메시지를 확인하고 위 pytest 명령이 통과하는지 검증한다.
