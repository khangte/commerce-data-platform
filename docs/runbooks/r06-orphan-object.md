# R-06 Orphan Object

## 문제

검증된 Object가 Metadata Commit 없이 남았을 때 수용 가능한 Orphan만 자동 재조정해야 한다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r06 -v`를 실행한다.

## 기대/실제 관측

Reject 0건 Object는 같은 Object Key로 COMMITTED가 되며, Quarantine Object가 있으면 자동 재조정을 거부한다.

## 원인과 불변 조건

Quarantine가 있으면 유실 여부를 안전하게 추론할 수 없으므로 Watermark와 Metadata를 변경하면 안 된다.

## 복구 절차

Orphan 후보의 `batch_id`, `run_id`, `table_batch_id`와 Manifest를 확인하고, 거부된 경우 수동 조치한다.

## 재검증 명령과 결과

Evidence `r06`에서 수용 상태와 Quarantine 거부 상태를 확인한다.
