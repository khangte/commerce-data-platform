# R-06 Orphan Object

## 문제

검증된 Object가 Metadata Commit 없이 남았을 때 수용 가능한 Orphan만 자동 재조정해야 한다.

## 재현 조건과 명령

`COMMERCE_SOURCE_DB=commerce_source_it PIPELINE_METADATA_DB=pipeline_metadata_it SEAWEEDFS_BUCKET=commerce-lake-it RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r06 -v`를 실행한다.

## 기대/실제 관측

Reject 0건 Object는 같은 Object Key로 COMMITTED가 되며, Quarantine Object가 있으면 자동 재조정을 거부한다.

## 원인과 불변 조건

Quarantine가 있으면 유실 여부를 안전하게 추론할 수 없으므로 Watermark와 Metadata를 변경하면 안 된다.

## 복구 절차

Orphan 후보의 `batch_id`, `run_id`, `table_batch_id`와 Manifest를 확인하고, 거부된 경우 수동 조치한다.

실행 명령(인자 없이 실행하면 읽기 전용 후보 목록을 JSON으로 출력한다):

```bash
uv run python -m src.ingestion.bronze.orphan                           # 후보 목록
uv run python -m src.ingestion.bronze.orphan --reconcile <object_key>  # 한 건 재조정, 거부되면 0이 아닌 종료 코드
```

같은 `batch_id` 재시도가 오면 Orphan은 재시도가 지우고 다시 수집한다([ADR 023](../adr/023-same-batch-retry-clears-uncommitted-objects.md)). 이 절차는 재시도가 소진됐거나 오지 않은 Orphan에 쓴다.

## 재검증 명령과 결과

Evidence `r06`에서 수용 상태와 Quarantine 거부 상태를 확인한다. R-06 테스트는 CLI 목록에 후보가 나타나는지, `--reconcile` 수용 시 종료 코드 0과 `COMMITTED` 출력, Quarantine 거부 시 종료 코드 1을 함께 검사한다. 없는 후보를 지정하면 종료 코드 2다. CLI 단위 테스트는 `uv run pytest tests/ingestion/test_orphan.py -q`로 실행한다.


2026-11-13 격리 환경 실제 관측: R-01~R-07 10 passed(6.15초). R-06 테스트는 CLI 목록에 수용 후보가 나타난 뒤 `--reconcile`이 종료 코드 0과 `COMMITTED`를 출력하는지 확인했다. Quarantine 후보는 종료 코드 1과 `Orphans with rejects require manual reconciliation`을 출력했고 고아 후보·워터마크는 그대로였다. 증적 `data/reliability/r06/20261113T034851139833Z.json`의 수용 배치는 `r06-accepted-31b0659e91b94d7295814f199630bbd5__20260920T000000Z`, 거부 배치는 `r06-rejected-6b99a9ebbbf44e0ea38a7a8853f14732__20260920T000000Z`다. 수용 배치의 고아 후보는 1건에서 0건으로 바뀌었다.
