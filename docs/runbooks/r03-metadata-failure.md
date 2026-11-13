# R-03 Metadata 실패

## 문제

검증된 Bronze Object 뒤 Metadata Commit이 중단되거나 처리된 실패로 끝날 수 있다.

## 재현 조건과 명령

`COMMERCE_SOURCE_DB=commerce_source_it PIPELINE_METADATA_DB=pipeline_metadata_it SEAWEEDFS_BUCKET=commerce-lake-it RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r01_r07_ingestion_commit.py -k r03 -v`를 실행한다.

## 기대/실제 관측

Crash는 `run_id`가 `RUNNING`인 고아 객체를 같은 Object Key로 `COMMITTED` 복구한다. 처리된 실패는 고아 객체의 자동 재조정을 거부하지만, 같은 `batch_id` 재시도는 테이블 잠금 아래에서 메타데이터가 없는 자기 객체를 지우고 원천에서 다시 추출·업로드해 복구한다([ADR 023](../adr/023-same-batch-retry-clears-uncommitted-objects.md)). Crash 뒤에도 같은 배치 재시도가 오면 재조정 없이 다시 수집한다.

R-03 테스트는 처리된 실패 직후 재조정 거부와 카탈로그 0건을 확인한 뒤, 같은 요청 재시도의 `SUCCESS`·`COMMITTED` 1건·워터마크 전진·고아 후보 제거를 검증한다. 2026-11-13 격리 환경의 R-01~R-07 10건이 모두 통과했다. 처리된 실패 증적 `data/reliability/r03/20261113T034854794449Z.json`에서 `retry_status=SUCCESS`, `retry_catalog_status=COMMITTED`, 재시도 후 고아 후보 0건을 확인했다.

## 원인과 불변 조건

Watermark와 `table_batch_id` Catalog는 Commit 전까지 변하지 않는다. 처리된 실패는 중단 증거가 없으므로 자동 재조정하지 않는다. 남은 Object를 채택하지 않는다는 뜻이며, 같은 Batch 재시도가 Metadata에 기록 없는 자기 Object를 지우고 원천에서 다시 뽑는 것은 허용한다(ADR 023). Commit된 Object는 지우지 않는다.

## 복구 절차

Crash는 `batch_id`, `run_id`, `table_batch_id`와 Manifest를 확인한 뒤 Orphan 재조정을 실행한다. Handled 실패는 같은 `batch_id` 재시도로 복구한다(ADR 023). 재시도가 소진돼 Object가 남았으면 Object·Manifest·Quarantine·Watermark를 확인하고 Orphan 재조정을 시도하거나 새 명시 Batch로 재수집한다. Orphan 재조정 명령은 [R-06](r06-orphan-object.md)을 따른다.

## 재검증 명령과 결과

R-03 증적의 Object Key, Watermark 전후값, Orphan 목록과 오류 메시지를 확인하고 위 pytest 명령이 통과하는지 검증한다.


2026-11-13 실제 관측: `batch_id=r03-handled-7d33290f364c4434840481d39ab881ee__20260920T000000Z`는 첫 실패가 `UNKNOWN_ERROR`로 기록되고 고아 후보 1건으로 남았다. 재조정은 `Only a crash-interrupted RUNNING pipeline run can be reconciled`로 거부됐으며, 같은 요청 재시도 뒤에는 `SUCCESS`·`COMMITTED`, 고아 후보 0건이다. 워터마크는 `2026-11-13T02:02:00+00:00`에서 `2026-11-13T03:41:59.138711+00:00`로 전진했다. Crash 분기의 기존 재조정도 `data/reliability/r03/20261113T034853915767Z.json`으로 재검증했다.
