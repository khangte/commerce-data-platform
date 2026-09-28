# R-11 누락된 Schedule

## 문제

누락 Schedule은 데이터 손실을 만들지 않는다. `extract_upper_bound`가 항상 Source의 실제 최대
Cursor이고(`_fetch_upper_bound`, `src/ingestion/extract.py`) Watermark가 연속이라, 건너뛴 창의
Row는 다음 실행이 함께 쓸어 담는다. 유일한 실제 영향은 그 Row가 건너뛴 창이 아니라 다음 실행의
`ingestion_date`/`batch_id` Partition에 귀속된다는 것이다. `source_simulation_dag`(`@hourly`)와
`warehouse_pipeline_dag`(Source 성공 시 트리거) 모두 `catchup=False`이므로 누락은 오직 다음
정기 실행이나 명시 Batch로만 회복된다.

2026-09-28부터 `source_simulation_dag`는 매시간 예약 실행되며 두 DAG 모두 `catchup=False`다.
Scheduler 누락으로 Source 예약 실행이 빠지면 해당 구간의 Source 변경 자체가 발생하지 않는다.
Source는 성공했으나 Warehouse가 실행되지 못한 경우에는 다음 Warehouse 실행이 누락된 변경을
함께 수집한다. 두 경우 모두 기존 R-11의 지연 수집·복구 계약으로 설명되므로 새 시나리오는
필요하지 않다([architect-review 047](../architect-review/047_source-dag-hourly-r11-premise.md)).

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability/test_r11_r15_reprocess_and_source.py -k r11 -v`를 실행한다.
윈도 A 실행 → 윈도 B 데이터만 삽입하고 Ingest 생략 → 윈도 C 데이터 삽입 후 실행 → Mart Hash를
건너뛴 적 없는 Full Refresh Control과 비교 → `rewind_tables`로 B 직전까지 되감고 B의 `logical_date`로
명시 Batch 실행 순서로 진행한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| C 실행 | `rows_extracted`가 B 구간 Row 포함, `watermark_after` == Source 최대 Cursor, Gap 없음 | 일치 |
| C 실행 직후 Mart | 건너뛴 적 없는 Full Refresh Control과 Hash 동일(회복 조치 전) | 일치 |
| C 실행 직후 Bronze | B 구간 Row가 `ingestion_date={C의 logical_date}`/`batch_id={C의 batch_id}` 아래 위치 | 일치 |
| 회복(명시 Batch) 후 | Mart Hash 여전히 Control과 동일(멱등), B 구간 Row는 `ingestion_date={B의 logical_date}` 아래로 이동 | 일치 |

## 원인과 불변 조건

Watermark를 앞으로 미는 경로는 `commit_table_run`뿐이고, 그것도 실제로 Commit된 Object의
`watermark_after`로만 민다. `rewind_watermark`는 뒤로만 가고 `record_success_no_data_run`은 Watermark를
건드리지 않는다. 즉 "Watermark가 아직 수집 안 한 데이터보다 앞서 있는 상태"는 시스템이 스스로
도달할 수 없다 — 이 시스템에서 누락 Schedule은 Gap이 아니라 다음 실행이 항상 함께 쓸어 담는
지연 수집이다.

Bronze Object Key는 `bronze/{source_table}/ingestion_date={logical_date.date()}/batch_id={batch_id}`로
만들어진다(`src/ingestion/service.py`). Partition Key는 데이터의 실제 시각이 아니라 그 Run의
`logical_date`이므로, 건너뛴 창의 Row는 그것을 대신 쓸어 담은 Run의 Partition에 귀속된다. 하지만
`_batch_id`/`_ingested_at`은 `stg_orders.sql` 등 Staging Model에서 중복 제거 정렬 키로만 쓰이고
필터로 쓰이지 않으므로 Mart는 영향을 받지 않는다 — Mart 수준 Self-healing은 완전하다.

되감기(`rewind_tables`) 뒤 재수집의 Upper Bound는 항상 Source 최대값이라, 되감은 지점부터 현재까지가
통째로 다시 쓸려 온다. 특정 과거 창만 격리해 재수집할 수단은 없다(부수 발견, 필요해지면 별도 판단).
지금은 문제가 아니다 — Mart가 멱등이고 Watermark 단조성이 유지되므로 정합성은 깨지지 않는다.

## 복구 절차

귀속을 되돌려야 할 이유가 있을 때만 수행한다(예: Partition 단위 감사·재처리 요구).
`rewind_tables(settings, source_tables=(대상 Table,), boundary=B의 시작 시각, pipeline_name=...)`로
B 직전 Cursor까지 되감은 뒤, B의 `logical_date`로 명시 Batch(`ingest_table`)를 실행한다. 재수집이
C 구간까지 함께 쓸어 담는 것은 정상이며 막을 필요가 없다 — 두 번째로 Watermark를 좁게 고정하지 않는다.
복구 후에는 B 구간 Row가 B의 `ingestion_date` Partition으로 옮겨졌는지, Mart Hash가 회복 전과 여전히
같은지 확인한다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r11", ...)` 증적에서
`mart_hashes_equal_control_before_recovery == true`, `mart_hashes_equal_control_after_recovery == true`,
`run_c_object_key`에 C의 `ingestion_date`/`batch_id`가, `recovery_object_key`에 B의 `ingestion_date`가
포함되는지 확인한다.
