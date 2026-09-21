# R-13 Re-extract

## 문제

특정 Table의 Watermark를 되감고 Source를 다시 읽는 명시 재수집이, 기존에 이미 COMMITTED된
Bronze Object를 건드리지 않으면서 새 식별자로 남는지 확인한다. `bronze_objects` 스키마에는
`reprocess_id` 칼럼이 없다 — 새 식별자는 새 `batch_id`/`dag_id`로 남는다
(`docs/architecture/08-bronze-replay-and-reextract-boundary.md` D-5).

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 uv run pytest tests/reliability/test_r11_r15_reprocess_and_source.py -v`를 실행한다.
`subscription_payments`에 결제 하나를 넣고 원본 수집(`sequence=1`)을 한 뒤, `rewind_tables`로 그
결제 직전까지 되감고 새 `sequence=2`로 재수집한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| 되감기 직후 | 우리 결제보다 앞선 실재 Row가 없어 Watermark가 최초 상태(`NULL`, `[]`)로 돌아간다 | 일치 |
| 재수집 직후 | 새 `batch_id`로 `pipeline_runs` 행이 하나 더 생기고(`총 2건`), Row Count가 원본과 같다 | 일치 |
| 재수집 직후 원본 Object | `object_key`/`row_count`/`logical_hash`/`status`가 재수집 전과 완전히 동일 | 일치 |

## 원인과 불변 조건

Bronze Object Key는 `batch_id`(=`dag_id`+`logical_date` 조합)로 갈라지므로, 되감기 뒤 새
`sequence`로 수집하면 원본과 다른 `table_batch_id`가 만들어져 원본 행을 덮어쓰지 않는다.
`rewind_tables`는 Watermark만 되돌리는 Metadata 전용 연산이고 실제 추출은 별도 `ingest_table`
호출에서 일어난다(`src/ingestion/reprocess.py`). 재수집의 Upper Bound는 항상 Source 최댓값이라
되감은 지점부터 현재까지가 통째로 다시 들어온다 — 특정 과거 창만 격리해 재수집하는 수단은 없다
(018 부수 발견, 필요해지면 별도 판단).

**알려진 한계:** 재수집은 Replay가 아니다. Source의 현재 상태가 과거 Snapshot과 다를 수 있으므로
(수정·삭제·추가 발생), 재수집 결과가 그 시점 Full Refresh와 Hash까지 같다는 보장이 없다 — 정의상
Replay와 Hash 동일성을 약속하지 않는다. 시점 재현이 목적이면 R-12의 Replay(`bronze_as_of`)가
기본 경로이고, Re-extract는 Source 최신 상태를 다시 끌어와야 할 때만 쓴다.

같은 Row가 서로 다른 `batch_id`로 두 번 Bronze에 올라도 Mart는 바뀌지 않는다. Staging Model이
`_ingested_at`/`_batch_id` 순서로 중복 제거하기 때문이다. 이 Runbook은 그 성질을 다시 증명하지
않는다 — 증거는 R-11 회복 단계의 `post_recovery_hashes == control_hashes`다
(`docs/runbooks/r11-missing-schedule.md`).

## 복구 절차

이 시나리오 자체가 복구 절차다. 대상 Table의 결제/Row 직전 시각을 `boundary`로
`rewind_tables(settings, source_tables=(대상 Table,), boundary=..., pipeline_name=...)`를 실행한 뒤,
새 `dag_id`/`sequence`로 `ingest_table`을 호출해 재수집한다. 재수집 후 원본 `bronze_objects` 행이
`COMMITTED` 상태로 그대로인지, 새 행의 `batch_id`가 원본과 다른지 확인한다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r13", ...)` 증적에서
`original_object_unchanged_after_re_extract == true`, `pipeline_run_count == 2`,
`re_extract_batch_id`가 `original_batch_id`와 다른지 확인한다.
