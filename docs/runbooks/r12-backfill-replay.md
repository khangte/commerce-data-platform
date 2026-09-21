# R-12 Backfill Replay

## 문제

`bronze_as_of` 경계로 과거 시점을 다시 Build하는 Replay가 Source를 다시 읽지 않고도, 그 시점의
Full Refresh와 값·Hash까지 똑같이 재현하는지 확인한다. 재현이 깨지면 과거 시점 감사·재현이 불가능해진다.

## 재현 조건과 명령

`RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability/test_r11_r15_reprocess_and_source.py -v`를 실행한다.
첫 Batch를 수집하고 그 시점을 `--full-refresh`로 Build해 Control Hash를 남긴 뒤(`pipeline_name`은
`test_r12_*`), 둘째(지연) Batch를 마저 수집한다. 첫 Build의 `control.bronze_files.committed_at`
최댓값을 `bronze_as_of` 경계로 삼아 `--full-refresh --vars '{"bronze_as_of": ...}'`로 Replay Build를
실행한다.

## 기대/실제 관측

| 시점 | 기대 | 실제 |
|---|---|---|
| Replay Build 직전/직후 | `pipeline_runs` 행 수 불변 (dbt build는 `ingest_table`을 호출하지 않는다) | 일치 |
| Replay Build 결과 | 첫 Batch만의 Full Refresh Control과 Mart Logical Hash 전부 동일 | 일치 |
| `control.dbt_replay_boundary` | 매 Build마다 한 행씩 기록되고, `bronze_as_of` 있는 행의 `object_count`는 첫 Batch Object 수와 같다 | 일치 (`test_as_of_replay_reproduces_the_build_of_that_moment` 기존 검증) |

## 원인과 불변 조건

`bronze_as_of`는 `committed_at <= bronze_as_of`로 Bronze Catalog를 위에서 잘라내는 Upper Bound
전용 경계다(`docs/architecture/08-bronze-replay-and-reextract-boundary.md` D-1). 둘째 Batch가 이미
Bronze에 Commit돼 있어도 경계 밖이라 Staging Model이 읽지 않는다. Replay는 `dbt build`만 실행하고
Source Connection을 열지 않으므로 Source Read는 항상 0건이다 — 이 Test가 `pipeline_runs` 행 수
불변으로 그 사실을 증거로 남긴다. `bronze_as_of`를 준 Build는 `--full-refresh`가 강제되고
(`REPLAY_BOUNDARY_ERROR`), `advance_processed_batch_watermark`가 건너뛰어 경계 Build가 "현재
상태를 처리했다"로 잘못 기록되지 않는다.

## 복구 절차

Replay는 읽기 전용 재현이라 복구 대상이 아니다. 특정 과거 시점의 Mart가 필요하면 그 시점
`committed_at`을 `bronze_as_of`로 주고 `--full-refresh` Build를 실행하면 된다. 별도 Backfill DAG나
Source 재접속이 필요 없다.

## 재검증 명령과 결과

위 pytest 명령이 통과하는지 확인한다. `write_evidence("r12", ...)` 증적에서
`mart_hashes_match_control == true`, `pipeline_run_count_before_replay_build ==
pipeline_run_count_after_replay_build`를 확인한다.
