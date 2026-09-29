# 019. Metabase의 Serving 파일 교체 가시성

## Status

Accepted — 구현 Gate 통과 (2026-09-29). ADR-012를 보완한다.

## Context

ADR-012는 `data/serving/mart.duckdb`를 `os.replace`로 교체하고 Metabase가 `/serving`을 읽기 전용으로 읽게 했다. 012는 "열린 Connection은 재연결 전까지 이전 Export를 볼 수 있다"고 적었다. 그러나 실제로는 재연결해도 이전 Export가 보인다.

- DuckDB JDBC `1.5.5`는 `jdbc_instance_cache`(기본 사용)로 같은 경로 문자열의 Connection에 프로세스 전역 Instance 하나를 공유한다. 이 Instance는 처음 연 파일 Descriptor(inode)를 유지한다.
- Metabase `sql-jdbc` C3P0 Pool은 최소 Connection을 유지하고 `maxIdleTime`이 3시간이다. 그래서 Instance가 해제되지 않는다.
- 2026-09-29 카드 47이 교체 전 값(99,540)을 반환했고 Metabase를 재시작해야 99,541이 됐다. PRD §17 "Metabase는 마지막 성공 Mart만 읽는다"를 위반했다. 원인과 재현은 `docs/architect-review/049_metabase-serving-stale-connection.md`에 있다.

## Decision

1. Export는 교체 직전 Build 파일에 `data/serving/exports/{export_id}.duckdb` Hardlink를 추가한다. `mart.duckdb`와 같은 inode를 가리키므로 디스크를 추가로 쓰지 않는다. `mart.duckdb`는 정본 경로로 유지한다.
2. `export_serving_mart` 다음 Task `repoint_metabase_serving`이 Metabase API로 Serving Database의 `details.database_file`을 `/serving/exports/{export_id}.duckdb`로 바꾼다. 그다음 Metabase 경유로 `serving_manifest.export_id`를 읽어 새 Export와 일치하는지 검증한다. 경로 문자열이 달라지므로 Instance Cache에 걸리지 않는다. details hash가 바뀌므로 Pool도 다시 만들어진다.
3. **불변식: Metabase details가 가리킬 수 있는 파일은 삭제하지 않는다.** `exports/` 정리(최신 3개 보존)는 재지정 결과가 `SUCCESS`이거나 설정이 없는 `SKIPPED`일 때만 한다.
4. 재지정은 `METABASE_URL`, `METABASE_API_KEY`, `METABASE_SERVING_DATABASE_ID`가 모두 있을 때만 켜진다. 기본값이 빈 `METABASE_SERVING_DATABASE_ID`가 Opt-in 스위치다. API Key는 `X-API-Key` Header에만 쓴다.

| 상황 | 상태 | Task | `exports/` 정리 |
| --- | --- | --- | --- |
| 설정 값 중 하나라도 없음 | `SKIPPED` | 성공 | 최신 3개 보존 |
| 설정은 있으나 접속 불능, HTTP 오류, Manifest 불일치 | `FAILED` | `METABASE_REPOINT_FAILED`로 실패(기본 재시도) | 하지 않음 |
| 재지정과 Manifest 검증 통과 | `SUCCESS` | 성공 | 최신 3개 보존 |

Run Summary의 `metabase_repoint_status`는 Serving Export가 없으면 `NOT_REQUESTED`다. Export 뒤 Task가 실패하면 `FAILED`다.

## Alternatives

| 대안 | 판단 |
| --- | --- |
| 경로는 두고 다른 details만 변경 | 같은 경로라 옛 Connection이 남으면 Instance Cache에 다시 걸려 기각 |
| Publish 뒤 Metabase 수동 재시작 | 사람이 기억할 때만 해소되고 서비스가 중단돼 기각. 재지정을 끈 환경의 운영 절차로만 남긴다. |
| Airflow가 docker.sock으로 컨테이너 재시작 | Host root 수준 권한이 필요해 기각 |
| `jdbc_instance_cache=false`와 Pool 조정 | Pool Connection이 각자 옛 inode를 유지하고, idle 시간은 Env로 바꿀 수 없어 기각 |
| 같은 inode에 In-place 쓰기 | 읽기 전용 Instance의 파일 Lock과 충돌하고 원자성을 잃어 기각 |
| 재지정 실패 시 details를 정본 경로로 되돌림 | 되돌리기도 같은 원인으로 실패하고 stale로 돌아가 기각 |
| 정리할 때 Metabase 참조 경로 제외 | 접속할 수 없으면 참조 경로를 알 수 없어 기각 |
| PostgreSQL Serving DB | Driver와 Lock Gate를 통과했으므로 전환 조건이 아니다 |

## Consequences

- Airflow가 Metabase API에 의존한다. `bi` Profile을 끈 채 설정을 남기면 매 실행이 `FAILED`가 된다. Metabase를 쓰지 않으면 `METABASE_SERVING_DATABASE_ID`를 비운다.
- **재지정을 끄기 전에 Metabase details의 `database_file`을 `/serving/mart.duckdb`로 되돌린다.** 그러지 않으면 `SKIPPED` 정리가 Metabase가 가리키는 파일을 지울 수 있다.
- 재지정이 계속 실패하면 `exports/`가 정리되지 않고 쌓인다. 파일이 쌓이는 것은 의도한 동작이다. Task 실패로 드러난다.
- Compose Mount는 디렉터리 Bind(`./data/serving:/serving:ro`)를 유지한다. 파일 Bind로 바꾸면 Mount 시점 inode가 고정된다. 저장소는 WSL ext4에 둔다.

## Validation

2026-09-29 Run `manual__2026-09-29T07:20:00+00:00`에서 확인했다.

- `repoint_metabase_serving`과 Summary가 `SUCCESS`였다. 카드 47의 Dashboard API는 Metabase 재시작 없이 새 Export의 99,545건을 반환했다(이전 99,544).
- Field ID 81·116과 Dashboard `parameter_mappings`가 유지됐다.
- 같은 Export로 연속 재지정 2번이 모두 `SUCCESS`였고 결과가 같았다.
- Metabase 재시작 뒤에도 `/serving/exports/5ae35bc1-02c7-4214-bf05-1def820b2dab.duckdb`와 99,545건이 유지됐다.
- `exports/`에는 파일 3개가 있었다. 최신 파일은 `mart.duckdb`와 같은 inode였다.
- Test: 재지정 `FAILED` 상태로 Export 4번 뒤 4개가 보존됐다(삭제 0건). `SUCCESS`와 `SKIPPED`에서는 3개가 보존됐다. 설정이 있을 때 접속 불능이면 `FAILED`와 Task 실패였고, Summary 매핑도 규칙대로였다. `pytest tests/serving tests/test_airflow_dags.py`는 15 passed, 5 skipped(opt-in)였다.
