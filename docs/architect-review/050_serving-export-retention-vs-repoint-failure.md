# 050. Serving Export 보존 정리와 Metabase 재지정 실패의 결합

- 일자: 2026-09-29
- 대상: 049 권고안 A 구현(미커밋). `src/serving/export.py`, `src/serving/metabase_repoint.py`, `airflow/dags/warehouse_pipeline_dag.py`
- 요청: reviewer의 설계 판단 요청 1건
- 판정: **수정 필요.** 대안 (c)를 채택하고 상태 규칙을 보완한다.

## 지적 요지

`export_serving_mart`는 재지정 결과와 관계없이 `exports/`에 최신 3개만 남긴다. 재지정이 실패하거나 Skip되면 Metabase details는 이전 Export 경로를 계속 가리킨다. 이 상태로 Export가 3번 더 쌓이면 그 파일이 unlink된다. 열린 fd는 계속 읽힌다. 그러나 Metabase 재시작이나 Pool 재생성 뒤에는 모든 Serving 카드가 존재하지 않는 파일을 연다.

049의 "unlink는 안전하다"는 **열린 fd에만** 성립한다. Metabase가 다시 열어야 하는 경로에는 성립하지 않는다. 지적이 타당하다. 설계에서 빠진 부분이다.

## 불변식

> Metabase details가 가리킬 수 있는 파일은 삭제하지 않는다.

파이프라인이 이 불변식을 증명할 수 있는 시점은 재지정 `SUCCESS`를 확인한 직후뿐이다. 그때 Metabase는 최신 Export를 가리키고, 최신 Export는 항상 보존 대상 3개 안에 든다.

## 대안 판정

| 대안 | 판정 | 근거 |
| --- | --- | --- |
| (a) 정리에서 Metabase 참조 경로 제외 | 기각 | 참조 경로를 알려면 Metabase에 접속해야 한다. 접속할 수 없으면 경로를 모르므로 결국 정리를 건너뛰어야 한다. 그러면 (c)와 같은 결과다. Export 단계에 Metabase 결합만 하나 더 생긴다. |
| (b) 실패 시 details를 `/serving/mart.duckdb`로 되돌림 | 기각 | 되돌리기도 같은 원인(접속 불능, Key 만료)으로 실패한다. 따라서 불변식을 보장하지 못한다. 되돌리면 049가 없애려던 stale 상태로 돌아간다. Metabase 쓰기 경로도 하나 더 생긴다. |
| (c) 정리를 재지정 `SUCCESS` 뒤로 이동 | **채택** | 불변식을 증명할 수 있는 유일한 시점에만 삭제한다. 실패하면 파일이 쌓인다. 파일이 쌓이는 것은 안전하고 눈에 보인다. 삭제되는 것은 조용히 깨진다. |

## 상태 규칙

재지정 활성 여부는 `METABASE_URL`, `METABASE_API_KEY`, `METABASE_SERVING_DATABASE_ID` 세 값이 **모두** 있는지로 정한다. `METABASE_URL`에는 Compose 기본값이 있다. 따라서 실제 Opt-in 스위치는 기본값이 빈 `METABASE_SERVING_DATABASE_ID`다.

| 상황 | 상태 | Task | `exports/` 정리 |
| --- | --- | --- | --- |
| 세 값 중 하나라도 비어 있음 | `SKIPPED` | 성공 | 최신 3개만 보존 |
| 설정은 있으나 접속 불능(URLError) | `FAILED` | **실패** | 하지 않음 |
| HTTP 오류(Key 만료, PUT 실패 등) | `FAILED` | **실패** | 하지 않음 |
| Manifest `export_id` 불일치 | `FAILED` | **실패** | 하지 않음 |
| 재지정과 Manifest 검증 통과 | `SUCCESS` | 성공 | 최신 3개만 보존 |

- **설정은 있는데 접속할 수 없으면 `FAILED`다.** 설정했다는 것은 파이프라인이 Metabase 연결을 관리하겠다는 의사 표시다. 이때 Skip으로 처리하면 stale 상태와 정리 중단이 둘 다 조용히 지나간다. `bi` Profile을 끈 채 운영하려면 `METABASE_SERVING_DATABASE_ID`를 비운다.
- `SKIPPED`일 때 정리하는 전제는 "파이프라인이 관리하지 않는 Metabase는 정본 `/serving/mart.duckdb`를 가리킨다"이다. 재지정을 운영하다 끄려면 먼저 Metabase details를 `/serving/mart.duckdb`로 되돌려야 한다. 이 절차를 `.env.example`과 ADR-019에 명시한다.
- `FAILED`는 문자열 반환이 아니라 Task 실패로 드러나야 한다. 지금 구현은 `FAILED`를 반환해도 Task가 초록색이다. 기존 `export_serving_mart`와 같은 방식으로 `AirflowFailException("METABASE_REPOINT_FAILED: …")`를 던진다.
- 재지정은 같은 경로를 PUT하므로 멱등이다. 일시적 접속 불능(Metabase 재시작 중 등)을 흡수하도록 `retries=0` 대신 DAG 기본 재시도를 쓴다.

## 수정 지시(developer)

1. `export_serving_mart`의 `exports/` 보존 정리(`src/serving/export.py:98-101`)를 뺀다. Hardlink 생성과 교체, fsync는 그대로 둔다.
2. 보존 정리를 별도 함수(예: `prune_serving_exports(versions_dir, keep=3)`)로 옮긴다. 이 함수는 재지정 Task가 상태가 `SUCCESS` 또는 `SKIPPED`일 때만 호출한다.
3. `repoint_metabase_serving`은 세 값이 모두 있을 때 접속 불능이면 `FAILED`를 반환한다. `SKIPPED`는 설정이 없을 때만 쓴다.
4. DAG Task는 `FAILED`이면 정리하지 않고 `AirflowFailException`을 던진다. `retries=0`을 지우고 DAG 기본 재시도를 따른다.
5. `publish_run_summary`의 `metabase_repoint or "SKIPPED"`를 고친다. Serving Export가 없으면 `NOT_REQUESTED`다. Export는 있는데 재지정 결과가 None(Task 실패)이면 `FAILED`다.
6. `.env.example`의 `METABASE_SERVING_DATABASE_ID` 옆에 두 가지를 적는다. Opt-in 스위치라는 점, 끌 때 Metabase details를 정본 경로로 되돌려야 한다는 점이다.
7. Test를 추가한다.
   - 재지정 `FAILED` 상태로 Export를 4번 하면 `exports/`에 4개가 남는다(삭제 0건).
   - `SUCCESS`와 `SKIPPED`에서는 3개만 남는다.
   - 설정이 있을 때 접속 불능이면 `FAILED`와 Task 실패가 된다.
   - Summary 매핑이 위 규칙대로 나온다.

## 후속

ADR-019에는 위 불변식, 상태 규칙, 비활성화 절차를 포함한다. 049 권고안의 "성공한 뒤에는 최근 3개만 남긴다"는 이 판정으로 대체한다.
