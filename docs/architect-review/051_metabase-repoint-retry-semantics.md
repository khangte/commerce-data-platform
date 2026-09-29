# 051. Metabase 재지정 실패의 재시도 의미

- 일자: 2026-09-29
- 대상: 050 반영분(미커밋). `airflow/dags/warehouse_pipeline_dag.py:301`, `src/serving/metabase_repoint.py`
- 요청: reviewer 재리뷰의 설계 판단 요청 1건과 사소한 지적 1건
- 판정: **수정 필요.** 대안 (b)를 채택한다. 050 상태 규칙의 모순은 architect 설계 오류다.

## 지적 요지

050은 두 가지를 동시에 지시했다.

1. `FAILED`면 `AirflowFailException`을 던진다.
2. `retries=0`을 지우고 DAG 기본 재시도(`retries=2`, 60초부터 지수 증가)로 일시적 접속 불능을 흡수한다.

그런데 `AirflowFailException`은 "Raise when the task should be failed without retrying."이다. reviewer가 airflow-dag-processor 컨테이너에서 확인했다. 따라서 재시도가 한 번도 일어나지 않는다. Metabase 재시작 1~2분 사이에 run이 겹치면 바로 실패한다. 050이 의도한 흡수가 동작하지 않는다. 지적이 타당하다.

## 대안 판정

| 대안 | 판정 | 근거 |
| --- | --- | --- |
| (a) 일시 오류(접속 불능, 5xx)는 재시도, 영구 오류(4xx, Manifest 불일치)는 즉시 실패 | 기각 | 실패 종류를 반환하도록 분류 코드를 새로 만들어야 한다. 얻는 것은 영구 오류를 몇 분 일찍 확정하는 것뿐이다. 재지정은 같은 경로를 PUT하므로 멱등이다. 영구 오류를 재시도해도 부작용이 없다. |
| (b) 모두 재시도 가능한 예외로 던짐 | **채택** | 분기가 없다. 기본 재시도 간격(60초, 120초)이 약 3분을 덮으므로 Metabase 재시작 구간을 흡수한다. 영구 오류는 마지막 시도 뒤 `FAILED`로 확정된다. |
| (c) 재시도 없이 즉시 실패 | 기각 | Metabase 재시작과 겹칠 때마다 사람이 개입해야 한다. |

재시도 대기 중인 Task는 종결 상태가 아니다. 그래서 `publish_run_summary`(`all_done`)는 재시도가 끝날 때까지 기다린다. Summary 매핑(최종 실패면 `FAILED`)은 바꾸지 않는다. 정리 규칙(`SUCCESS`·`SKIPPED`일 때만)도 그대로다. 재시도할 때마다 재지정과 정리를 처음부터 다시 수행하는데, 둘 다 멱등이다.

## 수정 지시(developer)

1. `warehouse_pipeline_dag.py:301`에서 `FAILED`일 때 `AirflowFailException` 대신 재시도 가능한 예외(`AirflowException`)를 던진다. 메시지는 `METABASE_REPOINT_FAILED: …`로 유지한다. Airflow 3 SDK의 import 경로는 컨테이너에서 확인한다.
2. `metabase_repoint.py`의 예외 로그를 원인별로 나눈다.
   - `ValueError`, `TypeError`, `KeyError`: "설정 또는 응답 형식 오류"
   - `TimeoutError`, `OSError`, `HTTPException`(`IncompleteRead` 포함): "요청 실패(Timeout 또는 연결 끊김)"
   - `URLError`: 기존 "server unreachable"
   - `HTTPError`: 기존 HTTP 코드 출력

   두 번째 `try` 블록도 같은 기준으로 나눈다. 반환값은 모두 `FAILED` 그대로다.
3. Test:
   - `FAILED`일 때 던지는 예외가 정확히 `AirflowException` 타입인지 확인한다. `AirflowFailException`은 `AirflowException`의 하위 클래스이므로 `isinstance`로 확인하면 안 된다.
   - DAG의 `repoint_metabase_serving` Task `retries`가 2인지 확인한다.

## 문서 영향

ADR-019 Decision 표의 "`METABASE_REPOINT_FAILED`로 실패(기본 재시도)"는 이 수정으로 사실이 된다. ADR은 고치지 않는다. 050의 "`AirflowFailException`을 던진다"는 이 판정으로 대체한다.
