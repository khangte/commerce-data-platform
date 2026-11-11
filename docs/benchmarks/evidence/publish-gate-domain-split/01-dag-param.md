# DAG Param 노출 (Check 1)

- 수집 시각: 2026-11-11T07:41Z (UTC)
- 환경: branch `feat/publish-gate-split` (HEAD 1d687ac, 브랜치 코드 수정 없이 실행), compose profile `airflow`, Airflow 3.3.1 LocalExecutor, dbt-core 1.12.3, dbt-duckdb 1.11.0
- 목적: DAG가 import 오류 없이 로드되고 `partial_publish` Param이 UI/trigger conf에 노출되는지 확인

## 판정: PASS

- import 오류 없음, `partial_publish`가 기본값 `true`의 boolean Param으로 노출된다.
- trigger conf로 `{"partial_publish": false}`를 넘기면 그 run의 conf에 그대로 남는다(03 시나리오).

## Import 오류

명령: `docker compose exec -T airflow-scheduler airflow dags list-import-errors`

```
No data found
```

## DAG Params (UI Trigger 폼이 그리는 스키마와 같다)

명령: `docker compose exec -T airflow-api-server python /tmp/af.py GET /api/v2/dags/warehouse_pipeline_dag/details` → `dag_id, is_paused, params`만 추출

```json
{
  "dag_id": "warehouse_pipeline_dag",
  "is_paused": false,
  "params": {
    "full_refresh": {
      "value": false,
      "schema": {
        "type": "boolean"
      },
      "description": "dbt build에 --full-refresh를 전달한다.",
      "source": null
    },
    "partial_publish": {
      "value": true,
      "schema": {
        "type": "boolean"
      },
      "description": "귀속 가능한 dbt Test 실패 때 해당 도메인만 보류하고 게시한다.",
      "source": null
    }
  }
}
```

## Trigger conf 반영

명령: `docker compose exec -T airflow-api-server python /tmp/af.py GET /api/v2/dags/warehouse_pipeline_dag/dagRuns/<run>`

```json
{"dag_run_id": "verify_partial_on", "run_type": "manual", "state": "success", "conf": {}, "logical_date": "2026-11-11T06:16:39Z", "start_date": "2026-11-11T06:28:23.672078Z", "end_date": "2026-11-11T06:31:30.230656Z", "triggered_by": "rest_api"}
{"dag_run_id": "verify_partial_off", "run_type": "manual", "state": "success", "conf": {"partial_publish": false}, "logical_date": "2026-11-11T06:32:25Z", "start_date": "2026-11-11T06:32:28.718724Z", "end_date": "2026-11-11T06:48:25.915898Z", "triggered_by": "rest_api"}
{"dag_run_id": "verify_partial_recover", "run_type": "manual", "state": "success", "conf": {}, "logical_date": "2026-11-11T06:48:46Z", "start_date": "2026-11-11T06:48:48.456706Z", "end_date": "2026-11-11T07:35:08.310285Z", "triggered_by": "rest_api"}
```
