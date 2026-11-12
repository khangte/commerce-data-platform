# 게시 SLO

> 기준: `src/observability/run_report.py`의 `judge_run_report`. 이 문서는 코드에 이미 있는 판정을 한 표로 모은 것이고 새 임계값을 만들지 않는다.
> 관련: [ADR 021](../adr/021-split-publish-gate-by-domain.md), [R-14 Runbook](../runbooks/r14-dbt-failure.md)

`warehouse_pipeline_dag` 실행이 끝날 때마다 Run report 한 건이 `warehouse_run_reports`에 남는다. 아래 조건을 모두 만족하면 `ok=true`다.

| 무엇을 지키나 | 측정값 (Run report 필드) | 조건 | 어기면 뜨는 판정 |
| ---- | ---- | ---- | ---- |
| 주문 데이터 지연 | `data_lag_s` = 지금 − 게시본 `fct_order`의 `max(purchase_at)` | `stale_after_s` 이하 | `DATA_STALE` |
| 도메인별 갱신 지연 | `domain_lag_s[d]` = 지금 − 도메인 `d`가 마지막으로 보류 없이 게시된 시각(`domain_refreshed_at[d]`) | `stale_after_s` 이하 | `DATA_STALE:<domain>` |
| 전 도메인 게시 | `held_domains` | 비어 있음 | `PUBLISH_PARTIAL:<domain>` |
| 게시 성공 | `publish_status` | `PUBLISHED` | `PUBLISH_FAILED` 등 |
| 실행 시간 | `dag_elapsed_s` | 600초 미만 (`RUN_LIMIT_SECONDS`) | `SLOW_RUN` |
| 행 수 유지 | Relation별 행 수 | 직전 게시 대비 1% 넘게 줄지 않음(Full refresh 제외) | `ROW_COUNT_DROP:<relation>` |
| 지표 Test | `tests_warned` | 0 | `DBT_TEST_WARN` |
| Dashboard가 최신 게시를 봄 | `fresh` | `false`가 아님 | `NOT_FRESH` |

`stale_after_s`는 원천 DAG 주기의 2배다(`STALE_AFTER_INTERVALS = 2`). 기본 주기 `@hourly`에서는 7,200초다.

## 도메인별 지연을 게시 시각으로 재는 이유

구독 결제는 드물다. 2026-11-12 게시본의 `fct_subscription_payment`는 9행이고 결제일 사이가 몇 주씩 빈다. 결제 시각으로 재면 보류가 없어도 항상 임계값을 넘는다. 그래서 "그 도메인이 마지막으로 정상 게시된 지 얼마나 됐나"를 잰다. 보류가 원천 주기 2회를 넘겨 이어지면 `DATA_STALE:<domain>`이 뜬다.

- 사건 시각 최댓값은 `domain_as_of`에 참고로 싣고 판정에 쓰지 않는다.
- 연속 보류 횟수는 `consecutive_holds`에 있다. 정상 게시 한 번이면 0으로 돌아간다.
- 마지막 비보류 게시를 찾지 못하면(이력 없음, 사슬 1,000건 초과) 값이 `null`이고 판정하지 않는다.
- 같은 값이 Serving 파일의 `serving_domain_status`(`domain`, `as_of`, `refreshed_at`, `held`, `consecutive_holds`)에 있다. Sales·Product·Customer Dashboard 맨 위의 `도메인 게시 상태` 표(card 60)가 이 Table을 보여 준다([BI 문서](../bi/dashboards.md)). 표는 임계값 판정을 하지 않는다.

## Task 실패로 드러나는 판정

DAG의 마지막 Task `domain_publish_alert`는 Run report를 기록한 뒤 아래 판정이 하나라도 있으면 실패한다. DagRun 상태가 `failed`가 되고 메시지 `DOMAIN_PUBLISH_ALERT: <판정 코드>`가 남는다. 게시·Serving 내보내기·Metabase 재지정이 끝난 뒤에 돌아 게시 결과는 바꾸지 않는다.

| 판정 | 뜻 |
| ---- | ---- |
| `PUBLISH_PARTIAL:<domain>` | 그 도메인이 보류된 부분 게시 |
| `DATA_STALE:<domain>` | 그 도메인의 보류가 원천 주기 2회를 넘겨 이어짐 |
| `PUBLISH_FAILED` | 전체 차단 |
| `ROW_COUNT_DROP:<relation>` | 게시된 Relation의 행 수가 직전 게시보다 1% 넘게 줄어듦. dbt Test가 모두 통과하고 게시도 정상인 채 뜬다 |

나머지 판정은 `ok=false`로 기록만 한다. `DATA_STALE`(주문 사건 시각)은 상시 운영이 아니면 늘 뜨고, `DBT_TEST_WARN`은 경고다. `SERVING_EXPORT_FAILED`처럼 이미 실패한 Task가 있는 판정도 있다. `NOT_FRESH`와 `SLOW_RUN`은 Task 실패 없이 기록만 남는다. `SLOW_RUN`은 재시도 대기와 수동 재실행으로 부풀고(이력 28건 중 2건, 둘 다 수동 검증 실행), `NOT_FRESH`의 원인은 대개 재지정 Task 실패로 이미 드러난다.

## 하지 않는 것

- 목표 비율(예: 99%)과 측정 기간을 정하지 않았다. 판정은 실행 한 건 단위다. 오류 예산도 계산하지 않는다.
- 외부 채널(Slack·메일·호출)로 알리지 않는다. 드러나는 곳은 Airflow의 Task·DagRun 상태뿐이다. 건다면 자리는 `domain_publish_alert`의 실패다. 부분 게시 때 `dbt_build` Task는 성공하므로 거기에 걸면 놓친다.
