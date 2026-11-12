# ADR 022. 운영 기록을 Metabase의 두 번째 원천으로 둔다

## Status

Accepted (2026-11-12). 구현 전이다. [ADR 012](012-metabase-serving-strategy.md)·[ADR 019](019-serving-file-visibility-for-metabase.md)를 보완한다. 사업 Dashboard가 Serving 파일만 읽는 구조는 그대로다.

계획: `docs/superpowers/plans/2026-11-12-metabase-ops-status-dashboard.md`

## Context

[ADR 021](021-split-publish-gate-by-domain.md) 뒤로 Dashboard 맨 위에 `도메인 게시 상태` 표(card 60)가 있다. 이 표는 Serving 파일의 `serving_domain_status`를 읽는다. Serving 파일은 게시와 내보내기가 성공해야 바뀐다. 그래서 실패한 실행은 Metabase에서 보이지 않는다.

| 상황 | Run report 판정 | card 60 |
| ---- | ---- | ---- |
| 전체 차단 | `PUBLISH_FAILED` | 직전 Serving 그대로. 두 도메인 "정상" |
| 내보내기·재지정 실패 | `SERVING_EXPORT_FAILED`, `REPOINT_FAILED` | 직전 Serving 그대로 |
| 행 수 급감 | `ROW_COUNT_DROP:<relation>` | 게시도 정상, 보류도 없다. "정상" |

`held = false`는 "보류되지 않았다"이지 "실행이 건강하다"가 아니다. 실행 한 건의 판정은 Postgres `pipeline_metadata.warehouse_run_reports`에만 있다. 이 Table은 `publish_run_summary`(`trigger_rule=all_done`)가 쓰므로 실패한 실행도 한 행을 남긴다. Metabase는 이 Table을 읽지 못한다.

PRD Section 17은 "Metabase는 마지막 성공 Mart만 읽는다"고 정한다. 운영 기록을 Metabase에서 보려면 이 계약에 닿는다.

## Decision

Metabase에 두 번째 DB 연결을 추가한다. 대상은 Postgres `pipeline_metadata`이고 읽기 전용 Role로 View 2개만 읽는다.

| 항목 | 내용 |
| ---- | ---- |
| Role | `metabase_ops_ro`. `pipeline_metadata`에 `CONNECT`, `public`에 `USAGE`, View 2개에만 `SELECT`. Table 권한은 없다 |
| View | `warehouse_run_report_status`(실행 1건에 1행), `warehouse_run_report_domain_status`(실행 × 도메인). 소유자는 `pipeline_metadata_rw` |
| 내보내는 값 | 정해진 Column만. `report` JSON 통째, Task 목록, 잠금 소유자는 내보내지 않는다 |
| 화면 | 사업 Dashboard 3종과 분리한 Dashboard 1개 |

"Mart만 읽는다"는 계약은 이렇게 좁힌다.

- 사업 데이터는 계속 Serving 파일의 Mart만 읽는다. Source, Bronze, Staging, Published Warehouse 파일은 여전히 읽지 않는다.
- 예외는 운영 기록 하나다. 범위는 위 View 2개이고 Role 권한으로 강제한다.
- 사업 Dashboard 3종의 Card는 새 연결을 쓰지 않는다.

값의 기준 시각은 둘로 나눈다.

- 지연, 임계값, 판정 코드, 게시 상태는 그 실행의 `recorded_at`에 잰 값이다. 조회할 때 다시 계산하지 않는다.
- 조회 시각으로 계산하는 값은 "기록 후 경과" 하나다. 마지막 기록이 그 실행의 `stale_after_s`보다 오래되면 상태를 `확인 불가`로 보인다. 실패 판정이 아니라 "마지막 실행 뒤를 모른다"는 표시다.

## Alternatives

| 대안 | 기각 이유 |
| ---- | ---- |
| Run report를 Serving 파일에 싣는다(`serving_domain_status`와 같은 방식) | 문제를 풀지 못한다. 전체 차단과 내보내기 실패 때 Serving이 바뀌지 않는다. Run report는 내보내기 뒤에 쓰므로 이번 실행의 판정은 다음 내보내기에야 실린다 |
| Run report 전용 DuckDB 파일을 따로 내보내 연결한다 | 실패 경로는 보이지만 파일 교체, Hardlink, 재지정(ADR 019)을 한 벌 더 만든다. DuckDB Instance Cache 문제도 다시 온다 |
| Metabase에 `warehouse_run_reports` Table을 직접 연다 | `report` JSON 전체가 보인다. 화면이 JSON 구조에 묶이고, 알림 대상 판정 규칙을 Card마다 다시 써야 한다 |
| Airflow 화면만 쓴다 | DagRun 상태와 메시지는 보이지만 지연, 임계값, 도메인별 값은 Task 로그를 열어야 한다. 사업 Dashboard를 보는 자리에서 확인할 수 없다 |

A안을 고른 근거:

- 실패 경로를 보여 주는 안 가운데 새 장치가 가장 적다. Metabase는 이미 같은 Postgres를 앱 DB로 쓰고 Postgres Driver는 기본 내장이다.
- 운영 기록은 사업 데이터가 아니다. 검증 전 계층을 읽지 않는다는 계약의 목적(틀린 수치를 보이지 않는다)과 충돌하지 않는다.

## Consequences

| 구분 | 내용 |
| ---- | ---- |
| 얻는 것 | 전체 차단, 부분 게시, 내보내기·재지정 실패, 행 수 급감이 Metabase에서 보인다. 지난 실패 실행도 같은 Card로 다시 볼 수 있다 |
| 비용 | Role 1개, View 2개, 환경 변수 2개, Metabase DB 등록 1회. Metabase Snapshot 도구가 DB 연결 둘을 다뤄야 한다 |
| 유지 | 사업 Dashboard의 원천과 Serving 파일 교체 절차. Run report의 내용과 판정 |
| 새 결합 | 알림 대상 판정의 접두사가 `ALERT_FAILURE_PREFIXES`와 View SQL 두 곳에 있다. 단위 테스트로 둘이 같은지 검사한다 |

알려진 한계:

- **Postgres가 멈추면 이 Dashboard도 없다.** Metabase 앱 DB가 같은 Postgres다(ADR 006). Metabase 자체가 뜨지 않는다.
- **기록이 남지 않은 실행은 보이지 않는다.** `publish_run_summary`가 실패한 실행, 저장 실패(`RUN_REPORT_WRITE_FAILED`, 로그만 남는다), 시작하지 못한 DagRun, 진행 중인 실행이다. 화면은 직전 기록을 보이고, 그 기록이 임계값보다 오래되면 `확인 불가`가 된다.
- **알림 시각은 따로 없다.** `domain_publish_alert`는 기록 직후에 도는 Task이고 자기 시각을 저장하지 않는다. `recorded_at`을 "판정 기록 시각"으로 보인다.
- **두 화면이 어긋날 수 있다.** 전체 차단 뒤 card 60은 정상, 운영 Dashboard는 알림 대상이다. 옳은 쪽은 운영 Dashboard다.
- 상시 운영이 아니므로 시연을 시작할 때는 대부분 `확인 불가`다.
- 계정·Group별 권한 분리는 하지 않는다. 사용자가 한 명이다.

## Validation

구현 전이다. 아래는 계획서의 Phase별 완료조건이고, 구현이 끝나면 결과로 바꾼다.

| 근거 | 기대 결과 |
| ---- | ---- |
| Postgres 통합 테스트: `metabase_ops_ro` 권한 | View 2개는 읽힌다. `warehouse_run_reports`와 `mart_publish_runs`는 권한 오류다 |
| 통합 테스트: 판정이 다른 기록 4건 | 알림 대상 판정과 상태가 계획서 표와 같다. 오래된 기록은 `확인 불가`다 |
| 단위 테스트: View SQL의 접두사 | `ALERT_FAILURE_PREFIXES`와 같다 |
| 화면 증적: 실제 기록 3건(정상, 부분 게시, 전체 차단) | 세 실행이 서로 다른 상태로 보인다 |
