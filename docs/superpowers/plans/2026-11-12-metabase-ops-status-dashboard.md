# Metabase 운영 상태 Dashboard 계획

> architect, 2026-11-12 · 설계만. 구현은 lead 승인 뒤 backend(원천)와 uiux(화면)가 한다.
> 근거: uiux Metabase 검토(2026-11-12) P1 ADD "operational status coverage", `docs/reference/publish-slo.md`, ADR 021
>
> **상태: 승인됨 (lead, 2026-11-12).** 원천은 A안으로 확정했다. 진행 순서는 §4.
> Card·Dashboard 제목은 `English (한글)` 형식으로 쓴다.

## 1. 문제

card 60 `도메인 게시 상태`는 Serving 파일의 `serving_domain_status`만 읽는다. Serving 파일은 게시와 내보내기가 성공해야 바뀐다. 그래서 아래 세 경우에 Dashboard는 정상처럼 보인다.

| 상황 | Run report 판정 | card 60이 보여 주는 것 |
| ---- | ---- | ---- |
| 전체 차단 | `PUBLISH_FAILED` | 직전 Serving 그대로. 두 도메인 "정상" |
| 내보내기·재지정 실패 | `SERVING_EXPORT_FAILED`, `REPOINT_FAILED` | 직전 Serving 그대로 |
| 행 수 급감 | `ROW_COUNT_DROP:<relation>` | 게시도 정상, 보류도 없음. "정상" |

`held = false`는 "보류되지 않았다"이지 "실행이 건강하다"가 아니다. 실행 한 건의 판정은 `pipeline_metadata.warehouse_run_reports`에만 있고 Metabase는 이 Table을 읽지 못한다.

**목표:** Metabase에서 마지막 실행의 게시 상태, 판정 코드, 지연과 임계값, 판정 시각을 본다. 게시가 실패한 실행도 보인다. 알 수 없는 것은 "정상"이 아니라 `확인 불가`로 보인다.

**범위 밖:** 외부 알림 채널, Airflow 화면 대체, 행 수 급감의 Relation별 상세 수치, 목표 비율·오류 예산, card 60의 문구 수정(검토 P2, 별도 작업).

## 2. 원천 결정 (A안 확정)

lead가 2026-11-12에 A안을 골랐다. 실패 경로를 보여 주는 안이 A뿐이다.

| 안 | 내용 | 실패 경로에서 보이나 | 비용 | 판정 |
| -- | ---- | ---- | ---- | ---- |
| **A** | Metabase에 두 번째 DB 연결을 추가한다. Postgres `pipeline_metadata`를 읽기 전용 Role로, View 2개만 보이게 한다 | 보인다. Run report는 `publish_run_summary`(`all_done`)가 쓰므로 전체 차단·내보내기 실패 실행도 한 행이 남는다 | Role 1개, View 2개, 환경 변수 2개, Metabase DB 등록 1회. PRD "Mart만 조회" 계약에 예외 문구가 필요하다(PRD v1.34) | **권장** |
| B | Run report를 Serving 파일에 싣는다(`serving_domain_status`와 같은 방식) | 안 보인다. 전체 차단·내보내기 실패 때 Serving이 안 바뀐다. 게다가 Run report는 내보내기 뒤에 써서 이번 실행의 판정은 다음 내보내기에야 실린다 | 낮음 | 기각. 문제를 못 푼다 |
| C | Run report 전용 DuckDB 파일을 따로 내보내고 Metabase에 연결한다 | 보인다 | 높음. 파일 교체·Hardlink·재지정(ADR 019)을 한 벌 더 만든다. DuckDB Instance Cache 문제도 다시 온다 | 기각 |

A안의 근거:

- Metabase는 이미 같은 Postgres에 붙어 있다(앱 DB `metabase_app`). Postgres Driver는 기본 내장이라 새 Driver가 없다.
- "Mart만 조회"는 사업 데이터를 검증 전 계층에서 읽지 않는다는 계약이다. 운영 기록은 사업 데이터가 아니다. 예외는 View 2개로 한정하고 Role 권한으로 강제한다.
- 운영 Dashboard는 사업 Dashboard 3종과 따로 둔다. 원천도 읽는 사람도 다르다.

## 3. 설계

```mermaid
graph LR
  S["publish_run_summary<br/>(all_done)"] -->|"Upsert 1행"| T[("warehouse_run_reports<br/>pipeline_metadata")]
  T --> V1["View warehouse_run_report_status<br/>실행 1건 = 1행"]
  T --> V2["View warehouse_run_report_domain_status<br/>실행 × 도메인"]
  V1 --> M["Metabase DB 연결<br/>Pipeline Metadata (읽기 전용 Role)"]
  V2 --> M
  M --> D["Dashboard: Pipeline Operations Status (Pipeline 운영 상태)"]
  X["export_serving_mart"] --> F[("Serving 파일")] --> C60["card 60 (사업 Dashboard 3종)"]
```

### 3.1 접근 권한

- 새 Role `metabase_ops_ro`(환경 변수 `METABASE_OPS_DB_USER`, `METABASE_OPS_DB_PASSWORD`). `pipeline_metadata`에 `CONNECT`, `public`에 `USAGE`, 아래 View 2개에만 `SELECT`. Table 권한은 주지 않는다.
- View 소유자는 `pipeline_metadata_rw`다. Postgres View는 소유자 권한으로 읽으므로 Role은 원본 Table을 볼 수 없다.
- View는 정해진 Column만 내보낸다. `report` JSON 통째와 `tasks`, `source_lease_owner`, 파일 크기류는 내보내지 않는다.
- Metabase 포트는 지금처럼 `127.0.0.1`에만 묶인다. 공개 공유는 꺼져 있다(`MB_ENABLE_PUBLIC_SHARING=false`).
- Role 생성은 `sql/bootstrap/01-create-databases-and-roles.sh`에 넣는다. 이 Script는 새 Volume에서만 돈다. 기존 Volume용 1회 명령을 `metabase/README.md`에 적는다.

### 3.2 View

`sql/metadata/007_create_warehouse_run_report_views.sql`(`CREATE OR REPLACE VIEW`, 멱등). `record_run_report`가 006 다음에 적용한다. Role이 있을 때만 `GRANT`한다.

**`warehouse_run_report_status`** — 실행 1건에 1행

| Column | 출처 | 비고 |
| ---- | ---- | ---- |
| `dag_run_id`, `logical_date`, `recorded_at`, `ok` | Table Column | |
| `publish_status`, `publish_error_type`, `serving_export_status`, `metabase_repoint_status` | `report` | |
| `fresh` | `report->'fresh'` | Metabase 환경 변수가 없으면 NULL |
| `failure_codes` | `failures`를 `, `로 이은 text | 빈 배열이면 빈 문자열이 아니라 NULL |
| `alert_codes` | `failures` 중 `PUBLISH_PARTIAL:`·`DATA_STALE:`·`PUBLISH_FAILED`·`ROW_COUNT_DROP:`로 시작하는 것 | `ALERT_FAILURE_PREFIXES`와 같아야 한다(§3.5) |
| `held_domains` | `report` | text |
| `data_lag_s`, `stale_after_s`, `dag_elapsed_s`, `tests_warned`, `full_refresh` | `report` | 숫자·Boolean으로 변환. 없으면 NULL |
| `record_age_s` | `now() - recorded_at` | **조회 시각 기준. 이 View에서 유일한 실시간 값** |
| `status` | 아래 규칙 | |

`status`는 위에서부터 먼저 맞는 것 하나다.

| 순서 | 조건 | 값 | 뜻 |
| ---- | ---- | ---- | ---- |
| 1 | `stale_after_s`가 NULL이거나 `record_age_s > stale_after_s` | `확인 불가` | 최근 실행 기록이 없다. 아래 값은 지난 실행의 것이다 |
| 2 | `alert_codes`가 있음 | `알림 대상` | `domain_publish_alert`가 실패한 실행 |
| 3 | `ok = false` | `판정 있음` | 기록만 남는 판정(`SLOW_RUN`, `DBT_TEST_WARN` 등) |
| 4 | 그 밖 | `정상` | |

**`warehouse_run_report_domain_status`** — 실행 × 도메인

`dag_run_id`, `recorded_at`, `domain`, `lag_s`(`domain_lag_s`), `refreshed_at`(`domain_refreshed_at`), `as_of`(`domain_as_of`), `consecutive_holds`, `held`(`held_domains`에 들어 있음), `stale_after_s`. 도메인 목록은 `domain_lag_s`의 Key에서 얻는다. 도메인 값이 없는 실행(Build 전 실패, 2026-11-12 이전 기록)은 행이 없다.

### 3.3 Dashboard와 Card

Dashboard `Pipeline Operations Status (Pipeline 운영 상태)`(사업 Dashboard와 분리). 제목은 아래 표대로 `English (한글)` 형식으로 쓴다. 배치·본문 문구·표시 형식은 uiux가 명세한다. 아래는 담아야 할 내용과 뜻이다. 기존 Card 40~60과 Dashboard 2~4의 제목은 이 작업에서 바꾸지 않는다.

| Card | 원천 | 내용 |
| ---- | ---- | ---- |
| `Last Run (마지막 실행)` | `warehouse_run_report_status`의 최신 `recorded_at` 1행 | `status`, 실행 ID, 논리 시각, 판정 기록 시각(UTC), 기록 후 경과, 게시·내보내기·재지정 상태, Dashboard 최신 여부(`fresh`) |
| `Verdict Codes (판정 코드)` | 같은 행 | `failure_codes` 전체와 `alert_codes`. 없으면 `판정 없음` |
| `Lag vs Threshold (지연과 임계값)` | 같은 행 + 도메인 View | 주문 데이터 지연(`data_lag_s`), 도메인별 갱신 지연(`lag_s`), 임계값(`stale_after_s`), 연속 보류 횟수 |
| `Recent Runs (최근 실행 이력)` | `warehouse_run_report_status` 최근 20건 | 기록 시각, `status`를 뺀 판정(`ok`, 게시 상태, 판정 코드, 알림 대상 여부), 실행 시간 |

- **실행 ID Filter**(선택, 기본값은 최신 실행). 지난 실패 실행을 같은 Card로 다시 볼 수 있다. 화면 검수도 이 Filter로 실제 기록을 쓴다(§4 Phase 3).
- **판정 시각**: `domain_publish_alert`는 기록 직후에 도는 Task이고 자기 시각을 따로 남기지 않는다. `recorded_at`을 판정 시각으로 쓰고 화면 이름도 "판정 기록 시각"으로 한다. "알림 발송 시각"이라고 쓰지 않는다. 외부 알림은 없다.
- **이력 Card에는 `status`를 쓰지 않는다.** `status`의 1번 규칙은 조회 시각 기준이라 지난 행은 전부 `확인 불가`가 된다.

### 3.4 확인 불가 상태

| 경우 | 화면 |
| ---- | ---- |
| Table이 비어 있음 | `Last Run (마지막 실행)` Card가 행 없이 비지 않고 `기록 없음` 한 행을 보인다(Card SQL이 고정 한 행에 `LEFT JOIN`) |
| 최근 기록이 임계값보다 오래됨 (Scheduler 정지, `publish_run_summary` 실패, `RUN_REPORT_WRITE_FAILED`) | `status = 확인 불가`. 지난 실행의 값은 그대로 보이되 "지난 실행"임을 표시 |
| 실행이 진행 중 | 직전에 끝난 실행을 보인다. Card 이름을 "마지막으로 끝난 실행"의 뜻으로 쓴다 |
| 개별 값이 NULL (`fresh`, `data_lag_s`, 도메인 값, Probe 실패) | 그 칸만 `확인 불가`. 0이나 `정상`으로 바꾸지 않는다 |
| Postgres 정지 | Metabase 자체가 뜨지 않는다(앱 DB가 같은 Postgres). 이 Dashboard로는 알 수 없다. 문서에 적는다 |

상시 운영이 아니라서 시연 때는 대부분 `확인 불가`로 시작한다. 이것이 맞는 표시다. 마지막 실행 뒤에 무슨 일이 있었는지 모르기 때문이다. card 60에 조회 시각 판정을 넣지 않은 것과 다른 점은, 여기서는 "지연"이라는 실패 판정이 아니라 "모름"이라는 중립 상태로 보인다는 것이다.

### 3.5 Snapshot과 실시간

| 값 | 기준 시각 |
| ---- | ---- |
| 지연(`data_lag_s`, `lag_s`), 임계값, 판정 코드, 게시 상태 | 그 실행의 `recorded_at`에 잰 값. 조회할 때 다시 계산하지 않는다 |
| `record_age_s`, `status`의 1번 규칙 | 조회 시각 |
| card 60(`serving_domain_status`) | 마지막으로 성공한 내보내기 시점. 실패 실행은 반영되지 않는다 |

- 화면에 "지연은 판정 기록 시각 기준"이라는 범위 표시를 둔다.
- 두 화면이 어긋날 수 있다. 전체 차단 뒤 card 60은 정상, 운영 Dashboard는 `알림 대상`이다. 이때 옳은 쪽은 운영 Dashboard다. `docs/bi/dashboards.md`에 적는다.
- Metabase Card Cache는 이 Dashboard에서 끈다(기본값 유지 확인).
- `alert_codes`의 접두사는 View SQL과 `ALERT_FAILURE_PREFIXES` 두 곳에 있다. 단위 테스트 하나가 SQL 파일에 네 접두사가 모두 있고 그 밖의 `LIKE` 접두사가 없음을 검사한다.

## 4. Phase

진행 순서는 아래와 같다. 앞 Phase의 완료조건이 충족돼야 다음으로 간다.

```mermaid
graph LR
  P1["Phase 1<br/>계약 문서<br/>architect"] --> P2["Phase 2<br/>원천<br/>backend"] --> P3["Phase 3<br/>화면<br/>uiux"] --> P4["Phase 4<br/>리뷰<br/>reviewer"] --> P5["Phase 5<br/>화면 문서<br/>architect"]
```

### Phase 1. 계약 문서 (architect)

구현보다 먼저 계약을 고친다. backend와 uiux는 이 문서를 기준으로 구현한다.

- ADR 022 신규: 운영 기록을 Metabase의 두 번째 원천으로 둔다(§2의 세 안과 A안을 고른 이유).
- PRD v1.34(v1.33은 `PRD.bak/`으로 옮긴다): "Mart만 조회" 계약의 예외, Role `metabase_ops_ro`, View 2개.

완료조건:

1. `docs/adr/022-*.md`가 있고 세 안, 결정, 한계(§3.4의 Postgres 정지, 기록 저장 실패)를 적는다.
2. `PRD_v1.34.md`가 있고 `PRD_v1.33.md`는 `PRD.bak/`에 있다. 저장소의 `PRD_v1.33` 참조가 모두 v1.34를 가리킨다.
3. PRD의 예외 문구가 View 2개와 읽기 전용 Role로 한정된다. 사업 Dashboard 3종의 "Mart만 조회"는 그대로다.
4. `tests/test_doc_links.py` 통과.

### Phase 2. 원천 (backend)

- `sql/metadata/007_create_warehouse_run_report_views.sql`, `record_run_report`에서 적용.
- Role 생성(`sql/bootstrap/01-create-databases-and-roles.sh`), `.env.example`, `compose.yaml`의 Postgres 환경 변수.
- `scripts/metabase_import.py`는 Card가 쓰는 DB마다 Serving 최신 여부를 검사한다(94~103행). Postgres 연결 Card에서는 이 검사를 건너뛰어야 한다. `scripts/metabase_snapshot.py`의 수집 대상 Collection에 새 Dashboard가 들어가는지도 확인한다.

완료조건:

- [x] 1. Postgres 통합 테스트: `metabase_ops_ro`로 View 2개를 읽을 수 있고 `warehouse_run_reports`·`mart_publish_runs`는 권한 오류가 난다.
   - 결과(backend, 2026-11-12): `test_ops_role_reads_only_views` 통과. `metabase_ops_ro`로 View 2개 SELECT 성공, 두 Table은 `InsufficientPrivilege`. 같은 Role의 `information_schema.tables`에는 View 2개만 보인다.
- [x] 2. 통합 테스트: 판정이 다른 기록 4건(정상, `PUBLISH_FAILED`, `ROW_COUNT_DROP:<relation>`만, `SLOW_RUN`만)에서 `alert_codes`와 `status`가 §3.2 표와 같다. `recorded_at`이 오래된 행은 `확인 불가`다.
   - 결과: `test_status_and_alert_codes` 통과. 정상 → `정상`, `PUBLISH_FAILED` → `알림 대상`(`alert_codes = PUBLISH_FAILED`), `ROW_COUNT_DROP:facts.fct_order` → `알림 대상`, `SLOW_RUN` → `판정 있음`, 1,300초 전 기록과 `stale_after_s` NULL 기록 → `확인 불가`.
- [x] 3. 통합 테스트: 도메인 값이 없는 기록은 도메인 View에 행이 없고 오류도 없다.
   - 결과: `test_domain_view_rows` 통과. `domain_lag_s`가 없거나 JSON null인 기록은 행이 없고, 값이 있는 기록은 도메인별 1행(NULL 지연은 NULL 그대로)이다.
- [x] 4. 단위 테스트: View SQL의 접두사가 `ALERT_FAILURE_PREFIXES`와 같다.
   - 결과: `test_view_sql_alert_prefixes_match_alert_failure_prefixes` 통과. 접두사는 `LIKE`가 아니라 `starts_with()`로 쓴다(`LIKE`의 `_`는 임의 한 글자라서). 테스트는 `starts_with` 접두사 집합이 `ALERT_FAILURE_PREFIXES`와 같고 SQL에 `LIKE`가 없음을 검사한다.
- [x] 5. 기존 Run report 테스트와 Metabase 가져오기 테스트가 통과한다. `uv run pytest`, Ruff 통과.
   - 결과: `uv run pytest` 862 통과·151 건너뜀, `RUN_POSTGRES_INTEGRATION=1` Run report·View 통합 14 통과, `uv run ruff check .` 통과.
- [x] 6. `record_run_report`의 실패 처리(저장 실패는 로그만)가 그대로다. View 적용 실패가 DAG를 실패시키지 않는다.
   - 결과: 007 적용은 기록 Insert 뒤 별도 연결·Transaction·`try`에서 돈다. 실패하면 `RUN_REPORT_VIEW_APPLY_FAILED: ...`만 출력하고 기록은 남는다(`test_view_apply_failure_keeps_record_and_does_not_raise`). 저장 실패의 `RUN_REPORT_WRITE_FAILED` 처리는 그대로다.

#### 파일·폴더별 변경 요약 (backend, 2026-11-12)

| 파일 | 변경 |
| ---- | ---- |
| `sql/metadata/007_create_warehouse_run_report_views.sql` | 신규. `warehouse_run_report_status`(실행 1행, §3.2 Column·`status` 규칙)와 `warehouse_run_report_domain_status`(실행 × 도메인)를 `CREATE OR REPLACE VIEW`로 만든다. 알림 접두사는 `starts_with()`. `domain_lag_s`·`held_domains`가 객체·배열이 아니면 행·값을 만들지 않는다. Role `metabase_ops_ro`가 있을 때만 `CONNECT`·`USAGE`·View 2개 `SELECT`를 준다. **Role 이름은 SQL에 고정**이라 `METABASE_OPS_DB_USER`를 바꾸면 권한이 안 붙는다 |
| `src/observability/run_report.py` | `record_run_report`가 기록 Insert 뒤 별도 연결에서 007을 적용한다. 실패는 `RUN_REPORT_VIEW_APPLY_FAILED` 로그만 남긴다 |
| `sql/bootstrap/01-create-databases-and-roles.sh` | `create_role "$METABASE_OPS_DB_USER" "$METABASE_OPS_DB_PASSWORD"` 추가(새 Volume에서만 실행) |
| `compose.yaml` | Postgres 서비스에 `METABASE_OPS_DB_USER`(기본 `metabase_ops_ro`)·`METABASE_OPS_DB_PASSWORD` 전달 |
| `.env.example` | 위 두 변수와 "이름을 바꾸지 않는다" 주석 |
| `scripts/metabase_import.py` | `database_engine()`이 `GET /api/database/{id}`로 엔진을 읽고, `--apply`에서 엔진이 `postgres`인 DB는 Serving 최신 여부 검사를 건너뛴다 |
| `tests/integration/test_run_report_views_integration.py` | 신규. 완료조건 1~3 |
| `tests/observability/test_run_report_alert.py` | 완료조건 4 |
| `tests/observability/test_run_report.py` | 완료조건 6(View 적용 실패 격리) |
| `tests/test_metabase_import.py` | Postgres 엔진 DB 검사 건너뜀, `database_engine` 요청 형태. Fixture는 기본 엔진을 `duckdb`로 둔다 |

후속 확인:

- 로컬 기존 Volume에는 backend가 `metabase_ops_ro`를 1회 생성하고 로컬 `.env`에 비밀번호를 넣었다. 기존 Volume용 명령의 문서화는 Phase 5(`metabase/README.md`).
- `scripts/metabase_snapshot.py`는 Collection `(5, 6)`만 수집한다. Phase 3에서 새 Dashboard·Card를 이 Collection에 두거나 `SNAPSHOT_COLLECTION_IDS`를 갱신해야 Snapshot에 들어간다.

### Phase 3. 화면 (uiux 명세 → uiux 구현 → uiux 검수)

- Metabase DB 연결 `Pipeline Metadata` 등록(Role `metabase_ops_ro`), Card 4개, Dashboard 1개, `metabase/export/` Snapshot.

**화면 명세:** 상단에 실행 ID 선택과 기준 시각·사업 화면과의 차이 안내를 둔다. 기본값은 마지막으로 끝난 실행이다. 왼쪽에는 상태·시각·게시·내보내기·재지정·당시 최신 여부를 세로 표로 배치하고, 오른쪽에는 알림 대상과 전체 판정 코드(한 코드 한 행), 주문·도메인 지연과 임계값을 배치한다. 아래 최근 20건 이력은 조회 시각에 따라 변하는 `status`를 사용하지 않고 기록 당시 결과를 표시한다. 지연·코드는 기록 시점, 기록 후 경과와 오래된 기록 판정은 조회 시점 기준이다. 누락값은 확인 불가로 표시하며, 값이 없는 실행은 마지막 실행 카드에 기록 없음 한 행을 보인다. 모든 시간은 UTC다.

**완료조건 (2026-11-12 uiux 구현·검수, architect 판정 대기):**

- [x] 네 Card는 View 2개만 읽고 지정된 `English (한글)` 제목을 쓴다. Dashboard ID 5, Card ID 61–64, 연결 ID 3이다. 새 연결·카드는 수집 대상 Collection 6에 둔다.
- [x] 실행 ID 선택으로 실제 [정상 실행](../../bi/screenshots/ops-status-success-2026-11-12.png)(`manual__2026-11-12T02:20:00+00:00`), [부분 게시](../../bi/screenshots/ops-status-partial-2026-11-12.png)(`verify_partial_on`), [전체 차단](../../bi/screenshots/ops-status-failed-2026-11-12.png)(`verify_partial_off`)을 확인했다. 조회 시 세 기록 모두 오래돼 상태는 확인 불가이며, 각각의 당시 게시 상태와 판정 코드는 그대로 보인다.
- [x] 실제 오래된 기록·전체 차단 실행의 NULL 값을 확인했다. [390px 모바일 화면](../../bi/screenshots/ops-status-mobile-2026-11-12.png)의 기준 안내와 첫 카드를 확인했다. 빈 Table을 만들기 위해 실제 이력을 지우지 않고, [없는 실행 ID 선택](../../bi/screenshots/ops-status-no-match-2026-11-12.png)으로 동일한 빈 선택 결과를 화면에서 확인했다. 별도로 읽기 전용 Role의 PostgreSQL 임시 Table이 빈 상태에서 card 61 SQL이 기록 없음 한 행을 반환함을 확인했다.
- [x] 실제 이력에 없는 `ROW_COUNT_DROP:facts.fct_order`를 PostgreSQL 임시 Table에만 넣어 card 62·64 SQL을 실행하고 알림 대상과 전체 판정에 모두 나타남을 확인했다. 실제 운영 이력은 변경하지 않았다.
- [x] UTC, 판정 기록 시각 기준, 사업 Dashboard의 마지막 성공 Serving 상태와의 차이를 화면에 표시했다. 캐시 설정 `enable-query-caching=false`를 확인했다.
- [x] [Metabase 관리 화면](../../bi/screenshots/ops-status-database-2026-11-12.png)과 DB metadata에서 `warehouse_run_report_status`, `warehouse_run_report_domain_status` 두 View만 보임을 확인했다. `metabase_ops_ro`로 연결한 결과도 동일하다.

**파일·폴더별 변경 요약 (uiux, 2026-11-12)**

| 파일·대상 | 변경 |
| ---- | ---- |
| `metabase/export/card-{61,62,63,64}.json` | 두 운영 View를 읽는 마지막 실행·판정 코드·지연·최근 이력 카드와 실행 ID 필터 쿼리 |
| `metabase/export/dashboard-5.json` | Collection 6의 운영 Dashboard, 실행 ID 필터 3개 카드 매핑, 기준 안내와 배치 |
| `docs/bi/screenshots/ops-status-{success,partial,failed,no-match,database,mobile}-2026-11-12.png` | 실제 기록 3건·없는 실행 선택·Metabase 관리 화면 증적 |
| 이 계획서 | 화면 명세·완료조건·검증 이력과 변경 요약 |
| Metabase 연결 `Pipeline Metadata` | PostgreSQL DB ID 3, `metabase_ops_ro` 사용. 연결 비밀은 Snapshot에 저장하지 않는다 |

**검증 한계:** 화면의 기록 없음은 실제 빈 Table이 아니라 없는 실행 ID를 선택한 대체 증적이다. 정상·부분·차단 기록은 실제이나 조회 시점에 오래돼 `정상`/`알림 대상` 현재 상태 배지는 재현되지 않았다. SQL 임시 Table 결과는 실제 이력이나 화면 캡처가 아니다. 최근 이력의 긴 코드와 추가 행은 표 안에서 스크롤해야 한다. 모바일 카드도 추가 행을 보려면 표 안을 스크롤해야 한다. PostgreSQL이 중단되면 같은 DB를 앱 저장소로 쓰는 Metabase 자체가 열리지 않는다.

### Phase 4. 리뷰 (reviewer)

Phase 2 코드와 Phase 3 Snapshot. Phase 2 코드는 Phase 3을 시작하기 전에 먼저 리뷰받아도 된다.

### Phase 5. 화면 문서 (architect)

화면이 있어야 쓸 수 있는 문서다. `docs/reference/publish-slo.md`, `docs/bi/dashboards.md`, `docs/bi/evidence.md`, `metabase/README.md`(기존 Volume용 Role 추가 명령 포함), `docs/phases/phase-10-bi.md`, README BI 절.

완료조건:

1. 위 문서가 Dashboard 이름, Card 4개, 원천 View, `확인 불가`의 뜻, card 60과 어긋날 때 어느 쪽이 옳은지를 적는다.
2. `docs/bi/evidence.md`가 Phase 3의 화면 증적을 가리킨다.
3. `tests/test_doc_links.py` 통과.

## 5. 가정과 확인하지 않은 것

- 가정: 운영 Dashboard는 사업 Dashboard와 같은 Metabase 계정으로 본다. 계정·Group별 권한 분리는 하지 않는다(사용자가 한 명이다).
- 가정: `확인 불가`의 임계값은 새 값을 만들지 않고 그 실행의 `stale_after_s`(원천 주기 2배)를 쓴다.
- 확인하지 않음: Metabase v0.63이 Postgres View를 Table처럼 동기화하는지. 일반적으로 되지만 이 환경에서 돌려 보지 않았다. Phase 3 첫 단계에서 확인한다. 안 되면 Card를 Native SQL로만 쓰면 된다.
- 확인하지 않음: `scripts/metabase_snapshot.py`·`metabase_import.py`가 DB 연결이 둘인 Snapshot을 다른 환경에서 되살릴 때 DB ID를 어떻게 맞추는지. Card 60은 `database: 2`가 고정돼 있다. 새 연결의 ID는 환경마다 다를 수 있다.
- 확인하지 않음: 기존 Volume에서 Role을 추가하는 1회 명령.
- 이력의 `stale_after_s`는 실행마다 다를 수 있다(최근 기록은 1,200초). View는 행마다 자기 값을 쓴다.
