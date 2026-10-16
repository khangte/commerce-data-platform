# 060. Metabase 계층 정리: Snapshot 재작성, 재지정 후 스키마 동기화, 미사용 콘텐츠 제거

- 날짜: 2026-10-16
- 상태: **채택·구현 완료.** 사용자 지시(2026-10-16)로 별도 승인 없이 진행, reviewer 승인, 실환경 검증(Sample DB 삭제 전 사본 보관, Snapshot 카드 40–45 포함, 재지정 뒤 sync_schema, serving status fresh=true).
- 판정: **채택.**
  - `scripts/metabase_snapshot.sh`를 Collection 기준 Python 도구로 바꾼다. 덮어쓰기 보호를 넣고, 배포에 필요한 필드만 저장한다.
  - Metabase 재지정이 성공하면 `POST /api/database/{id}/sync_schema`를 1회 요청한다.
  - Sample Database와 Examples 콘텐츠를 제거한다. 쓰지 않는 공개 공유와 익명 추적을 끈다.
  - `scripts/metabase_import.py`의 중복 검사와 광범위한 예외 처리를 줄인다.
  - BI 문서에서 SQL 정본, 롤백, 중복 설명을 정리한다.
- 범위: 감사 항목 중 N5(Capture 개선)를 뺀 전부. N5는 다음 캡처 때 다룬다.
- 계기: Serving 이후 Metabase 계층 감사(기준 HEAD 8540830).
- 관련:
  - 감사 보고서 `metabase-layer-audit.md`와 구현 계획 `2026-10-16-metabase-layer-cleanup-plan.md`. 둘 다 git이 추적하지 않는 Scratchpad 문서다. 이 문서가 판정의 정본이다.
  - 019·049(Versioned Hardlink + API 재지정), 050(정리 조건), 051(재지정 실패 재시도), 059(Serving 갱신 단일 경로). PRD v1.25 §17.1. 변경 계약은 PRD v1.26에 반영한다.

## 1. 관측 사실

Metabase v0.63.16.1 OSS에 GET 요청만 보내 확인했다.

| 대상 | 관측값 |
| --- | --- |
| Collection 5 "Commerce Mart 지표" | Metric 40 GMV·41 AOV·42 Orders, Question 43 카테고리 GMV·44 상품 판매 수량·45 고객 수. Dashboard에 올라가 있지 않다. |
| Collection 6 "Commerce Mart 대시보드" | 카드 46–59, Dashboard 2·3·4. |
| `metabase/export/` | 카드 46–59와 Dashboard 2·3·4만 있다. **카드 40–45는 백업되지 않았다.** |
| Export JSON | 카드 하나에 최상위 키가 57개다. `view_count`, `last_used_at`, `average_query_time`, `can_*`, `creator` 같은 실행·권한 필드가 diff를 만든다. |
| Database 2 Sync 일정 | `metadata_sync` 매시 8분, `cache_field_values` 매일 19:00 UTC, `is_full_sync=true`. |
| Database 1 Sample Database | 카드 1–39, Dashboard 1, Collection 2 "Examples". 최근 Task 200개 중 104개가 이 DB의 Sync다. |
| `main.serving_manifest` | Table id 18, `visibility_type=None`. 데이터 브라우저에 보인다. |
| 공개 설정 | `enable-public-sharing=true`이지만 `public_uuid`가 있는 카드·Dashboard는 0개다. `anon-tracking-enabled=true`. |
| Query Caching | 꺼져 있다. 모든 카드·Dashboard의 `cache_ttl=null`. |
| 알림 | Pulse·Alert·구독이 없다. Email·Slack이 설정되지 않았다. |
| Revision API | `GET /api/revision?entity=card&id=49`가 이력을 반환한다. |

## 2. 판정

### 2.1 Snapshot 재작성 (N1, O2, O3, O5) — 채택

- `scripts/metabase_snapshot.sh`를 지우고 `scripts/metabase_snapshot.py`로 바꾼다.
- 대상은 ID를 하드코딩하지 않는다. `SNAPSHOT_COLLECTION_IDS = (5, 6)`의 항목을 `GET /api/collection/{id}/items`로 모은다.
  - `card`·`metric`·`dataset`은 `/api/card/{id}`, `dashboard`는 `/api/dashboard/{id}`로 읽는다.
  - 하위 Collection은 따라가지 않는다. 지금은 없다.
- 카드 40–45는 유지하고 Snapshot에 넣는다(사용자 결정).
- 배포와 재생성에 필요한 필드만 저장한다. 목록은 구현 계획에 고정한다. Import가 쓰는 `id`·`database_id`·`dataset_query`와 Test가 읽는 `dataset_query.stages`는 반드시 남는다.
- 모든 GET이 성공한 뒤에만 파일을 쓴다. 대상에서 빠진 `card-*.json`·`dashboard-*.json`은 지운다. git이 추적하므로 복구할 수 있다.
- 출력 디렉터리에 커밋하지 않은 변경(추적되지 않은 파일 포함)이 있으면 쓰지 않고 종료 코드 1로 끝낸다. `--force`일 때만 덮어쓴다.
- Key는 `require_metabase_setting`, URL은 `host_metabase_url()`로 읽는다(059 공통 로더).

### 2.2 재지정 성공 뒤 스키마 동기화 (N2) — 채택

- 문제: 재지정은 `details.database_file`만 바꾼다. Field 메타데이터는 매시 Sync까지 최대 1시간 늦다.
  - Dashboard 2의 `dimension` Template Tag Field Filter(`utc_order_date`, `order_status`)가 이 메타데이터를 쓴다.
  - Native SQL 실행 자체는 Sync와 무관하다.
- 조치: `repoint_metabase_serving`이 Manifest 검증을 통과한 뒤 `POST /api/database/{id}/sync_schema`를 1회 보낸다.
  - 결과를 기다리지 않는다. Metabase가 비동기로 처리한다.
  - 실패하면 한 줄 경고만 출력하고 `SUCCESS`를 유지한다. 재지정과 Manifest 검증은 이미 끝났고, 매시 Sync가 따라잡는다.
  - Manifest 검증이 실패한 `FAILED`에서는 보내지 않는다.
- DAG와 CLI가 같은 함수를 쓰므로 두 경로 모두 적용된다(059).
- 기각: `rescan_values`. 드롭다운 값(`order_status`)은 거의 바뀌지 않고, 매일 19:00 UTC 갱신으로 충분하다.
- 기각: Full Sync·Fingerprint 끄기. 매시 Sync는 모두 성공하고 있고 부하 근거가 없다.

### 2.3 Sample Database 제거 (N3) — 채택, 사용자 승인

- 순서:
  1. 카드 1–39와 Dashboard 1의 GET 응답을 JSON으로 안전 사본을 만든다.
  2. Collection 2를 Archive한다.
  3. Database 1을 삭제한다.
- **Database 삭제는 되돌릴 수 없다.** 안전 사본은 git이 추적하지 않는 작업 디렉터리에 두고, 저장소에는 넣지 않는다.
- `compose.yaml` Metabase 환경에 `MB_LOAD_SAMPLE_CONTENT: "false"`를 넣는다. 새 설치에서 다시 생기지 않는다.
- `metabase/plugins/sample-database.sqlite`는 이미 `.gitignore`가 제외한다. 손대지 않는다.

### 2.4 `serving_manifest` 숨김 (N4) — 채택

- `PUT /api/table/18` `{"visibility_type": "hidden"}`. 1회 설정이다.
- 059의 지연 검사와 재지정 검증은 Native Query라 Table 숨김의 영향을 받지 않는다. 적용 뒤 `python -m src.serving status`로 확인한다.
- 앱 DB를 새로 만들면 Table id가 바뀐다. `metabase/README.md`에는 Table 이름 기준 절차로 적는다.

### 2.5 공개 공유·익명 추적 끄기 (N6) — 채택

- `compose.yaml`에 `MB_ENABLE_PUBLIC_SHARING: "false"`, `MB_ANON_TRACKING_ENABLED: "false"`를 넣는다.
- 공개 링크가 0개라 영향이 없다. 환경 변수로 정한 설정은 관리 화면에서 바꿀 수 없게 된다.

### 2.6 `metabase_import.py` 정리 (O4, O6, O7) — 채택

- O4: 검사는 하나만 남긴다. **Export `dataset_query.database` = 운영 카드 `database_id`.** PUT이 `dataset_query`만 보내므로 이 검사가 카드의 Database 변경을 막는다.
  - 운영 카드 id 검사는 기각한다. `GET /api/card/N`은 카드 N 또는 404를 반환한다.
  - Export 파일 안의 `database_id`·`dataset_query.database` 비교는 기각한다. 같은 파일끼리의 비교다.
  - Export 파일 id 검사는 기각한다. 새 Snapshot이 파일 이름을 응답 id로 만든다.
- O6: `main`은 `RuntimeError`, `OSError`, `ValueError`만 잡는다. `KeyError`·`TypeError`·`AttributeError`는 코드 결함이라 Traceback을 남긴다.
- O7: `test_http_error_is_clear_and_hides_credentials`를 지운다. `HTTPError`에는 요청 헤더가 들어가지 않는다.

### 2.6.1 카드 설명 불일치 (구현 중 발견) — 저장소 설명 보존

- 관측: 첫 Snapshot에서 카드 46·48·49·50·51의 운영 `description`이 저장소 HEAD보다 옛 값이었다. `metabase_import`는 `dataset_query`만 PUT하므로, 저장소에서 고친 설명이 운영에 반영된 적이 없다.
- 판정: 저장소 HEAD 설명이 검토를 거친 정본이다.
  - 다섯 카드에 `PUT /api/card/{id}` `{"description": <HEAD 값>}`만 1회 보낸다.
  - 그다음 `--force`로 다시 Snapshot한다. 다섯 카드의 `description` diff가 0이어야 한다.
  - 실수로 덮어써도 Revision 이력으로 되돌릴 수 있다.
- `metabase_import`의 PUT 범위는 넓히지 않는다. 이후 규칙: 카드 설명·이름·표시는 Metabase에서 고친 뒤 Snapshot으로 저장소에 내린다. 저장소 JSON을 직접 고치는 것은 `dataset_query`만이다.

### 2.7 문서 (O1, O9, O10) — 채택

- O1: 롤백 절차를 쓴다. `GET /api/revision?entity=card&id=<id>`로 이력을 보고 `POST /api/revision/revert`로 되돌린다.
- O9: BI SQL의 배포 정본은 `metabase/export/card-<id>.json`의 `dataset_query` 하나다. `metabase/queries/*.sql`은 Template Tag가 없는 대조용 참조다.
- O10: `docs/bi/dashboards.md`의 가져오기 절을 명령 2개와 핵심 항목으로 줄인다. 지연 검사 설명은 `metabase/README.md`를 가리킨다.
- `docs/bi/evidence.md`의 과거 `metabase_snapshot.sh` 언급은 당시 기록이라 고치지 않는다.

## 3. 유지·수용 항목

| 항목 | 판정 | 근거 |
| --- | --- | --- |
| Query Caching 꺼짐 | 유지 | 켜면 재지정 뒤에도 캐시 결과가 TTL 동안 남아 059 지연 검사와 카드 결과가 어긋날 수 있다. 데이터가 작아 이득도 없다. |
| 관리자 API Key 1개 | 수용 | 로컬 단일 사용자 환경이다. 용도별 Key 분리는 과설계다. |
| 권한 그래프 | 수용 | 같은 이유다. |
| 알림·구독 | 조치 없음 | 정리할 대상이 없다. |
| Python `metabase_import.py` | 유지 | 모든 GET 뒤 PUT, Database 검사, 059 지연 검사가 이 도구에 있다. |
| N5 Capture 고정 대기·URL 기본값 | 보류 | 사용자 결정으로 범위에서 뺐다. |

## 4. 완료 조건

- 전체 Test Suite와 `ruff check` 결과(통과·실패 수)를 보고한다.
- 실제 Snapshot 1회 실행:
  - 첫 실행 diff를 검토한다. 카드 40–45 파일이 생긴다.
  - 카드 46–59의 `dataset_query`는 바뀌지 않는다. 바뀐 것은 저장 필드뿐이다.
- 재지정이 `sync_schema`를 요청한다. `src.serving export` 1회 실행으로 확인한다.
- `serving_manifest`가 숨김이고, `python -m src.serving status`가 종료 코드 0, `fresh: true`다.
- Sample Database가 없다. Collection 2가 Archive됐다. Metabase를 재생성한 뒤에도 다시 생기지 않는다.
- 카드 49–51을 1회씩 실행해 `completed`를 확인한다.
- `metabase_import 40..59 --apply`가 지연 검사를 통과하고 변경 0건이다.
- reviewer가 승인한다. 커밋은 lead만 한다.

## 5. 재검토 조건

- Snapshot 대상 Collection이 늘거나 하위 Collection이 생기면 `SNAPSHOT_COLLECTION_IDS`와 재귀 여부를 다시 본다.
- 재지정 직후 Field Filter 오류가 실제로 관측되면 `sync_schema` 완료 대기 또는 `rescan_values`를 검토한다.
- 사용자가 늘거나 Key를 공유 환경에 두면 용도별 API Key와 권한 그래프를 다시 본다.
- 카드 응답이 체감될 만큼 느려지면 Query Caching과 재지정 뒤 캐시 무효화를 함께 설계한다.
