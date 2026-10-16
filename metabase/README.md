# Metabase Serving 연결

Metabase는 읽기 전용 `/serving` 마운트의 Mart 전용 파일을 읽는다. 최초 연결은 `/serving/mart.duckdb`로 등록하며 이후 재지정은 `/serving/exports/{export_id}.duckdb`를 사용한다(019·049). 애플리케이션 상태는 PostgreSQL의 `metabase_app` 데이터베이스에 저장하므로 컨테이너 재생성 뒤에도 Dashboard와 Connection이 남는다.

`compose.yaml`은 `metabase/Dockerfile`로 만든 glibc 기반 Java 이미지를 사용한다. Community DuckDB Driver의 native 라이브러리는 공식 Alpine 이미지의 musl과 호환되지 않으므로, 공식 이미지 태그를 직접 기동하지 않는다.

## DuckDB 드라이버

Community DuckDB Driver release `1.5.5.0`을 사용한다. 이 release jar의 `metabase-plugin.yaml`은 플러그인 자체 버전을 `1.4.1.0`으로 표시하므로, UI·로그의 플러그인 버전과 release 태그를 혼동하지 않는다. Serving 파일은 Export 단계에서 `STORAGE_VERSION 'v1.0.0'`으로 만들며 Metabase 연결 화면에는 이 값을 입력하지 않는다. 아래 명령으로 release jar를 내려받아 `metabase/plugins/duckdb.metabase-driver.jar`에 둔다. jar는 실행 코드이므로 저장소에는 커밋하지 않는다.

```bash
curl --fail --location \
  --output metabase/plugins/duckdb.metabase-driver.jar \
  https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.5.5.0/duckdb.metabase-driver.jar
```

출처: <https://github.com/motherduckdb/metabase_duckdb_driver/releases/tag/1.5.5.0>

Metabase 프로세스는 컨테이너의 `metabase` 사용자(UID/GID `2000`)로 실행된다. 플러그인 디렉터리를 그 사용자가 쓸 수 없으면 Metabase는 임시 디렉터리로 대체하고 DuckDB jar를 로드하지 않는다. 처음 설치하거나 권한이 바뀐 뒤에는 아래 명령을 한 번 실행한다.

```bash
docker compose --profile bi exec metabase chown -R 2000:2000 /plugins
docker compose --profile bi restart metabase
```

## 기동과 연결

```bash
docker compose --profile bi up -d metabase
docker compose --profile bi ps metabase
```

Metabase 관리 화면에서 DuckDB Connection의 `Database file`에 `/serving/mart.duckdb`를 등록하고 `Establish a read-only connection`을 켠다. Serving 파일은 최신 `PUBLISHED` 실행 기록이 있어야 `uv run python -m src.serving export`로 생성된다. Connection Gate가 완료되기 전에는 기존 Published 파일이나 `data/warehouse/`를 마운트하거나 등록하지 않는다.

관리자 API로 Connection Gate를 자동 검증할 때는 로컬 `.env`에만 `METABASE_API_KEY`를 설정하고 요청의 `X-API-Key` 헤더에 전달한다. 실제 key를 저장소·문서·로그에 기록하지 않는다.

## 갱신과 최신 상태 확인

운영에서는 `warehouse_pipeline_dag`가 게시 → 서빙 내보내기 → Metabase 재지정 →
오래된 내보내기 정리를 순서대로 수행한다. 스키마 변경도 같은 경로에서 전체 새로고침을
명시한다. `full_refresh`(전체 새로고침) 기본값은 `false`이므로 일반 원천 DAG 트리거는
기존 증분 동작을 유지한다.

```bash
airflow dags trigger warehouse_pipeline_dag --conf '{"full_refresh": true}'
```

개발·복구 목적으로 CLI 게시를 했다면 바로 다음 명령을 실행한다.
`export`는 서빙 파일을 만들고, Metabase를 새 버전 경로로 재지정해 매니페스트를 검증한 뒤
최근 내보내기 3개만 남긴다. 매니페스트 검증 성공 뒤 `sync_schema`(스키마 동기화)를
1회 요청한다. 동기화 요청 실패는 stderr 경고만 남기며 `SUCCESS`를 유지한다.
이때 필드 메타데이터는 매시 정기 Sync에서 반영된다(060). DAG 실행 중에는 CLI 내보내기를 함께 실행하지 않는다.

```bash
uv run python -m src.serving export
uv run python -m src.serving status
```

`status`는 Metabase가 실제로 읽는 `serving_manifest.publish_run_id`(게시 실행 ID)를
최신 `PUBLISHED` 실행과 비교한다. 최신 여부(`fresh`)가 `true`이면 종료 코드 0,
지연 상태이거나 조회에 실패하면 1이다. 하위 명령 없이 `python -m src.serving`을 실행하면
사용 오류(종료 코드 2)로 끝난다.

호스트 CLI의 `METABASE_URL`은 프로세스 환경 변수만 읽고 기본값은
`http://localhost:3000`이다. `.env`의 `http://metabase:3000`은 Airflow 컨테이너 주소여서
호스트 CLI에서는 읽지 않는다. API 키와 서빙 DB ID는 비어 있지 않은 프로세스 값을
우선하고 없으면 프로젝트 루트 `.env`에서 읽는다. 실제 키는 `X-API-Key` 헤더로만 전송한다.

내보내기 JSON의 `metabase_repoint`(재지정 결과)가 `SUCCESS`이면 정리 후 종료 0이다.
키 또는 DB ID가 없어서 `SKIPPED`이면 경고를 출력하고 정리 후 종료 0이므로
`status`로 실제 연결을 확인한다. `FAILED`이면 종료 1로 끝내며 신규·이전 파일을 모두
보존한다. 재지정 실패를 내보내기 파일의 원복으로 처리하지 않는다(049·050).

카드 SQL을 배포하는 `scripts/metabase_import.py --apply`는 모든 카드 조회·DB 식별자 검사
후 첫 PUT 전에 DB마다 최신 여부를 검사한다. 변경 대상이 0개여도 지연이면 중단한다.
오류에 안내된 `uv run python -m src.serving export`를 실행한 뒤 다시 배포한다.
기본 모의 실행은 이 검사를 수행하지 않는다.

재지정을 끄려면 Metabase DB의 `details.database_file`을 먼저 `/serving/mart.duckdb`로
되돌린 다음 `METABASE_SERVING_DATABASE_ID`를 비운다. 순서를 지켜야 이후 정리에서
Metabase가 가리키는 버전 파일이 제거되지 않는다(050).

## 내부 테이블과 샘플 콘텐츠

`serving_manifest`는 내부 검증용 테이블이므로 관리 화면의 Table Metadata에서 이름으로 찾아 숨긴다.
API를 쓰면 해당 DB의 테이블 ID를 확인하고 `PUT /api/table/{id}`에
`{"visibility_type": "hidden"}`을 보낸다. 앱 DB를 새로 만들면 ID가 바뀔 수 있다.
숨긴 뒤에도 Native Query로 읽을 수 있어 재지정 검증과 최신 여부 검사는 동작한다.

Sample Database와 Examples 콘텐츠는 안전 사본을 만든 뒤 제거했다.
`compose.yaml`의 `MB_LOAD_SAMPLE_CONTENT=false`는 새 설치에서 샘플 콘텐츠 생성을 막는다.
공개 공유와 익명 추적도 각각 `MB_ENABLE_PUBLIC_SHARING=false`,
`MB_ANON_TRACKING_ENABLED=false`로 끈다.
