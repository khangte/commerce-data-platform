# Metabase Serving 연결

Metabase는 Published Warehouse가 아니라 읽기 전용으로 마운트한 `/serving/mart.duckdb`만 읽는다. 애플리케이션 상태는 PostgreSQL의 `metabase_app` 데이터베이스에 저장하므로 컨테이너 재생성 뒤에도 Dashboard와 Connection이 남는다.

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

## 갱신 한계

Serving Export는 파일을 원자 교체한다. 열린 DuckDB Handle은 교체 전 inode를 계속 볼 수 있으므로, 새 Export 뒤에는 Connection을 다시 연결한다. 정확한 관측값과 재기동 검증은 Connection Gate 완료 후 `docs/bi/serving-refresh.md`에 기록한다.
