# 049. Metabase의 Serving 파일 stale 조회 원인과 권고

- 일자: 2026-09-29
- 요청: lead 분석 요청(코드 변경 금지)
- 판정: 원인 확정. 재현했다. 권고안은 A(버전 경로 Hardlink와 Metabase Connection 재지정)다. PRD는 경미하게 수정해야 하고, ADR은 새로 만든다(019).

## 현상

`export_serving_mart`는 `data/serving/mart.duckdb`를 `os.replace`로 교체한다. 교체 뒤에도 실행 중인 Metabase는 교체 전 파일을 조회했다. 2026-09-29 카드 47은 99,540행과 오늘 27건을 반환해 1건 뒤처졌다. Metabase를 재시작한 뒤에는 99,541행과 28건으로 일치했다.

## 결론

원인은 두 겹이다. 둘 다 Metabase 프로세스 안에 있다. Mount와 WSL2는 원인이 아니다.

1. DuckDB JDBC의 **프로세스 전역 Instance Cache**가 원인이다. 같은 경로 문자열로 여는 모든 Connection이 DuckDB Instance 하나를 공유한다. 그 Instance는 처음 열 때의 파일 Descriptor(inode)를 계속 쥐고 있다. 그래서 Connection을 **새로 열어도** 옛 inode를 본다. Instance는 그 경로의 Connection이 전부 닫혀야 해제된다.
2. Metabase `sql-jdbc`의 **C3P0 Pool**은 Connection을 계속 살려 둔다. 그래서 1번의 "전부 닫힘" 조건이 거의 오지 않는다.

그 결과 Metabase를 재시작하거나 Connection 상세 정보가 바뀌기 전까지는 stale 상태가 **무기한** 이어진다. ADR-012의 Consequences 문장 "열린 Connection은 재연결 전까지 이전 Export를 볼 수 있다"는 실제보다 약하게 서술돼 있다. 재연결만으로는 해소되지 않는다.

## 근거

### (1) Driver와 Connection 유지 방식

- Plugin은 `metabase/plugins/duckdb.metabase-driver.jar`다. `metabase-plugin.yaml` 기준 버전은 `1.4.1.0`, DuckDB JDBC는 `1.5.5`다. `parent: sql-jdbc`이므로 Metabase 표준 C3P0 Pool을 그대로 쓴다. `metabase/driver/duckdb.clj`는 Pool을 따로 제어하지 않는다. `do-with-connection-with-options`는 부모의 `do-with-resolved-connection` 위에 init SQL과 TimeZone만 얹는다.
- `org/duckdb/DuckDBDriver.class`의 Property 설명은 다음과 같다.
  - `jdbc_instance_cache`: "Reuse the process-wide DuckDB instance for the same database path. Disabling creates an isolated instance per connection and may cause local file lock conflicts." 기본값은 사용이다.
  - `jdbc_pin_db`: "Do not close the DB instance after all connections to it are closed." 기본값은 미사용이다. 따라서 Connection이 전부 닫히면 Instance가 해제된다.
- Metabase `v0.63.16.1`의 `metabase/driver/sql_jdbc/connection$fn__111620.class` 상수를 확인했다. Pool 속성은 `maxIdleTime` 3×60×60초(3시간), `maxIdleTimeExcessConnections` 5×60초, `minPoolSize`, `initialPoolSize`, `testConnectionOnCheckout TRUE`다. Env로 조정할 수 있는 것은 `jdbc-data-warehouse-max-connection-pool-size`, checkout timeout, unreturned timeout뿐이다. idle 시간은 Env로 바꿀 수 없다.
  - 최소 Pool 크기가 유지되므로 Connection 하나가 늘 살아 있다. 조회가 3시간 안에 한 번이라도 들어오면 idle 만료도 일어나지 않는다. idle 만료가 일어나도 C3P0가 보충 Connection을 먼저 열면, 그 Connection은 Instance Cache에 걸려 계속 옛 inode를 본다.
- 같은 Class 묶음에서 `db->pooled-connection-spec`은 `jdbc-spec-hash`를 참조한다. Connection을 꺼낼 때 Database details hash가 바뀌었으면 Pool을 다시 만든다는 뜻이다. `invalidate-pool-for-db!`와 `destroy-pool!`도 있다. 대안 A는 이 경로를 이용한다. Bytecode 근거이므로 실제 동작은 구현 Gate에서 실측으로 확인한다.
- 실행 중 컨테이너(06:36:11Z 재시작)에서 Java PID 20의 `fd 45 -> /serving/mart.duckdb`를 확인했다. 재시작 뒤라 `(deleted)`가 아니다.

### (2) Compose Mount와 WSL2

- `./data/serving:/serving:ro`는 **디렉터리** Bind다(`docker inspect` 결과 `Type=bind`, `Source=.../data/serving`). 디렉터리 Bind에서는 새로 여는 경로가 교체된 새 파일을 가리킨다. 파일 Bind였다면 Mount 시점 inode가 고정돼 Metabase 프로세스만 재시작해서는 해소되지 않는다. 컨테이너를 다시 만들어야 한다. 현재 구성이 맞다. **파일 Bind로 바꾸면 안 된다.**
- 호스트 `data/serving/mart.duckdb`와 컨테이너 `/serving/mart.duckdb`의 inode는 둘 다 `813663`이다. 컨테이너 안 Mount는 `/dev/sdd on /serving type ext4`다. Docker Desktop WSL2 Backend는 배포판과 같은 Kernel과 ext4를 공유한다. 따라서 rename과 inode 의미가 그대로 전달된다. 9p나 drvfs 변환은 없다.
- 주의할 점이 하나 있다. 저장소를 `/mnt/c` 아래(drvfs)로 옮기면 rename, lock, inode 의미가 달라진다. 저장소는 WSL ext4에 두어야 한다.

### (3) 읽기 전용 재현

scratchpad에서 Python `duckdb 1.5.5`로 재현했다. 프로젝트 파일은 건드리지 않았다. Python Client도 같은 C++ `DBInstanceCache`를 쓴다.

```text
c1 before replace 99540 inode 935653
replaced, new inode 935654
c1 after replace (same conn) 99540
c2 new connect while c1 open (instance cache) 99540   ← 새 Connection도 stale
c3 cursor/duplicate of c1 99540
open fds ['.../repro/mart.duckdb (deleted)']          ← 옛 inode를 쥔 fd
c4 after all closed 99541                             ← 모두 닫은 뒤에야 새 파일
```

관측값 99,540→99,541과 같은 형태다. JDBC 쪽은 위 Property 설명으로 같은 의미를 확인했다. 실행 중 Metabase에서의 재현은 Export(쓰기)가 필요해 하지 않았다.

## 대안 비교

| 대안 | 내용 | 해소 여부 | 판정 |
| --- | --- | --- | --- |
| A. 버전 경로와 재지정 | Export가 `exports/{export_id}.duckdb` Hardlink를 추가로 만든다. 새 Task가 Metabase API로 `database_file`을 새 경로로 바꾼다. | 확정적으로 해소된다. 경로 문자열이 달라져 Instance Cache에 걸리지 않고, details hash가 바뀌어 Pool도 다시 만들어진다. | **권고** |
| B. details 무의미 변경 | 경로는 그대로 두고 다른 detail을 바꿔 Pool만 무효화한다. | 불확실하다. 같은 경로이므로 옛 Connection이 하나라도 남으면 Instance Cache에 걸린다. | 기각 |
| C. 수동 재시작 Runbook | Publish 뒤 사람이 `docker compose --profile bi restart metabase`를 실행한다. | 사람이 기억할 때만 해소된다. 재시작 중 1~2분 동안 서비스가 중단된다. | 임시 운영 규칙으로만 유지 |
| D. Airflow가 컨테이너 재시작 | Airflow에 docker.sock을 Mount한다. | 해소된다. | 기각. 권한이 Host root 수준으로 커지고 서비스가 중단된다. |
| E. Connection 설정 | `jdbc_instance_cache=false`로 두고 Pool을 조정한다. | 해소되지 않는다. Pool의 Connection이 각자 옛 inode를 유지한다. idle 시간은 Env로 바꿀 수 없다. Instance마다 `memory_limit 1GB`가 붙어 최대 Pool 크기만큼 메모리가 늘어난다. | 기각 |
| F. In-place 쓰기 | 같은 inode에 덮어쓴다. | 불가능하다. Metabase의 읽기 전용 Instance가 파일 Lock을 잡고 있어 쓰기 Lock 획득에 실패한다. 획득하더라도 Buffer Cache가 옛 Page와 새 Page를 섞어 읽는다. 원자성도 잃는다. | 기각 |
| G. PostgreSQL Serving | PRD §19의 대안이다. | 해소된다. | 과잉이다. Driver와 Lock Gate는 통과했으므로 전환 조건이 아니다. |

## 권고안 A 설계

1. **Export**(`src/serving/export.py`): 기존 순서는 그대로 둔다. CHECKPOINT와 WAL 부재 확인이 끝나면 `os.link(build_path, serving_root/"exports"/f"{export_id}.duckdb")`를 먼저 실행한다. 그다음 기존 `os.replace(build_path, paths.serving)`를 실행한다. 두 경로는 같은 inode를 가리키므로 디스크를 추가로 쓰지 않는다. 디렉터리 fsync는 두 디렉터리 모두에 한다. 성공한 뒤에는 `exports/`에서 최근 3개만 남기고 나머지를 unlink한다. Metabase가 아직 옛 inode를 쥐고 있어도 unlink는 안전하다. `mart.duckdb`는 다른 소비자를 위해 계속 정본 경로로 남긴다.
2. **새 Task** `repoint_metabase_serving`(`export_serving_mart` 다음에 실행): 순서는 다음과 같다.
   1. `GET /api/database/{id}`를 호출한다.
   2. 응답의 `details.database_file`만 `/serving/exports/{export_id}.duckdb`로 바꾼다.
   3. `PUT /api/database/{id}`를 호출한다.
   4. Metabase 경유 native Query로 Serving Manifest의 `export_id`를 읽어 방금 만든 Export와 일치하는지 검증한다.
   - 설정은 `METABASE_URL`, `METABASE_API_KEY`, `METABASE_SERVING_DATABASE_ID`다. API Key는 `X-API-Key` Header에만 쓴다. ADR-012 원칙이다.
   - 설정이 없으면 Task를 Skip한다. `bi` Profile은 선택 사항이다.
   - 실패해도 Serving 파일은 이미 맞으므로 Rollback하지 않는다. Summary에 `metabase_repoint_status`(`SUCCESS`/`SKIPPED`/`FAILED`)를 추가한다.
3. **구현 Gate**(실측):
   - PUT 직후 카드 47의 Dashboard API 결과가 새 Export 행 수와 같아야 한다.
   - 카드의 Field ID(예: 81, 116)와 Dashboard 매핑이 변하지 않아야 한다. details 변경 뒤 Sync가 Table과 Field를 이름으로 다시 연결하는지 확인하는 것이다.
   - 재지정을 두 번 연속 해도 결과가 같아야 한다.
   - Metabase를 재시작해도 새 경로가 유지돼야 한다.
   - 하나라도 실패하면 A를 중단하고 architect에 보고한다.

A를 구현하기 전까지는 대안 C를 운영 규칙으로 명문화한다. 현재 `docs/bi/evidence.md`와 `dashboards.md`에 실제로 하고 있는 절차가 적혀 있다.

## PRD와 ADR 영향

- **PRD(v1.18 → v1.19, 경미)**
  - §17의 "Metabase는 마지막 성공 Mart만 읽는다"는 현재 구현이 **위반**하고 있다. 요구사항 문장은 그대로 두고 A로 충족시킨다.
  - §17.1 Publish 경계 표에는 Serving 경로가 없다. `data/serving/mart.duckdb`, `data/serving/exports/{export_id}.duckdb`(최근 3개 보존)와 Export 뒤 Metabase 재지정 단계를 추가한다.
  - 수정 규칙대로 기존 문서는 `PRD.bak/`으로 옮긴다.
- **ADR 신규 019 `serving-file-visibility-for-metabase`**
  - ADR-012의 Decision(Mart 전용 Serving 파일, 읽기 전용 디렉터리 Mount)은 **유지**한다.
  - 012의 Consequences 문장이 부정확한 점과 Validation 항목 "교체 뒤 재연결 가시성"이 재시작으로만 확인됐을 가능성을 019에서 바로잡는다.
  - 012에는 "019로 보완" 한 줄만 추가한다.
  - Accepted Gate 기록인 012 본문은 고쳐 쓰지 않는다.
- **범위**: Phase 10 BI 산출물의 신선도 결함이다. 새 기능이 아니다. 다만 Airflow와 Metabase API 사이에 결합이 새로 생긴다. 착수 여부는 lead가 결정한다.
