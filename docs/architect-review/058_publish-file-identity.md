# 058. Publish 파일 Identity로 Export·Recovery 재Hash 대체

- 날짜: 2026-10-16
- 상태: **채택·구현 완료.** 사용자 개발 승인(2026-10-16) 후 구현, reviewer 승인, 실운영 Publish run `539746b5-32f5-4df4-a57e-dbf442db18f8`로 검증.
- 판정: **채택.**
  - Publish 시 Build File의 `(st_ino, st_mtime_ns)`를 기록한다.
  - Serving Export와 Stale `PUBLISHING` 복구는 Mart 재Hash 대신 이 Identity를 비교한다.
- 결정: 사용자 결정(2026-10-16)에 따른다. 항목별 결정 주체는 각 절에 적었다.
- 계기: Warehouse PUBLISH 흐름 감사. 대상은 `src/warehouse/publish.py`, DAG `publish_mart`, Serving Handoff다.
- 관련:
  - 감사 보고서 `warehouse-publish-audit.md`와 구현 계획 `2026-10-16-publish-identity-plan.md`. 둘 다 git이 추적하지 않는 Scratchpad 문서다. 이 문서가 판정의 정본이다.
  - PRD v1.23 §12.8, §17.1, ADR-016. 변경 계약은 PRD v1.24에 반영한다.

## 1. 감사 결론

핵심 설계는 유지한다. PRD §17.1, ADR-016과 일치하고 근거가 있다.

```text
Published 복사 → Build File에 dbt build → CHECKPOINT → Hash → os.replace → fsync
+ Postgres 단일 활성 Mutex
```

문제는 Swap 주변의 검사다. 같은 내용을 여러 번 증명하고, 그러면서도 Serving 범위 전체를 증명하지 못한다.

측정 기준은 현재 `data/warehouse/warehouse.duckdb`(42MB)다.

| 항목 | 결과 |
| --- | --- |
| `mart_logical_hashes` | 453,254 Row, 5.3s |
| `mart_row_counts` | 0.02s |
| Hash 대상 | `MART_HASH_TARGETS` 9개 (dimensions 5, facts 4) |
| Serving 대상 | 13개 (dimensions 5, facts 4, metrics 4) |

## 2. 판정

### 2.1 Identity = `(st_ino, st_mtime_ns)`로 Export 재Hash 대체 (P1-1)

- 기존: `export_serving_mart`가 Published File을 다시 Hash해 기록 Hash와 비교한다.
- 문제:
  - Publish가 방금 한 Full Scan(5.3s)을 반복한다.
  - 9개만 덮는다. Customer 대시보드가 읽는 `metrics.rpt_*` 4개는 증명 없이 Serving된다.
  - PRD §17.1의 Export 흐름은 이 검사를 요구하지 않는다.
- 결정:
  - `mark_publishing`에서 Build File의 `os.stat` 결과를 기록한다. 새 Column은 `published_inode BIGINT`, `published_mtime_ns BIGINT`(NULL 허용)다.
  - Export는 복사 전에 stat을 비교한다. Swap 직전에 한 번 더 비교한다.
  - Publish의 Logical Hash는 유지한다. PRD가 요구하는 증거이고 `changed_relations`의 입력이다.
- 근거:
  - `os.replace`는 inode와 mtime을 보존한다. Swap 뒤 Published File의 stat이 기록값과 같으면 "그 Run의 파일"이다.
  - 비교 비용은 O(1)이다. 파일 전체를 덮으므로 metrics도 포함된다.
  - Export가 Identity 검사를 하는 실제 이유는 하나다. Mutex는 `PUBLISHED`에서 풀린다. 그래서 `publish_mart`와 `export_serving_mart` 사이에 CLI Publish가 파일을 바꿀 수 있다.
  - Swap 직전 재검사는 복사 도중 교체(TOCTOU)를 막는다.
- **inode 단독은 반려한다.** ext4는 해제된 inode를 재사용한다.
  - 예: 이후 Publish가 두 번 있었다. 그 뒤 오래된 Run을 `--publish-run-id`로 지정해 CLI Export를 한다. 이 경우 inode만 같은 다른 파일이 통과할 수 있다.
  - mtime_ns를 함께 비교하면 이 경우를 거부한다.
- Fail-closed 성질:
  - 읽기 전용 DuckDB 연결과 `shutil.copyfile`은 mtime을 바꾸지 않는다.
  - RW 열기, 복사본 교체, Relink는 Identity를 바꾼다. 이 경우 Export는 실패하고 잘못된 데이터를 내보내지 않는다.

### 2.2 Recovery 재Hash 대체 (P1-2)

- 기존: `recover_incomplete_publishes`가 Stale `PUBLISHING` Run마다 `_published_matches`로 전체 Logical Hash를 다시 계산한다.
- 문제: `PUBLISHING` 구간은 `os.replace` + fsync + UPDATE 1회뿐이다. `os.replace`는 원자적이다. "Swap이 일어났는가"에 내용 Hash는 과하다.
- 결정: 2.1과 같은 기록값을 쓴다.
  - Published File Identity가 기록값과 같으면 `PUBLISHED`로 닫는다.
  - 그 외는 `FAILED(UNKNOWN_ERROR)`, abandoned로 닫는다. Identity NULL도 여기에 포함된다.
  - `_published_matches`, `target_for` 사용, `duckdb` import를 `publish.py`에서 제거한다.
- 근거: Stuck Run의 Build File이 남아 있는 동안 그 inode는 해제되지 않는다. Mutex가 다른 Run을 막는다. 따라서 Recovery 구간에서 Identity 오판정은 생기지 않는다.
- 2.1과 2.2는 기록값 하나를 공유한다. 함께 하거나 둘 다 하지 않는다.

### 2.3 Export의 Published WAL 검사 제거, Prepare 검사 유지 (P2-4)

- 기존: Published WAL 검사가 `prepare_warehouse_build`와 `export_serving_mart` 시작부에서 두 번 돈다.
- 결정 (사용자):
  - Export 시작부 검사(`ValueError("Published warehouse has a WAL")`)를 제거한다.
  - Prepare 검사(`PublishedWalError` → `CONFIGURATION_ERROR`)는 유지한다. PRD §17.1 계약이다.
  - Serving Build의 CHECKPOINT 뒤 WAL 검사는 유지한다. 이는 Serving 내부 검사다.
- 근거:
  - Published File은 CHECKPOINT된 Build의 `os.replace`로만 써진다. WAL은 누군가 손으로 RW로 열 때만 생긴다.
  - RW 열기는 mtime을 바꾼다. 그러면 2.1의 Identity 검사가 실패한다. Export 쪽 WAL 검사는 Identity 검사에 포함된다.

### 2.4 Hash 범위 = dimensions·facts, PRD v1.24에 문서화 (P2-3)

- 문제:
  - `MART_HASH_TARGETS`는 9개다. Serving은 13개다.
  - Run Record의 `changed_relations`와 `mart_row_counts`는 `metrics.rpt_*` 변경을 보여주지 않는다.
  - Serving Schema 목록이 세 곳에 따로 있다: `SERVING_SCHEMAS`, `dbt_project.yml`의 `+schema`, `MART_HASH_TARGETS`.
- 결정 (architect): 코드를 바꾸지 않는다. PRD v1.24에 "Hash 범위는 dimensions·facts 9개, metrics는 Hash하지 않는다, Serving 동일성은 파일 Identity가 보장한다"를 명시한다.
- 근거:
  - 2.1 이후 이 차이는 증거 범위의 차이일 뿐 정합성 문제가 아니다.
  - metrics를 Hash에 넣으려면 order_by Key가 필요하다. 첫 실행에서 모든 Relation이 "changed"로 한 번 나온다. 저장된 Hash 비교와 Reliability Test에도 영향이 있다.
  - Reconciliation이 metrics Hash를 요구하지 않는 한 비용이 더 크다.
- ADR-016은 수정하지 않는다. PRD v1.24 변경 요약이 ADR-016의 "Hash 비교로 확정" 항목을 대체한다고 적는다.

### 2.5 Hash와 Row Count 단일 Scan (P3-5)

- 문제: `hash_cursor_rows`가 이미 `(digest, row_count)`를 반환한다. 그런데 `mart_logical_hash`가 count를 버린다. `publish_build`는 `mart_row_counts`로 한 번 더 읽는다.
- 결정: `mart_hashes_and_row_counts(warehouse_path, targets) -> tuple[dict[str, str], dict[str, int]]`를 추가한다. `publish_build`가 이 함수를 쓴다.
  - `mart_logical_hashes`는 이 함수에 위임한다.
  - `mart_logical_hash`, `mart_row_counts`는 유지한다. Reliability Harness와 여러 Test가 쓴다.
- 근거: 이득은 코드 정리다. `count(*)`는 0.02s라 성능 이득은 없다.

### 2.6 `src/warehouse/duckdb_files.py` 분리 (사용자 결정)

- 문제:
  - `src/serving/export.py`가 `src.warehouse.publish`에서 `DEFAULT_WAREHOUSE_ROOT`, `fsync_directory`, `wal_path`를 import한다. Serving이 Publish 구현에 역방향으로 의존한다.
  - `_checkpoint`가 `publish.py`와 `export.py`에 중복돼 있다.
- 결정: 좁은 범위의 새 모듈을 만든다. 포함 대상:
  - `DEFAULT_WAREHOUSE_ROOT`, `wal_path`, `fsync_directory`
  - `checkpoint`: 두 `_checkpoint`를 대체한다.
  - `FileIdentity`, `file_identity`: 2.1의 새 Helper다.
- 범위 제한:
  - `publish.py`는 기존 이름을 계속 import 가능하게 둔다. DAG와 `tests/test_published_mart_queryable.py`가 그 경로를 쓴다.
  - `export.py`의 `src.warehouse.publish_metadata` 의존은 남긴다. Metadata 조회는 Serving의 정당한 입력이다.
  - 2.1 이후 `export.py`에는 `mart_hash` import가 필요 없다.

## 3. 변경하지 않는 항목

### 3.1 `--dbt-vars` 중복 검증 제거 (P3-7): 변경 없음

감사 항목을 정정한다. 제거할 수 있는 중복이 없다.

- `main()`의 `dbt_vars_json(variables)` 호출은 DB 연결 전에 예약 Key를 검사하는 유일한 지점이다.
  - `tests/test_warehouse_publish_cli.py::test_invalid_dbt_vars_are_rejected_before_publish`가 이를 고정한다.
  - `{bronze_schemas: {}}`가 `PostgresSettings.from_environment` 전에 exit code 2를 내야 한다.
- `run_dbt_build` 안의 호출은 검증이 아니라 직렬화다.
- `isinstance` 검사는 JSON `null` 입력을 막는 데 필요하다.

### 3.2 유지 항목

| 항목 | 유지 근거 |
| --- | --- |
| `ensure_publish_metadata`의 매 실행 DDL (P3-6) | Repo 공통 관용구다. Lease·Generator Metadata도 같다. 비용이 작다. 2.1의 Migration(`ADD COLUMN IF NOT EXISTS`)이 이 동작에 의존한다 |
| DAG 3-Task 분리 (`prepare_warehouse_build` / `dbt_build` / `publish_mart`) | 실패 지점별 재시도와 관측이 분리된다 |
| CLI `publish_warehouse` 조합 | DAG와 같은 함수를 묶어 수동 실행 경로를 제공한다 |
| Postgres 단일 활성 Mutex | Airflow와 CLI 프로세스 경계를 넘는다 (ADR-016) |
| 실패 Build 격리, 최근 3개 보존 | 현재 약 100MB다. 실패 원인 분석 근거다 |
| `changed_relations` | Publish 간 변경 증거다 |
| 명시적 CHECKPOINT 뒤 WAL 검사 | Swap 대상 파일에 WAL이 없음을 보장한다 |

## 4. 열린 항목: Stale BUILDING 1시간 차단

- 현상: 비정상 종료한 `BUILDING` Run은 `PUBLISH_STALE_AFTER=1h` 동안 다음 Publish를 막는다. Prepare 재시도는 총 약 3분이다. 그래서 다음 DAG Run이 실패한다.
- 판정: 이번 범위에서 바꾸지 않는다. 운영에서 실제로 발생하면 다시 검토한다.
- 재검토 시 고려 사항: 임계값을 줄이면 정상 장시간 Build를 abandoned로 닫을 위험이 있다. 그래서 dbt build 소요 시간 측정이 먼저다.

## 5. 배포 전 조건

- 아래 결과가 0이어야 한다.
  ```sql
  SELECT count(*) FROM mart_publish_runs WHERE status IN ('BUILDING', 'PUBLISHING');
  ```
  - 이유: 배포 전 Run은 Identity가 NULL이다. 남아 있는 Stale `PUBLISHING` Run은 2.2 규칙에 따라 Swap 여부와 무관하게 `FAILED`로 닫힌다.
- Identity가 NULL인 기존 `PUBLISHED` Run은 CLI Export 대상이 될 수 없다. 명시적 오류로 실패한다. 배포 뒤 새 Publish를 한 번 실행해야 Export가 가능하다.
- Migration은 `ensure_publish_metadata`가 첫 Prepare에서 적용한다. 작은 Table에 짧은 `ACCESS EXCLUSIVE` Lock이 걸린다.
- 실 DB에서 `DROP COLUMN`을 하지 않는다. 실 Run의 Identity가 지워진다.

## 6. 유지되는 계약 (반영 후)

- `mart_publish_runs.published_inode`, `published_mtime_ns`는 Swap 직전 Build File의 stat이다. 둘 다 NULL 허용이다.
- Export와 Recovery의 동일성 판단은 `(st_ino, st_mtime_ns)` 쌍으로 한다. inode 단독은 금지다.
- Logical Hash는 Publish 시 증거로만 계산한다. 범위는 `MART_HASH_TARGETS`(dimensions·facts 9개)다.
- Published WAL 검사는 Prepare에서만 한다.

## 7. 재검토 조건

- Published File이 다른 Filesystem으로 옮겨지거나, `os.replace` 이외의 방법으로 교체되는 경우. Identity 보존 전제가 깨진다.
- Reconciliation이나 대시보드 검증이 metrics Relation의 Hash 증거를 요구하는 경우. 2.4를 다시 판단한다.
