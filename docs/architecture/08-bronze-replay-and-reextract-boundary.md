# Bronze Replay와 Re-extract 입력 경계

> 대상 Task: `P6-24`
> 브랜치: `feature/phase6-remaining`
> 상태: lead 승인 완료 — B안 확정 (2026-09-18)
> 선행 설계: `docs/architecture/06-late-arrival-affected-keys-and-incremental.md`, `docs/architecture/07-incremental-full-refresh-logical-hash.md`

## 1. 문제

PRD 16장은 Backfill을 두 가지로 정의한다.

1. **Replay**: Commit된 Bronze를 다시 적용한다. Source를 읽지 않는다. 기본 경로다.
2. **Re-extract**: 명시한 Cursor 범위를 새로 추출한다.

지금 Warehouse에는 둘 중 어느 쪽의 입력 경계도 없다. `dbt/macros/bronze_source.sql`의 `bronze_source()`는 `control.bronze_files`의 전체 행을 조건 없이 읽는다. 따라서 Build 입력은 "Build 시점의 Catalog에 들어 있던 모든 Object"로 암묵 정의된다. 결과는 두 가지다.

- **재현 불가**: 어제 만든 Mart를 오늘 다시 만들 수 없다. 그 사이에 Bronze Object가 늘었기 때문이다. Hash가 달라도 결함 때문인지 입력이 늘어서인지 구분할 수 없다.
- **증명 불가**: 장애 복구로 Mart를 다시 만든 뒤, 어떤 Object 집합으로 만들었는지 사후에 보일 근거가 없다.

`P6-23`은 "같은 입력 위에서" 두 Build의 Hash를 비교한다. 그 "같은 입력"은 지금 Warehouse 파일을 복사해서 물리적으로 확보한다. 입력 경계를 선언으로 표현하는 수단은 여전히 없다.

## 2. 원칙

- 경계는 **선언**이다. Build 명령에 경계를 주면 그 경계 안의 Bronze만 입력이 된다.
- 경계는 **전 Staging에 하나로** 적용된다. Table마다 다른 경계를 허용하면 참조 무결성이 깨진다.
- 경계 Build는 **기록을 남긴다**. 어떤 경계로 몇 개 Object를 읽었는지 사후 조회할 수 있어야 한다.
- 경계는 **기본값이 없다**. 경계를 주지 않은 Build는 지금과 완전히 같이 동작한다.
- Replay는 **Source를 읽지 않는다**. Source를 읽는 순간 그것은 Re-extract다.

## 3. 경계 축 (D-1)

**결정: 경계 축은 `control.bronze_files.committed_at`의 상한 하나다. 변수명은 `bronze_as_of`이며 값은 ISO-8601 UTC Timestamp다. 경계는 상한 포함(`committed_at <= bronze_as_of`)이다.**

의미는 "그 시각에 Commit돼 있던 Bronze만으로 Mart를 다시 만든다"다. 이것이 Replay의 정의 그대로다.

### `batch_id` 구간을 쓰지 않는 이유

`_batch_id`는 `{dag_id}__{UTC %Y%m%dT%H%M%SZ}` 형식이다. `P6-22`의 재계산 경계는 이 값을 사전순으로 비교하는데, 그 비교가 시간순인 것은 **같은 `dag_id` 안에서만** 참이다. Replay 경계는 그 전제를 쓸 수 없다. 재수집(`membership_grain_rebaseline`), 수동 실행, Re-extract가 각각 다른 `dag_id`를 쓰고, Catalog 안에 그 결과가 섞여 있기 때문이다. `dag_id` 접두사가 다르면 사전순 상한은 시간 상한이 아니다.

`committed_at`은 `dag_id`와 무관하게 단조롭다. Metadata의 `bronze_objects.committed_at`에서 오고, Commit 이후 바뀌지 않으며, Catalog를 다시 동기화해도 같은 값이 다시 실린다(`src/ingestion/catalog.py:78` `_committed_entries`).

### 하한을 두지 않는 이유

Bronze 하한을 자르면 그 이전의 Dimension 이력이 통째로 사라진다. Fact는 자신보다 오래된 Version을 참조하므로 Unknown Key가 발생하고, 결과 Mart는 어떤 시점의 재현도 아니다. Replay는 "Bronze 전량을 as-of 시점까지 다시 적용"이지 "구간만 적용"이 아니다. 하한이 필요해 보이는 요구는 실제로는 Fact의 부분 재계산 요구이며, 그것은 `P6-22`의 영향 Key 경로가 이미 담당한다.

### Object Key 목록을 쓰지 않는 이유

임의의 Object 집합을 지정하면 경계가 시점 의미를 잃고, 참조 무결성이 깨진 집합도 지정할 수 있게 된다. 운영자가 실제로 지목하는 단위는 "언제 시점"이다.

## 4. 경계 전달 경로 (D-2)

**결정: dbt var `bronze_as_of` 하나로 전달한다. 기본값은 none이다.**

- `bronze_source()`의 Catalog 조회에 `and committed_at <= timestamptz '<값>'`을 추가한다. `validate_bronze_catalog()`도 같은 경계를 적용한다. 두 곳이 갈라지면 검증이 Build와 다른 집합을 본다.
- Model 인자로는 받지 않는다. 전역 var만 허용한다. Model마다 다른 경계를 줄 수 있으면 Fact와 Dimension이 서로 다른 시점을 보게 된다.
- 값 검증은 `on-run-start`에서 한다. UTC Offset이 없거나 파싱할 수 없으면 `REPLAY_BOUNDARY_ERROR:` 접두사로 Compiler Error를 낸다. 잘못된 값이 조용히 전체 Build로 흐르면 안 된다.

### 경계 증거 기록

`control.dbt_replay_boundary`에 Build마다 한 행을 남긴다.

| Column | 의미 |
| --- | --- |
| `invocation_id` | dbt Invocation ID |
| `bronze_as_of` | 적용한 경계. 무경계 Build는 NULL |
| `object_count` | 경계 안에서 실제로 읽은 Object 수 |
| `max_committed_at` | 경계 안 Object의 최대 `committed_at` |
| `full_refresh` | Full Refresh 여부 |
| `recorded_at` | 기록 시각 |

무경계 Build도 기록한다. "경계를 주지 않았다"도 증거이며, 무경계 Build만 기록이 비면 사후에 두 경우를 구분할 수 없다.

## 5. Replay Build 모드 (D-3)

**결정: `bronze_as_of`가 설정된 Build는 Full Refresh만 허용한다. Incremental이면 `on-run-start`에서 Compiler Error로 막는다.**

Incremental Fact는 이미 적재된 행 위에 영향 Key만 교체한다. 경계를 과거로 자르면 경계 밖 Batch가 만든 행은 입력에 없지만 Table에는 남는다. 결과는 "as-of 시점의 재현"도 아니고 "현재 상태"도 아닌, 어느 쪽으로도 설명할 수 없는 혼합이다. Hash 비교의 기준으로도 쓸 수 없다.

판정은 `flags.FULL_REFRESH`로 한다. 메시지는 `REPLAY_BOUNDARY_ERROR: bronze_as_of requires --full-refresh`로 고정한다.

## 6. Watermark 상호작용 (D-4)

**결정: 경계 Build는 재계산 Watermark를 전진시키지 않는다. `advance_processed_batch_watermark()` 진입부에서 `bronze_as_of`가 설정돼 있으면 Skip하고 사유를 Log한다.**

두 가지 이유다.

1. 경계 Build의 Staging 최대 `_batch_id`는 실제 최신보다 과거다. 현재 하한은 `max(processed_batch_id)`이므로 과거 값을 넣어도 하한은 움직이지 않는다. 즉 전진이 아니라 감사 잡음만 남는다.
2. 경계 Build는 "지금 상태"를 만들지 않는다. 그 결과를 처리 완료로 기록하면 이후 Incremental이 경계 이후 Batch를 건너뛴다.

### 운영 규칙

- **운영 복구의 기본 경로는 무경계 Replay다.** 즉 `bronze_as_of` 없이 `--full-refresh`로 돌린다. 이것도 Source를 읽지 않으므로 PRD의 Replay 정의를 그대로 만족한다. 경계 var는 복구 수단이 아니다.
- **경계 Replay는 진단과 재현 검증용이다.** 운영 Warehouse 파일이 아니라 사본에 대해 돌린다. 사본 사용 절차는 `P6-23`이 이미 쓰고 있는 방식(`docs/architecture/07-incremental-full-refresh-logical-hash.md` D-3)과 같다.

이 두 줄을 문서에 남기지 않으면, 경계 Replay로 운영 Mart를 덮고 Watermark만 그대로인 상태가 만들어진다. 그 상태는 경계 이후 변경분이 Mart에 영영 반영되지 않는 조용한 유실이다.

## 7. Re-extract 입력 경계 (D-5)

**결정: lead가 B안을 확정했다. Watermark Cursor를 지정 시각으로 되감는 공개 경로를 추가한다. `bronze_objects` Schema는 바꾸지 않는다.**

Re-extract는 "명시한 Cursor 범위를 새로 추출"이다. 지금은 불가능하다. `ingest_table()`은 Metadata Watermark의 Cursor만 하한으로 쓰고(`src/ingestion/service.py:276`), Cursor를 내리는 공개 수단이 없다. `src/ingestion/extract.py`는 `cursor_override`를 이미 받지만 Service 경로에서 도달할 수 없다. 따라서 선택지는 전량 재수집(`src/rebaseline.py`) 아니면 "현 Cursor 이후"뿐이다.

### 되감기 경계의 의미

`--reprocess-from T`는 "`cursor_timestamp_column >= T`인 Source 행을 다시 추출한다"를 뜻한다. 상한은 두지 않는다. 되감은 뒤의 수집은 평소와 같이 Snapshot 상한까지 읽는다. 상한을 두면 그 뒤 구간이 영구히 건너뛰어진 상태로 Watermark만 앞서 나간다.

CLI는 되감기 전용이다.

```bash
python -m src.ingestion.reprocess --tables customers --reprocess-from 2026-09-10T00:00:00Z
```

### 되감기 대상 Cursor를 Source에서 읽는 이유

저장 Cursor는 `(timestamp, 전체 PK)` Composite이고 하한 비교는 `>` 이다(`src/ingestion/extract.py:214`). `T`를 그대로 넣으면 `T` 시각의 행이 빠진다. Timestamp만 있고 Key가 없는 Cursor는 Metadata 제약(`watermarks_initial_cursor_check`)이 금지한다.

그래서 되감기 값은 **Source에 실재하는 Cursor**로 정한다. `cursor_timestamp_column < T`인 행 중 Cursor 순서로 가장 큰 행의 `(timestamp, PK)`다. 그런 행이 없으면 초기 Cursor(`timestamp=None`)로 되감는다. 결과적으로 `>= T`인 모든 행이 재추출 범위에 들어가고, Metadata에는 가짜 Key가 아니라 실재하는 Cursor만 남는다.

### 되감기의 안전 조건

1. **Table Lease 아래에서만 수행한다.** Lease 없이 되감으면 수집 중인 Run이 자기 하한이 바뀐 줄 모르고 Commit한다.
2. **과거 방향만 허용한다.** 현재 Cursor보다 크거나 같은 값으로의 되감기는 거부한다(`WatermarkRewindError`). 앞으로 감는 것은 되감기가 아니라 데이터 건너뛰기다.
3. **CAS로 쓴다.** `version`과 현재 Cursor를 함께 조건에 넣고 `version + 1`로 올린다. `assert_table_lease()`가 `version`을 Fencing Token으로 쓰므로(`src/ingestion/lease.py:197`), 되감기는 진행 중인 다른 Run의 Lease를 자동으로 무효화한다. 이것은 의도한 동작이다.
4. **되감기는 추출하지 않는다.** 되감기 CLI는 Metadata만 바꾼다. 재추출은 운영자가 별도 `dag_id`로 기존 수집 CLI를 실행해서 수행한다. 두 단계를 나누면 되감기만 하고 멈춘 상태를 점검할 수 있다.

### 재추출분이 Warehouse에 반영되는 경로

별도 `dag_id`로 수집하면 새 `batch_id`의 Bronze Object가 Commit된다. 같은 Business Key의 행이 Bronze에 여러 Version 존재하게 되지만, `current_bronze_records()`가 `updated_at desc, _ingested_at desc, _batch_id desc`로 최신 Version만 고른다. 새 `batch_id`는 재계산 경계 위에 있으므로 `P6-22`의 영향 Key 계산이 해당 Key를 집어내고, Fact는 `DELETE + INSERT`로 교체한다.

즉 **Warehouse에 Re-extract 전용 분기는 없다**. 재추출분은 Late Arrival과 같은 경로로 흐른다. 이것이 `reprocess_id` Column을 추가하지 않는 근거다. Column을 추가해도 Warehouse가 그것으로 분기할 일이 없고, `bronze_objects` Schema 변경은 Phase 3 재작업을 부른다.

### 감사

되감기는 Metadata에 흔적을 남긴다. `watermarks.version`이 오르고 `updated_at`이 갱신된다. 재추출 Run은 `pipeline_runs`에 별도 `batch_id`와 `dag_id`로 남고, 그 Run이 만든 Object의 `watermark_before`가 되감긴 Cursor를 그대로 담는다(`bronze_objects.watermark_before`). 따라서 "어디까지 되감아 무엇을 다시 넣었는지"는 기존 Table만으로 재구성된다. 새 감사 Table을 만들지 않는다.

## 8. 검증 (D-6)

`P6-23`의 `src/warehouse/mart_hash.py`를 그대로 재사용한다. 새 Hash 구현을 만들지 않는다.

1. **경계가 실제로 자른다**: 두 Batch를 수집한 뒤 첫 Batch의 `committed_at`을 `bronze_as_of`로 주고 Full Refresh한다. 둘째 Batch에서만 등장한 Key가 Mart에 없어야 한다.
2. **시점 재현성 (핵심)**: 첫 Batch만 있는 상태에서 Full Refresh해 Hash `H1`을 얻는다. 둘째 Batch를 수집한다. `bronze_as_of`를 첫 Batch 시각으로 주고 다시 Full Refresh한 Hash가 `H1`과 같아야 한다.
3. **무경계 동등성**: `bronze_as_of`를 현재 시각으로 준 Build와 경계를 주지 않은 Build의 Hash가 같아야 한다.
4. **Guard**: 경계를 준 채 Incremental로 돌리면 `REPLAY_BOUNDARY_ERROR`로 실패한다. 경계 Build 뒤 `control.dbt_processed_batch`의 행 수가 늘지 않는다.
5. **증거**: 경계 Build 뒤 `control.dbt_replay_boundary`에 해당 `invocation_id` 행이 있고 `object_count`가 경계 안 Object 수와 같다.

2번이 AC-07("같은 범위 Full Refresh와 Key별 값/Hash 동일")의 Warehouse 쪽 증거다.

## 9. 범위 밖

- Bronze Object 자체의 보존 기간·삭제 정책.
- 경계를 Airflow DAG Parameter로 노출하는 작업. `P6-25`가 DAG 실행 경계를 다룰 때 함께 본다.
- Mart Grain 계약과 구현의 불일치(`docs/architect-review/002_mart-grain-contract-drift.md`). lead가 이번 브랜치 범위 밖으로 보류 결정했다.
