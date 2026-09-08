# Phase 3. Incremental Ingestion

> 상태: 구현 완료 · `orders` 최초 Bronze 적재 완료
> Milestone: 2 — Data Platform Core  
> 선행 Phase: [Phase 2. Deterministic Generator](phase-02-deterministic-generator.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.4](../../PRD_v1.4.md)

## 목표

PostgreSQL의 신규·변경 데이터를 고정된 Composite Cursor 범위로 추출하고 검증한 뒤, SeaweedFS의 불변 Parquet Bronze 또는 Quarantine으로 안전하게 Commit한다. 실패, 재시도, 동시 실행에서도 누락과 중복이 없어야 한다.

## 구현 전략

6개 Table을 동시에 구현하지 않는다. `orders` 하나로 Metadata부터 Watermark 전진까지의 Vertical Slice를 완성하고 실패 시나리오를 통과시킨 뒤 나머지 Table로 일반화한다.

```text
Phase 3A: orders Vertical Slice
    ↓
Phase 3B: 6개 Table 일반화
    ↓
Phase 3C: Quarantine과 Reject Threshold
    ↓
Phase 3D: 시간 제한 잠금, CAS, 수집 중 원천 변경 차단 동시성
```

## 핵심 계약

- 추출 범위는 `(watermark_before, extract_upper_bound]`다.
- 고정 Upper Bound와 Composite PK 전체 Tie-breaker를 사용한다.
- 전체 Table을 메모리에 적재하지 않고 Page 단위로 Parquet에 기록한다.
- 빈 Batch는 Object 없이 `SUCCESS_NO_DATA`로 종료하고 Watermark를 유지한다.
- Manifest의 `object_state=VERIFIED`는 Object 검증 상태일 뿐 Commit 상태가 아니다.
- Metadata의 `bronze_objects.status=COMMITTED`만 Commit Source of Truth다.
- Table Commit과 Watermark CAS는 하나의 Metadata Transaction이다.
- Commit된 Bronze Object는 수정, 덮어쓰기, 삭제하지 않는다.
- Row 오류는 Threshold 이하에서 격리하고 Valid Row는 Commit하지만, Batch 오류는 Watermark를 유지한다.

## 선행 조건

- Generator가 `updated_at` 단조 증가와 Source 제약조건을 준수한다.
- PostgreSQL Source에 Table별 증분 Index가 존재한다.
- SeaweedFS, `pipeline_metadata`, `commerce_source` 연결 설정이 준비됐다.
- Phase 2의 Late Arrival과 Lease 충돌 Fixture를 사용할 수 있다.

## Table별 Cursor

| Table            | Cursor Tuple                                 |
| ---------------- | -------------------------------------------- |
| `customers`      | `(updated_at, customer_id)`                  |
| `products`       | `(updated_at, product_id)`                   |
| `sellers`        | `(updated_at, seller_id)`                    |
| `orders`         | `(updated_at, order_id)`                     |
| `order_items`    | `(created_at, order_id, order_item_id)`      |
| `order_payments` | `(updated_at, order_id, payment_sequential)` |

초기 Watermark는 논리적 `-infinity`와 Table별 최소 Key로 해석한다.

## Phase 3A. `orders` Vertical Slice

### 3A-1. Metadata Schema

- [x] `P3-01` `watermarks`, `pipeline_runs`, `bronze_objects`, `quarantine_batches` DDL 구현
- [x] Metadata 상태/제약조건/Index 구현
- [x] Table Commit용 Metadata Transaction 구현
- [x] 상태 전이와 Rollback 테스트

### 3A-2. 고정 범위와 Pagination

- [x] `P3-02` `orders` Composite Cursor Query 구현
- [x] `P3-03` 고정 Upper Bound와 Keyset Pagination 구현
- [x] `INGESTION_PAGE_SIZE` 설정과 기본값 50,000 적용
- [x] 마지막 Page Cursor를 다음 Page Lower Bound로 사용
- [x] Empty Batch 처리

Query 의미:

```sql
WHERE cursor_tuple > :watermark_before
  AND cursor_tuple <= :extract_upper_bound
ORDER BY cursor_tuple
LIMIT :page_size
```

### 3A-3. Local Parquet

- [x] `P3-04` Page 단위 Arrow Table → Local Parquet Writer 구현
- [x] 명시적 Arrow Schema와 UTC microsecond Timestamp 적용
- [x] Decimal `decimal128(14,2)` 적용
- [x] Zstandard Compression과 Row Group Target 128K 적용
- [x] Bronze 기술 컬럼 추가

`orders`에는 Decimal Source Column이 없으므로 금액 Column은 `order_items`,
`order_payments` 공통 Bronze Writer에서 `decimal128(14,2)`로 실제 기록·검증한다.

기술 컬럼:

```text
_batch_id
_run_id
_ingested_at
_source_table
_schema_version
```

### 3A-4. SeaweedFS 호환성

- [x] `P3-05` Bucket Create/PUT/GET/HEAD/LIST/DELETE Smoke Test
- [x] Path-style S3 연결 검증
- [x] Overwrite 동작을 관측하되 Commit Object에는 사용하지 않음
- [x] DuckDB에서 업로드한 Parquet Read 검증
- [x] `bronze/`, `quarantine/`, 필요 시 `_staging/` Prefix 구성

S3 Prefix는 별도 디렉터리 생성 없이 Object Key로 표현한다. Smoke Test의 Overwrite와 삭제는
Commit Prefix가 아닌 `_smoke/{uuid}/`에서만 수행한다.

### 3A-5. Commit Protocol

- [x] `P3-06` Manifest 생성 및 `object_state=VERIFIED` 기록
- [x] `P3-07` `bronze_objects=COMMITTED` Metadata Commit 구현
- [x] `P3-08` Watermark Compare-and-swap 구현
- [x] 최종 Bronze 객체 HEAD/Size/Checksum/Row Count 검증
- [x] Final Key 충돌 실패와 Empty Batch에서 Watermark 유지 검증

Commit 순서:

```text
원천 데이터 동시성 잠금
→ 테이블별 수집 잠금
→ 범위 고정
→ Extract / Validate
→ Local Parquet
→ 최종 Bronze 객체 Upload
→ HEAD / Checksum / Row Count 검증
→ Manifest VERIFIED
→ Metadata Transaction
   - bronze_objects COMMITTED
   - pipeline_runs SUCCESS
   - watermark CAS Update
→ 테이블별 수집 잠금 해제
→ 모든 Table 종료 후 원천 데이터 동시성 잠금 해제
```

현재 `orders` 구현은 최종 Bronze 객체에 조건부 PUT을 사용하고, 기존 Key는 업로드 전에
명시적으로 거부한다. Parquet는 Local 파일에서 SHA-256을 스트리밍 계산해 업로드한 뒤,
HEAD의 Size·사용자 Metadata Checksum과 재수신한 Parquet의 Row Count를 확인한다. Metadata
Commit 전 Manifest는 `VERIFIED` 상태로만 작성하며, Commit 실패 후 남은 Object의 정리는
Phase 3D `P3-23` Orphan Reconciliation 범위다.

## Phase 3B. 전체 Table 일반화

- [x] `P3-09` Table별 Cursor/PK/Arrow Schema Config 정의
- [x] `P3-10` `customers`, `products`, `sellers` Mutable Extract 확장
- [x] `P3-11` `order_items` Append-oriented Composite Cursor 확장
- [x] `P3-12` `order_payments` Composite Cursor 확장
- [x] `P3-13` Parent Key Snapshot을 이용한 Broken Reference 검증
- [x] `P3-14` 6개 Table Batch Identity와 재실행 정책 구현

Batch Identity:

```text
batch_id       = {dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}
run_id         = UUIDv4 per execution attempt
table_batch_id = {batch_id}__{source_table}
```

동일 Table Batch가 이미 Commit됐고 Range/Schema Version이 같으면 재사용한다. 다르면 `BATCH_IDENTITY_CONFLICT`로 실패한다.

`TableIngestionRequest`와 `ingest_table()`은 6개 Table 모두에 공통으로 사용한다. 각 실행은
설정 기반 Snapshot·검증·Quarantine·Local Parquet·최종 Bronze 객체·VERIFIED Manifest·Metadata
Transaction·Watermark CAS를 동일한 순서로 처리한다. 기존 `ingest_orders()`는 이 공통 서비스의
호환 래퍼다.

## Phase 3C. Quarantine

- [x] `P3-15` 검증 Pipeline 구현
- [x] `P3-16` 결정적 `_record_id`와 Quarantine Parquet 구현
- [x] `P3-17` Error Count/Object Key를 `quarantine_batches`에 기록
- [x] `P3-18` `MAX_REJECT_RATE=5%` Threshold 처리
- [x] `P3-19` Corruption 주입 전후 Count 분리

검증 순서:

```text
Schema / 필수 Column
→ Type
→ Key NULL
→ Batch Duplicate
→ Source Status Domain
→ Numeric Range
→ Broken Reference
→ Cursor 범위
```

처리 원칙:

- Row 오류는 Quarantine으로 보내고 Valid Row 처리를 계속한다.
- Reject Rate가 Threshold 이하이면 Valid Row를 Commit하고 Watermark를 Upper까지 전진한다.
- Threshold 초과, Schema 누락, Cursor 위반은 Batch를 실패시키고 Watermark를 유지한다.
- Metadata에는 Raw Payload를 저장하지 않는다.

구현상 Schema 누락 또는 Cursor 범위 위반은 `SourceContractError`로 즉시 Batch를 실패시킨다.
일반 Row 오류는 Valid Row와 분리해 Quarantine Parquet에 기록한다. `_record_id`는
`table_batch_id + 추출 순번`의 UUIDv5이며, Quarantine 기술 시각은 `_detected_at`이다.
Quarantine Object·Manifest를 검증한 뒤 그 Object Key·Error Count는 Bronze Object·성공 Run·
Watermark CAS와 같은 Metadata Transaction에서 기록한다.

`CorruptionPlan`은 Extract 후 Validation 직전의 In-memory 복제본에만 5종 오류를 결정적으로
주입한다. Source DB는 변경하지 않으며, 결과는 `rows_extracted`, `rows_corrupted`,
`rows_rejected`, `rows_valid`로 분리해 반환·검증한다.

## Phase 3D. 동시성, Catalog, Schema Version

- [x] `P3-20` 테이블별 수집 잠금 획득/TTL/Renewal/Release 구현
- [x] `P3-21` 원천 데이터 동시성 잠금으로 Warehouse 수집 중 원천 변경 차단
- [x] `P3-22` 동시 Extract에서 Lock 실패 Run의 Source Read/Object 생성 차단
- [x] `P3-23` Orphan 탐지와 안전한 Reconciliation 구현
- [x] `P3-24` Metadata COMMITTED Object만 읽는 메타데이터 기반 Bronze 파일 목록 구현
- [x] `P3-25` `schema_version=1` 및 지원 Version Contract 구현
- [x] `P3-26` 미지원 Version을 `SOURCE_CONTRACT_ERROR`로 차단

원천 데이터 동시성 잠금은 Generator와 Warehouse 사이의 원천 변경을 막고, 테이블별 수집 잠금은 Warehouse Run끼리 동일 Watermark를 갱신하는 것을 막는다.

`warehouse_source_freeze()` Context Manager는 하나의 Warehouse Run이 공유할 `WAREHOUSE` 원천 데이터
동시성 잠금을 획득·해제한다. `LeaseHeartbeat`는 5분마다 원천 데이터 동시성 잠금과 테이블별 수집 잠금을
갱신하고, 갱신 실패를 Snapshot·Upload·Commit 직전에 수집 실패로 전파한다. `ingest_table()`과
`ingest_orders()`는 전달된 `WAREHOUSE` 원천 데이터 동시성 잠금을 fencing으로 확인하고, 없으면 Table 실행
동안 자체 Lease를 획득한다. 이어서 Watermark 행의 테이블별 수집 잠금을 확보한 뒤에만 Snapshot을 열며,
Lease 실패 Run은 `FAILED`로 기록하고 Source Read·최종 Bronze 객체 생성을 하지 않는다.

`sync_bronze_catalog()`은 `bronze_objects.status='COMMITTED'` Object만 DuckDB
`control.bronze_files`로 교체 동기화한다. Version 1만 지원하며, 미지원 Version은
`SOURCE_CONTRACT_ERROR`로 DuckDB 쓰기 전에 차단한다.

`find_orphan_candidates()`는 Bronze 최종 경로 Parquet와 VERIFIED Manifest가 있으나 Metadata
`COMMITTED`가 없는 Object를 Manifest 유무와 함께 찾는다. `reconcile_orphan()`은 Manifest가 유실된
후보, 이미 `FAILED`로 처리된 Run, Quarantine Object가 있는 Batch를 자동 복구하지 않는다. 나머지는
Manifest HEAD SHA-256, Bronze HEAD Size·SHA-256, 재수신 Parquet Row Count·Logical Hash, 범위·Schema
Version, `RUNNING` Run, 현재 Watermark를 모두 검증한 뒤 Bronze Object·Pipeline Run·Watermark를
하나의 Transaction으로 복원한다. Reject Quarantine는 Bronze보다 먼저 게시해 안전한 자동 복구 조건을
보장한다.

## 실제 수집 실행 계획

수집 서비스와 통합 테스트는 구현·검증됐고, 수동 실행 CLI로 `orders` 최초 Bronze 적재까지 완료했다.
통합 테스트가 만든 Object와 Metadata는 테스트 종료 시 정리되지만, 아래 최초 실행 Object와 Metadata는
유지한다.

### 0. 최초 실행 기록

2026-09-08 UTC에 아래 CLI를 실행했다.

```bash
uv run python -m src.ingestion \
  --dag-id manual_initial_load \
  --logical-date 2026-09-08T00:00:00Z \
  --tables orders
```

| 항목          | 값                                                                                                         |
| ------------- | ---------------------------------------------------------------------------------------------------------- |
| Batch ID      | `manual_initial_load__20260908T000000Z`                                                                    |
| Run ID        | `e9fca84e-5bee-4b13-8cfb-29d92057adc4`                                                                     |
| 결과          | `SUCCESS`, 추출·Valid·Loaded 각각 99,441행, Reject 0행                                                     |
| Bronze Object | `bronze/orders/ingestion_date=2026-09-08/batch_id=manual_initial_load__20260908T000000Z/data.parquet`      |
| Manifest      | 같은 경로의 `manifest.json`, `object_state=VERIFIED`                                                       |
| Metadata      | `bronze_objects=COMMITTED`, Watermark가 `2026-09-03T00:00:00Z / fffe41c64501cc87c801fd61db3f6244`까지 전진 |

Manifest·재수신 Parquet·PostgreSQL Metadata의 Row Count 99,441행과 Watermark 범위가 일치하는 것을
확인했다.

### 1. 실행 CLI 추가

`src/ingestion/__main__.py` CLI는 아래 책임을 수행한다.

- `.env`의 PostgreSQL·SeaweedFS 설정을 읽는다.
- `--dag-id`, `--logical-date`, `--tables` 입력을 검증한다.
- 한 번의 `warehouse_source_freeze()` 안에서 선택 Table의 `ingest_table()`을 순서대로 실행한다.
- Table별 상태·행 수·Bronze Object Key를 출력한다.
- 모든 성공 실행 뒤 `sync_bronze_catalog()`을 호출할 수 있게 한다.

이후 새 수동 Batch 실행은 아래 명령 형식을 사용한다.

```bash
uv run python -m src.ingestion \
  --dag-id manual_initial_load \
  --logical-date 2026-09-08T00:00:00Z \
  --tables orders
```

`batch_id`는 `{dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}`로 결정되고 최종 Bronze 객체 Key에
포함된다. 정상 재실행은 같은 Batch를 재사용하지만, 최종 Bronze 객체만 남은 실패는 새 Batch로
덮어쓰지 않고 Orphan 절차를 먼저 점검한다.

### 2. 사전 점검

1. `docker compose ps`에서 `postgres`, `seaweedfs`가 `running` 또는 `healthy`인지 확인한다.
2. Source Seed가 완료됐고 `.env`의 PostgreSQL·SeaweedFS·Bucket 설정이 있는지 확인한다.
3. 선택한 `dag_id`와 Logical Date의 Commit 이력이 없는지 `pipeline_runs`, `bronze_objects`에서 확인한다.
4. 첫 실행은 `orders` 하나로 제한한다.

### 3. `orders` 최초 실행과 검증

첫 실행은 Snapshot → Validate → Local Parquet → Quarantine → Bronze Object → VERIFIED Manifest
→ Metadata CAS 순서로 수행한다. 성공 후 다음 증적이 모두 일치해야 한다.

- SeaweedFS: `bronze/orders/.../data.parquet`, `manifest.json`
- `pipeline_runs`: `SUCCESS`
- `bronze_objects`: `COMMITTED`
- `watermarks`: 실행의 `extract_upper_bound` Cursor
- Manifest·Parquet·`bronze_objects`의 Row Count와 Hash

Manifest JSON을 먼저 확인하고, 이후 Python S3 Client로 Parquet 앞 5행을 조회한다. SeaweedFS의
`8333` 포트는 인증된 S3 API이므로 브라우저로 직접 열면 `AccessDenied`가 정상이다.

반복 조회는 아래 스크립트를 사용한다.

```bash
uv run python scripts/inspect_bronze.py \
  --source-table orders \
  --batch-id manual_initial_load__20260908T000000Z \
  --logical-date 2026-09-08T00:00:00Z \
  --limit 5
```

스크립트는 `manifest`, Parquet `schema`, 전체 `parquet_row_count`, 앞부분 `sample_rows`를 JSON으로
출력한다.

### 4. 전체 Table 실행과 Catalog 동기화

`orders`가 확인된 뒤 다음 순서로 전체 Table을 실행한다.

```text
customers → products → sellers → orders → order_items → order_payments
```

이 실행은 하나의 원천 데이터 동시성 잠금 안에서 수행해 Generator의 원천 변경을 막는다.
모든 실행이 성공하면 `bronze_objects.status='COMMITTED'` Object만 DuckDB
`control.bronze_files` Catalog에 동기화한다.

### 5. 실패 대응

| 상황                                       | 처리                                                                                      |
| ------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `SUCCESS_NO_DATA`                          | 정상 종료다. Object와 Watermark는 바뀌지 않는다.                                          |
| Lease 충돌                                 | Source Read·최종 Bronze 객체 생성 없이 `FAILED`로 기록한다. 활성 실행 종료 후 재시도한다. |
| Reject Rate 초과                           | Batch 실패이며 Watermark는 유지한다. Quarantine을 점검한다.                               |
| 최종 Bronze 객체만 남음                    | Orphan 후보를 검증한다. Reject 없는 `RUNNING` Run만 자동 복구 대상이다.                   |
| Manifest 유실·Quarantine 존재·`FAILED` Run | 자동 복구하지 않고 수동 점검한다.                                                         |

## Object 구조

```text
commerce-lake/
├── _staging/{run_id}/{table}/
├── bronze/{table}/ingestion_date=YYYY-MM-DD/batch_id={batch_id}/
│   ├── data.parquet
│   └── manifest.json
└── quarantine/{table}/ingestion_date=YYYY-MM-DD/batch_id={batch_id}/
    ├── records.parquet
    └── manifest.json
```

Manifest와 Metadata의 필드 및 상태 정의는 PRD Sections 10과 12를 그대로 따른다. Manifest에는 Credential, Local Absolute Path, Raw Payload, `status=COMMITTED`를 기록하지 않는다.

## 범위 밖

- Airflow Operator 내부 구현
- dbt Staging/Mart 모델
- Dashboard
- 운영 장애 Runbook의 최종 문서화

Phase 3에서는 Framework-independent Python Pipeline을 완성하고 Phase 4는 이를 호출만 한다.

## 테스트와 Gate

| AC    | 시나리오                   | 합격 증거                                                 |
| ----- | -------------------------- | --------------------------------------------------------- |
| AC-02 | Incremental                | Cursor 조건과 실제 변경 Key Set 일치                      |
| AC-03 | 동일 Batch 3회             | Object/Key Count/Mart 이전 단계 Logical Hash 동일, 중복 0 |
| AC-04 | Upload 전후 실패           | 실패 중 Watermark 유지, 성공 후 Upper로 전진              |
| AC-05 | 동일 Timestamp가 Page 초과 | Page/Batch 경계 누락·중복 0                               |
| AC-06 | 동시 Extract               | 하나만 Lease 획득, 나머지는 Object 없이 Conflict          |
| AC-08 | 5종 Corruption             | Reject와 Error Code 일치, Source 무오염                   |
| AC-20 | Mutation Cursor Safety     | 과거 Event의 새 Mutation을 다음 Batch가 정확히 1회 수집   |
| AC-21 | Generator vs Warehouse     | Lease 중 Source 변경 0, Parent/Child 불일치 0             |
| AC-23 | Manifest vs Metadata       | VERIFIED만 있고 COMMITTED가 없으면 Catalog Read 0         |
| AC-24 | Schema Version             | 미지원 Version을 dbt 이전에 차단                          |

추가 단위/통합 테스트:

- Initial/Empty/Single-page/Multi-page Cursor
- Composite Key 동률과 Page 경계
- Local Write, Upload, HEAD, Manifest, Metadata 각 실패 지점
- Reject 0건, Threshold 이하, Threshold 초과
- Lease 만료와 Renewal
- Batch Identity Conflict와 Already Committed Skip
- Orphan 일치/불일치 Reconciliation

## 요구사항 추적

| 구분 | 연결 항목                                                        |
| ---- | ---------------------------------------------------------------- |
| PRD  | Sections 8~12 증분, Identity, Bronze, Validation, Metadata       |
| ADR  | ADR-001/003 SeaweedFS와 Parquet Bronze                           |
| ADR  | ADR-004/005 Table별 증분과 Composite Watermark                   |
| ADR  | ADR-010 메타데이터 기반 Bronze 파일 목록                         |
| ADR  | ADR-013 원천 변경/수집 동시성                                    |
| ADR  | ADR-014 메타데이터 커밋 상태 기준                                |
| ADR  | ADR-015 Bronze Schema Evolution                                  |
| FR   | FR-03~08 Incremental/Watermark/Lease/Metadata/Bronze/Idempotency |
| FR   | FR-13 Backfill 입력 경계                                         |
| FR   | FR-16/17 Quarantine와 Bronze Schema Version                      |

## 산출물

- Metadata Physical Schema와 상태 전이 로직
- Table Config 기반 Incremental Extractor
- Page 기반 Parquet Writer
- SeaweedFS Client와 Compatibility Smoke Test
- Bronze/Quarantine Manifest와 Commit Protocol
- Lease/CAS/Reconciliation 구현
- 메타데이터 기반 Bronze 파일 목록
- Schema Version Contract
- AC-02~06, 08, 20, 21, 23, 24 자동 테스트

## 파일·폴더별 변경 요약

| 경로                                                             | 변경      | 요약                                                                                                                                                                                                                                                                               |
| ---------------------------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sql/metadata/004_create_ingestion_metadata.sql`                 | 생성      | Watermark, 수집 실행, Bronze Object, Quarantine Batch의 상태·제약조건·Index를 추가했다.                                                                                                                                                                                            |
| `src/ingestion/metadata.py`                                      | 생성·수정 | 초기 Watermark, RUNNING/FAILED/SUCCESS_NO_DATA/SKIPPED_ALREADY_COMMITTED 상태 전이와 Bronze·Quarantine Object·Run·Watermark CAS의 원자적 Commit을 추가했다.                                                                                                                        |
| `src/ingestion/config.py`                                        | 생성      | `INGESTION_PAGE_SIZE` 환경 설정과 기본값 50,000 검증을 추가했다.                                                                                                                                                                                                                   |
| `src/ingestion/orders.py`                                        | 생성      | 동일 Read-only Snapshot에서 `orders` Upper Bound 고정과 Keyset Pagination을 추가했다.                                                                                                                                                                                              |
| `src/ingestion/bronze.py`                                        | 생성·수정 | 기존 `orders` Writer와 함께 6개 Table의 명시적 Arrow Schema·기술 Column·Zstandard Local Parquet Writer를 추가하고, 재수신 Parquet Byte에서도 PK 기준 Logical Hash를 다시 계산하게 했다.                                                                                            |
| `src/ingestion/storage.py`                                       | 생성      | SeaweedFS Path-style S3 Client, Bucket 준비, 최종 Bronze 객체의 조건부 PUT·HEAD·Parquet 검증을 추가했다.                                                                                                                                                                           |
| `src/ingestion/tables.py`                                        | 생성      | 6개 Source Table의 전체 PK Tie-breaker, 증분 Cursor, Raw-compatible Arrow Schema와 공통 Bronze 기술 Column 계약을 추가했다.                                                                                                                                                        |
| `src/ingestion/extract.py`                                       | 생성·수정 | 등록된 Table Config만 사용해 동일 Read-only Snapshot, 고정 Upper Bound, Composite Keyset Page를 읽고 Corruption 복제본도 원 Cursor로 검증할 수 있게 했다.                                                                                                                          |
| `src/ingestion/references.py`                                    | 생성      | Child Page의 Orders·Products·Sellers Parent Key를 같은 Snapshot Connection에서 검사하며 공통 수집 서비스가 결과를 Reject로 연결한다.                                                                                                                                               |
| `src/ingestion/batch.py`                                         | 생성      | DAG·UTC Logical Date 기반 6개 Table Batch Identity와 Commit 범위·Schema 재사용/Conflict 판정을 추가했다.                                                                                                                                                                           |
| `src/ingestion/validation.py`                                    | 생성      | Source Schema·Type·Key·Batch Duplicate·Status Domain·Numeric·Broken Reference·Cursor 범위를 검사해 Valid/Reject를 분리하고 Schema·Cursor 계약 오류를 Batch Failure로 전환한다.                                                                                                     |
| `src/ingestion/lease.py`                                         | 생성·수정 | Watermark 기반 테이블별 수집 잠금의 획득·30분 TTL·Fencing·Release와 여러 Table이 공유하는 원천 데이터 동시성 잠금 Context Manager를 추가하고, 5분 Heartbeat로 두 잠금을 자동 갱신하게 했다.                                                                                        |
| `src/ingestion/catalog.py`                                       | 생성      | Metadata의 COMMITTED Bronze Object만 DuckDB `control.bronze_files`로 원자적으로 동기화한다.                                                                                                                                                                                        |
| `src/ingestion/schema.py`                                        | 생성      | 지원 Bronze Schema Version 1을 정의하고 미지원 Version을 `SOURCE_CONTRACT_ERROR`로 차단한다.                                                                                                                                                                                       |
| `src/ingestion/orphan.py`                                        | 생성·수정 | Manifest 유실 Object도 수동 처리 후보로 탐지하고, Manifest·Object SHA-256/크기/Row Count/Logical Hash·Schema·RUNNING Run·Watermark가 모두 일치하며 Quarantine가 없는 Orphan만 트랜잭션으로 재조정하게 했다.                                                                        |
| `src/ingestion/quarantine.py`                                    | 생성      | `table_batch_id + 추출 순번` 결정 ID, `_detected_at`, Raw Payload·오류 Code Quarantine Parquet Writer와 5% Reject Threshold 정책을 추가했다.                                                                                                                                       |
| `src/ingestion/corruption.py`                                    | 생성      | Extract 후 Validation 전 복제본에 NULL Key, Invalid Status, 음수값, Type, Broken Reference 5종 오류를 결정적으로 주입한다.                                                                                                                                                         |
| `src/ingestion/manifest.py`                                      | 생성·수정 | Credential·Local 경로·Metadata Commit 상태 없이 Bronze와 Quarantine의 `VERIFIED` Object 증적을 기록하는 정규화 JSON Manifest를 추가했다.                                                                                                                                           |
| `src/ingestion/service.py`                                       | 생성·수정 | 6개 Table 공통 수집 서비스를 추가해 검증·Quarantine·최종 Bronze 객체·Manifest·Metadata CAS를 연결하고, Heartbeat·Lease 충돌 FAILED 기록·공유 원천 데이터 동시성 잠금을 적용했다. Quarantine는 Bronze보다 먼저 게시하며 `ingest_orders()`도 공유 Lease를 받는 호환 래퍼로 유지했다. |
| `src/ingestion/__main__.py`                                      | 생성      | `--dag-id`, `--logical-date`, `--tables`로 선택 Table을 원천 데이터 동시성 잠금 안에서 수동 적재하고 결과 JSON을 출력하는 CLI를 추가했다.                                                                                                                                          |
| `scripts/inspect_bronze.py`                                      | 생성      | Source Table·Batch·Logical Date로 SeaweedFS Bronze Manifest, Parquet Schema·행 수·샘플 행을 조회하는 운영 보조 스크립트를 추가했다.                                                                                                                                                |
| `compose.yaml`                                                   | 수정      | SeaweedFS 4.45 S3 API Service, 영속 Volume과 Master Healthcheck를 추가했다.                                                                                                                                                                                                        |
| `.env.example`                                                   | 수정      | SeaweedFS Host 환경 변수 Key를 추가했다.                                                                                                                                                                                                                                           |
| `src/common/database.py`                                         | 수정      | 공통 환경 변수 Reader를 공개해 수집 설정도 로컬 `.env`를 사용할 수 있게 했다.                                                                                                                                                                                                      |
| `tests/ingestion/test_config.py`                                 | 생성      | Page Size 기본값과 유효하지 않은 환경 변수 값을 검증한다.                                                                                                                                                                                                                          |
| `tests/ingestion/test_bronze.py`                                 | 생성      | Local Parquet Schema, UTC microsecond Timestamp, 기술 컬럼, 압축·Row Group을 검증한다.                                                                                                                                                                                             |
| `tests/ingestion/test_storage.py`                                | 생성      | SeaweedFS 연결 설정과 Object Storage Prefix 계약을 검증한다.                                                                                                                                                                                                                       |
| `tests/ingestion/test_tables.py`                                 | 생성      | 6개 Table Cursor·PK·Arrow Schema와 금액 Decimal 정밀도 계약을 검증한다.                                                                                                                                                                                                            |
| `tests/ingestion/test_batch.py`                                  | 생성      | 6개 Table 표준 Batch ID와 Cursor·Schema 재사용 범위 계약을 검증한다.                                                                                                                                                                                                               |
| `tests/ingestion/test_validation.py`                             | 생성      | Source 검증 오류와 Valid/Reject 분리, Cursor 범위 계약을 검증한다.                                                                                                                                                                                                                 |
| `tests/ingestion/test_quarantine.py`                             | 생성      | 결정적 Quarantine Record, Raw Payload·오류 집계와 Reject Threshold를 검증한다.                                                                                                                                                                                                     |
| `tests/ingestion/test_corruption.py`                             | 생성      | 5종 In-memory Corruption의 Source 무오염, Valid/Reject·Error Code Count 분리를 검증한다.                                                                                                                                                                                           |
| `tests/ingestion/test_table_bronze.py`                           | 생성·수정 | `orders` 외 Decimal Table도 공통 Bronze Writer와 실행 독립 Logical Hash를 사용하고, 재수신 Byte Hash가 Local Artifact Hash와 같은지 검증한다.                                                                                                                                      |
| `tests/ingestion/test_catalog.py`                                | 생성      | COMMITTED Snapshot만 DuckDB Catalog에 남기고 미지원 Schema Version이 쓰기 전에 차단되는지 검증한다.                                                                                                                                                                                |
| `tests/ingestion/test_schema.py`                                 | 생성      | Version 1 지원과 미지원 Version의 `SOURCE_CONTRACT_ERROR` 계약을 검증한다.                                                                                                                                                                                                         |
| `tests/ingestion/test_lease.py`                                  | 생성      | Heartbeat가 원천 데이터 동시성 잠금과 테이블별 수집 잠금을 함께 갱신하고 갱신 실패를 수집 흐름에 전달하는지 검증한다.                                                                                                                                                              |
| `tests/ingestion/test_orphan.py`                                 | 생성      | Manifest 유실 최종 경로 Parquet도 수동 처리 대상 Orphan 후보로 빠짐없이 탐지하는지 검증한다.                                                                                                                                                                                       |
| `tests/ingestion/test_cli.py`                                    | 생성      | 수동 수집 CLI의 UTC Logical Date 검증, 선택 Table 실행, 공유 원천 데이터 동시성 잠금과 결과 JSON 출력을 검증한다.                                                                                                                                                                  |
| `tests/ingestion/test_inspect_bronze.py`                         | 생성      | Bronze 조회 스크립트가 Manifest·Parquet 행 수·Schema·Sample을 읽고 음수 Sample 제한을 거부하는지 검증한다.                                                                                                                                                                         |
| `tests/ingestion/test_manifest.py`                               | 생성      | VERIFIED Manifest의 공개 필드와 Metadata Commit 경계를 검증한다.                                                                                                                                                                                                                   |
| `tests/integration/test_ingestion_metadata_integration.py`       | 생성      | 성공 Commit과 Watermark 충돌 시 Rollback·실패 상태 전이를 PostgreSQL에서 검증했다.                                                                                                                                                                                                 |
| `tests/integration/test_orders_incremental_integration.py`       | 생성      | `orders` Composite Cursor의 같은 Timestamp Page 경계와 Empty Range를 검증한다.                                                                                                                                                                                                     |
| `tests/integration/test_orders_bronze_integration.py`            | 생성      | 실제 Source Page가 하나의 Local Bronze Parquet으로 기록되는지 검증한다.                                                                                                                                                                                                            |
| `tests/integration/test_seaweedfs_s3_integration.py`             | 생성      | SeaweedFS S3 Lifecycle과 DuckDB Parquet Read 호환성을 검증한다.                                                                                                                                                                                                                    |
| `tests/integration/test_orders_ingestion_service_integration.py` | 생성·수정 | 실제 컨테이너에서 성공 Commit, Final Key 충돌, Empty Batch, Batch 재사용·범위 Conflict, Threshold 이하 Reject의 Quarantine·Metadata Commit, Generator와 원천 데이터 동시성 잠금 충돌 차단, Reject 0건 Orphan 복구와 Reject Orphan 거부를 검증한다.                                 |
| `tests/integration/test_table_ingestion_service_integration.py`  | 생성      | 실제 `customers`가 `orders`와 같은 공통 Bronze Commit Protocol로 수집되는지 검증한다.                                                                                                                                                                                              |
| `tests/integration/test_mutable_table_extraction_integration.py` | 생성      | 실제 `customers`·`products`·`sellers`의 설정 기반 고정 범위 Keyset 추출을 검증한다.                                                                                                                                                                                                |
| `tests/integration/test_child_table_extraction_integration.py`   | 생성      | 실제 `order_items`·`order_payments`의 전체 복합 PK Keyset Page 경계를 검증한다.                                                                                                                                                                                                    |
| `tests/integration/test_child_parent_references_integration.py`  | 생성      | 실제 Child Page가 동일 Snapshot의 모든 Parent Key를 참조하는지 검증한다.                                                                                                                                                                                                           |
| `tests/integration/test_validation_integration.py`               | 생성      | 실제 `orders` Snapshot Page가 Schema·Domain·Cursor 검증에서 Reject 없이 통과하는지 검증한다.                                                                                                                                                                                       |
| `tests/integration/test_ingestion_lease_integration.py`          | 생성      | 실제 PostgreSQL에서 테이블별 수집 잠금의 충돌·갱신·해제와 원천 데이터 동시성 잠금 해제를 검증한다.                                                                                                                                                                                 |
| `docs/phases/phase-03-incremental-ingestion.md`                  | 수정      | Phase 3A Decimal, Phase 3B 공통 Commit, Phase 3C Quarantine, Phase 3D 잠금·Catalog·Schema Contract 진행 상태와 실제 수동 수집 실행·검증·실패 대응 계획, 내부 용어의 한국어 표기를 기록했다.                                                                                        |
| `src/ingestion/`, `scripts/`, `tests/`                           | 수정      | Phase 3 관련 모듈·운영 스크립트·테스트의 docstring을 개조식 접두사 없이 짧은 일반 문장으로 통일했다.                                                                                                                                                                               |

## Definition of Done

- [x] `orders` Vertical Slice가 모든 실패 지점 테스트를 통과했다.
- [x] 같은 Framework가 6개 Table에 일반화됐다.
- [x] Commit되지 않은 Object는 Catalog에서 보이지 않는다.
- [x] 실패한 Table Watermark가 전진하지 않는다.
- [x] 동일 Batch 재실행이 중복 Object/Row를 만들지 않는다.
- [x] Reject와 Batch Failure 정책이 구분된다.
- [x] 원천 데이터 동시성 잠금과 테이블별 수집 잠금, Watermark CAS가 경쟁 조건을 차단한다.
- [x] Phase 3의 모든 AC가 재현 가능한 명령으로 통과한다.

## Portfolio Evidence

- Cursor/Index/고정 Upper Bound 설명과 실행 계획
- Watermark 실패 전후 Metadata 비교
- 동일 Batch 3회 Logical Hash
- Manifest VERIFIED와 Metadata COMMITTED 차이
- Orphan Reconciliation 성공/거부 사례
- Quarantine Record와 Error Count
- 원천 데이터 동시성 잠금과 테이블별 수집 잠금 동시성 Timeline
- Bronze Schema Version Contract Test

## 권장 Commit

```text
feat: implement reliable incremental ingestion
```

## 다음 Phase 인계

Phase 4는 이 Phase의 Python API와 상태 전이를 그대로 호출한다. DAG에 추출, 검증, Commit 로직을 복제하지 않는다.
