# Phase 3. Incremental Ingestion

> 상태: In Progress
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
Phase 3D: Lease, CAS, Source Freeze 동시성
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

- [ ] `P3-04` Page 단위 Arrow Table → Local Parquet Writer 구현
- [ ] 명시적 Arrow Schema와 UTC microsecond Timestamp 적용
- [ ] Decimal `decimal128(14,2)` 적용
- [ ] Zstandard Compression과 Row Group Target 128K 적용
- [ ] Bronze 기술 컬럼 추가

기술 컬럼:

```text
_batch_id
_run_id
_ingested_at
_source_table
_schema_version
```

### 3A-4. SeaweedFS 호환성

- [ ] `P3-05` Bucket Create/PUT/GET/HEAD/LIST/DELETE Smoke Test
- [ ] Path-style S3 연결 검증
- [ ] Overwrite 동작을 관측하되 Commit Object에는 사용하지 않음
- [ ] DuckDB에서 업로드한 Parquet Read 검증
- [ ] `bronze/`, `quarantine/`, 필요 시 `_staging/` Prefix 구성

### 3A-5. Commit Protocol

- [ ] `P3-06` Manifest 생성 및 `object_state=VERIFIED` 기록
- [ ] `P3-07` `bronze_objects=COMMITTED` Metadata Commit 구현
- [ ] `P3-08` Watermark Compare-and-swap 구현
- [ ] Final Object HEAD/Size/Checksum/Row Count 검증
- [ ] 각 실패 지점에서 Watermark가 유지되는지 검증

Commit 순서:

```text
Global Source Lease
→ Table Lease
→ 범위 고정
→ Extract / Validate
→ Local Parquet
→ Final Object Upload
→ HEAD / Checksum / Row Count 검증
→ Manifest VERIFIED
→ Metadata Transaction
   - bronze_objects COMMITTED
   - pipeline_runs SUCCESS
   - watermark CAS Update
→ Table Lease 해제
→ 모든 Table 종료 후 Global Source Lease 해제
```

## Phase 3B. 전체 Table 일반화

- [ ] `P3-09` Table별 Cursor/PK/Arrow Schema Config 정의
- [ ] `P3-10` `customers`, `products`, `sellers` Mutable Extract 확장
- [ ] `P3-11` `order_items` Append-oriented Composite Cursor 확장
- [ ] `P3-12` `order_payments` Composite Cursor 확장
- [ ] `P3-13` Parent Key Snapshot을 이용한 Broken Reference 검증
- [ ] `P3-14` 6개 Table Batch Identity와 재실행 정책 구현

Batch Identity:

```text
batch_id       = {dag_id}__{logical_date_utc:%Y%m%dT%H%M%SZ}
run_id         = UUIDv4 per execution attempt
table_batch_id = {batch_id}__{source_table}
```

동일 Table Batch가 이미 Commit됐고 Range/Schema Version이 같으면 재사용한다. 다르면 `BATCH_IDENTITY_CONFLICT`로 실패한다.

## Phase 3C. Quarantine

- [ ] `P3-15` 검증 Pipeline 구현
- [ ] `P3-16` 결정적 `_record_id`와 Quarantine Parquet 구현
- [ ] `P3-17` Error Count/Object Key를 `quarantine_batches`에 기록
- [ ] `P3-18` `MAX_REJECT_RATE=5%` Threshold 처리
- [ ] `P3-19` Corruption 주입 전후 Count 분리

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

## Phase 3D. 동시성, Catalog, Schema Version

- [ ] `P3-20` Table Lease 획득/TTL/Renewal/Release 구현
- [ ] `P3-21` Global Source Lease로 Warehouse Run 전체 Source Freeze
- [ ] `P3-22` 동시 Extract에서 Lock 실패 Run의 Source Read/Object 생성 차단
- [ ] `P3-23` Orphan 탐지와 안전한 Reconciliation 구현
- [ ] `P3-24` Metadata COMMITTED Object만 읽는 Bronze File Catalog 구현
- [ ] `P3-25` `schema_version=1` 및 지원 Version Contract 구현
- [ ] `P3-26` 미지원 Version을 `SOURCE_CONTRACT_ERROR`로 차단

Global Lease는 Generator와 Warehouse 사이의 Source Mutation을 막고, Table Lease는 Warehouse Run끼리 동일 Watermark를 갱신하는 것을 막는다.

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
| ADR  | ADR-010 Metadata-backed Bronze Catalog                           |
| ADR  | ADR-013 Source Mutation/Extract 동시성                           |
| ADR  | ADR-014 Metadata Commit Authority                                |
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
- Metadata-backed Bronze File Catalog
- Schema Version Contract
- AC-02~06, 08, 20, 21, 23, 24 자동 테스트

## 파일·폴더별 변경 요약

| 경로 | 변경 | 요약 |
| ---- | ---- | ---- |
| `sql/metadata/004_create_ingestion_metadata.sql` | 생성 | Watermark, 수집 실행, Bronze Object, Quarantine Batch의 상태·제약조건·Index를 추가했다. |
| `src/ingestion/metadata.py` | 생성 | 초기 Watermark, RUNNING/FAILED 상태 전이, Object·Run·Watermark CAS의 원자적 Commit을 추가했다. |
| `src/ingestion/config.py` | 생성 | `INGESTION_PAGE_SIZE` 환경 설정과 기본값 50,000 검증을 추가했다. |
| `src/ingestion/orders.py` | 생성 | 동일 Read-only Snapshot에서 `orders` Upper Bound 고정과 Keyset Pagination을 추가했다. |
| `src/common/database.py` | 수정 | 공통 환경 변수 Reader를 공개해 수집 설정도 로컬 `.env`를 사용할 수 있게 했다. |
| `tests/ingestion/test_config.py` | 생성 | Page Size 기본값과 유효하지 않은 환경 변수 값을 검증한다. |
| `tests/integration/test_ingestion_metadata_integration.py` | 생성 | 성공 Commit과 Watermark 충돌 시 Rollback·실패 상태 전이를 PostgreSQL에서 검증했다. |
| `tests/integration/test_orders_incremental_integration.py` | 생성 | `orders` Composite Cursor의 같은 Timestamp Page 경계와 Empty Range를 검증한다. |
| `docs/phases/phase-03-incremental-ingestion.md` | 수정 | Phase 3 상태, P3-01~03 진행 상태와 파일별 변경 요약을 기록했다. |

## Definition of Done

- [ ] `orders` Vertical Slice가 모든 실패 지점 테스트를 통과했다.
- [ ] 같은 Framework가 6개 Table에 일반화됐다.
- [ ] Commit되지 않은 Object는 Catalog에서 보이지 않는다.
- [ ] 실패한 Table Watermark가 전진하지 않는다.
- [ ] 동일 Batch 재실행이 중복 Object/Row를 만들지 않는다.
- [ ] Reject와 Batch Failure 정책이 구분된다.
- [ ] Global/Table Lease와 Watermark CAS가 경쟁 조건을 차단한다.
- [ ] Phase 3의 모든 AC가 재현 가능한 명령으로 통과한다.

## Portfolio Evidence

- Cursor/Index/고정 Upper Bound 설명과 실행 계획
- Watermark 실패 전후 Metadata 비교
- 동일 Batch 3회 Logical Hash
- Manifest VERIFIED와 Metadata COMMITTED 차이
- Orphan Reconciliation 성공/거부 사례
- Quarantine Record와 Error Count
- Global/Table Lease 동시성 Timeline
- Bronze Schema Version Contract Test

## 권장 Commit

```text
feat: implement reliable incremental ingestion
```

## 다음 Phase 인계

Phase 4는 이 Phase의 Python API와 상태 전이를 그대로 호출한다. DAG에 추출, 검증, Commit 로직을 복제하지 않는다.
