# Phase 8 Reliability Scenarios 설계

> 작성: architect, 2026-09-20
> 기준: [phase-08](../../phases/phase-08-reliability.md), [ROADMAP](../../phases/ROADMAP.md), PRD v1.13 §8·§9·§10·§16·§18, [ADR 016](../../adr/016-publish-mart-via-warehouse-file-swap.md)
> 선행: Phase 7 종료(`335ef10`), `uv run pytest` → `185 passed, 80 skipped`
> 상태: lead 승인 대기

## 1. Phase 8의 성격

Phase 8은 기능 Phase가 아니다. Phase 0~7에서 구현한 불변 조건을 의도적으로 깨뜨리고, 탐지·복구·재검증을 재현 가능한 증거로 남긴다.

따라서 이 Phase의 산출물 비중은 다음과 같다.

| 산출물 | 비중 | 비고 |
| ------ | ---- | ---- |
| Runbook / Troubleshooting 문서 | 크다 | `docs/runbooks/`, `docs/troubleshooting/`가 현재 `.gitkeep`만 있다. |
| Reliability Harness (Fixture·수집기·판정) | 중간 | 새 테스트 코드. 프로덕션 코드 변경은 최소로 한다. |
| 신규 Scenario 테스트 | 중간 | 기존 테스트가 이미 덮는 부분이 많다. |
| 프로덕션 코드 변경 | 작다 | Fault 주입용 Seam이 없는 곳에만 추가한다. |

## 2. 현재 커버리지 조사 결과

`R-01`~`R-15`를 기존 테스트 자산과 대조했다. 이미 불변 조건을 검증하는 테스트가 있는 시나리오는 "증거 수집과 문서화"만 남고, 없는 시나리오는 "주입 + 검증 + 문서화"가 모두 필요하다.

| ID | 시나리오 | 기존 자산 | 남은 작업 |
| -- | -------- | --------- | --------- |
| `R-01` | Duplicate Batch | `test_orders_ingestion_service_integration.py::test_orders_service_reuses_a_committed_standard_batch_without_new_object_or_watermark`, `...::test_orders_service_keeps_watermark_when_final_object_already_exists` | 3회 반복 실행과 Object Key·Hash 동일 증거 수집 |
| `R-02` | Upload Failure | 없음 | Upload 단계 실패 주입 + Watermark·Metadata 불변 검증 |
| `R-03` | Metadata Failure | `test_ingestion_metadata_integration.py::test_watermark_conflict_rolls_back_object_and_success_state` | Upload 성공 후 Metadata Commit 실패 주입, VERIFIED Object의 Orphan 후보화와 Catalog Read 0 검증 |
| `R-04` | Watermark CAS Conflict | `test_ingestion_metadata_integration.py::test_watermark_conflict_rolls_back_object_and_success_state` | 동시 Run 2개 기준의 승자/패자 Timeline 증거 |
| `R-05` | Expired Lease | `test_ingestion_lease_integration.py`, `test_source_mutation_lease_integration.py::test_source_mutation_lease_is_exclusive_and_fences_a_stale_owner` | Lease 만료 후 Commit 거부 경로 증거 |
| `R-06` | Orphan Object | `test_orders_ingestion_service_integration.py::test_orphan_reconciliation_commits_an_interrupted_verified_bronze_object`, `...::test_orphan_reconciliation_rejects_a_batch_with_a_quarantine_object` | 성공/거부 비교표 문서화 |
| `R-07` | Broken Manifest | `tests/ingestion/test_manifest.py`(단위), `src/ingestion/manifest.py`의 Checksum·Version 검증 | Checksum/Range/Version 3종 변형의 자동 Commit 금지 통합 검증 |
| `R-08` | Late Order | `test_order_e2e_and_late_order_mart_integration.py::test_late_order_updates_the_past_business_date_mart` | Mart Diff 전후 증거 수집 |
| `R-09` | Late Payment | 없음(`test_subscription_payment_temporal_join_integration.py`는 시간 결합만 검증) | 늦은 결제가 연결 주문 구매일을 영향 범위에 넣는지 검증 |
| `R-10` | Customer 이력 변경 | `test_generator_transition_integration.py`, `dbt/tests/dim_customer_*` | 상태·등급 변경이 Version 구간과 사건 시점 결합에 반영되는 E2E 증거 |
| `R-11` | Missing Schedule | 없음. 두 DAG 모두 `schedule=None`, `catchup=False` | Airflow Catchup이 아닌 명시 Batch/Replay 회복 경로로 정의 |
| `R-12` | Backfill Replay | `test_replay_boundary_hash_integration.py::test_as_of_replay_reproduces_the_build_of_that_moment`, `test_incremental_full_refresh_hash_integration.py` | Source Read 0 증거와 Full Refresh Hash 비교표 |
| `R-13` | Re-extract | `test_reprocess_rewind_integration.py`(5건) | Re-extract 한계(Source 현재 상태 ≠ 과거 Snapshot) 명시 문서 |
| `R-14` | dbt Failure | `test_publish_gate_dbt_integration.py::test_dbt_test_failure_keeps_the_published_warehouse`, `test_warehouse_publish_integration.py`(5건) | Hash 전후 비교 증거 수집 |
| `R-15` | Source Connection Failure | 없음 | 연결 실패 주입 + Object 미생성·Watermark 미전진 검증 |

정리: **신규 검증 필요 = R-02, R-03(부분), R-07, R-09, R-10, R-11, R-15.** 나머지 8개는 Harness로 증거만 수집하고 문서화한다.

## 3. 설계 결정

### D1. Fault 주입은 테스트 전용 Seam으로 한다

**결정.** `src/`에 Fault Injection Hook을 추가하지 않는다. 주입은 `tests/reliability/` 안에서 기존 함수 경계를 Monkeypatch 하는 방식으로 한다.

근거:

- `src/ingestion/storage.py`, `metadata.py`, `extract.py`가 이미 모듈 수준 함수로 나뉘어 있어 주입점이 충분하다.
- 프로덕션 경로에 `if fault_mode:` 분기를 넣으면 Phase 7까지 지켜온 "정상 경로 오염 0" 원칙이 깨진다.
- `src/ingestion/corruption.py`는 예외다. 이미 프로덕션에 있고 Ingestion 검증 규칙의 입력 데이터를 만든다. Phase 8은 그것을 재사용만 한다.

주입점 목록:

| Scenario | 주입 대상 | 방식 |
| -------- | --------- | ---- |
| `R-02` | `src.ingestion.storage.upload_new_file` | 최초 호출에 `ClientError` 발생 |
| `R-03` | `src.ingestion.metadata`의 Commit Transaction 함수 | Object Upload 성공 후 예외 발생 |
| `R-07` | Manifest Object Bytes | Checksum·Row Range·Schema Version을 변형해 재업로드 |
| `R-15` | `src.ingestion.extract`의 Source 연결 획득 | `OperationalError` 발생 |

`R-03`과 `R-15`의 정확한 함수 이름은 구현 시 `metadata.py`·`extract.py`를 읽고 확정한다. 함수 경계가 주입에 부적합하면 그때 최소 Seam 추가를 architect에 다시 문의한다.

### D2. Harness는 `tests/reliability/` 아래 두고 pytest로 실행한다

**결정.** 별도 실행기를 만들지 않는다. Scenario 1개 = 테스트 함수 1개.

```text
tests/reliability/
├── __init__.py
├── harness.py        # 기준 Snapshot, 상태 수집, Evidence 직렬화
├── faults.py         # 주입 Context Manager
├── test_r01_r07_ingestion_commit.py
├── test_r08_r10_late_and_history.py
└── test_r11_r15_reprocess_and_source.py
```

Gate 환경 변수는 기존 규약을 따른다. `RUN_POSTGRES_INTEGRATION`, `RUN_SEAWEEDFS_INTEGRATION`을 쓰고, dbt가 필요한 Scenario(`R-08`~`R-10`, `R-12`, `R-14`)는 `RUN_DBT_PUBLISH_INTEGRATION`을 추가한다. 새 Flag를 만들지 않는다.

Marker는 `pytest.mark.integration`에 `pytest.mark.reliability`를 더한다. `pyproject.toml`에 Marker를 등록한다.

### D3. 상태 수집기는 기존 조회 자산을 조립한다

**결정.** 새 SQL을 만들지 않는다. `harness.py`의 `collect_state()`가 다음을 모아 하나의 Dataclass로 반환한다.

| 영역 | 출처 |
| ---- | ---- |
| Ingestion Run·Batch·Watermark 상태 | `sql/validation/observability_run_status.sql` |
| Object Key 목록 | `src.ingestion.storage.list_object_keys` |
| Orphan 후보 | `src.ingestion.orphan.find_orphan_candidates` |
| Publish Run 상태 | `src.warehouse.publish_metadata.get_publish_run` |
| Mart Hash·Row Count | `src.warehouse.mart_hash.mart_logical_hashes`, `mart_row_counts` |

Mart 목록은 `mart_hash.target_for`가 이미 받는 Relation 이름으로 주입한다. `P8-03`이 요구한 "Schema 이름을 설정으로 받는다"를 이 방식으로 만족한다.

### D4. Evidence는 파일로 남기고 문서에는 요약만 커밋한다

**결정.**

- Harness는 Scenario마다 `data/reliability/{scenario_id}/{utc_timestamp}.json`을 쓴다. 이 경로는 `.gitignore`에 추가한다.
- Runbook 문서에는 JSON 원본이 아니라 요약표(식별자, 상태 전이, Hash 앞 12자, Count)를 손으로 옮긴다.
- 문서에는 Credential, Raw Payload, 환경별 Absolute Path를 쓰지 않는다. `batch_id`, `run_id`, `table_batch_id`, `publish_run_id`, `reprocess_id`만 남긴다.

근거: Phase 7 문서가 `/tmp/cdp-clean-clone-*.log` 같은 Absolute Path를 남겨 새 Clone에서 재현 불가능한 참조가 됐다. Phase 8은 같은 실수를 반복하지 않는다.

### D5. Pass/Fail 판정은 불변 조건 단언으로 한다

**결정.** 판정 로직을 별도 Engine으로 만들지 않는다. Scenario 테스트가 `before`/`after` State를 비교하는 pytest 단언을 직접 쓴다. Harness는 비교 도우미만 제공한다.

```text
assert_unchanged(before, after, fields=("watermark", "committed_object_keys", "mart_hashes"))
assert_transitioned(after.run_status, expected="FAILED", error_code=UPLOAD_ERROR)
```

근거: Phase 8 Scenario는 15개뿐이고 각 불변 조건이 서로 다르다. 범용 판정 Engine은 YAGNI다.

### D6. `R-11` Missing Schedule은 Airflow Catchup이 아니라 명시 Batch로 정의한다

**결정.** 두 DAG 모두 `schedule=None`, `catchup=False`다. Scheduler 누락을 재현할 대상이 없다.

대신 다음을 Missing Schedule로 정의한다.

1. 특정 기간의 Ingestion Run을 실행하지 않은 채 다음 기간을 실행한다.
2. Watermark가 전진해 누락 구간이 생긴 것을 관측한다.
3. `src/ingestion/reprocess.py`의 Rewind로 Cursor를 되돌리고 명시 Batch로 재수집한다.
4. 복구 후 Mart Hash가 누락 없이 실행한 경우와 같은지 비교한다.

이 정의를 Phase 8 문서에 반영하고, ROADMAP의 `R-11` 설명도 같은 문장으로 맞춘다.

### D7. 문서 구조는 Scenario 1개 = Runbook 1개로 한다

```text
docs/runbooks/
├── README.md                 # 색인과 공통 사전 조건
├── r01-duplicate-batch.md
├── ...
└── r15-source-connection-failure.md

docs/troubleshooting/
├── README.md                 # Error Code → 원인 → 조치 색인
├── ingestion-errors.md       # Quarantine·Batch 실패 Error Code
├── commit-and-lease.md       # CAS 충돌, Lease 만료, Orphan
└── warehouse-publish.md      # DBT_BUILD_ERROR, DBT_TEST_ERROR, abandoned Run
```

Runbook 본문은 Phase 8 문서가 정한 6단 순서를 그대로 쓴다. Troubleshooting은 Error Code 기준으로 역인덱스를 만든다. Runbook 15개를 한 파일에 몰면 검색성이 떨어지고, Error Code 역인덱스는 Scenario 경계와 맞지 않으므로 분리한다.

## 4. 범위 밖

- Chaos 자동화 도구(Toxiproxy 등) 도입. 15개 Scenario에 과하다.
- 성능 수치 측정. Phase 9가 한다.
- 모든 Error Code의 Runbook화. `R-*`가 다루는 Code만 쓴다.
- 프로덕션 Fault Injection Flag.

## 5. 위험

| 위험 | 대응 |
| ---- | ---- |
| `R-03`·`R-15` 주입점이 함수 경계와 맞지 않음 | 구현 중 발견 시 architect에 Seam 추가를 문의한다. 임의로 프로덕션 분기를 넣지 않는다. |
| dbt 필요 Scenario의 실행 시간 | `RUN_DBT_PUBLISH_INTEGRATION` Gate로 기본 실행에서 제외한다. |
| Scenario 간 Metadata 오염 | Scenario마다 고유 `batch_id` Prefix를 쓰고 Fixture에서 정리한다. Phase 7 Publish Fixture 규약을 따른다. |
| 문서 15개 작성 부담 | Runbook Template을 `P8-05`에서 먼저 확정하고 기계적으로 채운다. |

## 6. Task 매핑

| Phase 8 Task | 이 설계의 반영 |
| ------------ | -------------- |
| `P8-01` | D3 `collect_state()`, 기준 Snapshot Fixture |
| `P8-02` | D1 주입점 목록, `faults.py` |
| `P8-03` | D3 수집기, Mart 목록 주입 |
| `P8-04` | D4 Evidence 형식, D5 판정 방식 |
| `P8-05` | D7 문서 구조와 Template |
| `P8-06`~`P8-12` | §2 표의 `R-01`~`R-07` |
| `P8-13`~`P8-15` | §2 표의 `R-08`~`R-10` |
| `P8-16`~`P8-20` | §2 표의 `R-11`~`R-15` |

## 7. Acceptance 연결

Phase 8 문서의 AC 표를 그대로 쓴다. 추가 판단 두 가지를 기록한다.

- **AC-07**은 `R-12` Replay 결과와 같은 범위 Full Refresh의 Key별 값·Logical Hash 일치로 판정한다. `test_incremental_full_refresh_hash_integration.py`가 이미 비교 도구를 제공한다.
- **AC-21**(Generator 충돌)과 **AC-24**(Unsupported Schema Version)는 Phase 8 Task 목록에 대응 `R-*`가 없다. 각각 `R-05` Lease Scenario와 `R-07` Manifest Scenario의 변형 Case로 흡수한다. 새 `R-16`을 만들지 않는다.
