# README 트러블슈팅 보강 후보

- 작성일: 2026-10-12
- 범위: `docs/architect-review/`(55건), `docs/architecture/`(8건), `docs/troubleshooting/`(4건)과 현재 `README.md`를 대조
- 상태: 후보 목록. README에는 아직 반영하지 않았다.
- 읽은 범위: architect-review는 판정·핵심 섹션을 읽었다. 054의 세부 수정(3.1~3.4)과 커밋만 있고 문서 근거가 없는 항목(#24, #25)은 미확인이다.

README 표에 이미 있는 항목은 제외했다: Fan-out, Source 동시성 Lease, Lease 해제 에러 은폐, SCD2 미래 컬럼, 벤치마크 모집단 혼합, Scale 한계, Row 단위 변환 병목, `drop_caches`.

## 1. 트러블슈팅 표 후보

### 1.1 데이터 정합성·모델링

| # | 문제 | 원인 | 해결 | 출처 |
| --- | --- | --- | --- | --- |
| 1 | 실패한 Build에서 Watermark가 전진 | dbt `on-run-end`는 Model 실패와 무관하게 실행됨. 매크로는 Fact가 "선택"됐는지만 보고 "성공"은 확인하지 않음 | `advance_processed_batch_watermark()`에 `results` 상태 Guard 추가. `error`/`fail`/`runtime error`/`skipped`가 하나라도 있으면 전진하지 않고 로그만 남김(`warn`은 허용). 기존 `selected_resources` Guard는 유지. 다음 Build가 상위 집합을 재계산하며 재계산은 멱등. `--fail-fast`와 운영 절차 문서화는 기각 | 001, architecture 06 |
| 2 | SCD2 Contract 검사 122건 실패 | Olist `customer_id`(주문별 계정)와 `customer_unique_id`(사람)가 다름. 재구매 고객 중 122명이 병행 배송지를 사용하는데 이를 "값이 순서대로 바뀐다"는 SCD2에 넣음 | Source만 `customer_subscriptions` + `customer_membership_tiers`로 분리(B안). `updated_at`만 고치면 사실과 다른 이력이 조용히 생김 | architecture 01, 04 |
| 3 | 주문이 자기 고객보다 먼저 존재 (Early-arriving Fact) | 005는 "Fixture의 비현실적 상태"로 판정했으나 틀림. 운영 Generator가 같은 형태를 직접 생성 | Dimension 첫 버전 `effective_from = -infinity` | 005→006, 008 |
| 4 | Mart Hash에 `-infinity` 혼입 | DuckDB Python이 `-infinity`를 tz 없는 `datetime.min`으로 반환해 `_json_default()`가 거부 | naive가 `datetime.min`/`max`일 때만 `"-infinity"`/`"infinity"`로 정규화, 나머지 naive는 계속 `ValueError`. UTC를 붙이면 진짜 Timestamp와 Hash가 충돌하고 Offset 유실 Guard가 사라져 기각 | 007 |
| 5 | Mart Grain 계약과 구현 컬럼명 불일치 | 계약 문서와 구현이 따로 변함 | 계약을 구현에 맞춤. 구현을 바꾸면 `mart_hash.py` 정렬 Key와 Phase 9 Hash 증거가 깨짐. Fact 이름만 사용자 결정으로 `fct_*`에 구현을 맞춤 | 002, 009, 010, 042 |
| 6 | Item Grain에 날짜가 없어 기간 필터 불가 | 날짜는 `fct_order` Join으로만 얻는데 Fact끼리 Join은 계약 §6 위반 | `fct_order_item.purchase_date_key` 추가(계약 개정) | 045 |
| 7 | Mart가 실행 환경에 따라 달라짐 (Key·날짜 하루 밀림) | `dbt/profiles.yml`에 TimeZone이 없어 Process TimeZone(KST/UTC)을 따름. `cast(TIMESTAMPTZ as varchar)`가 Offset을 문자열에 넣어 `customer_key`도 변함 | `TimeZone: 'UTC'` 고정, 계약 §1에 규칙 추가. Key 식은 그대로(바꾸면 Key가 또 변함). Rollback 대신 `--full-refresh` 재Publish | 046 |
| 8 | `payment_status` accepted_values Test 실패 | 소문자 Domain과 Test 불일치 | Test를 완화하지 않고 원복. Gate는 설계대로 동작해 실패 Build를 `failed/`로 격리 | 011 |
| 9 | 구독 기간과 결제 청구 기간 어긋남 | 053 조사 중 발견한 별건 | 사용자 규칙: 활성 상태면 추가 결제 없음, 예정일 또는 재활성화 때만 결제. 재시도 성공(e)·실패(d)·`subscription-payment-failed` Profile(f) 수정. 세부는 054 3.1~3.4 확인 필요 | 053, 054 |

### 1.2 수집·Commit·Lease

| # | 문제 | 원인 | 해결 | 출처 |
| --- | --- | --- | --- | --- |
| 10 | 만료된 Lease 소유자가 Commit에 성공 | `commit_table_run`이 Watermark CAS만 보고 `lease_owner`는 안 봄. Lease 인수 직후엔 `version`이 같아 CAS 통과(R-05 실측) | `commit_table_run` Transaction 맨 앞에서 `watermarks` 행을 `FOR UPDATE`로 잠그고 `lease_owner`/`lease_expires_at` 검사 (ADR-017) | 016, 017, troubleshooting/commit-and-lease |
| 11 | Generator 최종 Lease 확인과 Source COMMIT 사이 창 | 확인은 별도 커넥션, COMMIT은 다른 DB라 그 사이 Lease 만료·인수 가능. 조용한 데이터 누락으로 이어짐 | `fenced_source_commit` 컨텍스트 매니저로 COMMIT 동안 Pipeline DB의 Lease Row를 `FOR SHARE`로 잡음. 인수자의 `FOR UPDATE`가 대기하므로 2PC 없이 순서 보장. `FOR KEY SHARE`는 일반 `UPDATE`를 못 막아 제외. Fence 토큰 방식은 Reader가 Writer를 막지 못해 기각 | 055 |
| 12 | Ingestion 실패의 `error_type`이 `ClientError`로 기록 | 실패 경로가 `classify_error`를 안 씀 | `_record_lease_failure`가 리터럴 대신 `classify_error`를 거침. 잠금 종류가 아니라 무슨 일이 일어났는지로 분류: 잠금 불가는 `LEASE_UNAVAILABLE`(재시도 가능), 소유권 상실은 `LEASE_OWNERSHIP_LOST`(불가). 잠금 종류별 분류는 재시도 판정을 깨 기각 | 014, 024 |
| 13 | Source 연결 실패 시 `pipeline_runs`에 행이 없음 | 연결 열기가 `record_started_run`보다 앞이고 감싸는 `except`가 없음 | `run_started` 플래그로 추적, 시작 기록 전 실패는 `_record_prestart_failure`가 `extract_upper_bound=None`인 FAILED Run으로 기록(이중 기록 방지) | 019, 024, troubleshooting/ingestion-errors |
| 14 | R-11 시나리오 전제가 틀림 | `extract_upper_bound`가 `logical_date`가 아니라 실제 최대 Cursor라 누락 구간이 다음 Run에 자동 포함됨. 손실이 아니라 귀속 이동(Row가 다음 Run의 `ingestion_date`·`batch_id` 아래로 들어옴). Mart는 `_batch_id`를 필터로 안 써서 자동 복구 | 시나리오 재구성: 건너뛴 구간이 스스로 메워짐 + Mart Hash가 Full Refresh Control과 같음 + rewind는 귀속 교정. Watermark를 위조하는 `_set_watermark` 방식은 반려 | 018 |

### 1.3 Benchmark·운영 환경

| # | 문제 | 원인 | 해결 | 출처 |
| --- | --- | --- | --- | --- |
| 15 | Publish 중 WSL 반복 멈춤, S3 HTTP 500/Timeout | `bench-*` 428건(1.38억 행)이 운영 Bronze 카탈로그에 COMMITTED로 섞여 운영의 약 31배를 읽음. Staging이 view라 test마다 S3에서 전량 재조회. 자원 상한 부재가 증폭 | `bronze_objects` TRUNCATE와 `bronze/` 비움으로 재기준화. Benchmark 전용 설정 함수가 `BENCHMARK_COMMERCE_SOURCE_DB`/`BENCHMARK_PIPELINE_METADATA_DB`/`BENCHMARK_SEAWEEDFS_BUCKET`을 요구하고, 없거나 운영 값과 같으면 실행 거부(fail-closed). DuckDB `memory_limit 2GB`/`threads 2`, Metabase `-Xmx1g`/`mem_limit 1536m`, `.wslconfig` `memory=12GB`/`swap=4GB` | 044, troubleshooting/2026-09-23-wsl-freeze-investigation |
| 16 | 실험 A 설계를 6번 고침 | 공통 원인: 실험 A만 공유·가변·누적되는 실제 원천을 측정 대상으로 씀 | 030 반복 루프의 `run_generator()` 제거 / 031 되감기 목표 == 현재 처리 / 032 원천 누적 인정(`orders` 약 52만 행) / 033 T0와 경계 시각 분리 / 034 무동작 Gate 정정 / 035 `t0 = max_existing + 1분`으로 계산. 교훈: 설계 전에 `COUNT(*)`, `MIN/MAX(updated_at)` 한 번이면 032가 나옴. 정지 규칙: XS가 5/5 VALID일 때까지 S를 돌리지 않음 | 030~036 |
| 17 | 승인된 수치가 틀림 (143.58초) | Full 5회와 Incremental 5회를 한 모집단으로 합친 Median. 그럴듯함을 검증으로 착각, Raw 기록을 안 읽음 | 정본은 Full 282.09초, Incremental 5.85초, 48.2배. `median_duration`에 Arm Guard 추가. 승인 시 `runs.jsonl`을 직접 읽음 | 037 |
| 18 | 실험 D의 Cache 효과를 분해할 수 없음 | S Scale Workload가 작음(약 105,000 Row, 3컬럼). WSL2 Guest의 `fadvise`는 Host VHD Cache를 못 비움. `cache_reset_paths`가 `scenario_config_hash`에 유입되는 회귀도 있었음 | "Cache 효과 없음"은 반려하고 "이 환경에서 분해할 수 없었다"로 기록, M Scale 재측정. `_NON_DEFINING_PARAMETERS`로 Hash 입력에서 제외, `default=str` 제거 | 029, 041 |

### 1.4 Serving·BI·Airflow

| # | 문제 | 원인 | 해결 | 출처 |
| --- | --- | --- | --- | --- |
| 19 | Metabase가 교체 전 Serving 파일을 조회 (99,540행 vs 99,541행) | `os.replace` 후에도 DuckDB JDBC의 프로세스 전역 Instance Cache가 옛 inode를 유지. C3P0 Pool이 Connection을 살려 둬 재시작 전까지 무기한 stale | Export 시 `os.link`로 버전 경로 Hardlink를 먼저 만들고 `os.replace`. `repoint_metabase_serving` Task가 `details.database_file`을 새 경로로 PUT하고 Manifest `export_id`로 검증 (ADR-019) | 049 |
| 20 | Export 보존 정리가 재지정 실패와 겹치면 파일 소실 | 최신 3개만 남기는데 재지정 실패 시 Metabase는 옛 경로를 계속 가리킴. 재시작하면 모든 카드가 없는 파일을 엶 | 보존 정리를 재지정 `SUCCESS`/`SKIPPED` 뒤로 이동. 실패 시 파일이 쌓임(삭제되는 것은 조용히 깨지고, 쌓이는 것은 보임). 불변식: "Metabase details가 가리킬 수 있는 파일은 삭제하지 않는다" | 050 |
| 21 | 재지정 실패 시 Airflow 재시도가 일어나지 않음 | `AirflowFailException`은 재시도 없이 실패시킴. 050의 지시가 모순(architect 설계 오류) | `AirflowException`을 던져 DAG 기본 `retries=2`(60초부터 지수 증가, 약 3분)로 Metabase 재시작 구간 흡수 | 051 |
| 22 | Mart 주문 최대 일자가 미래(2026-10-28), 매시간 신규 행 없음 | 미래 `logical_date`로 Generator를 수동 실행해 Watermark가 10-28까지 전진, 이후 실제 시각 행이 Watermark 아래라 `SUCCESS_NO_DATA`로 조용히 유실. 신규 행에는 단조성 검사가 없었음 | `run_generator`가 Lease 획득 직후 첫 쓰기 전에 `logical_date <= max(updated_at)`이면 쓰기 없이 Contract 위반으로 실패. 미래 날짜 자체는 허용하고 "역행"만 차단 | 048 |
| 23 | Published Mart 검증 Test 실패 | 산출물이 `marts.metrics`를 view로 만든 시점 것 | 조건부 skip. Phase 10은 이 Test가 실제로 그린이 될 때까지 미완료 | 043 |
| 24 | Dashboard 필터 미동작 | 필터 매핑 방식 (커밋 `c13b483`) | Field Filter로 매핑. 문서 근거 없음, 커밋 diff 확인 필요 | git log |
| 25 | Airflow Task 간 인증 실패 | Execution API URL과 JWT Secret 미설정 (커밋 `3357c9e`) | 둘 다 설정. 문서 근거 없음, 커밋 diff 확인 필요 | git log |

### 1.5 과거 보류 항목 (2026-10-12 재판정: 전부 해소)

| # | 항목 | 상태 | 출처 |
| --- | --- | --- | --- |
| 26 | Seed Guard 순서 의존 Test 2건 | 해소(`d441323`, 004 선택지 1). Test 시작 시 `generator_runs` SUCCESS 행 삭제. 단 이 방식이 운영 공유 DB의 Generator 이력을 지우고 `run_seed`가 등급·구독을 Seed 값으로 덮는 신규 위험이 있어 격리 Fixture로 수정 중. 수정 전까지 `RUN_POSTGRES_INTEGRATION=1` 전체 실행 금지 | 004 |
| 27 | Generator 신규 주문이 `CREATED`에 머묾 | 해소(`9afeddc`, 권고안 B 채택·구현). `src/generator/order_progress.py`, 실행당 1단계 전이, `GENERATOR_VERSION` 1.14.0, PRD v1.21 반영. 남은 확인: Dashboard Card의 "최근 일자 매출 증가 가능" 설명 반영 여부 | 052 |
| 28 | E2E Fixture의 Dimension이 비어 relationships Test 실패 | 해소(`3b1ae58`). Fixture가 products·sellers를 수집하고 `>0` 단언. phase-07에 2 passed 기록 | 012 |

## 2. 설계 결정으로 소개할 후보

| 주제 | 핵심 | 출처 |
| --- | --- | --- |
| 영향 Key 기반 Incremental | 영향 Key 모델을 도메인별로 분리, 재계산 경계는 처리 Watermark | architecture 06 |
| Incremental vs Full Refresh Logical Hash | `table`로 물리화된 Mart만 대상, Key 정렬 Canonical JSON 누적 SHA-256, Warehouse 파일 복사본 위에서 Full Refresh와 비교 | architecture 07 |
| Replay·Re-extract 입력 경계 | Bronze Replay 경계 축과 전달 경로, Watermark 상호작용 | architecture 08 |
| 구독 생명주기 | 결제는 Dimension이 아니라 Fact | architecture 03 |
| Fact 계층 책임 분리 | Intermediate에서 사전 집계, Fact는 적재만 | architecture 05 |
| 구독/등급 분리 3안 비교 | B안(Source만 분리) 채택 근거: CHECK 제약 오염 해소, Temporal Join 1회, Late Arrival 처리 지점 1곳 | architecture 04 |

## 3. 현재 README와 어긋날 수 있는 부분

- README는 `source_simulation_dag`를 `@hourly`로 적었다. 052에는 2026-09-29부터 `SOURCE_DAG_SCHEDULE='*/10 * * * *'`로 10분마다 실행한다는 기록이 있다. 현재 값은 `.env`와 DAG 코드로 확인해야 한다.
    + 빠른 테스트를 위해 10분 간격으로 실행했음. 이상없는 부분.
- `docs/troubleshooting/README.md`는 오류 코드 색인이고 README의 "트러블슈팅"은 사례 표다. 둘의 역할을 구분해서 서술해야 한다.

## 4. 작성 추천

README 표는 현재 8행이다. 사례 중심으로 한눈에 읽히는 분량(최대 14~15행)을 유지하려면 아래 순서로 고른다.

### 4.1 필수 (사고·원인·조치·교훈이 문서로 확인됨)

| 순위 | # | 이유 |
| --- | --- | --- |
| 1 | 15 WSL 멈춤 | 실제 장애이고 원인(31배 혼입)이 수치로 확인됨. 기존 "벤치마크 모집단 혼합" 행과 병합하거나 그 앞에 둔다 |
| 2 | 1 Watermark 전진 | 프로젝트 핵심 주장(실패 Batch는 Watermark를 전진시키지 않는다)의 구멍이었음. 성공 기준 2번과 직결 |
| 3 | 10 + 11 Stale Lease Commit | 성공 기준 6번과 직결. 두 건을 "Lease Fencing" 한 행으로 합친다 |
| 4 | 2 SCD2 122건 | 데이터 모델링 판단이 드러남. Star Schema 설계 검증이라는 핵심 목적에 부합 |
| 5 | 22 Generator 역행 유실 | 조용한 데이터 유실이 Watermark 단조성과 부딪친 사례. 성공 기준 4번과 연결 |
| 6 | 19 + 20 + 21 Metabase stale | 한 흐름(원인 → 보존 정리 → 재시도)이라 한 행으로 합친다 |

### 4.2 있으면 좋음

| # | 이유 |
| --- | --- |
| 17 승인 수치 오류 | 모집단 혼합이 검수 단계에서도 재발한 사례. 기존 "벤치마크 모집단 혼합" 행에 합치면 중복이 줄어든다 |
| 7 TimeZone 고정 | 같은 입력 → 같은 Mart(성공 기준 1번)를 환경이 깰 수 있었다는 근거 |
| 4 `-infinity` Hash | 한 줄 사례로 짧고 명확함 |
| 14 R-11 전제 오류 | "시나리오를 가정하고 짜면 안 된다"는 교훈. 손실이 아니라 귀속 이동이라는 결론이 흥미로움 |
| 27 Generator `CREATED` 정체 | 명세·부품은 있는데 실행 경로가 연결되지 않아 지표가 0이 된 사례. 해소됨 |

### 4.3 넣지 않는 것을 추천

- 5, 6, 8, 9, 12, 13, 23, 24, 25: 세부가 너무 좁거나, Runbook/색인에 이미 있거나, 근거 확인이 덜 됐다. 필요하면 `docs/troubleshooting/` 링크로 대신한다.
- 26, 28: 해소된 Test 측 수정이라 README 사례로 쓸 만큼 크지 않다.
- 16 실험 A 6회 수정, 18 실험 D: Benchmark 내부 이야기라 README 본문이 길어진다. `docs/benchmarks/` 링크로 충분하다.
- "알려진 한계" 섹션은 만들지 않는다. §1.5의 3건이 모두 해소됐다.

### 4.4 표현 방식 제안

- 표에 `출처` 열을 추가하지 말고, 해결 칸 끝에 `docs/` 링크 한 개만 붙인다.
- 기존 "배운 점" 문단에 한 문장을 추가한다: "조용한 유실(Watermark 역행, Lease 창, 실패 Build)은 에러가 아니라 정상 종료로 드러나므로, 불변식을 코드와 Test로 고정해야 한다."
- Metabase 비밀번호 리셋 절차(README 5번 섹션)는 트러블슈팅이 아니라 운영 가이드라 현재 위치가 맞다.
