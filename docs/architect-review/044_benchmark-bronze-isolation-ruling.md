# 044. Benchmark Bronze 혼입 판정 — 운영 기준선 재기준화와 Benchmark 격리

- 일자: 2026-09-23
- 상태: **판정 완료**
- 대상: `src/benchmark/experiments/extract.py`, `src/benchmark/experiments/file_format.py`, `src/rebaseline.py`,
  `pipeline_metadata`(`bronze_objects`·`watermarks`·`pipeline_runs`·`generator_runs`), `commerce_source`,
  SeaweedFS `commerce-lake` Bucket
- 근거 문서: [WSL 멈춤 조사](../troubleshooting/2026-09-23-wsl-freeze-investigation.md)
- 관련 판정: [032 공유 원천 누적](032_phase9-shared-source-accumulation.md), [035 Anchor 산출](035_phase9-extract-anchor-derivation.md)
- 판정:
  1. **원천 DB는 오염됐다(§3.4 → (b)).** 기존 `src/rebaseline.py`로 원천·Metadata·Bronze를 한 번에 재기준화한다.
  2. **B안(ORPHANED 전환)은 채택하지 않는다.** 재기준화가 `bench-*` 428건과 S3 Object를 함께 정리하므로 별도 단계가 필요 없다.
  3. **E안은 채택하되 범위를 넓힌다.** 격리 대상에 원천 DB를 추가한다. 지금은 fail-closed Guard만 구현하고,
     전용 DB·Bucket 준비는 다음 Benchmark 실행이 계획될 때 한다.

## 1. 사실

### 1.1 운영 카탈로그 오염

`pipeline_metadata.bronze_objects` COMMITTED 행:

| 구분 | Object 수 | 행 수 |
| --- | ---: | ---: |
| 운영 (`warehouse_pipeline_dag__20260922T030000Z`) | 7 | 4,404,997 |
| `bench-extract-*` (2026-09-21 03:31~12:40 UTC) | 428 | 137,951,230 |

- `bronze_source()`는 `source_table`과 `replay_boundary_predicate()`로만 거른다. `batch_id`나 `pipeline_name`으로는 거르지 않는다.
  따라서 dbt Build가 `bench-*` Object까지 전부 `read_parquet`으로 읽는다.
- 결과: Phase 10 full publish가 운영 대비 약 31배를 읽는다. SeaweedFS HTTP Timeout, `DBT_BUILD_ERROR`, WSL 메모리 고갈로 이어졌다.

### 1.2 오염 경로

- extract 실험은 `PostgresSettings.from_environment()`·`SeaweedFSSettings.from_environment()`로 운영 설정을 읽는다.
  그다음 운영 적재 함수 `ingest_table()`을 호출한다. 실험 후 정리 단계는 없다.
- Full Arm은 반복마다 Watermark를 되감아 9개 Table 전량을 재적재한다. 실행 1회당 5회 반복한다.
- S 7회, XS 3회 실행이 누적됐다(실패·INVALID 재실행 포함).
- file_format 실험도 운영 Bucket에 Fixture를 쓴다. Key는 `benchmark/file_format/...`이고 `bronze_objects`에 등록하지 않는다.
  따라서 Publish 부하와는 무관하다. 다만 운영 Bucket을 공유하는 점은 extract와 같다.
- scan·cache_effect·harness_overhead 실험은 운영 저장소를 쓰지 않는다(코드 확인).

### 1.3 Phase 9 증거는 영향이 없다 (architect 확인)

- `data/benchmarks`와 `docs/benchmarks`에는 `bench-extract` Key나 `bronze/` Key 참조가 없다(`grep` 확인).
- 결과 Hash는 실험 중 임시 로컬 Catalog(`control.bronze_files`)에서 계산했다.
- `src/rebaseline.py`가 지우는 Prefix는 `bronze/`·`quarantine/`·`_staging/`뿐이다. file_format Fixture(`benchmark/`)는 남는다.
- 따라서 운영 카탈로그·Bronze를 재기준화해도 Phase 9 증거는 깨지지 않는다.

### 1.4 원천 DB 오염은 가능성이 아니라 사실이다

- extract 실험의 Setup은 `run_generator()`를 T0·T1 두 번 호출해 운영 원천(`commerce_source`)에 행을 넣는다(`extract.py` 79·98행).
  설계상 의도된 쓰기다.
- 032 §5는 "원천 누적은 한 실행 안의 Arm 비교를 깨지 않는다"는 이유로 Benchmark 앞 원천 초기화를 하지 않기로 했다.
  그러나 032는 **그 누적된 원천을 운영 파이프라인이 읽는 경우**를 검토하지 않았다. 이것이 이번 판정의 공백이다.
- `generator_runs` 21건이 모두 Benchmark 기간에 있다. 운영 적재(2026-09-22)의 `orders` 742,110행은 `bench-*` S Object의 최대 행 수와 같다.
  즉 운영 Bronze 7개 Object도 Benchmark가 키운 원천에서 나왔다.

## 2. 선택지

### 2.1 기존 오염 정리

| 안 | 내용 | 평가 |
| --- | --- | --- |
| A | `bronze_objects` 행 DELETE + S3 Object 삭제(임의 SQL·스크립트) | Lifecycle을 우회하는 일회성 삭제. 기각 |
| B | `bench-*` 행을 `status='ORPHANED'`로 전환, S3 Object는 별도 회수 | 카탈로그만 고친다. 원천과 운영 Bronze 7건은 여전히 Benchmark 산출에서 나온 상태로 남는다. §3.4(b)와 함께 쓰면 중복이다. 기각 |
| C | `bronze_source()`에 `batch_id not like 'bench-%'` 필터 추가 | 운영 SQL에 Benchmark 예외를 영구히 박는다. 기각 |
| **R** | 기존 `src/rebaseline.py --confirm`으로 원천·Metadata·Bronze·DuckDB Catalog를 Seed 기준으로 재생성 | 이미 있는 재기준화 경로다. Inventory 출력, `--confirm` 게이트, `assert_no_active_publish`, Source Mutation Lease를 갖췄다. 원천·카탈로그·S3를 한 번에 정리한다 |

### 2.2 재발 방지

| 안 | 내용 | 평가 |
| --- | --- | --- |
| D | Full Arm 반복 전에 이전 적재분 삭제 | 운영 저장소 안에서 DELETE하는 코드가 Benchmark에 생긴다. 조건 오류 시 운영 데이터 손실. 기각 |
| E(초안) | 전용 Metadata DB·전용 Bucket + 운영 값 Guard | 원천 DB가 빠졌다. extract의 `run_generator()`가 계속 운영 원천에 쓴다. §1.4가 그대로 재발한다 |
| **E'** | 원천 DB·Metadata DB·Bucket 세 가지를 모두 Benchmark 전용 값으로 요구하는 fail-closed Guard | 채택 |

## 3. 판정

### 3.1 원천 DB — (b) 재기준화

(a) "현 상태를 운영 기준선으로 인정"은 기각한다. 근거:

1. **운영 Bronze도 이미 오염됐다.** 운영 적재 7건이 Benchmark가 넣은 원천 행을 담고 있다.
   카탈로그의 `bench-*`만 빼도 Mart에는 Benchmark 산출이 남는다.
2. **Phase 10은 BI다.** 035 Anchor 규칙상 Benchmark 행의 논리 시각은 Seed 최대 시각 직후에 1분 간격으로 몰린다.
   21회 Generator 호출분이 좁은 구간에 쌓여 일별 지표에 인위적 급증이 생긴다. Dashboard 기준선으로 쓸 수 없다.
3. **재현할 수 없다.** 현재 원천은 실패·INVALID 재실행 횟수에 따라 크기가 달라진 우연한 상태다.
   Seed에서 다시 만들 방법이 없다. 운영 기준선은 Seed로 재현돼야 한다.
4. **032는 이 경우를 허용하지 않았다.** 032가 허용한 것은 "Benchmark 실행 안의 비교 타당성"뿐이다.

032 §5가 재기준화를 거절한 근거 1("다른 Phase의 산출물까지 파괴")은 이번에는 해당하지 않는다.
삭제 범위 `bronze/`·`quarantine/`·`_staging/`에 있는 것은 모두 이번에 다시 만들 대상이다. `benchmark/` Fixture는 범위 밖이다.

### 3.2 기존 `bench-*` 428건 — R로 정리, B 기각

- R이 `bronze_objects`를 TRUNCATE하고 `bronze/` Prefix를 비운다. `bench-*` 행과 S3 Object가 함께 사라진다.
- 초안이 A를 기각한 이유(되돌릴 수 없음, Lifecycle 우회)는 임의 SQL 삭제에 대한 것이다. R은 설계된 재기준화 경로라서 해당하지 않는다.
- 되돌릴 수 없는 부분은 백업으로 보완한다(§4 1단계). S3의 `bench-*` Object는 백업하지 않는다. Phase 9 증거가 참조하지 않고(§1.3), 재현할 가치가 없다.
- `mart_publish_runs`는 R의 TRUNCATE 대상이 아니다. Publish 이력은 남는다.

### 3.3 재발 방지 — E' 채택, 구현 범위는 Guard까지

- 저장소를 쓰는 Benchmark 실험(extract, file_format)은 설정을 한 곳에서만 얻는다.
  `src/benchmark/` 안에 Benchmark 전용 설정 함수를 하나 두고, 실험 모듈은 `PostgresSettings.from_environment()`·`SeaweedFSSettings.from_environment()`를 직접 호출하지 않는다.
- 그 함수는 `BENCHMARK_COMMERCE_SOURCE_DB`·`BENCHMARK_PIPELINE_METADATA_DB`·`BENCHMARK_SEAWEEDFS_BUCKET` 세 값을 요구한다.
  하나라도 없거나, 대응하는 운영 값(`COMMERCE_SOURCE_DB`·`PIPELINE_METADATA_DB`·`SEAWEEDFS_BUCKET`)과 같으면 실행을 거부한다.
  나머지 접속 정보(Host·Port·계정)는 운영 값을 그대로 쓴다.
- Guard는 fail-closed다. 값이 준비되지 않은 지금은 extract·file_format 실험이 실행되지 않는다. 이것이 "격리 전 실행 금지"를 규칙이 아니라 코드로 강제한다.
- 전용 DB 생성(bootstrap)·Migration 적용·Bucket 생성·폐기 절차는 **지금 만들지 않는다.** Phase 9는 종료됐고 다음 Benchmark 계획이 없다(YAGNI). 다음 Benchmark가 계획될 때 그 계획에 포함한다.
- scan·cache_effect·harness_overhead는 운영 저장소를 쓰지 않으므로 금지 대상이 아니다.

### 3.4 실행 금지 해제 조건

| 대상 | 해제 조건 |
| --- | --- |
| full publish / `dbt build` | §4 1~4단계 완료 후 |
| extract·file_format Benchmark | 전용 DB·Bucket 준비 후. Guard가 코드로 막는다 |
| scan·cache_effect·harness_overhead | 금지 대상 아님 |

## 4. 후속 작업 (순서 고정)

1. **자원 상한 먼저 적용.** 조사 문서 §5 조치 2·3을 반영한다.
   `dbt/profiles.yml`에 DuckDB `memory_limit: '2GB'`·`threads: 2`, Metabase에 `JAVA_OPTS=-Xmx1g`·`mem_limit`·`restart: "no"`.
   재기준화와 Publish 중에는 Metabase를 내린다.
2. **백업.** `pg_dump`로 `pipeline_metadata` 전체와 `commerce_source` Schema+Data를 `data/reliability/` 아래(Git 제외)에 저장한다.
   `bench-*` 행 집계(Scale·Arm별 Object 수·행 수)를 결과 기록에 남긴다.
3. **활성 Publish 회수.** `mart_publish_runs`에 BUILDING·PUBLISHING이 있으면 R의 `assert_no_active_publish`가 막는다.
   `python -m src.warehouse.publish --recover-only`로 회수한다. 수동 `UPDATE`는 금지한다.
4. **재기준화.** `python -m src.rebaseline`을 `--confirm` 없이 실행해 Inventory를 먼저 출력해 기록한다. 그다음 `--confirm`으로 실행한다.
   결과의 Seed 행 수·Bronze 행 수·Catalog Entry 수를 기록한다. `bronze_objects`에 `bench-*` 0건, `docker system df`로 `seaweedfs_data` 감소를 확인한다.
5. **Guard 구현.** §3.3의 Benchmark 전용 설정 함수와 Guard Test를 추가한다.
   Test는 최소 두 건이다: 값 누락 시 거부, 운영 값과 같을 때 거부.
6. **문서.** `docs/benchmarks/00-environment.md`에 격리 누락 사실과 이 판정 링크를 추가한다.

## 5. 실행 기록

### 5.1 재기준화 전 검증 (2026-09-23)

- DuckDB 상한은 `memory_limit: '2GB'`, `threads: 2`로 적용했고, Metabase는
  `JAVA_OPTS=-Xmx1g`, `mem_limit: 1536m`, `restart: "no"`를 적용한 뒤 중지했다.
- `data/reliability/044-20260923T093003Z-pipeline_metadata.dump`(전체, 99,261 bytes)와
  `data/reliability/044-20260923T093003Z-commerce_source-schema-data.dump`(스키마+데이터,
  188,717,755 bytes)를 만들고 `pg_restore --list`로 읽기 검증했다. 두 파일은 Git 제외 경로다.
- `bench-*` Bronze Object는 총 428건이다. Scale·Arm별 집계는 S/full 140건·56,839,066행,
  S/incremental 114건·17,493,207행, XS/full 98건·52,391,005행, XS/incremental
  76건·11,227,952행이다.
- `src.warehouse.publish --recover-only`가 남은 Publish Run
  `fcad9633-a525-48f0-b2ef-01a459d1b1ef`를 회수했고, 확인 시 활성 상태는 없었다.
- `src.rebaseline --seeded-at 2026-09-03T00:00:00Z` 드라이런 Inventory를 확인했다.
  삭제 대상은 Source 9개 Table(행 수 합계 4,404,997),
  Metadata `bronze_objects=435`, `generator_runs=21`, `pipeline_runs=622`,
  `quarantine_batches=0`, `seed_runs=16`, `watermarks=189`, S3 `bronze/=870`,
  `quarantine/=0`, `_staging/=0`, 기존 Warehouse Catalog 1개다. Source별 행 수는
  `customer_membership_tiers=738765`, `customer_subscriptions=0`, `customers=742110`,
  `order_items=1399411`, `order_payments=746555`, `orders=742110`, `products=32951`,
  `sellers=3095`, `subscription_payments=0`다.

이 기록을 확인한 뒤에만 `--confirm`을 실행한다.

### 5.2 재기준화·Publish 결과 (2026-09-23)

- 첫 `--confirm` 컨테이너 실행은 Raw CSV Mount 누락으로 초기화 뒤 Seed 전에 중단됐다.
  백업을 보존하고 `data/raw`·`data/generated` Mount를 추가해 같은 `--confirm`을 다시 실행했으며
  종료 코드 0으로 재기준화를 완료했다.
- 최종 Seed Source 행 수 합계는 547,560행이다. Table별 값은
  `customer_membership_tiers=96096`, `customer_subscriptions=0`, `customers=99441`,
  `order_items=112650`, `order_payments=103886`, `orders=99441`, `products=32951`,
  `sellers=3095`, `subscription_payments=0`이다.
- 최종 Metadata는 `bronze_objects=7`, `bench-* bronze_objects=0`, `generator_runs=0`,
  `pipeline_runs=9`, `seed_runs=1`, `watermarks=9`다. Bronze Prefix는 Parquet과 Manifest를
  합쳐 14개 Object이고 DuckDB Catalog Entry는 7개다. `benchmark/` Fixture 7개는 보존됐다.
- 재기준화 뒤 `docker system df -v`의 `commerce-data-platform_seaweedfs_data`는 4.544GB다.
  실행 전 같은 명령의 값을 기록하지 않아 Volume의 절대 감소량은 비교할 수 없으며,
  삭제 검증은 Bronze Object 870개에서 14개로 줄고 `bench-*`가 0건인 것으로 수행했다.
- 최종 전체 Publish Run `5d74a326-954b-4850-a3d6-56d1556fec8d`는 `PUBLISHED`다. 9개 Mart를
  새 Catalog로 Build·검증·교체했고, 이전 Published Run은 없었다.
   032에 "운영 파이프라인 영향은 044에서 재판정" 한 줄을 추가한다.
7. **Phase 10 full publish 재실행.** 1~4단계 완료 후 진행한다. 이후 043 게이트에 따라 Phase 10 실환경 검증을 재개한다.
