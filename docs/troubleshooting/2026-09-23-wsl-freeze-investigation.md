# WSL 멈춤 조사 — Phase 10 Publish 중 자원 고갈

- 일자: 2026-09-23
- 범위: 2026-09-22 16:15 ~ 2026-09-23 13:45(KST) 사이 반복된 WSL 멈춤
- 상태: 조치 1~4 완료, WSL 재시작 후 자원 상한 적용 확인
- 근거: `.claude-logs/*.jsonl`, `.team/developer/.codex-home/sessions/`, `dbt/logs/dbt.log`,
  `pipeline_metadata.bronze_objects`, `docker stats`, `free -h`

## 1. 요약

Phase 9 Benchmark가 적재한 `bench-*` Batch가 운영 Bronze 카탈로그(`pipeline_metadata.bronze_objects`)에
COMMITTED 상태로 섞여 있었다. 그래서 Phase 10 full publish(`dbt build`)가 운영 데이터의 약 31배(1억 3,800만 행)를
SeaweedFS S3에서 읽었다. 이후 044 판정으로 운영 기준선을 재기준화하고 DuckDB·Metabase·WSL 자원 상한을 설정했다.

## 2. 경과

| 시각(KST) | 사건 | 근거 |
| --- | --- | --- |
| 09-22 11:49 | Docker 기동 후 developer가 실환경 검증(SeaweedFS/PostgreSQL 재기동, full publish, Metabase Connection Gate) 착수 | `lead.jsonl` |
| 09-22 16:15 | 사용자: "계속 wsl 이 꺼지는데" | `lead.jsonl` |
| 09-22 16:16 | Metabase CPU 300% 관측 | `lead.jsonl` |
| 09-22 16:42 | WSL 재차 멈춤 | `lead.jsonl` |
| 09-22 16:52 | Publish Run `9720c52d` FAILED(`DBT_BUILD_ERROR`) — SeaweedFS S3 Bronze Parquet 조회 HTTP 500 | developer 보고 |
| 09-22 17:00 | 프로세스 없는 Run `73ebc06a`가 BUILDING으로 남음 → `UNKNOWN_ERROR` 처리 | developer 보고 |
| 09-23 13:33 | WSL 재기동 후 `uv.lock`(1,113줄) 전체 삭제 상태 발견, developer가 `git restore`로 복구 | Codex 세션 로그 |
| 09-23 13:36 | developer가 `uv run python -m src.warehouse.publish --pipeline-name phase10_batch1_rebuild` 실행 | Codex 세션 로그 |
| 09-23 13:36:15 | dbt node 1~16 정상(각 0.1초 이내) | `dbt/logs/dbt.log` |
| 09-23 13:43:00 | test 17 `accepted_values_stg_customer_tier_observations_membership_tier` 405초 후 ERROR | `dbt/logs/dbt.log` |
| 09-23 13:45:37 | test 18 `int_customer_history_no_conflicting_hash` 약 2.5분 소요 | `dbt/logs/dbt.log` |
| 09-23 13:45:47 | test 36에서 로그 중단 — WSL 멈춤. 직후 5분 load average 약 39(8 Core) | `dbt/logs/dbt.log`, `uptime` |
| 09-23 재기동 후 | 사용자가 `wsl --shutdown` 후 WSL을 재기동하고 `free -h` 확인 | Mem 총 11GiB(설정값 12GB의 이진 단위 표시), Swap 총 4.0GiB |

test 17의 오류:

```text
IO Error: Timeout was reached error for HTTP GET to
'http://localhost:8333/commerce-lake/bronze/customer_membership_tiers/ingestion_date%3D2026-09-21/batch_id%3Dbench-extract-...'
```

같은 test는 2026-09-22 09:17 실행에서 0.005초 만에 끝났다.

## 3. 원인

### 3.1 주원인 — 운영 Bronze 카탈로그에 Benchmark Batch 혼입

`pipeline_metadata.bronze_objects`의 COMMITTED 행 집계:

| 구분 | Object 수 | 행 수 |
| --- | ---: | ---: |
| 운영 Batch | 7 | 4,404,997 |
| `bench-*` Batch | 428 | 137,951,230 |

- `bench-*` 행의 `committed_at`은 2026-09-21 03:31 ~ 12:40 UTC, 즉 Phase 9 Benchmark 실행 기간이다.
- Benchmark가 운영과 같은 `pipeline_metadata` DB와 `commerce-lake` Bucket에 적재했다.
- Table별 `bench-*` Object 하나의 최대 행 수는 `order_items` 약 140만, `orders`·`customers`·`order_payments` 약 74만이다.
- `bronze_source()` Macro(`dbt/macros/`)는 이 카탈로그의 Object 전체를 `read_parquet`으로 읽는다.
  따라서 운영 대비 약 31배의 데이터를 읽는다.
- Staging Model은 view다. dbt test 하나마다 S3에서 Parquet 전체를 다시 읽는다.
  그 결과 SeaweedFS가 HTTP Timeout·500을 내고, DuckDB는 메모리를 한계까지 쓴다.

#### Benchmark 데이터가 많은 이유

1. **운영 경로 그대로 사용.** extract 실험(`src/benchmark/experiments/extract.py`)은
   `PostgresSettings.from_environment()`·`SeaweedFSSettings.from_environment()`로 운영 설정을 읽고,
   운영 적재 함수 `ingest_table()`을 호출한다. 그래서 결과가 운영 `bronze_objects`와 `commerce-lake`
   Bucket에 COMMITTED로 남는다. 실험 후 정리(teardown) 단계도 없다.
2. **Full Arm은 반복마다 전량 재적재.** Full Arm은 매 반복 Watermark를 되감아 9개 Table 전량을 다시
   적재한다. 실행 1회당 5회 반복한다. Incremental Arm은 T0 전량 1회와 반복마다 Delta를 적재한다.
3. **실행 재시도 누적.** 실패하거나 INVALID로 판정된 실행을 다시 돌려, 적재분이 실행 횟수만큼 쌓였다.

   | Scale | Arm | 실행 수 | Object 수 | 행 수 |
   | --- | --- | ---: | ---: | ---: |
   | S | full | 7 | 140 | 56,839,066 |
   | S | incremental | 7 | 114 | 17,493,207 |
   | XS | full | 3 | 98 | 52,391,005 |
   | XS | incremental | 3 | 76 | 11,227,952 |

4. **XS도 작지 않음.** Full 적재는 Scale 설정과 무관하게 원천 DB 전량을 읽는다. 실험 중 Generator가
   원천에 Delta를 추가하므로, 원천이 커진 상태에서 전량이 적재됐다. XS Object 하나의 최대 행 수는 약 118만이다.

설계 누락: Benchmark 전용 Metadata DB·Bucket(또는 Prefix) 격리와 실험 후 정리 단계가 없었다.

### 3.2 증폭 요인 — 자원 상한 부재

| 대상 | 현재 설정 | 영향 |
| --- | --- | --- |
| DuckDB (`dbt/profiles.yml`) | `memory_limit: '2GB'`, `threads: 2` 적용 | dbt Node 병렬도와 별개인 DuckDB 내부 메모리·Thread 사용량을 제한한다 |
| Metabase (`compose.yaml`) | `JAVA_OPTS=-Xmx1g`, `mem_limit: 1536m`, `restart: "no"` 적용 | 필요할 때만 기동하고 JVM·Container 메모리 사용량을 제한한다 |
| WSL (`C:\\Users\\kang\\.wslconfig`) | `memory=12GB`, `swap=4GB` 적용 확인 | 재기동 후 `free -h`에서 Mem 총 11GiB(12GB), Swap 총 4.0GiB를 확인했다 |
| Agent 프로세스 | Claude·Codex 4개 + MCP 서버 | 상시 약 2GiB 점유 |

Agent 약 2GiB에 DuckDB(수 GiB), Metabase(1~2GiB), PostgreSQL, SeaweedFS가 더해지면 7.6GiB를 넘는다.

## 4. 부수 피해

- 작업 트리 손상: 멈춤 후 `uv.lock` 전체 삭제 상태(HEAD에서 복구됨).
- Publish Run이 BUILDING으로 남음: `73ebc06a`(09-22). 다음 실행의 Abandoned 회수 대상이다.
- Phase 10 실환경 검증(P6-26·27, P10-01~05)이 두 차례 모두 완료되지 않았다.

## 5. 조치 현황 (2026-09-23)

| # | 조치 | 상태 | 확인/후속 |
| --- | --- | --- | --- |
| 1 | 운영 기준선 재기준화와 다음 Benchmark 저장소 격리 | 완료 | 044 판정에 따라 `bench-*` 428건을 제거했고 후속 Extract·File Format은 전용 DB·Bucket 없이는 차단된다 |
| 2 | `dbt/profiles.yml` `settings`에 `memory_limit: '2GB'`, `threads: 2` 추가 | 완료 | 044 재기준화 뒤 전체 Publish가 `PUBLISHED`로 완료됐다 |
| 3 | Metabase에 `JAVA_OPTS=-Xmx1g`, `mem_limit: 1536m`, `restart: "no"` 적용 | 완료 | 재기준화·Publish 동안 Metabase를 중지했다 |
| 4 | `.wslconfig`에 `memory=12GB`, `swap=4GB` 지정 | 완료 | 사용자가 `wsl --shutdown` 후 WSL을 재기동했고, `free -h`에서 Mem 총 11GiB(설정값 12GB)·Swap 총 4.0GiB를 확인했다 |

조치 1은 근본 원인을 제거했고, 조치 2~4는 같은 유형의 자원 고갈을 예방하는 상한이다.
조치 4는 `.wslconfig`만 바꿔서는 현재 VM에 적용되지 않지만, 사용자가 모든 WSL 배포판과 Docker Desktop의
WSL 연동 작업을 멈춘 뒤 Windows Host에서 `wsl --shutdown`을 실행해 적용했다.
