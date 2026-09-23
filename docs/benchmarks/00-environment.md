# Phase 9 벤치마크 — 실행 환경 (S Scale)

버전 2. Task 13(S Scale Harness 검증) 완료 시점과 2026-09-23 Benchmark 저장소 격리 조치 기준.

## 1. 재현 명령

Harness는 `uv run python -m src.benchmark <command>` CLI로 실행한다(`src/benchmark/__main__.py`).

```bash
# 결과 조회 (Cold/Warm 비교표)
uv run python -m src.benchmark report --benchmark-id <benchmark_id>
```

실행(run) 명령은 실험별로 다르다.

### 저장소를 쓰는 실험의 격리 전제

2026-09-23 이전 Extract·File Format 실험은 운영 `commerce_source`,
`pipeline_metadata`, SeaweedFS Bucket을 함께 사용해 운영 Bronze에 `bench-*` Object를
혼입시켰다. 이 실행 결과는 당시의 성능 증거로 보존하되, 같은 저장소에 다시 실행해서는
안 된다. 판정과 재기준화 결과는 [044 Benchmark Bronze 혼입 판정](../architect-review/044_benchmark-bronze-isolation-ruling.md)이 정본이다.

`extract`와 `file_format`은 다음 세 환경 변수가 모두 있어야 실행된다.

```text
BENCHMARK_COMMERCE_SOURCE_DB
BENCHMARK_PIPELINE_METADATA_DB
BENCHMARK_SEAWEEDFS_BUCKET
```

각 값은 대응하는 운영 값과 반드시 달라야 한다. 누락하거나 같으면 코드가 fail-closed로
실행을 거부한다. Host·Port·계정은 운영 연결 값을 재사용한다. 전용 DB·Bucket은 다음
Benchmark 계획에서 준비한다. 저장소를 쓰지 않는 `scan`과 `cache_effect`는 이 Guard 대상이 아니다.

### Experiment B(File Format)

Fixture 크기가 `--scale`의 `order_count`에서 그대로 나오므로 바로 실행한다.

```bash
uv run python -m src.benchmark run --scenario file_format --scale S
```

### Experiment C(Scan) / D(Cache Effect)

두 실험의 Fixture 크기는 `ScaleProfile.order_count`와 무관하게 호출자가 명시적으로
정한다([[028_phase9-batch2-experiment-method]] — `old_rows = current_rows * 20` 같은
고정 배수를 Scale 표와 어긋난다는 이유로 제외했다). `--fixture-old-rows`/
`--fixture-current-rows`로 넘긴다. `cache_effect`는 `run` Subcommand 실행 전
`PREPARE_HOOKS`에 등록된 `prepare_cache_effect_fixture`가 Fixture를 반복 밖에서
먼저 만들고(Cold 회차 안에서 처음 쓰이는 걸 막음), Cold Config에는
`cache_reset_paths`를, Warm Config에는 측정에 넣지 않는 예열 1회를 심어 둔다.

```bash
uv run python -m src.benchmark run --scenario scan --scale S \
  --fixture-old-rows 2000 --fixture-current-rows 100
uv run python -m src.benchmark run --scenario cache_effect --scale S --cold \
  --fixture-old-rows 2000 --fixture-current-rows 100
uv run python -m src.benchmark run --scenario cache_effect --scale S --warm \
  --fixture-old-rows 2000 --fixture-current-rows 100
```

Fixture는 `{scale.name}`별로 한 번만 쓰고 재사용한다(`data/benchmarks/fixtures/<scale>/`) —
이미 있으면 위 Row 수 인자는 무시되고 기존 파일을 그대로 쓴다.

### Experiment A(Extract, Full vs Incremental)

`extract` Scenario는 반복 시작 전 `prepare_extract_fixture`로 Watermark Anchor(t0/t_boundary/t1,
[[035_phase9-extract-anchor-derivation]] 참조)를 먼저 산출해야 한다. `run` Subcommand는
`src/benchmark/experiments/__init__.py`의 `PREPARE_HOOKS` Registry를 확인해 Scenario 이름이
등록돼 있으면 `run_experiment` 전에 자동으로 호출한다(`extract`만 등록됨) — 다른 실험과 동일하게
아래 한 줄로 재현한다.

```bash
uv run python -m src.benchmark run --scenario extract --scale S
```

### Harness Overhead(측정 바닥값)

```bash
uv run python -m src.benchmark run --scenario harness_overhead --scale S
```

## 2. 환경 메타데이터

각 Run은 `BenchmarkRun`(`src/benchmark/store.py`)에 아래 값을 자동 기록한다(`src/benchmark/metadata.py:collect_environment`).
아래는 `harness_overhead-S-20260921T104453Z` 실행 시점 실측값이다.

| 항목 | 값 |
|---|---|
| `git_commit` | `5f0adc5645b884c66e69d5e7ef22f031e295dd5e` (당시 `-dirty`) |
| `python_version` | 3.12.3 |
| `dependency_lock_hash` | `a40e99ad99908a4b104568316b0458e571dc563c183ad251faa65e5569bf1cbb` |
| `cpu_model` | 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz |
| `cpu_count` | 8 |
| `mem_total` | 7990908 kB |
| `kernel_release` | 6.18.33.2-microsoft-standard-WSL2 |
| `postgres` | postgres:18.6 |
| `seaweedfs` | chrislusf/seaweedfs:4.45 |
| `airflow` | commerce-data-platform-airflow:3.3.1 |

## 3. Cache 초기화 절차 (`src/benchmark/cache.py:reset_caches`)

Cold Run 매 회차 전 아래 순서로 하나를 선택한다(상위 실패 시 다음 단계로 대체):

1. `drop_caches` — `sync` 후 `/proc/sys/vm/drop_caches`에 `3` 기록. 권한 없으면 2로.
2. `fadvise_dontneed` — 지정 경로에 `sync` 후 `posix_fadvise(POSIX_FADV_DONTNEED)`. Clean Page만 비우므로 Dirty Page 유실 없음. 대상 파일 없거나 실패하면 3으로.
3. `process_restart_only` — `docker compose restart <services>`로 대체. 그마저 실패하면 Cache 미초기화로 기록.

Experiment D(S Scale) 실측: `fadvise_dontneed`로 성공(Cold 5/5 VALID, Warm 5/5 VALID, `result_hash` Cold/Warm 각각 일치).

## 4. Dataset Identity (S Scale)

- Generator 입력(불변): `order_count=100000`, `random_seed=20260921` (`FIXED_RANDOM_SEED`, `src/benchmark/config.py`).
- **원천 DB는 Benchmark 실행마다 초기화되지 않고 영구 누적된다**([[032_phase9-shared-source-accumulation]]) — Source Table의 실제 행 수는 위 Generator 입력만으로 고정되지 않으며, 그 시점까지 누적된 모든 실행분을 포함한다. 따라서 절대 행 수 비교는 Run 간에 무효고, 각 결과 문서는 **자기 Run이 실측한 `t1_row_count`**를 그대로 남겨야 한다(단발 스냅샷으로 취급).
- 참고용 실측값 — `extract-S-20260921T113012Z`(이 문서 작성 시점 가장 최근 S Scale Extract 실행)의 Setup 단계 실측:

  | Table | t0 | delta | t1 |
  |---|---|---|---|
  | customers | 732110 | 10000 | 742110 |
  | customer_membership_tiers | 728765 | 10000 | 738765 |
  | orders | 732110 | 10000 | 742110 |
  | order_items | 1379429 | 19982 | 1399411 |
  | order_payments | 736555 | 10000 | 746555 |
  | products | 32951 | 0 | 32951 |
  | sellers | 3095 | 0 | 3095 |
  | customer_subscriptions | 0 | 0 | 0 |
  | subscription_payments | 0 | 0 | 0 |

- Watermark Anchor(같은 실행): `t0=2026-09-21T11:52:53+00:00`, `t_boundary=11:53:53`, `t1=11:54:53` — 원천 실측 최대 Cursor 기준 산출([[035_phase9-extract-anchor-derivation]]), 매 실행마다 단조 증가.

## 5. Harness Overhead (측정 바닥값)

No-op Scenario 5회 실측(`harness_overhead-S-20260921T104453Z`):

- Raw: `[4.09e-05, 5.05e-05, 7.42e-05, 4.82e-05, 4.95e-05]` 초
- Median: `4.95e-05`초 (약 49.5 마이크로초)

Experiment A Incremental Arm의 최소 관측 시간(S Scale, 5.77~5.94초)과 비교하면 Harness 자체 오버헤드는
무시 가능한 수준(약 0.001% 미만)이다.

## 6. Task 13 S Scale 검증 결과 요약

4개 실험 + Harness Overhead 전부 5/5 `VALID`, Arm 간 `result_hash` 일치 확인:

| 실험 | Benchmark ID | 상태 |
|---|---|---|
| A(Extract) | `extract-S-20260921T113012Z` | 5/5 VALID, hash `e21c24726b9c` |
| B(File Format) | `file_format-S-20260921T050038Z` | 5/5 VALID, hash `a207239af6af` |
| C(Scan) | `scan-S-20260921T050036Z` | 5/5 VALID, hash `12393763410f` |
| D(Cache Effect, Cold) | `cache_effect_cold-S-20260921T050505Z` | 5/5 VALID, hash `12393763410f` |
| D(Cache Effect, Warm) | `cache_effect_warm-S-20260921T050505Z` | 5/5 VALID, hash `12393763410f` |
| Harness Overhead | `harness_overhead-S-20260921T104453Z` | 5/5 VALID |

Experiment A는 Watermark Anchor 산출 방식([[031_phase9-extract-watermark-and-change-rate]] →
[[033_phase9-extract-t0-anchor-collision]] → [[034_phase9-extract-watermark-noop-gate]] →
[[035_phase9-extract-anchor-derivation]])을 4차례 정정한 뒤 이 결과에 도달했다. 세부 경과는
`docs/architect-review/029`~`036`에 있다.

## 7. 알려진 한계

- Bronze Object 정리(Rebaseline)는 이번 Phase 범위에서 명시적으로 제외했다([[032_phase9-shared-source-accumulation]] §5).
