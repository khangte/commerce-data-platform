# 04. 실험 D — Cache 효과(Cold vs Warm)

버전 1. Task 14(M Scale Baseline) 결과.

> Scenario: `cache_effect` · Scale: M · Repeats: 5(Cold)+5(Warm)
> Cold Benchmark ID: `cache_effect-M-20260921T134502Z`
> Warm Benchmark ID: `cache_effect-M-20260921T134505Z`

## 가설·측정 범위

실험 C의 Filtered Scan Query(`filtered_scan` Arm과 완전히 동일한 SQL·Fixture)
하나만 떼어, Cold Cache와 Warm Cache 상태에서 각각 5회씩 측정해 Cache 효과가
관측되는지 확인한다. Cold·Warm은 서로 다른 Benchmark ID로 완전히 분리해
기록하며 한 모집단에 섞지 않는다.

S Scale(`cache_effect_cold-S-20260921T050505Z` /
`cache_effect_warm-S-20260921T050505Z`)에서는 Cold/Warm 차이가 관측되지
않았다 — Workload가 너무 작아(Fixture `old_rows=2000, current_rows=100`)
약 10ms 수준의 측정 잡음보다 작았기 때문으로 판단해, M Scale에서
Fixture를 키워 재측정하는 것이 판정 조건이었다([[028_phase9-batch2-experiment-method]]
Task 12 결정).

## Fixture

실험 C와 같은 Fixture를 공유한다(`ensure_scan_fixture`, Scale별 1회 생성).
S Scale 그대로 단순 비례 확대하면 여전히 Byte 수가 작아 Cache 효과가
노이즈에 묻힐 것으로 판단해, Parquet 행당 실측 Byte 수(약 8.3 byte/row)를
먼저 재고 `fixture_old_rows=5,000,000`, `fixture_current_rows=1,000,000`으로
정했다.

| 파일 | On-disk 크기 |
| --- | --- |
| `old.parquet` | 39.6 MB (Row Group Pruning으로 실제 Scan 경로에서 제외됨, 아래 참조) |
| `current.parquet` | 7.9 MB |

이 Query(`WHERE bucket='current'`)의 `rows_scanned`는 실험 C의 같은 Query
측정에서 1,000,000으로, `fixture_current_rows`와 정확히 일치한다 — Pruning이
`old.parquet`을 완전히 건너뛰므로 이 실험의 실질 Cache 대상은 `current.parquet`
(7.9 MB)이다. `PRAGMA enable_profiling` 기준 실제 읽은 Byte 수는 19,322
bytes(Column 단위 압축·Dictionary Encoding 이후 값, 실험 C 문서 참조).

## Cache 초기화

`cache_reset_method` 실측값: **`fadvise_dontneed`**(Cold 5회 전부). `drop_caches`
권한이 없어 File 단위 `posix_fadvise(POSIX_FADV_DONTNEED)`로 대체됐다
(`src/benchmark/cache.py:reset_caches`). Cold 회차 전에 Fixture가 이미
디스크에 있음을 `prepare_cache_effect_fixture`(PREPARE_HOOKS)가 먼저
보장한다 — 그렇지 않으면 첫 Cold 회차가 방금 쓴(=이미 Page Cache에 올라간)
파일을 읽어 Cold 측정이 깨진다([[028_phase9-batch2-experiment-method]] Task 12
지적 사항).

## 결과

```
Cold: raw=[0.029063759, 0.027450639, 0.029915592, 0.029785389, 0.027975996]
      median=0.029063759
Warm: raw=[0.021908307, 0.027196593, 0.023983334, 0.026824280, 0.025608052]
      median=0.025608052
result_hash(양쪽 동일): ba79ec6cffe9f3d5676ce4008ccedb108392efcf87548ce2daf8388dd2a1f711
```

5/5 `VALID` 양쪽 모두, `result_hash` 일치.

**M Scale에서 Cold/Warm 차이가 관측됐다.** Median 기준 Cold가 Warm보다
0.003456초(3.46ms, +13.5%) 느리다. Raw 값 정렬 기준 Cold 최솟값(0.027451)이
Warm 최댓값(0.027197)보다 커서 두 모집단의 관측 범위가 겹치지 않는다:

- Cold 정렬: `[0.027451, 0.027976, 0.029064, 0.029785, 0.029916]`
- Warm 정렬: `[0.021908, 0.023983, 0.025608, 0.026824, 0.027197]`

## 한계

- WSL2 환경에서는 Guest의 `fadvise`가 Host VHD의 Page Cache까지는 못 비운다는
  제약이 이미 알려져 있다([[028_phase9-batch2-experiment-method]]). 이번
  측정에서는 그럼에도 Cold/Warm이 겹치지 않는 차이를 보였지만, 그 차이가
  "Guest fadvise가 부분적으로만 효과가 있었다"는 뜻인지 "Host VHD 계층에서도
  별도의 I/O 경로 차이가 났다"는 뜻인지까지는 이 측정만으로 구분할 수 없다.
- 5회 반복은 최솟/최댓값 비교로 겹침 여부를 판단하기에는 적은 표본이다 —
  통계적 유의성 검정을 하지 않았고, Median 차이(13.5%)를 그대로 보고할 뿐
  신뢰구간은 산출하지 않았다.
