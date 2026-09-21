# 04. 실험 D — Cache 효과(Cold vs Warm)

버전 3. Task 15(L Scale) 결과 추가.

> M Scale · Repeats: 5(Cold)+5(Warm)
> Cold Benchmark ID: `cache_effect-M-20260921T134502Z`
> Warm Benchmark ID: `cache_effect-M-20260921T134505Z`
> L Scale · Repeats: 5(Cold)+5(Warm)
> Cold Benchmark ID: `cache_effect-L-20260921T142746Z`
> Warm Benchmark ID: `cache_effect-L-20260921T142752Z`

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

**Cold/Warm 두 모집단이 분리되어 관측됐으나, 대조군 실측 결과 그 차이를
Page Cache 효과로 귀속할 수 없다 — 이 환경에서는 Cache 효과를 분해할 수
없었다.** Median 기준 Cold가 Warm보다 0.003456초(3.46ms, +13.5%) 느리고,
Raw 값 정렬 기준 Cold 최솟값(0.027451)이 Warm 최댓값(0.027197)보다 커서
관측 범위가 겹치지 않는 것 자체는 사실이다:

- Cold 정렬: `[0.027451, 0.027976, 0.029064, 0.029785, 0.029916]`
- Warm 정렬: `[0.021908, 0.023983, 0.025608, 0.026824, 0.027197]`

하지만 이 비중첩을 "Cache 효과가 관측됐다"로 귀속하려면 아래 §대조군
실측에서 확인한 세 가지 반증을 넘어서야 하는데, 넘어서지 못했다.

## 대조군 실측

Cold와 Warm이 분리된 원인이 진짜 Page Cache 재적재인지, `os.sync()`
Writeback인지, 아니면 Arm/시간 순서 교락인지를 가르기 위해 두 대조군을
같은 Fixture·Query로 추가 실측했다(위 결과 재현 이후, 별도 진단 스크립트로
실행 — `store.py` 적재 파이프라인에는 넣지 않았다. 1회성 진단이라 영구
Scenario로 등록하지 않았다).

```
대조군 A(sync만, fadvise 없음): raw=[0.026025, 0.026395, 0.023088, 0.022247, 0.021987]
                                median=0.023088
교차 실행 Cold(5쌍 중 Cold):    raw=[0.030232, 0.027424, 0.029388, 0.028819, 0.026512]
                                median=0.028819
교차 실행 Warm(5쌍 중 Warm):    raw=[0.029460, 0.024380, 0.023340, 0.025105, 0.023164]
                                median=0.024380
```

- **대조군 A ≈ Warm**: median 0.023088초로 원래 Warm median(0.025608초)에
  더 가깝고 원래 Cold median(0.029064초)과는 거리가 멀다. `sync`만으로는
  원래 관측된 Cold 수준의 느려짐이 재현되지 않는다 — `os.sync()` 단독
  귀속(한계 2번)은 기각된다.
- **교차 실행에서는 분리가 무너진다**: 교차 Cold 최솟값(0.026512)보다
  교차 Warm 최댓값(0.029460)이 더 커서 두 모집단의 관측 범위가 겹친다.
  같은 Fixture·Query인데도 Cold를 먼저 순차 블록으로 5회 몰아 돌렸을 때만
  분리가 나타나고, Cold·Warm을 번갈아 돌리면 분리가 사라진다 — 원래 관측된
  비중첩은 Page Cache 상태가 아니라 **실행 순서**에 달려 있었다는 뜻이다.

**지시된 판정 규칙**([[037_phase9-task14-review-and-median-correction]] §4)에
따르면 대조군 A ≈ Warm **이고** 교차 실행에서도 분리가 유지돼야 Cache 효과로
적을 수 있다. 대조군 A는 Warm에 가깝지만 교차 실행에서 분리가 무너졌으므로
조건을 만족하지 못한다 — [[029_phase9-cache-reset-and-config-hash]]가 예고한
대로 "이 환경에서는 Cache 효과를 분해할 수 없었다"로 닫는다.

## L Scale 결과

```
Cold: raw=[0.038781000, 0.039863487, 0.041221464, 0.041856500, 0.053842433]
      median=0.041221464
Warm: raw=[0.028627063, 0.031761185, 0.033184949, 0.034307656, 0.035779098]
      median=0.033184949
result_hash(양쪽 동일): d70750ab6d096a880e5529163119f3aa99ac5fe8f8ec582b450ade9c2fb2e899
```

5/5 `VALID` 양쪽 모두, `result_hash` 일치(실험 C의 L Scale `filtered_scan`과
같은 Query·Fixture라 Hash도 같다).

Median 기준 Cold가 Warm보다 0.008037초(24.2%) 느리고, Raw 정렬 기준으로도
Cold 최솟값(0.038781)이 Warm 최댓값(0.035779)보다 커서 M Scale과 같은
비중첩 형태가 다시 나타난다. 하지만 이 실험의 귀속 판정은 이미 위
§대조군 실측에서 닫혔다 — 대조군 A·교차 실행 실측 결과 M Scale의 비중첩은
Page Cache 재적재가 아니라 Arm/시간 순서 교락(순차 블록 실행)에서 나온다는
것이 확인됐고, L Scale도 Cold를 먼저, Warm을 나중에 돌리는 같은 순차 블록
구조로 실행했다. 대조군을 L에서 다시 실측하지 않았으므로(037은 이 진단을
M Scale 1회로 요청했다), L의 비중첩을 독립적 증거로 쓰지 않는다 — 같은
구조적 교락을 가진 반복 관측으로만 기록한다. 결론은 바뀌지 않는다: "이
환경에서는 Cache 효과를 분해할 수 없었다."

## 한계

- **크기가 안 맞는다.** 이 Query가 실제 읽은 Byte는 19,322 bytes(§Fixture
  참조)다. Page Cache에서 밀려난 19KB를 다시 읽는 비용은 어떤 매체에서도
  1ms 미만이어야 하는데, 원래 관측된 Cold/Warm 차이는 3.46ms로 한 자릿수
  이상 크다 — Page Cache 재적재만으로는 이 크기의 차이를 설명할 수 없다.
- **`os.sync()` 교란.** `reset_caches()`는 `fadvise` 전에 매 Cold 회차마다
  `os.sync()`를 부른다(`src/benchmark/cache.py`). 시스템 전역 Dirty Page
  Writeback이 Cold 측정 직전마다 발생해 측정 구간에 겹치면 Cold에만
  계통적으로 시간이 붙을 수 있다. 다만 대조군 A 실측 결과 `sync` 단독
  으로는 원래 Cold 수준 지연이 재현되지 않아, 이 교란만으로 전체를
  설명하지는 못한다.
- **Arm과 시간 순서가 교락됐다.** 원 측정은 Cold(`…134502Z`)가 먼저, Warm
  (`…134505Z`)이 나중인 별개 순차 블록이었지 교차 실행이 아니었다. S
  Scale에서는 Cold가 오히려 Warm보다 빨랐던 것도 순서·잡음이 지배할 때
  나오는 모습이었다. 위 교차 실행 대조군에서 분리가 실제로 무너져, 이
  교락이 원 관측의 실질적 원인이었음을 뒷받침한다.
- WSL2 환경에서는 Guest의 `fadvise`가 Host VHD의 Page Cache까지는 못 비운다는
  제약이 이미 알려져 있다([[028_phase9-batch2-experiment-method]]). 이
  구분은 어차피 위 판정으로 이 실험 범위 밖이 됐다 — Cache 효과 자체를
  분해하지 못했으므로 Guest/Host 계층 구분은 다음 단계가 아니다.
- 5회 반복(대조군도 각 5회)은 최솟/최댓값 비교로 겹침 여부를 판단하기에는
  적은 표본이다 — 통계적 유의성 검정을 하지 않았다.
