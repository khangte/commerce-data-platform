# 029. Phase 9 실험 D Cache 초기화 수단과 `scenario_config_hash` 회귀 판정

> 판정일: 2026-09-21 · architect
> 대상: `fadvise_dontneed` 도입 이후의 `src/benchmark/cache.py`, `runner.py`, `config.py`, `experiments/cache_effect.py`
> 선행 판정: [028](028_phase9-batch2-experiment-method.md)

## 판정 요약

| 항목                                              | 판정                                                      |
| ------------------------------------------------- | --------------------------------------------------------- |
| `fadvise_dontneed` 도입과 우선순위                | 승인. 이 환경에서 실제로 동작한 유일한 무권한 초기화 수단 |
| 실험 D S Scale 측정 자체                          | 승인. 5회 전부 `fadvise_dontneed`, Hash 일치, INVALID 0   |
| "Cache 효과 없음"을 최종 결론으로 확정            | **반려.** S Scale Workload가 결론을 내리기에 너무 작다    |
| `scenario_config_hash`에 `cache_reset_paths` 유입 | **반려.** Task 2 계약 회귀                                |

## 1. 실험 D 결론 확정 반려

관측값은 cold `[0.025404, 0.022020, 0.016653, 0.016178, 0.017103]` median `0.017103`,
warm `[0.020127, 0.015764, 0.019563, 0.015180, 0.019656]` median `0.019563`이다. 두 분포는 완전히 겹친다.
"유의차 없음"이라는 서술 자체는 데이터에 충실하다. 문제는 그 다음 문장이다.

이 관측에서 "Cache 효과가 없다"는 결론은 나오지 않는다. 두 가지 교란 요인이 결론을 막는다.

1. **Workload가 너무 작다.** S Scale Fixture는 3개 Column, 약 105,000 Row의 압축 Parquet이다. Cold Read가
   추가로 옮겨야 하는 Byte 수가 회차 간 산포(약 10ms)보다 작으면 Cache 효과는 원리상 노이즈에 묻힌다.
   측정 하한 대비 228배라는 값은 "Query가 무시할 만큼 짧지 않다"만 보증하고, "Cache 차이를 분해할 수 있다"는
   보증하지 않는다. 두 기준은 다르다.
2. **WSL2 Guest의 `fadvise`는 Host Cache를 비우지 못한다.** Guest Page Cache에서 쫓아낸 Page가 Host의 VHD
   Cache에 그대로 남아 있으면, Guest 기준 Cold Read가 물리 Disk까지 내려가지 않는다. 이것은 이 플랫폼의
   실제 한계이며 숨길 것이 아니라 문서에 적을 사실이다.

Cold `raw`의 앞 두 회차가 가장 크고 이후 낮아지는 것도 Cache 효과가 아니라 Process 예열 추세로 읽힌다.

### 지시

- 실험 D의 최종 결론은 **M Scale에서 다시 측정한 뒤** 확정한다. S Scale 결과는 "Harness 검증용 관측"으로만 남긴다.
- M Scale 재측정 시 Fixture의 **실제 On-disk Byte 수**를 문서에 적는다. Cold/Warm 판단의 전제가 되는 값이다.
- M Scale에서도 두 분포가 겹치면 그것이 결론이다. 그때는 "이 환경에서는 Cache 효과를 분해할 수 없었다"로 쓰고,
  위 교란 요인 2개를 한계 절에 명시한다. "Cache 효과가 없다"로 쓰지 않는다. 두 문장은 다른 주장이다.
- 초기화 수단 3가지 실측 기록(명령·에러 메시지 원문 포함)과 `process_restart_only` 참고 관측치는 그대로 보존한다.

## 2. `scenario_config_hash` 회귀 반려

`config.py`가 `json.dumps(..., default=str)`로 바뀌면서 `config.parameters`에 새로 들어온
`cache_reset_paths`(절대 경로 `Path`)가 Hash 입력에 포함됐다. 두 가지가 깨진다.

1. **Cold와 Warm이 서로 다른 `scenario_config_hash`를 갖는다.** Cold Config에만 `cache_reset_paths`가 심긴다.
   이 필드는 "같은 Scenario 정의로 측정했다"를 증명하려고 만든 것인데, 정작 같은 Scenario의 두 모집단이
   다른 값을 갖게 된다. 실험 D의 증거력을 직접 깎는다.
2. **Hash가 장비 의존이 된다.** 절대 경로가 들어가므로 다른 Clone에서 같은 Scenario를 돌려도 Hash가 달라진다.
   재현 가능성을 증명하는 필드가 재현 불가능해진다.

### 지시

- 실행 배선용 Key를 Hash 입력에서 제외한다. `_NON_DEFINING_PARAMETERS = frozenset({"cache_reset_paths"})`를 두고
  `parameters`에서 걸러낸 뒤 직렬화한다.
- `default=str`를 제거한다. 직렬화할 수 없는 값이 들어오면 조용히 문자열로 바꾸지 말고 실패해야 한다.
  그래야 다음에 같은 유입이 생길 때 즉시 드러난다.
- 제외 후 Cold Config와 Warm Config의 `scenario_config_hash`가 같은지 확인하는 회귀 테스트를 추가한다.
