# 032 — Phase 9 실험A 공유 원천 누적 판정

- 일자: 2026-09-21
- 판정자: architect
- 대상: `src/benchmark/experiments/extract.py` 설정 단계, 031이 요구한 원시 3개 값 보고
- 선행 판정: [030](030_phase9-extract-experiment-repeat-protocol.md), [031](031_phase9-extract-watermark-and-change-rate.md)

## 보고된 사실

`orders` 등 9개 Source Table은 Benchmark 실행 전용이 아니다. 실행마다 행이 누적되고 삭제되지 않는다.
실측: `COUNT(*)=520460`, `MIN(updated_at)=2026-01-01`, `MAX(updated_at)=2026-09-21 07:10:43`.

발견 자체는 정확하다. 재실행 전에 멈추고 물어본 판단도 옳다.

## 판정 요약

| 항목                                                                           | 판정        |
| ------------------------------------------------------------------------------ | ----------- |
| 제안 1: `t0_row_count`/`t1_row_count`를 `GeneratorResult.result_counts`로 대체 | 반려        |
| 제안 2: `orders`만 정밀 보고하고 나머지 8개는 한계로 명시                      | 반려        |
| 실측 SQL Count 유지 + 누적 상태를 산출물에 기록                                | 승인 (정본) |
| `change_rate` 분모를 `t1_row_count`로 교정                                     | 수정 요구   |
| 재실행 전 원천 초기화(`src/rebaseline.py`)                                     | 하지 않는다 |

## 1. 사실 정정 — 분모는 이미 실측값이다

보고에 "지금 코드도 사실 분모는 `order_count_t0`라는 상수를 쓰고 있어 change_rate 계산 자체는 안전하다"고
적혀 있으나, 현재 코드는 그렇지 않다.

```python
measured_change_rate = orders_stats.delta_row_count / orders_stats.t0_row_count
```

`order_count_t0`는 Generator Config에만 들어가고 비율 계산에는 쓰이지 않는다.
즉 분모는 이미 실측 Count이며, 오염되는 쪽은 "보고용 부가 수치"가 아니라 **지표 본체**다.
이 전제가 뒤집히므로 제안 1의 근거도 성립하지 않는다.

## 2. 제안 1 반려 — `result_counts`는 다른 질문의 답이다

`result_counts["orders_inserted"]`가 답하는 질문은 "이번 Generator 호출이 몇 행을 썼는가"다.
실험A가 답해야 하는 질문은 "Full Arm이 몇 행을 읽는가"다. 둘은 다르다.

Full Arm은 Watermark를 전 구간 앞으로 되감아 **테이블 전량**을 적재한다. 과거 실행이 남긴 42만 행도
똑같이 읽는다. 따라서 Full Arm의 비용을 설명하는 수치는 테이블 전체 Count이지 이번 호출 삽입 건수가
아니다. `result_counts`를 쓰면 "10만 행 읽는 데 걸린 시간"이라고 적어 놓고 실제로는 52만 행을 읽은
시간을 기록하게 된다. 오염을 지우는 게 아니라 오염을 감추는 방향이다.

추가로 `_run_subscription_expiry_scan`은 `_fetch_subscriptions_ordered`로 **구독 테이블 전체**를 훑고
`record.updated_at >= now`만 건너뛴다. 범위 제한이 없으므로 과거 실행이 남긴 행도 갱신한다.
`subscriptions_updated`는 호출 단위로는 정확하지만 "이번 실행이 만든 행"과는 일치하지 않는다.
보고에서 "추가 확인이 필요하다"고 한 부분의 답이다 — 같은 방식으로 쓰면 안전하지 않다.

## 3. 제안 2 반려 — 9개 테이블 전부 실측한다

`orders`만 정밀 보고하는 선택은 다른 8개 테이블의 Row 수를 모르는 상태로 남긴다.
Ingest는 9개 테이블 전부를 적재하므로, 전체 소요 시간을 설명하려면 9개 Count가 전부 필요하다.
실측 SQL은 테이블당 Count 두 번이고 51분짜리 Setup 옆에서 비용이 무시할 수준이다. 생략할 이유가 없다.

`_measure_change_stats`가 이미 테이블별 `TableChangeStats(t0/delta/t1)`를 실측하고 있다. 그대로 쓴다.

## 4. 수정 요구 — `change_rate` 분모를 `t1_row_count`로

`change_rate`의 뜻을 "Full Arm이 읽는 모집단 중 Incremental Arm이 읽는 비율"로 고정한다.
Full Arm의 모집단은 T1 시점 전량이므로 분모는 `t1_row_count`다.

```python
measured_change_rate = orders_stats.delta_row_count / orders_stats.t1_row_count
```

`t0_row_count`를 분모로 쓰면 값이 delta만큼 과대 계상된다. 현재 Scale에서는 차이가 작지만,
정의가 "Incremental이 절약하는 비율"과 어긋나므로 교정한다. 세 값(`t0`/`delta`/`t1`)은 그대로 전부 보고한다.

## 5. 원천 초기화는 하지 않는다

`src/rebaseline.py`의 `run_rebaseline()`이 Source Table Drop·재생성, Bronze Object 삭제, Pipeline
Metadata Truncate, Seed 재적재까지 수행한다. 이걸 실험A 앞에 붙이면 매 실행이 같은 상태에서 출발한다.
그럼에도 쓰지 않는다. 근거:

1. `_reset_bronze_objects`가 Bronze Prefix의 Object를 전부 지운다. 실험B의 SeaweedFS Fixture와
   다른 Phase의 산출물까지 파괴 범위에 들어간다. Benchmark 편의로 공유 상태를 지우는 건 계획의
   "Benchmark 편의를 위해 신뢰성 계약을 건드리지 않는다" 제약에 정면으로 걸린다.
2. Seed 재적재와 Baseline Ingest가 이미 3,057초인 Setup 위에 더 얹힌다.
3. **누적은 한 실행의 타당성을 깨지 않는다.** 5회 반복 동안 원천은 T1에서 동결되고, 두 Arm은 같은
   원천을 읽으며, `result_hash` Gate가 두 Arm 산출이 같음을 강제한다. 비교의 전제는 성립한다.

누적이 실제로 깨는 것은 **실행 간 재현성**과 **Scale 이름표의 정확성**뿐이다. 그 둘은 초기화가 아니라
기록으로 해결한다.

## 6. 정본 — 측정하고 기록한다

1. 9개 테이블 전부 `t0_row_count`/`delta_row_count`/`t1_row_count`를 실측 SQL로 기록한다.
2. `change_rate`는 `delta / t1`로 계산하고, 정의를 "변경 Row 비율(신규 삽입 + 기존 행 갱신)"로 명시한다.
3. **Scale 이름표의 뜻을 문서에 정정한다.** `S = order_count 100,000`은 *Generator 호출 인자*이지
   테이블 Row 수가 아니다. 실제 Full Arm 모집단은 측정된 `t1_row_count`다. 결과 문서 본문 수치는
   전부 실측값을 쓰고, 의도값은 쓰지 않는다.
4. `docs/benchmarks/00-environment.md`에 한계를 명시한다:
   - Source DB는 실행마다 누적되며 Benchmark가 초기화하지 않는다.
   - 따라서 **실행 간 절대 시간 비교는 무효**다. 유효한 비교는 한 실행 안의 Arm 간 비교뿐이다.
   - 각 결과 문서는 그 실행의 실측 `t1_row_count`를 같이 싣는다. 이 수치 없이는 시간 값이 해석되지 않는다.
5. 031의 "목표 변경률 10%"는 **폐기한다.** 분모가 테이블 전량이라 Generator 인자로 10%를 맞출 수 없다.
   대신 `delta_order_count`는 지금 값(`order_count_t0 * change_rate_target`)을 유지하고,
   **실측된 값이 얼마로 나오든 그대로 기록한다.** 수치를 목표에 맞추려고 조정하지 않는다.

## 6-1. 210%의 원인 (031 갱신)

031은 210%를 "시나리오 설정 오류"로 보고 T1 `order_count`를 낮추라고 했다. 실제 원인은 그게 아니다.
T1 Generator 호출에 총량을 넘기면서 결정적 ID가 T0와 서로소인 집합을 새로 만든 것이 원인이고,
이미 `delta_order_count` 수정으로 해결됐다. 031의 해당 지시는 6절 5항으로 대체한다.

## 후속

- developer: 4절 분모 교정, 6절 기록 항목 반영 후 S Scale 실험A 재실행.
- `docs/benchmarks/00-environment.md`는 Task 13 마감 산출물에 포함한다.

## Lead 승인 (2026-09-21)

- 실측값 표기 정본화 승인 — 결과 문서 수치는 전부 실측값. Scale 이름표는 Generator 호출 인자로 정정.
- 실행 간 절대 시간 비교 무효 명시 승인 — Phase 9가 주장하는 범위는 한 실행 안 Arm 간 차이로 한정한다.
