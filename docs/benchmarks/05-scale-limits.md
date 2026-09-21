# 05. Scale 한계 — Task 15(L Scale)

버전 1. Task 15(L Scale) 결과.

> Scale: L(5,000,000 주문) · 실행 실험: B(`file_format`)/C(`scan`)/D(`cache_effect`)/`harness_overhead`
> Extract(A)는 실행하지 않았다 — §1 참조.

## 1. Extract(A) 제외 사유

L Scale에서도 Extract(실험 A)는 실행하지 않는다. M Scale 제외 결정
([[037_phase9-task14-review-and-median-correction]])을 그대로 적용한
것이지 Plan 위반이 아니다 — `docs/superpowers/plans/2026-09-21-phase9-benchmark.md`의
Task 15 "동일 실험 세트" 문구는 lead의 M 제외 결정보다 먼저 쓰였다.

M에서 제외한 두 사유는 L에서 개선되지 않고 악화된다:

1. **Setup 소요.** M 외삽 약 15,071초(약 4.2시간)에서 L은 외삽 약 72,533초
   이상(약 20.1시간 이상, §2 참조)으로 늘어난다.
2. **원천 영구 누적으로 인한 Scale 간 비교 무효화**([[032]]). 원천 DB가
   재설정되지 않고 계속 누적되므로 Scale이 커질수록 이 문제도 그대로
   남는다.

L에서 새로 추가되는 사유:

3. **되돌릴 수 없는 쓰기.** L Extract를 돌리면 Generator가 주문
   5,000,000건을 공유 원천에 영구히 쓴다. 실행을 멈추거나 실패해도 이미
   쓴 분량은 되돌릴 수 없고, 이후 모든 Full Arm 측정의 모집단이 그만큼
   영구히 커진다. 가용 메모리가 8GB 중 약 2.6~3.3GB 수준인 이 환경에서
   20시간 이상 걸리는 실행이 중간에 실패하면 원천만 오염되고 얻는 결과는
   없다.

## 2. Setup 소요 — 실측 대 외삽

**실측값**(01 문서, 수정 이후 같은 경로 실행, 단일 실행값 — Setup은
반복 측정 항목이 아니라 실행당 1회만 나오므로 Raw 여러 값의 범위가 없다):

| Scale | 주문 건수 | Setup(초) |
| --- | --- | --- |
| XS | 500 | 487.8(실측) |
| S | 100,000 | 1931.2(실측) |

과거에 "S Setup 1793~3057초 범위"로 기록하려던 시도가 있었으나 폐기했다
— 두 값은 033/035 수정 이전, 서로 다른 두 크래시/무효 실행
(`extract-S-20260921T061043Z` WatermarkRewindError, `extract-S-20260921T071933Z`
5/5 INVALID)에서 나온 단일값이라 같은 모집단이 아니다. 이걸 범위로 묶는
것은 143.58초 사고와 같은 종류의 모집단 혼합 오류이므로 이 문서에는
싣지 않는다.

**외삽값**(01 문서와 같은 2점 분해 방법 재사용): XS→S 구간에서 주문
99,500건 증가에 Setup이 1443.4초 늘었으므로, Generator 성분은 주문당
약 0.0145065초(약 14.5ms/주문)다. 이 비율은 새로 생성되는 Row 수(주문
건수)에 비례하는 성분이고, 나머지는 원천 누적 Row 수에 걸리는 Ingest
성분으로 간주한다.

| Scale | 주문 건수 | Setup(초, 외삽) | 비고 |
| --- | --- | --- | --- |
| M | 1,000,000 | 약 15,071(01 문서 기존 값) | Generator 성분 약 14,506.5초 + Ingest 성분(역산) 약 564.5초 |
| L | 5,000,000 | 약 72,533초 이상(약 20.1시간 이상) | Generator 성분 약 72,532.7초만 정밀 외삽. L 시점 원천은 M 때보다 더 누적돼 있어 Ingest 성분이 더 붙지만 정량화하지 않았다 — 그래서 "이상" |

재계산 방법: `per_order = (1931.2 - 487.8) / (100000 - 500) ≈ 0.0145065초`,
`L Generator = per_order * 5,000,000 ≈ 72,532.66초`. M 총계(15,071초)에서
`M Generator ≈ 14,506.5초`를 빼면 `M Ingest(역산) ≈ 564.5초`가 나온다 — 이
방법 자체는 01 문서의 기존 방법이다.

## 3. L 실행 전 확인

디스크: 실행 전 `df -h /` 기준 1007G 중 33G 사용(4%), 923G 여유 — 충분.

메모리: 8GB(7.6Gi) 총 RAM, 2GB Swap. `file_format` L 실행 중·후 `free -h`
기준 가용(`available`) 메모리가 최저 약 703Mi(`free` 열)까지, Swap
사용량이 최대 약 1.8Gi까지 관측됐다 — 8GB 박스에서 L Scale은 실제로
메모리 압박이 있었다.

Fixture On-disk Byte 크기(M의 약 5배로 예상 → 실측 확인):

| 실험 | 파일 | M 크기 | L 크기(실측) |
| --- | --- | --- | --- |
| B(`file_format`) | CSV `sellers.csv` | 71,500,057 bytes (68.2 MB) | 361,500,057 bytes (344.8 MB) |
| B(`file_format`) | Parquet `sellers.parquet` | 19,699,382 bytes (18.8 MB) | 30,633,598 bytes (29.2 MB, §4 압축 인공물 참조) |
| C/D(`scan`/`cache_effect`) | `old.parquet` | 39.6 MB | 200.0 MB |
| C/D(`scan`/`cache_effect`) | `current.parquet` | 7.9 MB | 40.5 MB |

## 4. 실험별 결과 요약

네 실험 전부 자원 한계 없이 성공했다(5/5 `VALID`, 결과 정합성 확인).
자세한 Raw 값·해석은 각 실험 문서(02/03/04) 참조.

| 실험 | Benchmark ID | 상태 | 요약 |
| --- | --- | --- | --- |
| B(`file_format`) | `file_format-L-20260921T140402Z` | 성공 | csv median 93.762s, parquet median 93.819s, change +0.1%(M +0.4%와 같은 Null 결과) |
| C(`scan`) | `scan-L-20260921T142352Z` | 성공 | full_scan median 0.12422s, filtered_scan median 0.03601s, change -71.0% |
| D(`cache_effect`) Cold | `cache_effect-L-20260921T142746Z` | 성공 | median 0.04122s |
| D(`cache_effect`) Warm | `cache_effect-L-20260921T142752Z` | 성공 | median 0.03318s (M과 같은 구조적 교락으로 귀속하지 않음, 04 문서 참조) |
| `harness_overhead` | `harness_overhead-L-20260921T142810Z` | 성공 | median 5.393e-05초, Scale 무관(예상대로 일정) |

B에서 실행 도중 `file_format.py`의 `_ensure_fixture`가 Row 수에 비례해
날짜를 무한정 늘리는 버그(`timedelta(days=index)`)로 첫 시도가
`OverflowError`로 실패했다 — 이건 자원 한계가 아니라 결정적 코드 결함으로,
`days=index % 3650` Bound를 기존 `seller_city`/`seller_state` 패턴에 맞춰
추가해 고쳤다(commit `12c9dec`). 고친 뒤 재시도는 성공했다.

## 5. 결론

L Scale에서 B/C/D/`harness_overhead` 네 실험 모두 자원 한계 없이 완료했다.
Extract(A)는 §1의 사유로 처음부터 실행하지 않기로 결정했으며, 이는 Plan
Task 15의 실패 기록 규칙(실행해서 실패한 경우 실측값만 기록) 대상이
아니다 — 실행하지 않기로 한 범위 결정이므로 §2의 외삽 계산 근거로
대신한다.
