# 031. Phase 9 실험 A Watermark 되감기 예외와 `change_rate` 정의 판정

> 판정일: 2026-09-21 · architect
> 증상: `WatermarkRewindError: Rewind target must be earlier than the current cursor` (`extract-S-20260921T061043Z`, run_number=1 Incremental Arm)
> 선행 판정: [030](030_phase9-extract-experiment-repeat-protocol.md)

## 1. 되감기 예외 — 대안 (a)(b) 모두 반려, 상태 단언 방식 채택

developer의 원인 분석은 정확하다. T0 선적재가 Incremental Pipeline의 Watermark를 이미
`cursor_before_timestamp(t_boundary)`와 같은 지점까지 전진시키므로, 1회차에서 "목표 == 현재"가 되어
엄격 부등호 제약에 걸린다. `rewind_watermark`의 엄격 부등호를 건드리지 않기로 한 판단은 옳다.
그 제약은 운영 재처리의 안전장치다.

두 대안을 모두 반려한다.

- **(a) `run_number == 1`일 때 되감기 생략** — 반려. "1회차는 특별하다"를 코드에 박는다. 설정 절차가 조금만
  바뀌어도(예: 선적재 범위 변경) 이 가정이 조용히 깨지고, 그때는 되감기가 필요한데도 건너뛴다.
- **(b) `WatermarkRewindError`를 무시** — 반려. 이 예외는 "이미 목표 지점"일 때만 나는 것이 아니다.
  목표가 현재보다 **미래**인 경우, 즉 진짜로 잘못된 상태에서도 같은 예외가 난다. 삼키면 그 사고를 못 본다.

### 채택: 되감기가 아니라 "목표 지점 보장"으로 바꾼다

회차 시작 시 필요한 불변식은 "되감기를 실행했다"가 아니라 **"Watermark가 W0에 있다"**이다. Benchmark 쪽에
멱등 Helper를 두고, 되감기는 그 수단으로만 쓴다.

```text
ensure_watermark_at(pipeline_name, source_table, target_boundary):
    current = get_or_create_watermark(...)      # 기존 공개 함수
    target  = cursor_before_timestamp(...)      # 기존 공개 함수
    current.cursor == target  → 아무것도 하지 않는다 (이미 목표 지점)
    그 외                      → rewind_tables(boundary=target_boundary)
```

- 운영 코드 변경 없음. 두 함수 모두 이미 공개돼 있다.
- 회차 번호에 의존하지 않는다. 1회차든 5회차든 같은 규칙이다.
- 목표가 현재보다 미래인 진짜 이상 상태에서는 `rewind_tables`가 그대로 예외를 올린다. 실패가 보존된다.
- Full Arm에도 같은 Helper를 쓴다. 1회차에 Full Pipeline Watermark가 비어 있으면 같은 충돌이 난다.

## 2. `change_rate` — 측정 방식은 유지, 값은 조정 대상

`updated_at >= t_boundary` Row를 세는 방식 자체는 **옳다.** Incremental Extract의 Cursor가 `updated_at`이므로,
Incremental Arm이 실제로 Scan하는 모집단이 바로 그 집합이다. 신규 삽입만 세면 측정 대상과 지표가 어긋난다.

따라서 지표 정의를 바꾸지 않는다. 대신 두 가지를 고친다.

1. **이름과 정의를 명시한다.** `change_rate`는 "신규 비율"이 아니라 **"변경 Row 비율(신규 삽입 + 기존 행 갱신)"**이다.
   결과 문서에 분자·분모를 그대로 적는다. 지금은 분모가 무엇인지 보고만으로 확인되지 않는다.
2. **원시 3개 값을 보고한다.** T0 Row 수, `updated_at >= t_boundary` Row 수, T1 전체 Row 수. 2.1이라는 비율이
   어느 분모에서 나온 값인지 이 셋 없이는 검증할 수 없다. 표는 Table별로 적는다 — 수집 대상은 9개 Table인데
   지금 값은 `orders` 하나에서 나왔다.

### 값 조정

변경률 210%는 지표 오류가 아니라 **시나리오 설정 오류**로 본다. Delta가 T0보다 크면 Incremental이 Full보다
유리할 이유가 사라지고, Phase 9 문서가 요구한 "변경률이 고정된 Incremental Extract"도 성립하지 않는다.

- T1 Generator 호출의 `order_count`를 낮춰 목표 변경률(기본 10%) 근처로 맞춘다.
- Generator는 신규 주문 외에 기존 Row의 상태 전이·구독 갱신도 수행하므로, `order_count`만으로 변경률이
  선형으로 움직이지 않는다. 몇 개 값을 실측해 목표에 가장 가까운 설정을 고르고, **의도값이 아니라 실측값**을
  기록한다.
- 목표 근처로 맞출 수 없으면 맞추지 말고, 도달 가능한 최소 변경률과 그 이유를 기록한다. 수치를 꾸미지 않는다.

## 3. 일정 관측

S Scale setup이 3,057초(약 51분)다. M Scale은 Order 수가 10배이므로 설정 비용만으로도 수 시간대가 된다.
Task 14 진입 전에 M Scale setup 예상 시간을 실측 근거로 산출해 보고한다. lead에 별도 보고한다.
