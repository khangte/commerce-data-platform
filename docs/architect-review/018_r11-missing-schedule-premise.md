# 018. R-11의 전제가 틀렸다 — Watermark 연속성은 누락 창을 스스로 메운다

> 판정: architect, 2026-09-21 (Phase 8 Task 9 착수 중 developer 질의)
> 관련: AC-11, R-11, [Phase 8 계획](../superpowers/plans/2026-09-20-phase8-reliability-scenarios.md) Task 9

## 질의

계획 Task 9의 R-11 Step은 "윈도 A 실행, 윈도 B 건너뛰기, 윈도 C 실행, **Gap 관측**, rewind + 명시 Batch로 회복"이다. developer가 이를 문자 그대로 구현하려다 Gap이 생기지 않음을 발견했다. `open_table_snapshot`의 `extract_upper_bound`는 `logical_date`가 아니라 호출 시점 Source의 실제 최대 Cursor다(`_fetch_upper_bound`, `src/ingestion/extract.py:131`). Watermark는 연속이므로 C 실행이 B 구간까지 함께 쓸어 담는다.

Gap을 만들려면 Test가 `_set_watermark`로 Watermark를 C 직전으로 인위적으로 밀어야 한다. 그 구성으로 진행해도 되는지 물었다.

## 판정

**developer의 진단이 맞다. 계획의 전제가 틀렸다. `_set_watermark`로 Gap을 위조하는 구성은 반려한다. R-11을 재구성한다.**

### `_set_watermark` Gap 위조를 반려하는 이유

Watermark를 앞으로 미는 경로는 `commit_table_run` 하나뿐이고, 그것은 **실제로 Commit된 Object의 `watermark_after`로만** 민다. 다른 이동 경로인 `rewind_watermark`는 뒤로만 간다. `record_success_no_data_run`은 Watermark를 건드리지 않는다. 따라서 "Watermark가 데이터보다 앞서 있는 상태"는 이 시스템이 스스로 도달할 수 없다.

도달 불가능한 상태를 Test가 손으로 만들면, 그 Test는 시스템이 아니라 Test Harness를 검증한다. 더 나쁜 점이 있다. 나중에 어떤 변경이 진짜로 Gap을 만들 수 있게 되더라도, 이미 Gap을 전제하고 회복만 확인하는 Test는 그 회귀를 잡지 못한다. **지킬 대상은 회복 절차가 아니라 "Gap이 생기지 않는다"는 성질 자체다.**

[015](015_r07-schema-version-variant.md)에서 도달 불가 분기를 위해 소스를 바꾸지 않은 것과 같은 판단이다. 이번에는 Test 쪽에서 상태를 위조하려는 것이고, 방향만 다를 뿐 같은 종류의 훼손이다.

### 그러면 R-11이 증명할 것은 무엇인가

누락된 Schedule의 실제 운영 결과는 **데이터 손실이 아니라 귀속(attribution) 이동**이다.

Bronze Object Key는 `src/ingestion/service.py:373`에서 이렇게 만들어진다.

```
bronze/{source_table}/ingestion_date={logical_date.date()}/batch_id={batch_id}
```

Partition Key는 데이터의 시각이 아니라 **그 Run의 `logical_date`**다. B를 건너뛰면 B 구간 Row는 C의 `ingestion_date` Partition에 C의 `batch_id`를 달고 들어간다. 데이터는 다 있지만 언제 들어온 것으로 기록되는지가 달라진다.

Mart는 영향을 받지 않는다. `_batch_id`와 `_ingested_at`은 `stg_orders.sql` 등에서 중복 제거의 정렬 키로만 쓰이고 필터로 쓰이지 않는다. 즉 **Mart 수준에서는 Self-healing이 완전하다.** 이것이 R-11이 증명해야 할 핵심이고, 지금까지 아무도 증명하지 않았다.

## 재구성한 R-11

Test 이름을 `test_r11_a_skipped_window_self_heals_and_rewind_only_restores_attribution`으로 바꾼다. `_set_watermark`를 쓰지 않는다.

1. **A 실행.** Watermark 전진 확인.
2. **B 구간 데이터 삽입, Ingest 생략.** Scheduler가 건너뛴 상황 그대로다.
3. **C 구간 데이터 삽입, C 실행.** 여기서 단언한다.
   - C Run의 `rows_extracted`가 B 구간 Row를 포함한다.
   - C Run의 `watermark_after`가 Source 최대 Cursor와 같다.
   - **Gap이 없다.** 어떤 Row도 누락되지 않았다.
4. **Mart 비교.** 건너뛴 적 없는 Full Refresh Control과 Mart Hash가 같다. 회복 조치 없이 같아야 한다.
5. **귀속 이동 관측.** B 구간 Row가 `ingestion_date={C의 logical_date}` / `batch_id={C의 batch_id}` 아래에 있음을 확인한다. 이것이 누락 Schedule의 유일한 실제 비용이다.
6. **회복은 귀속 교정으로 재정의한다.** `rewind_tables`로 B 직전 경계까지 되감고 B의 `logical_date`로 명시 Batch를 돌린다. 재수집이 C까지 함께 쓸어 담는 것은 정상이며 막지 않는다. 단언은 두 가지다 — Mart Hash가 Control과 여전히 같다(멱등), 그리고 B 구간 Row가 이제 B의 `ingestion_date` Partition에 있다.

두 번째 `_set_watermark` 핀으로 B만 좁혀 잡으려던 계획도 같은 이유로 반려한다. 설계와 싸우지 말고 설계가 하는 대로 두고 단언하라.

## 부수 발견 — 과거 창 단독 재수집 수단이 없다

`rewind_tables` 뒤 수집을 돌리면 Upper Bound가 항상 Source 최대값이므로, 특정 과거 창만 격리해 재수집할 방법이 없다. 되감은 지점부터 현재까지가 통째로 딸려온다.

지금은 문제가 아니다. Mart가 멱등이고 Watermark 단조성이 유지되므로 정합성은 깨지지 않는다. 다만 Partition 단위로 좁혀 고쳐야 하는 상황 — 예를 들어 한 `ingestion_date` Partition만 잘못 만들어진 경우 — 에는 수단이 없다. Upper Bound를 인자로 받는 Bounded Batch가 필요해질 수 있다.

**Task 9에서 만들지 않는다.** R-12(Backfill Replay) 실측에서 실제로 필요해지는지 먼저 본다. 필요 없다면 만들지 않는다. R-12 증적을 본 뒤 다시 판단한다.

## 조치

| 대상 | 조치 |
| ---- | ---- |
| Phase 8 계획 Task 9 | R-11 Step을 위 6단계로 교체한다. Test 이름도 바꾼다. |
| `tests/reliability/test_r11_r15_reprocess_and_source.py` | `_set_watermark`를 쓰지 않는다. |
| `docs/runbooks/r11-missing-schedule.md` | "누락 Schedule은 데이터 손실을 만들지 않는다"를 먼저 쓴다. 근거는 연속 Watermark와 Source 최대 Upper Bound다. 그다음 유일한 실제 영향인 `ingestion_date` 귀속 이동과, 귀속을 되돌려야 할 때만 쓰는 rewind + 명시 Batch 절차를 적는다. 기존에 요구한 "두 DAG 모두 `catchup`을 쓰지 않는다"도 유지한다. |
| R-15 | 이 판정과 무관하다. 계획대로 진행한다. |

## 보충 — 부트스트랩 Pin은 금지 대상이 아니다 (2026-09-21)

developer가 범위를 확인해 왔다. 판정은 **맞다. 부트스트랩 Pin은 유지한다.**

금지 대상은 두 가지뿐이다.

1. B/C 사이에 Watermark를 밀어 Gap을 위조하는 것
2. 회복 재수집 범위를 좁히려고 Watermark를 다시 고정하는 것

`tests/integration/test_subscription_payment_temporal_join_integration.py`가 쓰는 부트스트랩 Pin은 다르다. Table마다 **자기 첫 Row의 직전**(`updated_at - timedelta(microseconds=1)`)에 Watermark를 두어, 공유 Postgres의 기존 Seed와 섞이지 않게 하는 격리 장치다.

이 상태는 **도달 가능하다.** 더 오래된 Row를 이미 수집하고 Commit한 Pipeline의 Watermark와 구분할 수 없다. Watermark가 데이터보다 앞서지 않고 정확히 뒤에 있다. 018이 막으려는 것은 "Watermark가 아직 수집하지 않은 데이터를 지나쳐 있는 상태"이고, 부트스트랩 Pin은 그 반대다.

`FIXTURE_START`의 격리된 미래 날짜에만 의존하도록 바꿀 이유도 없다. Sibling Test들과 격리 방식이 갈라지면 나중에 읽는 사람이 왜 R-11만 다른지 다시 조사하게 된다. 같은 패턴을 쓴다.
