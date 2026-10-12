# 004 Seed Guard 순서 의존 Test (보류)

- 기록일: 2026-09-18
- 판정: **Phase 6 범위 밖. lead 지시로 보류한다.** 기록만 남기고 이번 Phase에서는 고치지 않는다.
- 관련: [003 Phase 6 마감 판정](003_phase6-closure-gate.md) 3절
- 현재 상태(2026-10-12): **원 증상 해소. 해소 방식에서 신규 위험 발견, developer 수정 진행 중.** 아래 "해소 상태" 절 참조.

## 증상

통합 Test 전체 실행에서 아래 2건이 실패한다.

- `tests/integration/test_seed_integration.py::test_same_raw_input_and_seeded_at_are_idempotent`
- `tests/integration/test_seed_integration.py::test_seed_guard_rejects_a_changed_baseline_after_success`

두 실패 모두 같은 메시지에서 나온다.

```
ValueError: Seed is blocked because a successful generator run already exists
```

둘째 Test는 `pytest.raises(ValueError, match="baseline input differs")`로 다른 Guard를 기대하다가 이 메시지를 만나 `AssertionError: Regex pattern did not match`로 끝난다.

## 원인

`_assert_seed_guard()`(`src/seed/loader.py:425`)는 Guard를 두 단계로 건다.

1. `generator_runs`에 `status = 'SUCCESS'`인 행이 하나라도 있으면 Seed 자체를 막는다.
2. 그다음에야 `seed_runs`의 기존 성공 Run과 Baseline 입력을 비교한다.

두 Test는 `run_seed()`를 직접 호출한다. 따라서 같은 Database에서 Generator 통합 Test가 먼저 성공하면 1번 Guard에 걸려 2번 Guard까지 도달하지 못한다. 즉 Generator Test보다 먼저 실행되는 순서에서만 통과하는 순서 의존 Test다. 단독 실행에서도 이전 세션의 Generator 성공 행이 남아 있으면 같은 이유로 실패한다.

이것은 Phase 1 Seed Test의 설계 문제이며, Seed Guard 구현이나 Phase 6 Model·Mart 로직의 결함이 아니다. Guard는 의도대로 동작한다.

## 영향

- Phase 6 AC(AC-01, 09, 10, 11, 12)와 무관하다. Mart·Fact 경로를 건드리지 않는다.
- 전체 Test 실행 결과를 "실패 0건"으로 읽지 못하게 만든다. 이후 Phase에서 회귀를 판정할 때 Noise가 된다.

## 고칠 때의 선택지

1. Test가 자기 전제를 직접 만든다. Test 시작에서 `generator_runs`의 성공 행을 지우거나, 두 Test를 Generator가 없는 Database Fixture 위에서 실행한다. Guard 의미는 그대로 둔다.
2. 둘째 Test의 기대를 현실에 맞춘다. Generator 성공 행이 있으면 `already exists` Guard를, 없으면 `baseline input differs` Guard를 확인하도록 분기한다. 다만 이 방식은 검증 의도를 약화한다.

1번을 권한다. 2번은 Baseline 비교 Guard가 실제로 도는 경로를 영영 검증하지 않게 된다.

## 처리

lead가 보류를 결정했다. 별도 Task로 끊을 시점은 Phase 7 착수 전이 적절하다. Phase 7도 같은 공유 Database 위에서 통합 Test를 돌리므로 Noise가 그대로 이어진다.

## 해소 상태 (2026-10-12)

### 원 증상: 해소

- 해소 커밋: `d441323` (2026-09-18, `fix: stabilize phase6 closure tests`). 위 선택지 1을 적용했다.
- 근거: `tests/integration/test_seed_integration.py:19`, `:67`에서 두 Test가 시작 시 `_remove_successful_generator_runs()`를 호출한다. 이 함수(`:77-81`)가 `generator_runs`의 `SUCCESS` 행을 지운다. 따라서 1단계 Guard(`src/seed/loader.py:418-430`)를 건너 2단계 Baseline 비교 Guard까지 도달한다.

### 신규 위험: 공유 Database 오염

해소 방식이 Test 전제를 격리하지 않고 공유 Database의 상태를 지워서 만든다. 통합 Test는 운영과 같은 Database(`PostgresSettings.from_environment()`)에서 돈다. 이 판정을 위해 해당 Test는 실행하지 않았다. 아래는 코드 기준 판단이다.

1. **Generator 성공 이력 전량 삭제.** `DELETE FROM generator_runs WHERE status = 'SUCCESS'`(`test_seed_integration.py:80`)가 운영 Generator 이력도 지운다.
   - Seed Guard 1단계(`src/seed/loader.py:418-430`)가 무력화된다. Generator가 쓴 Source 위에 Seed를 다시 적재할 수 있게 된다.
   - Generator 재실행 재사용 근거(`src/generator/service.py:272` `_successful_result`)가 사라진다. 이미 성공한 `logical_date`를 재시도하면 결과를 재사용하지 못한다. 대신 Cursor 단조성 Guard(`src/generator/service.py:258` `_assert_source_cursor_forward`, 048)에 막혀 실패한다.
2. **Seed 덮어쓰기.** `test_same_raw_input_and_seeded_at_are_idempotent`는 `run_seed()`를 실제로 두 번 실행한다. 적재는 `ON CONFLICT ... DO UPDATE`(`src/seed/loader.py:388-389`)이고 갱신 대상에 `updated_at`이 포함된다(`src/seed/loader.py:70`, `:76`).
   - Generator가 갱신한 Seed 고객의 구독·등급 행(`src/generator/customers.py:465`, `:510`)이 Seed 값과 과거 `updated_at`으로 되돌아갈 수 있다.
   - 되돌아간 행은 Cursor가 Watermark 아래로 역행하므로 수집되지 않는다. Source와 Warehouse가 에러 없이 어긋난다(미실측, 가능성 높음).

### 처리

- 원 항목(순서 의존 실패)은 종결한다.
- 신규 위험은 developer가 수정 중이다. 방향: 두 Test를 격리된 Database·Schema Fixture에서 실행하거나, 공유 Database에 Generator 성공 행이 있으면 skip한다. 공유 Database의 행을 지우는 방식은 쓰지 않는다.
- 수정이 끝날 때까지 공유 Database에서 `RUN_POSTGRES_INTEGRATION=1`로 이 파일을 포함한 전체 통합 Test를 실행하지 않는다.
