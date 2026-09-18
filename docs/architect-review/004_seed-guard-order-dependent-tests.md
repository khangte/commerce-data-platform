# 004 Seed Guard 순서 의존 Test (보류)

- 기록일: 2026-09-18
- 판정: **Phase 6 범위 밖. lead 지시로 보류한다.** 기록만 남기고 이번 Phase에서는 고치지 않는다.
- 관련: [003 Phase 6 마감 판정](003_phase6-closure-gate.md) 3절

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
