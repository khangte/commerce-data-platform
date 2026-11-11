# ADR 021. Publish Gate를 도메인으로 나눈다

## Status

Accepted (2026-11-10). ADR 016·020을 보완한다. Build-then-swap 구조와 파일 1개를 통째로 교체하는 방식은 그대로다.

## Context

ADR 016의 전체 Gate는 "게시본의 모든 Table이 같은 Build에서 나온다"는 일관성을 얻기 위한 설계다. ADR 020은 그 보장 중 지표와 Fact 사이의 정합성만 내주고 지표 계층 Test를 `warn`으로 내렸다.

남은 `error` 등급 Test는 여전히 하나라도 실패하면 전체를 막는다. 그런데 Mart는 서로 참조하지 않는 두 도메인으로 나뉜다.

| 도메인 | Relation |
| ---- | ---- |
| `orders` | `fct_order`, `fct_order_item`, `fct_order_payment`, `dim_product`, `dim_seller`, `rpt_customer_order_activity_daily`, `rpt_membership_tier_performance` |
| `subscriptions` | `dim_subscription`, `fct_subscription_payment`, `rpt_subscription_funnel_daily`, `rpt_subscription_payment_outcomes_daily` |
| 공유 | `dim_customer`, `dim_date` |

두 도메인은 공유 Dimension을 통해서만 만난다. Test 148개 중 두 도메인에 걸친 것은 없다. 그런데도 구독 Test 하나가 실패하면 주문 Fact가 게시되지 못한다. 재현 실험([08](../benchmarks/08-publish-gate-domain-split.md))에서 구독 Canary 하나가 `fct_order` 5행(주문 시각 600초분)을 묶었다.

제약은 게시 방식이다. 파일 1개를 `os.replace`로 교체하므로 실패한 도메인만 "빼고" 게시할 수 없다.

## Decision

1. **실패 도메인을 Build 파일 안에서 되돌린 뒤 통째로 교체한다.** `DBT_TEST_ERROR`가 한 도메인에 귀속되면 Published 파일을 읽기 전용으로 붙여 그 도메인의 Relation 전부를 한 Transaction에서 직전 게시 값으로 바꾼다. 게시 절차(`publish_build`)는 바뀌지 않는다.
2. **귀속은 dbt 산출물로 한다.** `run_results.json`의 `error`·`fail`·`skipped` Node와 `manifest.json`의 `depends_on`으로 판정한다. `skipped`가 된 staging·intermediate Node는 원인이 아니라 결과이므로 판정에서 뺀다.
3. **표에 없으면 막는다.** 공유 Dimension만 참조하는 Test, staging·intermediate Test, Model·Seed·Snapshot 오류, 두 도메인 동시 실패, Published 파일이 없는 첫 게시는 전체 차단이다.
4. **되돌린 뒤 경계 Test를 재검증한다.** 되돌린 Fact(직전 시점)와 새 공유 Dimension(최신 시점)이 한 파일에 놓인다. 보류 도메인과 공유 Dimension을 함께 참조하는 `error` 등급 Test를 다시 검사하고, 하나라도 실패하면 전체 차단으로 물러난다. `relationships`는 Manifest 정보로 SQL을 만들어 검사하고, 그 외는 dbt가 컴파일한 SQL을 실행한다.
5. **재계산 경계는 전역 1개를 유지한다.** 실패가 있는 Build는 원래 경계를 전진시키지 않는다. 다음 실행이 밀린 Key를 다시 계산하고, Key 단위 교체라 통과 도메인의 결과는 같다.
6. **부분 게시는 기록한다.** 상태는 `PUBLISHED`, `mart_publish_runs.held_domains`에 도메인 이름, Run report에 `PUBLISH_PARTIAL:<domain>`(`ok=false`), `dbt_build` 로그에 실패·경고 Node 이름을 남긴다. 되돌리기 전 Build는 `failed/`에 복사한다. `held_domains`는 `tests_warned`와 같은 이유로 전용 조회로만 읽는다([063](../architect-review/063_publish-gate-split-tests-warned-reader-compat.md)).
7. **스위치는 기본 켬이다.** DAG Param `partial_publish` 또는 CLI `--no-partial-publish`로 끄면 ADR 020까지의 동작이다.

## Alternatives

| 대안 | 기각 사유 |
| ---- | --------- |
| 도메인별 DuckDB 파일 | 공유 Dimension의 소속을 정할 수 없고 도메인 간 Join이 파일을 넘는다. Serving·Metabase가 단일 파일을 전제한다([ADR 019](019-serving-file-visibility-for-metabase.md)) |
| Snapshot Table Format(DuckLake 등) | 저장 계층 교체다. ADR 016의 전환 기준(1GB 또는 복사 60초)에 못 미친다 |
| 도메인별 dbt 2회 실행 | 공유 Dimension을 두 번 만들거나 실행 순서를 관리해야 한다. 1회 실행 결과를 귀속하면 dbt 쪽 변경이 없다 |
| 도메인별 재계산 경계 | 전역 경계와 반복 가능한 재계산으로 충분하다. 부분 게시 실행 소요는 정상 실행과 1.21초 차이였다 |
| 되돌린 뒤 `dbt test`로 재검증 | 실행 전후 Hook이 `dbt test`에서도 돌아 영향 Key 감사 기록이 다시 쌓이고, 원래 실행과 같은 var·Flag를 요구한다 |
| `dim_subscription` 실패는 전체 차단으로 두기 | 하위 Node가 건너뛰어져 귀속이 안 되는 경우였다. 구독 도메인 `error` Test의 큰 부분이 이 Model에 있어 목표를 못 이룬다. 건너뛴 Node 처리 규칙(Decision 2·4)으로 해결했다 |

## Consequences

| 구분 | 내용 |
| ---- | ---- |
| 포기 | 게시본의 모든 Table이 같은 Bronze 시점이라는 보장. 보류 도메인은 직전 게시 시점, 나머지는 최신 시점이다 |
| 유지 | 파일 단위 원자 교체. 도메인 안의 정합성(도메인 Relation은 함께 되돌린다). 공유 Dimension·staging·Model 오류의 전체 차단 |
| 보완 | 경계 Test 재검증, 귀속 불가 시 전체 차단, `held_domains`·`PUBLISH_PARTIAL` 기록, 되돌리기 전 Build 보존, 끄는 스위치 |
| 미보완 | 도메인을 가로지르는 분석(같은 고객의 주문과 구독)은 시점이 어긋난 값을 볼 수 있다. Dashboard에는 표시되지 않는다 |
| 운영 변화 | `dbt_build` Task가 Test 실패에도 성공으로 끝난다. 감지는 Run report로 한다([R-14 Runbook](../runbooks/r14-dbt-failure.md)) |

게시 파일이 지키는 불변식: 통과 도메인은 이번 Build의 `error` Test를 모두 통과했고, 보류 도메인은 직전 게시 값과 같으며 경계 Test를 다시 통과했다.

알려진 한계:

- 보류된 도메인 자체는 여전히 묶인다. 실패의 범위를 좁힐 뿐이다.
- `metrics_test_severity=error`에서는 지표 Test 하나가 경계 Test에 들어가고 `fct_order` 실패 때 건너뛰어지므로 `orders` 실패가 전체 차단으로 간다. 그 값은 전체 Gate로 돌아가는 설정이라 의도와 맞는다.
- `failed/` 보존 3개를 부분 게시 증거와 전체 실패가 나눠 쓴다.
- 도메인은 2개로 고정이고 표는 코드에 있다. Mart Model을 추가하고 표를 고치지 않으면 재현 실험의 단언이 실패한다.

## Validation

| 근거 | 결과 |
| ---- | ---- |
| 재현 실험 `tests/reliability/test_domain_gate_experiment.py` A | 구독 Canary 실패: 끔 `FAILED`, 묶인 주문 5행·600초 → 켬 `PUBLISHED`, `held_domains = ["subscriptions"]`, 묶인 주문 0행 |
| 같은 실험 B·E | 주문 Canary 실패: 켬 `held_domains = ["orders"]`. `dim_subscription` Canary 실패(28개 Node 건너뜀): 켬 `PUBLISHED`, 묶인 주문 0행 |
| 같은 실험 C | 의존 없는 Canary: 스위치와 무관하게 `FAILED` |
| 같은 실험 D·D-orders | 부분 게시 뒤 재실행: `held_domains = []`, 13개 Relation이 기준 파일과 모두 같음 |
| 합성 파일 단위 테스트 | 경계 Test 실패와 되돌리기 실패에서 전체 차단, `failed/`에 되돌리기 전 파일 유지 |
| R-14·기존 Canary·1단계 실험 | 수정 없이 통과 |

검증하지 못한 것: 이 Bronze의 마지막 Batch에 구독 변화가 없어 `subscriptions`의 증분 복구는 측정하지 못했다(같은 경로를 `orders` 보류로 확인). 실제 고객 이력 재구성으로 경계 Test가 끊기는 시나리오는 합성 파일로만 검증했다. C·E의 Run report 판정은 측정하지 않았다. 소요 시간은 1회 측정이다.
