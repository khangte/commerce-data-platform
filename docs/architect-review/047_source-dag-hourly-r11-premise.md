# 047. `source_simulation_dag` @hourly 전환 — R-11 전제와 PRD 13.1 정합성

> 판정: architect, 2026-09-28 (reviewer의 미커밋 변경 리뷰에서 설계 판단 요청)
> 관련: R-11, [018](018_r11-missing-schedule-premise.md), PRD v1.15 Section 13.1, Phase 8 설계 D6

## 질의

미커밋 변경은 `airflow/dags/source_simulation_dag.py`의 `schedule`을 `None`에서 `"@hourly"`로 바꾼다. `catchup=False`와 `max_active_runs=1`은 유지한다. `warehouse_pipeline_dag`는 `schedule=None`을 유지하고 Source 성공 시 `TriggerDagRunOperator`로만 실행된다.

reviewer는 코드에 문제가 없다고 판정했다. 그러나 Phase 8 설계 D6의 전제가 거짓이 된다고 지적했다(`docs/superpowers/specs/2026-09-20-phase8-reliability-scenarios-design.md:37,123`). 해당 전제는 "두 DAG 모두 `schedule=None`이라 Scheduler 누락을 재현할 대상이 없다"이다. reviewer가 요청한 판단은 다음 둘 중 하나다.

- (a) 과거 spec은 이력으로 두고 phase-08과 runbook에 주석만 단다.
- (b) D6 자체를 재검토해 R-11에 Scheduler 누락을 포함한다.

## 판정

**(a)를 채택한다. D6 결정은 유지한다. 코드 변경은 승인한다. 문서 보완 2건과 PRD 13.1 갱신 1건이 필요하다.** 판정은 수정필요다.

### D6를 다시 열지 않는 이유

D6에서 틀린 것은 전제 문장뿐이다. 결정은 틀리지 않았다. [018](018_r11-missing-schedule-premise.md)이 R-11의 핵심 성질을 이미 증명했다. 누락된 창은 Gap을 만들지 않는다. 연속 Watermark와 Source 최대 Upper Bound 때문에 다음 실행이 누락 구간을 함께 가져온다. 이 성질은 누락이 어떻게 생겼는지와 무관하다. 사람이 실행을 빠뜨린 경우와 Scheduler가 멈춘 경우가 같다.

현재 토폴로지에서 Scheduler 누락은 두 경우 중 하나다. 두 경우 모두 새 시나리오가 필요 없다.

1. **Source 예약 실행 누락.** Scheduler가 멈추면 그 시간대 Generator 실행이 없다. `catchup=False`이므로 재개 후 최신 구간 1건만 실행된다. 이 경우 Source 변경 자체가 생기지 않는다. 수집할 데이터가 없으므로 누락도 없다. 영향은 시뮬레이션 변경량 감소뿐이고 데이터 정합성 문제가 아니다.
2. **Source 성공 후 Warehouse 미실행 또는 실패.** 이 경우 Source 변경은 Bronze에 들어가지 않은 채 남는다. 다음 시간대 Warehouse 실행이 누락분을 함께 가져온다. 이것이 R-11 Test가 이미 검증한 A→(B 생략)→C 경로와 같다.

Airflow Catchup을 회복 경로로 쓰지 않는다는 D6의 핵심 결정도 유지된다. 두 DAG 모두 여전히 `catchup=False`다. 따라서 R-11 범위를 넓히면 이미 증명한 성질을 Airflow 계층에서 한 번 더 확인하는 Test만 늘어난다. 이 Test는 새 불변 조건을 지키지 않는다. YAGNI로 만들지 않는다.

### 과거 spec을 고치지 않는 이유

`docs/superpowers/specs/`의 두 문서(2026-09-11 trigger 설계와 2026-09-20 Phase 8 설계)는 작성 시점의 설계 이력이다. 둘 다 작성 당시에는 사실이었다. 고쳐 쓰면 판단 근거의 시점이 흐려진다. 현재 사실은 운영 문서(phase, runbook)와 PRD에 둔다.

### reviewer가 놓친 충돌 — PRD 13.1

PRD v1.15 Section 13.1은 다음을 계약으로 정한다.

```text
- source_simulation_dag: Manual
- warehouse_pipeline_dag: @daily
- 개발 시 WAREHOUSE_DAG_SCHEDULE 빈 값으로 비활성화
```

이번 변경 후 실제 동작은 계약과 다르다. Source는 `@hourly`다. Warehouse는 예약이 없고 Source 성공 시 트리거된다. `WAREHOUSE_DAG_SCHEDULE`은 `compose.yaml:31`과 `.env.example:28`에만 있다. 이 변수를 읽는 DAG 코드는 없다. 이 차이는 이번 변경 전부터 있었다(Warehouse `@daily` 미구현). 이번 변경으로 차이가 두 DAG 모두로 커졌다.

코드를 PRD에 맞추지 않는다. Warehouse를 `@daily`로 따로 예약하면 Source 트리거와 같은 Batch를 두고 경쟁한다. 2026-09-11 trigger 설계가 이미 "Source 성공 → 같은 `logical_date`로 Warehouse" 구조를 택했다. PRD를 실제 구조에 맞춘다.

## 조치

| 대상 | 담당 | 조치 |
| ---- | ---- | ---- |
| `airflow/dags/source_simulation_dag.py` | — | 변경 승인. 추가 수정 없음. |
| `docs/phases/phase-08-reliability.md` R-11 행(81행) | developer | 행을 유지하고 주석을 추가한다. 문구: "2026-09-28부터 `source_simulation_dag`는 `@hourly`, 두 DAG 모두 `catchup=False`다. Scheduler 누락은 Source 변경 미발생 또는 다음 Warehouse 실행의 Self-heal로 귀결되며 새 시나리오가 필요 없다(architect-review 047)." |
| `docs/runbooks/r11-missing-schedule.md` | developer | `## 문제` 절에 같은 취지 문단을 추가한다. 같은 절의 "`subscription_payments` DAG도 `catchup`을 쓰지 않으므로"는 존재하지 않는 DAG 이름이다. "`source_simulation_dag`(`@hourly`)와 `warehouse_pipeline_dag`(Source 성공 시 트리거) 모두 `catchup=False`이므로"로 고친다. 018이 요구한 원래 문장이다. |
| `compose.yaml:31`, `.env.example:28` | developer | 읽는 코드가 없는 `WAREHOUSE_DAG_SCHEDULE`을 삭제한다. 남겨 두면 설정으로 Warehouse 예약을 켤 수 있다고 오해하게 된다. |
| `docs/superpowers/specs/` 2건 | — | 수정하지 않는다. 이력으로 둔다. |
| PRD Section 13.1 | lead | PRD v1.16으로 올리고 v1.15는 `PRD.bak/`로 이동한다. 13.1을 다음으로 바꾼다. `source_simulation_dag`: `@hourly`. `warehouse_pipeline_dag`: 예약 없음(`schedule=None`), Source 성공 시 같은 `logical_date`로 트리거. `WAREHOUSE_DAG_SCHEDULE` 줄은 삭제한다. 나머지(`catchup=False`, `max_active_runs=1` 등)는 유지한다. |

## 검증 기준

- `uv run pytest tests/test_airflow_dags.py` 통과.
- `grep -rn WAREHOUSE_DAG_SCHEDULE compose.yaml .env.example airflow src` 결과 0건.
- runbook과 phase-08 R-11 행에 047 참조 존재.

## 검수 결과 (2026-09-28)

**최종 판정: 승인.** 조치 표의 항목은 모두 반영됐다.

- `docs/phases/phase-08-reliability.md`: R-11 표 아래에 047을 참조하는 주석이 추가됐다.
- `docs/runbooks/r11-missing-schedule.md`: 잘못된 DAG 이름을 고쳤다. Scheduler 누락 두 경우를 설명하는 문단과 047 참조가 추가됐다.
- `compose.yaml`, `.env.example`: `WAREHOUSE_DAG_SCHEDULE`을 삭제했다. `grep -rn WAREHOUSE_DAG_SCHEDULE compose.yaml .env.example airflow src` 결과는 0건이다.
- PRD v1.16 Section 13.1: Source `@hourly`, Warehouse `schedule=None`과 Source 성공 시 트리거가 반영됐다. `WAREHOUSE_DAG_SCHEDULE` 줄은 삭제됐다.
- `uv run pytest tests/test_airflow_dags.py`: 1 passed, 5 skipped. 5건은 `RUN_AIRFLOW_SMOKE_TEST` opt-in이라 실행하지 않았다.

잔여 1건: `docs/reference/mart-grain.md:4,594`와 `docs/reference/data-transformation-flow.md:4,437`이 아직 `../../PRD_v1.15.md`를 가리킨다. 이 파일은 `PRD.bak/`로 이동됐으므로 링크가 깨진다. `PRD_v1.16.md`로 바꿔야 한다.
