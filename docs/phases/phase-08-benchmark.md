# Phase 8. Benchmark

> 상태: Planned  
> Milestone: 3 — Portfolio Evidence  
> 선행 Phase: [Phase 7. Reliability Scenarios](phase-07-reliability.md)  
> 기준 문서: [ROADMAP](ROADMAP.md), [PRD v1.8](../../PRD_v1.8.md)

## 목표

절대 처리시간을 성공 조건으로 삼지 않고, 고정된 데이터·환경·Query에서 Baseline과 개선 전후의 차이를 정량적으로 증명한다. Raw 측정값, Median, Result Hash, 환경 Metadata를 함께 보존한다.

## 원칙

- 100K Orders에서 측정 Framework를 먼저 검증한 뒤 1M과 5M으로 확장한다.
- 각 주요 실험은 같은 조건으로 5회 실행하고 Raw 값을 모두 보존한다.
- 평균보다 Median을 대표값으로 사용한다.
- 성능 개선 전후 결과의 Logical Hash가 같아야 한다.
- Cold/Warm Run을 구분하며 섞어서 비교하지 않는다.
- 한 번에 하나의 주요 변수를 변경한다.
- 8GB RAM에서 5M이 불가능하면 실패 조건과 관측치를 숨기지 않고 기록한다.

## 선행 조건

- Phase 7 Reliability Scenario와 E2E가 안정적으로 통과한다.
- Dataset을 결정적으로 생성하고 Scale을 식별할 수 있다.
- Full/Incremental 결과의 Logical Hash를 계산할 수 있다.
- Host/WSL/Docker Resource 정보를 기록할 수 있다.

## Scale

| Scale | Orders | 목적                                  |
| ----- | -----: | ------------------------------------- |
| S     |   100K | Harness와 측정 오버헤드 검증          |
| M     |     1M | 기본 Portfolio Baseline               |
| L     |     5M | Page Write/Memory/파일 크기 한계 관측 |

## 구현 순서

### 1. Benchmark Harness

- [ ] `P8-01` Benchmark Scenario/Run ID와 Config Schema 정의
- [ ] `P8-02` Wall Time, CPU/Memory, I/O, Row Count 수집
- [ ] `P8-03` Raw Result 저장 형식과 Median 계산 구현
- [ ] `P8-04` Result Hash와 정확성 Gate 연결
- [ ] `P8-05` Cold/Warm Run 구분과 Cache Reset 절차 문서화
- [ ] `P8-06` Dependency Lock/Image/Dataset 식별 정보 기록

Run Metadata 최소 필드:

```text
benchmark_id
scenario
dataset_scale
run_number
is_cold_run
duration_seconds
rows_scanned
rows_changed
input_bytes
output_bytes
result_hash
git_commit
python_version
dependency_lock_hash
docker_image_versions
host_wsl_spec
random_seed
query_or_command
```

### 2. Experiment A — Full vs Incremental Extract

- [ ] `P8-07` 동일 최종 결과를 만드는 Full Extract Baseline
- [ ] `P8-08` 변경률이 고정된 Incremental Extract 측정
- [ ] `P8-09` Rows Scanned/Changed, Bytes, Duration 비교

변경률과 Cursor 범위를 결과에 기록한다. 결과 Hash가 다르면 성능 수치를 채택하지 않는다.

### 3. Experiment B — CSV vs Parquet

- [ ] `P8-10` 같은 Column/Row 범위의 CSV Read 측정
- [ ] `P8-11` 같은 결과를 만드는 Parquet Read 측정
- [ ] `P8-12` 파일 크기, Scan Bytes, Duration 비교

### 4. Experiment C — Full Scan vs Filtered Scan

- [ ] `P8-13` 전체 Dataset Scan Baseline
- [ ] `P8-14` 동일 분석 결과 범위의 Predicate/Column Projection 적용
- [ ] `P8-15` Scan Rows/Bytes와 Duration 비교

### 5. Experiment D — Cold vs Warm

- [ ] `P8-16` Cold Run 절차로 5회 측정
- [ ] `P8-17` Warm Run 절차로 5회 측정
- [ ] `P8-18` Cache 효과를 별도 결과로 해석

### 6. Scale 확장과 개선 Loop

- [ ] `P8-19` S Scale에서 Harness 검증
- [ ] `P8-20` M Scale 전체 주요 실험 수행
- [ ] `P8-21` L Scale 실행 또는 자원 한계 Evidence 기록
- [ ] `P8-22` 가장 큰 Bottleneck 하나 선정
- [ ] `P8-23` 개선 적용 후 동일 조건 재측정
- [ ] `P8-24` Baseline/개선 결과와 Trade-off 문서화

```text
Baseline
    ↓
Bottleneck 관측
    ↓
하나의 변경 적용
    ↓
같은 Dataset/환경/Scenario로 5회 재측정
    ↓
Median과 Result Hash 비교
```

## 실험 통제 Matrix

각 비교에서 다음 값이 같아야 한다.

| 통제 변수                       | 기록 위치              |
| ------------------------------- | ---------------------- |
| Dataset Scale/Checksum          | Benchmark Run Metadata |
| Random Seed                     | Benchmark Run Metadata |
| Git Commit                      | Benchmark Run Metadata |
| Python/Dependency/Image Version | 환경 Metadata          |
| Host/WSL/Docker Resource        | 환경 Metadata          |
| Query/Scenario                  | Scenario 정의          |
| Cold/Warm 조건                  | Run별 Flag와 절차      |
| 출력 정확성                     | `result_hash`          |

## 결과 문서 형식

```text
가설
측정 대상과 제외 범위
환경과 Dataset
실행 명령
Raw 5회 결과
Median
Result Hash
관측된 Bottleneck
변경 사항
개선 후 Raw 5회/Median
차이와 해석
한계
```

개선율은 원본 Raw 값에서 재계산 가능해야 하며, 예시는 다음 형태로 표현한다.

```text
Baseline Median 18.2s
Improved Median 7.4s
Duration 59% 감소
Result Hash 동일
```

## 범위 밖

- 근거 없는 목표 처리시간 선언
- 서로 다른 결과를 만드는 Query의 속도 비교
- 단일 실행값만을 대표 결과로 사용
- Cache 상태가 다른 결과를 같은 모집단으로 집계
- Benchmark를 위해 신뢰성 계약을 약화하는 변경

## 요구사항 추적

| 구분 | 연결 항목                              | 증거                                 |
| ---- | -------------------------------------- | ------------------------------------ |
| PRD  | Section 20 비기능 요구사항과 Benchmark | 환경/Raw/Median 문서                 |
| FR   | FR-19 1M+ Scale                        | M/L Scale 결과                       |
| FR   | FR-20 Benchmark                        | 실험 A~D                             |
| AC   | AC-17 Benchmark                        | Raw 5회, Median, Hash, 환경 Metadata |

## 산출물

- 재사용 가능한 Benchmark Harness
- S/M/L Dataset 정의와 Checksum
- 실험 A~D의 Raw Result
- Median과 Result Hash를 포함한 결과 문서
- Bottleneck 분석과 최소 1개 개선 전후 비교
- `docs/benchmarks/`의 환경/재현 가이드

## Definition of Done

- [ ] 모든 `P8-*` Task가 완료됐다.
- [ ] 주요 실험마다 Raw 5회 결과가 있다.
- [ ] 대표값으로 Median을 계산했다.
- [ ] 비교 전후 Result Hash가 같다.
- [ ] 환경과 Dataset Metadata가 누락 없이 기록됐다.
- [ ] M Scale이 완료되고 L Scale은 성공 또는 자원 한계가 증명됐다.
- [ ] AC-17이 통과한다.

## Portfolio Evidence

- Raw Result와 Median 계산이 연결된 표/그래프
- Full/Incremental Scan Rows와 Duration 비교
- CSV/Parquet Bytes와 Duration 비교
- Cold/Warm 차이
- Bottleneck 관측에서 개선 결정으로 이어지는 기록
- 정확성 Hash가 유지된 개선 결과

## 권장 Commit

```text
perf: add reproducible benchmark evidence
```

## 다음 Phase 인계

Phase 9는 Benchmark가 검증한 Mart를 소비 대상으로 사용한다. BI 연결을 위해 Mart Grain이나 Metric 의미를 변경하지 않는다.
