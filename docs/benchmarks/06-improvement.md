# 06. Task 16 — 병목 선정과 개선 1건

버전 1. [[038_phase9-task16-bottleneck-selection]] 승인 반영.

> Scenario: `file_format` · Scale: M(1,000,000 주문) · Repeats: 5(Before-fix)+5(After-fix), 두 실행 모두 csv/parquet 각 5회
> Before-fix Benchmark ID: `file_format-M-20260922T000307Z`
> After-fix Benchmark ID: `file_format-M-20260922T000636Z`
> 변경 파일: `src/benchmark/experiments/file_format.py`(`_hash_rows_with_payload_size` 1개 함수)

## 병목 선정

실험 B(File Format) M Scale 읽기 구간이 두 Arm 모두 약 19~20초로, 실제
읽는 Payload(`rows_scanned=1,000,000`)에 비해 크게 느렸다. 원인을
`scripts/profile_file_format_read.py`(1회성 진단, 이번에 커밋)로 세
구간으로 쪼개 실측했다(실제 `benchmark/file_format/M/sellers.parquet`
사용):

1. DuckDB S3 Read + `cursor.fetchmany(10_000)`로 Row를 Python Tuple로
   적재: **10.251초**
2. Python `canonical_row_json` + SHA-256 누적 Hash Loop: **10.253초**
3. (비교용) 같은 Query를 `cursor.to_arrow_reader()`로 Columnar 적재(Row
   단위 Python 변환 없음): **0.295초**

1과 3의 차이(10.251초 대 0.295초, 약 35배)로 Duration의 절반이 실제
S3 전송·DuckDB Scan이 아니라 **Cursor가 Row를 1개씩 Python Tuple 객체로
바꾸는 구간**임을 확인했다. 애초에 짐작했던 "S3 접속·`httpfs` 초기화"
후보([[02-experiment-b-file-format|02 문서]] 구 버전)는 이 실측으로
기각됐다 — 접속·초기화에 해당하는 부분(위 3번, 0.295초)은 전체 20초에서
무시할 수준이다.

## 변경

`_hash_rows_with_payload_size`(`csv`/`parquet` 두 Arm이 공유하는 헬퍼)
에서 `cursor.fetchmany(10_000)` Tuple Loop를 `cursor.to_arrow_reader(10_000)`
Columnar Batch 순회 + `to_pylist()`/`zip`으로 Row 값 구성으로 바꿨다.
Hash 알고리즘(`canonical_row_json` + SHA-256)과 Row 순서는 그대로다 —
바뀐 건 DuckDB Cursor에서 Row를 Python 객체로 꺼내는 방식 하나뿐이다.

두 Arm이 이 헬퍼를 공유하므로 csv/parquet 둘 다 같은 방향·같은 크기로
바뀐다. architect 판단([[038_phase9-task16-bottleneck-selection]]): 이건
여전히 "변수 1개"다 — Arm 간 비교 변수는 그대로 "Fixture 형식" 하나이고,
한쪽 Arm만 고쳤다면 오히려 Arm 사이에 두 번째 변수가 생겨 설계 위반이
됐을 것이다.

## 결과

Before-fix와 After-fix를 같은 실행 창에서, Fixture 재생성 없이 연속
측정했다(M Scale `sellers.parquet`/`sellers.csv`는 이미 존재해 재사용).
Baseline은 커밋된 과거 M Scale 수치를 쓰지 않고 이번에 다시 쟀다
([[032]] §5, 실행 간 절대 비교 무효 원칙).

Before-fix는 이 변경을 적용하기 전에, After-fix는 적용한 뒤 실행했다. 다만
두 `runs.jsonl`의 `git_commit`은 모두 `a487eb23...-dirty`이고
`scenario_config_hash`·`dependency_lock_hash`도 같아서, Run Record만으로는
두 실행의 코드를 구분할 수 없다. 따라서 Before/After 판별은 실행 순서와 이
문서의 기록에 의존한다.

```
Before-fix csv:     raw=[19.507938443, 18.582611306, 18.089865698, 18.357045121, 19.197751308]
                     median=18.582611306
Before-fix parquet:  raw=[19.022457860, 19.018734949, 18.434774840, 18.898190720, 18.636265179]
                     median=18.898190720
result_hash(양쪽 동일): 66d27f16e98f043514b6d3a23c9f883a1f9a9cd86340e840ff5e8bce996bd33b

After-fix csv:      raw=[15.591232315, 15.629954482, 15.650599831, 16.050633881, 15.486272026]
                     median=15.629954482
After-fix parquet:   raw=[15.722569291, 15.573698818, 15.798917868, 15.537235240, 15.416942768]
                     median=15.573698818
result_hash(양쪽 동일): 66d27f16e98f043514b6d3a23c9f883a1f9a9cd86340e840ff5e8bce996bd33b
```

두 Benchmark ID의 20 Run 전부 `VALID`이며, 모두
같은 `result_hash`(`66d27f16...`) — csv·parquet 두 Arm 모두에서 개선
전후 Hash가 동일함을 확인했다. 하나라도 어긋났으면 이 개선은 기각
대상이었다.

Median 기준 변화(Raw 값에서 재계산):

| Arm | Before | After | 변화 |
| --- | --- | --- | --- |
| csv | 18.582611306s | 15.629954482s | **-15.9%** |
| parquet | 18.898190720s | 15.573698818s | **-17.6%** |

진단의 `fetchmany` 적재 10.251초와 Arrow 적재 0.295초를 그대로 개선폭으로
기대할 수는 없다. 이번 구현도 `to_pylist()`와 `zip`으로 각 Row를 Python
객체로 다시 만들기 때문에, 적재 구간 약 10.25초 중 실제 회수한 것은 Arm별
약 3초이고 약 7초는 그 변환 비용으로 남는다. Hash Loop 10.253초는 바꾸지
않았으므로 이번 변경의 이론상 상한부터 전체 Duration의 약 50%였고 그 일부만
회수한 결과다. 0.295초는 Row를 Python으로 꺼내지 않을 때만 가능한 대조값이며,
Row별 Canonical Hash를 유지하는 이번 범위에서는 도달할 수 없다.

## B의 결론은 바뀌는가

바뀌지 않는다. csv/parquet Arm 간 상대 차이는 Before-fix +1.70%
(csv 18.583s 대 parquet 18.898s)에서 After-fix -0.36%(csv 15.630s 대
parquet 15.574s)로, 둘 다 M Scale의 기존 관측(+0.4%) 및 L Scale
관측(+0.1%)과 같은 수준의 잡음 범위 안에 있다. 고정 비용을 줄였더니
형식 차이가 드러나거나 반대로 더 가려지는 일은 일어나지 않았다 — 두
Arm 모두 거의 같은 폭으로 빨라졌을 뿐, Arm 간 상대적 우열 관계는
여전히 분해되지 않는다. [[02-experiment-b-file-format|02 문서]]의
"형식 차이가 고정 비용에 가려 분해되지 않았다"는 결론은 유지된다 —
다만 그 고정 비용의 정체가 "접속·초기화"가 아니라 "Row 단위 Python
객체 변환"이었다는 점만 갱신됐다.

## 트레이드오프

- `to_arrow_reader`는 pyarrow Batch 객체를 거친다 — `fetchmany`보다
  메모리상 한 Batch(기본 10,000 Row)만큼 Columnar 표현을 추가로 들고
  있는다. Row 단위 스트리밍 처리는 아니라는 점은 기존 `fetchmany`도
  같아서(둘 다 Batch 단위) 실질적 Peak 메모리 차이는 크지 않다.
- pyarrow는 이미 `pyproject.toml`에 고정 의존성으로 있어(`pyarrow==25.0.1`)
  신규 의존성 추가는 아니다.
- 코드가 `to_pylist()` + `zip(*columns)`으로 한 단계 더 간접적이다 —
  DuckDB Tuple을 직접 순회하던 기존 코드보다 읽기에 약간 더 손이 간다.

## 범위 밖 — 같은 패턴이 제품 코드에 있음

`src/common/row_hash.py:51-65`의 `hash_cursor_rows`가 같은 `fetchmany`
Tuple Loop 패턴을 쓴다. 이 함수는 Mart Hash(정합성 경로)가 쓰는 공용
함수이고, 이번 Phase 9 실측 대상이 아니다. architect 지시대로 이번엔
건드리지 않는다 — 사실만 기록해 둔다. 같은 최적화가 적용 가능한지는
별도 후보로 남겨 둔다.

## 검증

```bash
uv run ruff check .
uv run pytest -q
RUN_POSTGRES_INTEGRATION=1 RUN_SEAWEEDFS_INTEGRATION=1 RUN_DBT_PUBLISH_INTEGRATION=1 uv run pytest tests/reliability -q
```

## 재현

```bash
PYTHONPATH=. uv run python scripts/profile_file_format_read.py
uv run python -m src.benchmark run --scenario file_format --scale M
uv run python -m src.benchmark report --benchmark-id <benchmark_id>
```
