# 02. 실험 B — File Format(CSV vs Parquet)

버전 2. Task 15(L Scale) 결과 추가.

> M Scale — Benchmark ID: `file_format-M-20260921T134121Z` (Repeats 5, Cold: 아니오)
> L Scale — Benchmark ID: `file_format-L-20260921T140402Z` (Repeats 5, Cold: 아니오)

## 가설·측정 범위

같은 Bronze 테이블(`sellers`) 내용을 CSV로 내보낸 사본과 원본 Parquet, 두
형식으로 각각 읽어 집계하는 비용을 비교한다. 두 Arm은 완전히 같은 Row
집합·같은 집계 결과에 도달해야 하며, CSV 사본을 만드는 비용은 측정 구간
밖에서 미리 처리한다(`_ensure_fixture`).

- `csv` Arm: 측정 시작 전 만들어 둔 `sellers.csv` 사본을 읽는다.
- `parquet` Arm: SeaweedFS(S3 호환)에 저장된 원본 `sellers.parquet`를
  DuckDB `httpfs`로 직접 읽는다.
- Fixture Row 수는 `config.scale.order_count`를 그대로 쓴다 — 실험 C/D와
  달리 별도 CLI 인자가 필요 없고, `--scale M`만으로 1,000,000 Row로 자동
  확장된다.

## 환경·데이터셋

- `git_commit`: `cc7ae05b223ef9aff15a0b0263d47a988d4dbf26`
- `python_version`: 3.12.3, `dependency_lock_hash`:
  `a40e99ad99908a4b104568316b0458e571dc563c183ad251faa65e5569bf1cbb`
- `postgres:18.6`, `chrislusf/seaweedfs:4.45`,
  `commerce-data-platform-airflow:3.3.1`
- `sellers` Fixture Row 수: 1,000,000(`config.scale.order_count`, M Scale)

## 재현

```bash
uv run python -m src.benchmark run --scenario file_format --scale M
uv run python -m src.benchmark report --benchmark-id file_format-M-20260921T134121Z
uv run python -m src.benchmark run --scenario file_format --scale L
uv run python -m src.benchmark report --benchmark-id file_format-L-20260921T140402Z
```

## 결과 — M Scale

```
baseline(csv):     raw=[19.910847433, 19.916478154, 20.211880747, 19.772287507, 19.796481632]
                    median=19.910847433
improved(parquet): raw=[19.746248845, 19.992092046, 20.297432443, 19.794478147, 20.176451217]
                    median=19.992092046
result_hash(양쪽 동일): 66d27f16e98f043514b6d3a23c9f883a1f9a9cd86340e840ff5e8bce996bd33b
change: +0.4% (baseline median -> improved median)
```

5/5 `VALID`, 두 Arm `result_hash` 일치.

Fixture 실측 On-disk Byte 크기(M Scale, 1,000,000 Row 동일 내용):

| 형식 | 경로 | 크기 |
| --- | --- | --- |
| CSV | `data/benchmarks/file_format-M-20260921T134121Z/csv_mirror/sellers.csv` | 71,500,057 bytes (68.2 MB) |
| Parquet | SeaweedFS `benchmark/file_format/M/sellers.parquet` | 19,699,382 bytes (18.8 MB) |

## 결과 — L Scale

```
baseline(csv):     raw=[96.4794046379975, 93.72700138899381, 93.7623935760057, 94.771024821006, 93.73653949599247]
                    median=93.7623935760057
improved(parquet): raw=[112.15481100400211, 94.71081389399478, 93.12747174101241, 93.35696254199138, 93.81883751899295]
                    median=93.81883751899295
result_hash(양쪽 동일): 89142b501fd8c569a76843b051f3b5afc135884bf8d3e8e94f2fd3c187dfc8d7
change: +0.1% (baseline median -> improved median)
```

5/5 `VALID`, 두 Arm `result_hash` 일치.

Fixture 실측 On-disk Byte 크기(L Scale, 5,000,000 Row 동일 내용):

| 형식 | 경로 | 크기 |
| --- | --- | --- |
| CSV | `data/benchmarks/file_format-L-20260921T140402Z/csv_mirror/sellers.csv` | 361,500,057 bytes (344.8 MB) |
| Parquet | SeaweedFS `benchmark/file_format/L/sellers.parquet` | 30,633,598 bytes (29.2 MB) |

L Scale에서 CSV/Parquet 크기 비는 약 11.8배로, M Scale의 약 3.6배보다 크게
벌어졌다. 이는 Parquet Column 압축 효율 때문으로 보인다 — Fixture 생성 코드가
`created_at`/`updated_at`을 `timedelta(days=index % 3650)`로 3650일 주기로
순환시키는데(`src/benchmark/experiments/file_format.py:135-138`, L Scale
`OverflowError` 수정으로 도입된 Bound), M Scale(1,000,000 Row)은 이 주기가
약 274회 반복되지만 L Scale(5,000,000 Row)은 약 1,370회 반복돼 Dictionary
Encoding 대상 고유값 밀도가 더 낮아진다. CSV는 이런 반복 압축 이득이 없어
Byte 수가 Row 수에 선형으로만 늘어난다. 즉 이 비율 증가는 실제 두 형식의
본질적 차이가 아니라 이 Fixture의 날짜 순환 주기 Bound에서 나온 인공물이다
— File Format 실험의 결론(고정 비용에 가려 분해되지 않는다)과는 별개다.

## 관찰

**이 측정 구간에서는 형식 차이가 고정 비용에 가려 분해되지 않았다.** CSV가
Parquet보다 3.6배 큰 On-disk 크기인데도 측정된 읽기 시간은 두 Arm이 거의
같다(+0.4%, Raw 값도 19.7~20.3초 범위에서 서로 겹친다). Duration 대부분이
Byte 수가 아니라 다른 고정 비용에 지배되는 것으로 보이지만, 이 실험
설계로는 그 고정 비용을 측정 구간에서 분리해내지 못했다.

Task 16([[038_phase9-task16-bottleneck-selection]])에서 이 고정 비용을
직접 진단했다 — 후보로 짐작했던 "SeaweedFS S3 접속·`httpfs` 초기화"는
실측으로 기각됐다. 실제로는 Duration의 약 절반이 DuckDB Cursor가 Row를
1개씩 Python Tuple로 변환하는 구간(`fetchmany`)이었다(`scripts/profile_file_format_read.py`).
접속·초기화 구간(`to_arrow_reader` 기준 약 0.3초)은 20초 전체에서 무시할
수준이다. 이 Row 변환 구간을 고친 결과는
[[06-improvement|docs/benchmarks/06-improvement.md]] 참조.

S Scale 결과와 직접 비교하지 않는다 — S Scale `file_format` 실행 시점의
On-disk 크기를 이 작성 시점에 재확인하지 않았고, 두 실행의 절대 비교가
이 문서의 목적이 아니다.

L Scale(+0.1%)도 M Scale(+0.4%)과 같은 Null 결과다 — On-disk 크기 차이가
M보다 더 벌어졌는데도(위 §L Scale 결과 참조) 읽기 시간 차이는 오히려 더
작다. 이는 두 Arm 모두 고정 비용 지배 가설을 더 강하게 뒷받침한다.

## 한계

- CSV 내보내기 자체의 비용(Bronze → CSV 변환)은 측정 구간 밖이라 이 문서에
  없다. `_ensure_fixture`가 CSV·Parquet 둘 다 Scale별로 한 번만 만들고
  재사용하므로, 이 실행이 실제로 처음부터 CSV를 새로 썼는지 기존 파일을
  재사용했는지는 별도로 구분해 기록하지 않았다.
