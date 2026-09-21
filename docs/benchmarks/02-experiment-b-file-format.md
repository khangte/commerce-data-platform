# 02. 실험 B — File Format(CSV vs Parquet)

버전 1. Task 14(M Scale Baseline) 결과.

> Scenario: `file_format` · Scale: M(1,000,000 주문) · Repeats: 5 · Cold: 아니오(Warm)
> Benchmark ID: `file_format-M-20260921T134121Z`

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
```

## 결과

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

## 관찰

- CSV가 Parquet보다 3.6배 큰 On-disk 크기인데도 측정된 읽기 시간은 두 Arm이
  거의 같다(+0.4%, Raw 값도 19.7~20.3초 범위에서 서로 겹친다). Duration
  대부분이 Byte 수가 아니라 다른 고정 비용(SeaweedFS S3 접속·`httpfs` 초기화,
  DuckDB CSV Parser 오버헤드 등)에 지배되는 것으로 보인다 — 이 실험 설계로는
  원인을 더 분해할 수 없다.
- S Scale 결과와 직접 비교하지 않는다 — S Scale `file_format` 실행 시점의
  On-disk 크기를 이 작성 시점에 재확인하지 않았고, 두 실행의 절대 비교가
  이 문서의 목적이 아니다.

## 한계

- CSV 내보내기 자체의 비용(Bronze → CSV 변환)은 측정 구간 밖이라 이 문서에
  없다. `_ensure_fixture`가 CSV·Parquet 둘 다 Scale별로 한 번만 만들고
  재사용하므로, 이 실행이 실제로 처음부터 CSV를 새로 썼는지 기존 파일을
  재사용했는지는 별도로 구분해 기록하지 않았다.
