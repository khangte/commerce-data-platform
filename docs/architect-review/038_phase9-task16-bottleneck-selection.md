# 038. Phase 9 Task 16 — 병목 선정 및 개선 범위 승인

- 일자: 2026-09-22
- 요청: developer (Task 16 병목 선정, `src/` 편집 전 승인)
- 판정: **승인 (조건 5건)**

## 1. 선정 내용

실험 B(`file_format`) M Scale 읽기 구간을 병목으로 선정했다. developer가
실제 M Scale Fixture로 구간을 쪼개 실측했다.

| 구간 | 실측(초) |
| --- | --- |
| DuckDB S3 Read + `fetchmany(10_000)` Tuple 적재 | 10.251 |
| `canonical_row_json` + SHA-256 누적 Hash Loop | 10.253 |
| 합계 | 20.504 |
| (대조) `fetch_arrow_table()` 적재 | 0.295 |

M Scale 실측 Duration 19.9~20.0초와 합계가 맞는다. Arrow 적재가 약 1/35라는
점이 Row 단위 Python 객체 변환이 비용의 실체임을 가른다. S3 전송·DuckDB
Scan·`ORDER BY` 자체는 병목이 아니다.

**이 실측은 02 문서가 Task 16 후보로 올려 둔 "접속·초기화 구간"을 기각한다.**
고정 비용의 실체는 접속·초기화가 아니라 Row materialization이었다. 후보를
글로 세운 쪽은 나였고, 실측이 그것을 뒤집었으므로 실측을 따른다.

## 2. 승인 근거

- 변경 대상이 `src/benchmark/experiments/file_format.py`의
  `_hash_rows_with_payload_size` 한 함수다. Hash 알고리즘·JSON 직렬화는
  그대로고 Row를 Python 객체로 꺼내는 방법만 바뀐다 — 변수 1개다.
- 실제 데이터로 `result_hash` 동일성을 이미 확인했다
  (`66d27f16e98f...`, row_count·payload_bytes 동일). 개선이 결과를 바꾸지
  않는다는 것을 주장이 아니라 실측으로 보였다.
- `pyarrow==25.0.1`은 `pyproject.toml:15`에 이미 고정돼 있다. 신규 런타임
  의존성이 아니다.
- `sort_keys` 대안을 기각한 판단이 맞다. 이득이 작고,
  `canonical_row_json`은 Mart Hash와 공유하므로 영향이 실험 밖으로 번진다.

## 3. csv/parquet 공용 헬퍼 질의 — 변수 1개가 맞다

두 Arm이 같은 헬퍼를 호출하므로 두 Arm이 **같은 방향으로 같은 크기만큼**
바뀐다. 실험 B가 비교하는 것은 두 Arm의 차이이고, 그 차이에 대한 변수는
여전히 "Fixture 형식" 하나다. 한쪽 Arm만 바꾸면 Arm 사이에 두 번째 변수를
넣는 셈이라 오히려 설계 위반이다. 제안대로 공용 헬퍼를 고쳐라.

## 4. 조건

1. **Baseline을 이번 실행에서 다시 측정한다.** 이미 커밋된 M Scale 수치를
   Baseline으로 쓰지 마라. 032 §5에 따라 실행 간 절대 비교는 무효다.
   Baseline 5회와 Improved 5회를 같은 실행 창에서, 같은 Fixture로, Fixture
   재생성 없이 연속 수행한다.
2. **`result_hash` 동일성을 csv·parquet 두 Arm 모두에서 확인한다.**
   현재 확인은 `sellers.parquet` 한 건이다. 하나라도 어긋나면 개선을
   채택하지 않는다.
3. **구간 분해 진단 스크립트를 `scripts/`에 커밋한다.** 037 §5.4와 같은
   이유다. 06 문서에 실릴 10.251/10.253/0.295가 저장소에서 되짚히지 않는
   상태로 남으면 안 된다.
4. **02 문서의 Task 16 후보 문장을 이 실측 결과로 교체한다.** "접속·초기화
   구간을 측정에서 제외하고 재실행"은 기각됐다.
5. **B의 결론이 바뀌는지를 결과로 적는다.** 고정 비용이 줄면 형식 차이가
   드러날 수도, 여전히 가려질 수도 있다. 어느 쪽이든 나온 대로 쓴다.
   특정 결과를 목표로 삼지 마라.

## 5. 이번 범위에서 제외 — 같은 패턴이 제품 코드에도 있다

`src/common/row_hash.py:51-65`의 `hash_cursor_rows`가
`_hash_rows_with_payload_size`와 사실상 같은 `fetchmany` Loop다. Mart Hash가
그 함수를 쓴다. 따라서 이번 실측 결과는 벤치마크 Harness 안에서 끝나지 않고
제품 경로에도 그대로 적용된다.

그래도 이번에는 건드리지 않는다. Mart Hash 정합성 경로라 영향 범위가 크고,
Phase 9에 그 경로를 실측한 데이터가 없다. 근거 없이 같이 고치면 이번 Task의
변수 1개가 깨진다. 06 문서에 "같은 패턴이 제품 코드에 존재하며 별도 후보다"를
사실로 기록하고, 개선은 별도 건으로 남긴다.
