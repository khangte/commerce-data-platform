# 007 Mart Hash의 무한대 Sentinel 처리

- 판정일: 2026-09-18
- 대상: developer가 [006](006_early-arriving-fact-effective-from.md) 구현 중 보고한 파급
- 판정: **Sentinel 문자열로 정규화한다.** naive datetime에 UTC를 붙여 통과시키지 않는다.

## 1. 증상

`effective_from`이 Dimension에 들어가면서 `timestamptz '-infinity'` 값이 Mart Hash 입력에 실린다. DuckDB Python Binding은 이 값을 tzinfo 없는 `datetime.min`으로 돌려준다. 확인 결과는 다음과 같다.

```
timestamptz '-infinity'  → datetime.datetime(1, 1, 1, 0, 0)                      (naive)
timestamptz 'infinity'   → datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)   (naive)
```

`src/warehouse/mart_hash.py:153`의 `_json_default()`는 naive datetime을 `ValueError: Mart timestamp must include a UTC offset`으로 거부한다. 그래서 `tests/integration/test_incremental_full_refresh_hash_integration.py`가 실패한다.

## 2. 결정

`_json_default()`의 naive 분기에서 두 Sentinel만 문자열로 정규화하고, 나머지 naive datetime은 지금처럼 거부한다.

```python
if isinstance(value, datetime):
    if value.tzinfo is None or value.utcoffset() is None:
        if value == datetime.min:
            return "-infinity"
        if value == datetime.max:
            return "infinity"
        raise ValueError("Mart timestamp must include a UTC offset")
    return value.astimezone(UTC).isoformat()
```

`infinity`까지 함께 처리하는 이유는 대칭이다. 지금은 `-infinity`만 나오지만, 상한을 여는 Model이 생기면 같은 경로로 들어온다.

### naive에 UTC를 붙이지 않는 이유

1. `-infinity`는 시각이 아니라 Sentinel이다. `0001-01-01T00:00:00+00:00`으로 적으면 실제 시각처럼 보이고, 같은 값을 갖는 진짜 Timestamp와 Hash가 충돌한다.
2. 그 검사는 Mart가 Offset을 잃었을 때 잡아내려고 둔 Guard다. naive 전체에 UTC를 붙이면 Guard가 사라진다. Offset 유실은 Timestamp를 조용히 어긋나게 만드는 종류의 결함이라 Guard를 유지해야 한다.
3. Sentinel 두 값만 예외로 두면 Guard의 적용 범위가 그대로 남는다.

## 3. 검증

`tests/test_mart_hash.py`에 `_json_default()` 단위 Test 3건을 추가한다.

1. `datetime.min`이 `"-infinity"`를 반환한다.
2. `datetime.max`가 `"infinity"`를 반환한다.
3. 그 밖의 naive datetime(예: `datetime(2026, 9, 18, 0, 0)`)은 `ValueError`를 낸다.

그리고 `tests/integration/test_incremental_full_refresh_hash_integration.py`와 `tests/integration/test_replay_boundary_hash_integration.py`가 통과하는지 확인한다.

## 4. 범위

이 변경은 `src/warehouse/mart_hash.py`와 `tests/test_mart_hash.py`에 한정한다. Model과 Dimension은 006 그대로 둔다.
