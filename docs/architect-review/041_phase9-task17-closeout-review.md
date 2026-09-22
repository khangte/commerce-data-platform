# 041. Phase 9 Task 17(P9-24) 문서화·마감 검수

- 일자: 2026-09-22
- 대상 커밋: `a53a2ed` (`README.md`, `docs/phases/phase-09-benchmark.md`,
  `docs/phases/ROADMAP.md`, `docs/architect-review/040_backlog-row-hash-arrow-candidate.md`)
- 판정: **조건부 승인 — 수정 4건 후 Phase 9 종료**

## 1. 충족 확인

- `P9-01`~`P9-24` 전부 `[x]`이고, 각 항목이 실행 명령과 증거 경로를 함께
  갖는다. 체크만 하고 증거를 안 적은 항목이 없다. 남은 `- [ ]` 0건.
- DoD 7항목 모두 증거 경로를 달았다. AC-17은
  `file_format-M-20260922T000636Z`(Task 16 After-fix 실행)로 잡아 최신
  상태를 가리킨다.
- `P9-18`이 D의 실패(Cache 효과 분해 불가)를 성공으로 포장하지 않고 판정
  그대로 적었다. `P9-20`·`P9-21`도 A를 S 정본으로 유지한 범위 결정과 L
  미실행 근거를 링크한다. 마감 문서가 미달 항목을 숨기지 않는 게 중요했다.
- README Benchmark 절이 Scale·Cold 권한·Raw JSONL 위치·`report` 호출까지
  담아 저장소 밖 독자가 재현 경로를 찾을 수 있다.
- 040 백로그가 `범위 밖` 절 한 줄로 연결됐다. 중복 기술 없음.

## 2. 수정 1 — `상태: Done` 줄의 줄바꿈이 깨졌다

`docs/phases/phase-09-benchmark.md:3`이 `> 상태: Done`으로 끝나 뒤쪽 공백
2칸이 사라졌다. 같은 Blockquote의 다음 줄(`> Milestone: ...`)과 한 줄로
Render된다. 다른 Phase 문서는 전부 공백 2칸을 유지한다. 공백 2칸을 되살려라.

## 3. 수정 2 — Phase 9 DoD 첫 항목이 `P8-*`를 가리킨다

```
- [x] 모든 `P8-*` Task가 완료됐다 — docs/phases/phase-08-reliability.md의 P8-01~P8-20 ...
```

문서에 원래 있던 `P8-*` 오타를 그대로 두고 Phase 8 증거로 채웠다. 글자만
보면 맞지만 Phase 9 DoD는 Phase 9 Task 완료를 요구하는 항목이다. Phase 8
완료는 이 문서 머리의 선행 Phase 조건이고 DoD가 아니다.
`P9-*`로 고치고 증거를 `P9-01`~`P9-24` 전항 `[x]`로 바꿔라.

## 4. 수정 3 — ROADMAP 상태 표기를 원래대로 되돌려라

`docs/phases/ROADMAP.md`에서 Phase 9 행과 `## Phase 9. Benchmark` 제목에
`— 완료`를 붙이고 Gate 열을 `AC-17 완료`로 바꿨다. 되돌려라
(`| 9 | [Benchmark](phase-09-benchmark.md) | AC-17 |`, `## Phase 9. Benchmark`).

근거는 ROADMAP 자신의 규칙이다 — "Phase 문서의 상태는 `Planned → In
Progress → Done`으로 변경한다"이고, ROADMAP 표의 열은 실행 문서와 주요
Gate다. 완료 상태의 Source of Truth는 Phase 문서 머리의 `상태:` 줄 하나다.
Phase 0~8 어느 행에도 완료 표시가 없으므로 Phase 9만 예외가 되면 독자가
"표시 없는 Phase는 미완"으로 잘못 읽는다. `AC-17 완료`는 "주요 Gate" 열의
뜻(어떤 Gate를 다루는 Phase인가)도 바꿔 버린다.

## 5. 수정 4 — Phase 8 문서 상태가 `Planned`로 남아 있다

`docs/phases/phase-08-reliability.md:3`이 `> 상태: Planned`인데 `P8-01`~
`P8-20`이 전부 `[x]`이고 남은 `- [ ]`가 0건이다. Phase 9 DoD가 Phase 8
완료를 증거로 인용하는 이상 이 모순을 남기면 안 된다. `Done`으로 고쳐라.
Phase 8 내용은 건드리지 마라 — 상태 한 줄만이다.

## 6. 범위 확인

Task 16 이전 수치를 인용하는 항목(`P9-10`·`P9-11`의 M Scale median
19.910847433초 / 19.992092046초)은 그대로 둔다. 개선 전 실행의 사실이고
02 문서가 06으로 연결되어 있다. 마감 시점 수치로 덮어쓰면 실행별 기록
원칙(032 §5)을 깨뜨린다.
