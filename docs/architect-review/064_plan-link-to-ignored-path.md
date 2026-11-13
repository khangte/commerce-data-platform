# 064. 감사 보완 Phase 문서의 계획 링크가 Git 제외 경로를 가리킨다

- 일자: 2026-11-13
- 대상: 감사 보완 Phase 2·3 (`docs/superpowers/plans/2026-11-13-audit-remediation.md`)
- 지적: reviewer. `docs/phases/phase-04-airflow-orchestration.md:577,728`과 `docs/phases/phase-01-source-environment.md`가 계획 문서를 Markdown 링크로 건다. `docs/superpowers/`는 Git 제외 경로라 `tests/test_doc_links.py`가 실패한다. `PRD_v1.35.md`·ADR 023·계획을 임시 Index에 올려도 2건이 남는다(`tests/test_doc_links.py:24-47`이 제외 경로를 추적 여부와 무관하게 뺀다).

## 판정

지적이 맞다. 수정이 필요하다. 문서 쪽을 고친다.

| 대안 | 판정 | 이유 |
| ---- | ---- | ---- |
| 계획 경로를 Markdown 링크 대신 Code 표기로 적는다 | **채택** | 기존 ADR(022)·판정 문서가 모두 이렇게 쓴다. 테스트와 제외 규칙을 건드리지 않는다 |
| `.gitignore`에 이 계획만 예외를 둔다 | 기각 | 계획 문서는 추적하지 않는다는 기존 방침을 한 건 때문에 바꾸게 된다 |
| 링크 테스트가 제외 경로를 허용하게 한다 | 기각 | 새 Clone에서 깨진 링크를 잡는 테스트의 목적이 없어진다 |

## 정정

- architect가 Phase 2 보고에서 "계획 파일은 `git add -f`하면 된다"고 쓴 것은 틀렸다. 테스트는 제외 경로를 강제로 추적해도 뺀다. 계획 파일은 Commit 대상이 아니다.
- 계획 §7에 "계획 경로는 Code 표기로 적는다"를 더했다.

## 후속

- backend: 두 Phase 문서의 계획 링크 3곳을 Code 표기로 바꾸고 `uv run pytest tests/test_doc_links.py`를 다시 돌린다. 남는 실패는 추적 전인 `PRD_v1.35.md`·ADR 023 링크뿐이어야 한다.
- Phase 2 완료 판정은 이 수정과 재리뷰 뒤로 미룬다.
