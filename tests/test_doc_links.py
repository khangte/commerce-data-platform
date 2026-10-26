"""README와 docs 문서의 상대 링크 대상이 존재하는지 검증한다."""

import re
from pathlib import Path
from urllib.parse import unquote

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INLINE_LINK = re.compile(
    r'\[[^\]\n]*\]\(\s*(?:<([^>\n]+)>|([^\s)]+))(?:\s+["\'][^"\'\n]*["\'])?\s*\)'
)
CODE_BLOCK = re.compile(
    r"^ {0,3}(`{3,}|~{3,})[^\n]*\n.*?^ {0,3}\1[ \t]*$",
    re.MULTILINE | re.DOTALL,
)


def _broken_links(document: Path) -> list[str]:
    """문서 위치를 기준으로 존재하지 않는 상대 링크를 반환한다."""
    broken = []
    markdown = CODE_BLOCK.sub("", document.read_text(encoding="utf-8"))
    markdown = re.sub(r"(`+).*?\1", "", markdown, flags=re.DOTALL)
    for match in INLINE_LINK.finditer(markdown):
        target = match.group(1) or match.group(2)
        if target.lower().startswith(("http://", "https://", "mailto:", "#")):
            continue
        path = unquote(target.split("#", maxsplit=1)[0])
        if not (document.parent / path).exists():
            broken.append(target)
    return broken


@pytest.mark.parametrize(
    ("markdown", "expected"),
    [
        ("[문서](target.md)", []),
        ("[문서](target.md#없는-제목)", []),
        ("[누락](missing.md)", ["missing.md"]),
        ("[누락](missing.md#제목)", ["missing.md#제목"]),
        ("[외부](https://example.com) [외부](http://example.com)", []),
        ("[메일](mailto:team@example.com) [제목](#없는-제목)", []),
        ("[상위](../target.md)", []),
        ("[문서](<target.md>)", []),
        ('[문서](target.md "설명")', []),
        ("[누락](<missing.md>)", ["missing.md"]),
        ("[문서](target%2Emd#제목)", []),
        ("[문서](target.md) [누락](missing.md)", ["missing.md"]),
        ("`[예시](missing.md)`", []),
        ("```markdown\n[예시](missing.md)\n```\n[누락](other.md)", ["other.md"]),
        ("~~~~markdown\n```\n[예시](missing.md)\n```\n~~~~", []),
        ("[`코드 이름`](missing.md)", ["missing.md"]),
        ("````markdown\n```\n```\n[예시](missing.md)\n````", []),
    ],
)
def test_relative_link_detection(tmp_path: Path, markdown: str, expected: list[str]) -> None:
    """누락 파일은 검출하고 외부 링크와 제목의 존재 여부는 검사하지 않는다."""
    (tmp_path / "target.md").touch()
    directory = tmp_path / "nested"
    directory.mkdir()
    (directory / "target.md").touch()
    document = directory / "document.md"
    document.write_text(markdown, encoding="utf-8")

    assert _broken_links(document) == expected


@pytest.mark.parametrize(
    "document",
    [PROJECT_ROOT / "README.md", *sorted((PROJECT_ROOT / "docs").rglob("*.md"))],
    ids=lambda path: str(path.relative_to(PROJECT_ROOT)),
)
def test_document_relative_links_exist(document: Path) -> None:
    """README와 모든 docs 문서의 상대 링크가 가리키는 경로를 확인한다."""
    broken = _broken_links(document)
    assert not broken, f"{document.relative_to(PROJECT_ROOT)}: 존재하지 않는 링크: {broken}"
