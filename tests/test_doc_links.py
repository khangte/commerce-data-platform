"""문서 상대 링크 대상이 Git 추적 파일과 폴더로 재현되는지 검증한다."""

import os
import re
import subprocess
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


def _tracked_targets(root: Path) -> set[Path]:
    """Git에서 추적하고 제외하지 않은 파일과 상위 폴더를 반환한다."""
    tracked = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "--cached", "-z"], text=True,
    ).split("\0")
    ignored = set(subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "--cached", "--ignored", "--exclude-standard", "-z"],
        text=True,
    ).split("\0"))
    root = root.resolve()
    targets = set()
    for name in set(tracked) - ignored - {""}:
        path = root / name
        if path.is_relative_to(root):
            targets.add(path)
            targets.update(parent for parent in path.parents if parent.is_relative_to(root))
    return targets


@pytest.fixture(scope="session")
def tracked_targets() -> set[Path]:
    """전체 문서 검사에서 Git 추적 대상 목록을 한 번만 읽는다."""
    return _tracked_targets(PROJECT_ROOT)


def _broken_links(document: Path, tracked_targets: set[Path]) -> list[str]:
    """존재하지 않거나 Git 추적 대상이 아닌 상대 링크를 반환한다."""
    broken = []
    markdown = CODE_BLOCK.sub("", document.read_text(encoding="utf-8"))
    markdown = re.sub(r"(`+).*?\1", "", markdown, flags=re.DOTALL)
    for match in INLINE_LINK.finditer(markdown):
        target = match.group(1) or match.group(2)
        if target.lower().startswith(("http://", "https://", "mailto:", "#")):
            continue
        path = unquote(target.split("#", maxsplit=1)[0])
        resolved = Path(os.path.abspath(document.parent / path))
        if not resolved.exists() or resolved not in tracked_targets:
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

    targets = {tmp_path / "target.md", directory / "target.md"}
    assert _broken_links(document, targets) == expected


@pytest.mark.parametrize(
    "document",
    [PROJECT_ROOT / "README.md", *sorted((PROJECT_ROOT / "docs").rglob("*.md"))],
    ids=lambda path: str(path.relative_to(PROJECT_ROOT)),
)
def test_document_relative_links_exist(document: Path, tracked_targets: set[Path]) -> None:
    """상대 링크가 새 Clone에서도 존재하는 Git 추적 경로인지 확인한다."""
    broken = _broken_links(document, tracked_targets)
    assert not broken, f"{document.relative_to(PROJECT_ROOT)}: 누락·미추적·제외된 링크: {broken}"


@pytest.mark.parametrize(("kind", "target", "expected"), [
    ("tracked_file", "target.md", []),
    ("tracked_directory", "assets/", []),
    ("untracked_file", "local.md", ["local.md"]),
    ("untracked_directory", "local/", ["local/"]),
    ("ignored_file", "ignored.md", ["ignored.md"]),
    ("ignored_directory", "ignored/", ["ignored/"]),
    ("tracked_ignored_file", "forced.md", ["forced.md"]),
    ("missing_tracked_file", "deleted.md", ["deleted.md"]),
    ("outside_repository", "../outside.md", ["../outside.md"]),
    ("untracked_symlink", "alias.md", ["alias.md"]),
    ("ignored_symlink", "ignored-alias.md", ["ignored-alias.md"]),
])
def test_relative_links_require_tracked_targets(tmp_path, kind, target, expected):
    """로컬에만 있거나 Git에서 제외된 파일·폴더 링크를 거부한다."""
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(["git", "init", "--quiet", str(root)], check=True)
    (root / ".gitignore").write_text("ignored.md\nignored/\nforced.md\n", encoding="utf-8")
    for name in ("target.md", "local.md", "ignored.md", "forced.md", "deleted.md"):
        (root / name).touch()
    for name in ("assets", "local", "ignored"):
        (root / name).mkdir()
        (root / name / "증적.txt").touch()
    (tmp_path / "outside.md").touch()
    (root / "alias.md").symlink_to("target.md")
    (root / "ignored-alias.md").symlink_to("target.md")
    with (root / ".gitignore").open("a", encoding="utf-8") as stream:
        stream.write("ignored-alias.md\n")
    subprocess.run(["git", "-C", str(root), "add", ".gitignore", "target.md",
                    "assets", "deleted.md"], check=True)
    subprocess.run(["git", "-C", str(root), "add", "-f", "forced.md"], check=True)
    (root / "deleted.md").unlink()
    document = root / "README.md"
    document.write_text(f"[대상]({target})", encoding="utf-8")
    targets = _tracked_targets(root)
    assert _broken_links(document, targets) == expected
