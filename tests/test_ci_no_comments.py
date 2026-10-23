import runpy
from pathlib import Path

import pytest

CI_ROOT = Path(__file__).resolve().parents[1] / "scripts/ci"
COMMENT_POLICY = runpy.run_path(str(CI_ROOT / "no_comments.py"))
files = COMMENT_POLICY["files"]
go_comments = COMMENT_POLICY["go_comments"]
python_comments = COMMENT_POLICY["python_comments"]


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ('value = "# literal"\n', []),
        ('"""# documentation"""\n', []),
        ("#!/usr/bin/env python3\nvalue = 1\n", []),
        ("value = 1 # ordinary comment\n", [1]),
        ("\n#!/usr/bin/env python3\n", [2]),
    ],
)
def test_python_comment_tokens(tmp_path: Path, source: str, expected: list[int]) -> None:
    path = tmp_path / "example.py"
    path.write_text(source)
    assert python_comments(path) == expected


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ('package p\nvar url = "https://example.org/a/*b*/"\n', []),
        ("package p\nvar raw = `first\n// literal\n/* literal */`\n", []),
        ("package p\n// ordinary\nvar x = 1 /* ordinary */\n", [2, 3]),
        ("//go:build linux\n// +build linux\n\npackage p\n//go:embed asset.json\nvar raw string\n", []),
        ("package p\n//line generated.go:42\n/*line generated.go:43*/var x = 1\n", []),
        ("package p\nvar x = 1 //go:embed invalid\n", [2]),
        ('package p\n/*#include <stdlib.h>*/\nimport "C"\n//export Callback\nfunc Callback() {}\n', []),
        ('package p\n// ordinary\n\nimport "C"\n', [2]),
        ("package p\nvar raw = `first\nsecond`\n// ordinary\n", [4]),
    ],
)
def test_go_comment_lexer(tmp_path: Path, source: str, expected: list[int]) -> None:
    path = tmp_path / "example.go"
    path.write_text(source)
    assert go_comments(path) == expected


def test_immutable_history_and_upstream_are_excluded(tmp_path: Path) -> None:
    for name in (
        "deploy/history/old.py",
        "deploy/upstream/base.go",
        "deploy/vendor/copy.go",
        "capture/testdata/input.go",
        "src/active.py",
    ):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# retained\n" if path.suffix == ".py" else "// retained\n")
    assert list(files([str(tmp_path)])) == [tmp_path / "src/active.py"]
