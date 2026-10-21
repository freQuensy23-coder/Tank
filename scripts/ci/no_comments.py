import re
import sys
import tokenize
from collections.abc import Iterator
from pathlib import Path

SKIP_DIRS = {
    ".git",
    ".venv",
    ".tools",
    "venv",
    "node_modules",
    "vendor",
    "upstream",
    "history",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "docs",
    "audit",
    "fixture_check",
    "diagrams",
}
SKIP_PATHS = {("capture", "testdata")}


def python_comments(path: Path) -> list[int]:
    lines = []
    with path.open("rb") as handle:
        for token in tokenize.tokenize(handle.readline):
            if token.type == tokenize.COMMENT:
                if token.start[0] == 1 and token.string.startswith("#!"):
                    continue
                lines.append(token.start[0])
    return lines


def go_comments(path: Path) -> list[int]:
    source = path.read_text(encoding="utf-8")
    comments: list[tuple[int, int, int]] = []
    i, line, size = 0, 1, len(source)
    while i < size:
        char = source[i]
        if char == "\n":
            line += 1
            i += 1
        elif source.startswith("//", i):
            end = source.find("\n", i)
            end = size if end < 0 else end
            comments.append((i, end, line))
            i = end
        elif source.startswith("/*", i):
            end = source.find("*/", i + 2)
            if end < 0:
                raise ValueError(f"unterminated Go comment at line {line}")
            end += 2
            comments.append((i, end, line))
            line += source.count("\n", i, end)
            i = end
        elif char in {chr(34), "'", "`"}:
            quote = char
            start = i
            i += 1
            while i < size:
                if source[i] == quote:
                    i += 1
                    break
                i += 2 if quote != "`" and source[i] == "\\" else 1
            else:
                raise ValueError(f"unterminated Go literal at line {line}")
            line += source.count("\n", start, i)
        else:
            i += 1
    preambles: set[int] = set()
    has_cgo = False
    for index, (_, end, _) in enumerate(comments):
        if re.match(r'[ \t]*(?:\n[ \t]*)?import[ \t]+"C"', source[end:]):
            has_cgo = True
            preambles.add(index)
            while index > 0:
                previous_end = comments[index - 1][1]
                start = comments[index][0]
                gap = source[previous_end:start]
                if gap.strip() or gap.count("\n") > 1:
                    break
                index -= 1
                preambles.add(index)
    lines = []
    for index, (start, end, number) in enumerate(comments):
        value = source[start:end]
        if index in preambles:
            continue
        at_line_start = not source[source.rfind("\n", 0, start) + 1 : start]
        if at_line_start and (
            value.startswith("//go:") or re.fullmatch(r"//line [^\n]+:\d+(?::\d+)?", value)
        ):
            continue
        if re.fullmatch(r"/\*line [^\n]+:\d+(?::\d+)?\*/", value):
            continue
        if at_line_start and (
            value.startswith("// +build ")
            or (has_cgo and re.fullmatch(r"//export [A-Za-z_][A-Za-z_0-9]*", value))
        ):
            continue
        lines.append(number)
    return lines


def files(roots: list[str]) -> Iterator[Path]:
    seen: set[Path] = set()
    for root in roots:
        base = Path(root)
        if not base.exists():
            raise ValueError(f"source path does not exist: {base}")
        candidates = [base] if base.is_file() else base.rglob("*")
        for path in candidates:
            parts = path.parts
            if SKIP_DIRS.intersection(parts):
                continue
            if any(all(part in parts for part in pattern) for pattern in SKIP_PATHS):
                continue
            if path.suffix in {".py", ".go"} and path.is_file() and path not in seen:
                seen.add(path)
                yield path


def main() -> int:
    found = 0
    try:
        paths = sorted(files(sys.argv[1:] or ["src", "tests", "scripts", "deploy", "capture"]))
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    for path in paths:
        check = python_comments if path.suffix == ".py" else go_comments
        try:
            numbers = check(path)
        except (OSError, SyntaxError, ValueError, tokenize.TokenError) as error:
            print(f"{path}: cannot verify comments: {error}", file=sys.stderr)
            found += 1
            continue
        for number in numbers:
            print(f"{path}:{number}: comment is not allowed")
            found += 1
    if found:
        print(f"{found} comment(s) or scanner error(s) found", file=sys.stderr)
    return int(bool(found))


if __name__ == "__main__":
    sys.exit(main())
