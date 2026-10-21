import sys
import tokenize
from pathlib import Path

SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "vendor", "__pycache__", "upstream"}


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
    text = path.read_text(encoding="utf-8")
    lines = []
    i, line, n = 0, 1, len(text)
    while i < n:
        c = text[i]
        if c == "\n":
            line += 1
            i += 1
        elif text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end < 0 else end
            if not text.startswith("//go:", i):
                lines.append(line)
            i = end
        elif text.startswith("/*", i):
            end = text.find("*/", i + 2)
            end = n if end < 0 else end + 2
            lines.append(line)
            line += text.count("\n", i, end)
            i = end
        elif c in "\"'":
            j = i + 1
            while j < n and text[j] != c:
                j += 2 if text[j] == "\\" else 1
            i = j + 1
        elif c == "`":
            end = text.find("`", i + 1)
            end = n if end < 0 else end + 1
            line += text.count("\n", i, end)
            i = end
        else:
            i += 1
    return lines


def files(roots: list[str]):
    for root in roots:
        base = Path(root)
        candidates = [base] if base.is_file() else base.rglob("*")
        for path in candidates:
            if path.suffix in {".py", ".go"} and not SKIP_DIRS & set(path.parts):
                yield path


def main() -> int:
    found = 0
    for path in sorted(files(sys.argv[1:] or ["."])):
        check = python_comments if path.suffix == ".py" else go_comments
        for number in check(path):
            print(f"{path}:{number}: comment is not allowed")
            found += 1
    if found:
        print(f"{found} comment(s) found", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
