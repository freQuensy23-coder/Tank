"""Check that a pull request description has the blocks of .github/pull_request_template.md in order.

The description comes from the PR_BODY environment variable. Only the block headings are checked:
Summary and Cut corners must be present, and the template blocks that are present must keep the
template's order.
"""

import os
import re
import sys

ORDER: list[str] = [
    "Summary",
    "Product specification changes",
    "Product diagram",
    "Technical changes",
    "Technical specifications",
    "Database",
    "New queries",
    "Database load",
    "Tests",
    "Cut corners",
]
REQUIRED: list[str] = ["Summary", "Cut corners"]


def headings(body: str) -> list[str]:
    text = re.sub(r"<!--.*?-->", "", body.replace("\r\n", "\n"), flags=re.S)
    return [match.group(1) for match in re.finditer(r"^#{2,3} +(.+?) *$", text, flags=re.M)]


def problems(body: str) -> list[str]:
    found = [name for name in headings(body) if name in ORDER]
    out = [f"Missing block: {name}" for name in REQUIRED if name not in found]
    positions = [ORDER.index(name) for name in found]
    if positions != sorted(positions):
        out.append(
            "Blocks are out of order; expected: " + " → ".join(name for name in ORDER if name in found)
        )
    return out


def main() -> int:
    found = problems(os.environ.get("PR_BODY", ""))
    for line in found:
        print(f"::error::{line}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
