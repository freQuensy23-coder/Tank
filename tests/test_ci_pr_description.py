import runpy
from pathlib import Path

import pytest

CI_ROOT = Path(__file__).resolve().parents[1] / "scripts/ci"
problems = runpy.run_path(str(CI_ROOT / "check_pr_description.py"))["problems"]

VALID = "## Summary\n\nFixed.\n\n## Cut corners\n\n- None.\n"


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (VALID, []),
        (VALID.replace("\n", "\r\n"), []),
        ("## Summary\r\n\r\nFixed.\r\n", ["Missing block: Cut corners"]),
        (
            "## Cut corners\n\n- None.\n\n## Summary\n\nFixed.\n",
            ["Blocks are out of order; expected: Summary → Cut corners"],
        ),
    ],
)
def test_description_blocks(body: str, expected: list[str]) -> None:
    assert problems(body) == expected
