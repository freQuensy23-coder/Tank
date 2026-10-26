import subprocess
import sys
from pathlib import Path

import pytest

from scripts.ci.check_pr_description import problems

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


@pytest.mark.parametrize(
    ("body", "status"),
    [
        ("## Summary\r\nNot echoed.\r\n## Cut corners\r\nNone.\r\n", 0),
        (None, 1),
        ("## Cut corners\nNone.\n## Summary\nNot echoed.\n", 1),
    ],
)
def test_actual_checker_cli_retains_crlf_and_missing_body_refusal(body: str | None, status: int) -> None:
    environment = {} if body is None else {"PR_BODY": body}
    script = Path(__file__).resolve().parents[1] / "scripts/ci/check_pr_description.py"
    process = subprocess.run(
        [sys.executable, str(script)], env=environment, capture_output=True, text=True, check=False
    )
    assert process.returncode == status
    assert "Not echoed." not in process.stdout + process.stderr
    if status:
        assert "::error::" in process.stdout
    else:
        assert process.stdout == "" and process.stderr == ""
