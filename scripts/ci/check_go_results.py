"""Require actual Go test cases to pass with no failed or skipped cases."""

import argparse
from collections import Counter
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from tank.contracts.scalars import ZeroInt


class TestEvent(BaseModel):
    """Project Go 1.25 test2json fields used by the completeness gate."""

    model_config = ConfigDict(strict=True, extra="ignore")
    action: Literal["start", "run", "pause", "cont", "pass", "bench", "fail", "output", "skip"] = Field(
        alias="Action"
    )
    package: str = Field(alias="Package")
    test: str | None = Field(default=None, alias="Test")


class TestSummary(BaseModel):
    """Aggregate output; verify rejects every test skip before using the zero default."""

    model_config = ConfigDict(strict=True, extra="forbid")
    cases: int
    passed: int
    skipped: ZeroInt = 0


def verify(report: Path) -> TestSummary:
    events = [TestEvent.model_validate_json(line) for line in report.read_text().splitlines() if line.strip()]
    if any(event.action == "fail" or (event.action == "skip" and event.test is not None) for event in events):
        raise ValueError("Go tests require zero failures and skips")
    if any(event.action == "run" and not event.test for event in events):
        raise ValueError("Go run events require test identity")
    started = Counter(
        (event.package, event.test) for event in events if event.action == "run" and event.test is not None
    )
    passed = Counter(
        (event.package, event.test) for event in events if event.action == "pass" and event.test is not None
    )
    if not started or started != passed:
        raise ValueError("Go report must contain a complete nonempty set of passed test cases")
    count = sum(passed.values())
    return TestSummary(cases=count, passed=count)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    try:
        result = verify(args.report)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(result.model_dump_json())


if __name__ == "__main__":
    main()
