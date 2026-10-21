"""Check the unit budget from pytest's standard total-duration JUnit report."""

import argparse
import sys
import xml.etree.ElementTree as ET
from decimal import Decimal
from math import ceil
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="JUnit XML produced by the default unit command")
    args = parser.parse_args()
    cases = ET.parse(args.report).getroot().findall(".//testcase")
    if not cases:
        parser.error("the report contains no test cases")
    if any(case.find("skipped") is not None for case in cases):
        parser.error("unit timing cannot qualify skipped cases; fix the selection or missing unit input")
    if any(case.find(tag) is not None for case in cases for tag in ("failure", "error")):
        parser.error("unit timing cannot qualify a failed run")
    durations = [Decimal(case.attrib["time"]) for case in cases]
    if any(not duration.is_finite() or duration < 0 for duration in durations):
        parser.error("the report contains an invalid duration")
    durations.sort()
    p90 = durations[ceil(len(durations) * Decimal("0.90")) - 1]
    maximum = durations[-1]
    print(f"{len(cases)} unit cases; p90={p90}s; max={maximum}s (setup + call + teardown)")
    if p90 >= 1 or maximum >= 10:
        print("Unit budget failed: every case must be <10s and p90 must be <1s.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
