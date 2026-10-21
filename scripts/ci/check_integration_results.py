"""Require the complete integration collection to pass with no skipped cases."""

from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--collection", type=Path, required=True)
    args = parser.parse_args()
    expected = json.loads(args.collection.read_text())
    if not expected:
        parser.error("the expected integration collection is empty")
    cases = ET.parse(args.report).getroot().findall(".//testcase")
    if not cases:
        parser.error("the integration report contains no cases")
    if any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
        parser.error("integration requires zero failures, errors and skips")
    expected_ids = Counter((case["classname"], case["name"]) for case in expected)
    actual_ids = Counter((case.get("classname"), case.get("name")) for case in cases)
    if expected_ids != actual_ids:
        parser.error("integration report does not match the full collected case set")
    print(json.dumps({"cases": len(cases), "passed": len(cases), "skipped": 0}))


if __name__ == "__main__":
    main()
