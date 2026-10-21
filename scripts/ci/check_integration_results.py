"""Keep existing unverified prerequisites explicit; reject any additional skip."""

from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    cases = ET.parse(args.report).getroot().findall(".//testcase")
    if len(cases) < 389:
        parser.error("expected the complete integration collection (at least 389 cases)")
    if any(case.find(tag) is not None for case in cases for tag in ("failure", "error")):
        parser.error("integration report contains failures or errors")
    policy = json.loads(Path(__file__).with_name("integration-prerequisites.json").read_text())
    allowed = {(row["classname"], row["name"]): row["message"] for row in policy["allowed_skips"]}
    unverified = []
    for case in cases:
        skipped = case.find("skipped")
        if skipped is None:
            continue
        key = (case.get("classname"), case.get("name"))
        reason = skipped.get("message", "")
        if key not in allowed or reason != allowed[key]:
            parser.error(f"unexpected integration skip: {key}: {reason}")
        unverified.append({"case": "::".join(key), "reason": reason})
    print(
        json.dumps(
            {"cases": len(cases), "passed": len(cases) - len(unverified), "unverified": unverified}, indent=2
        )
    )


if __name__ == "__main__":
    main()
