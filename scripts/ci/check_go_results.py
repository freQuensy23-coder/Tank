"""Require actual Go test cases to pass with no failed or skipped cases."""

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    events = [json.loads(line) for line in args.report.read_text().splitlines() if line.strip()]
    if any(
        event.get("Action") == "fail" or (event.get("Action") == "skip" and "Test" in event)
        for event in events
    ):
        parser.error("Go tests require zero failures and skips")
    started = {
        (event["Package"], event["Test"])
        for event in events
        if event.get("Action") == "run" and "Test" in event
    }
    passed = {
        (event["Package"], event["Test"])
        for event in events
        if event.get("Action") == "pass" and "Test" in event
    }
    if not started or started != passed:
        parser.error("Go report must contain a complete nonempty set of passed test cases")
    print(json.dumps({"cases": len(passed), "passed": len(passed), "skipped": 0}))


if __name__ == "__main__":
    main()
