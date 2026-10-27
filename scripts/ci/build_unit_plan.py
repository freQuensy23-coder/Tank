"""Calibrate three whole-file unit lanes from an actual complete successful run."""

import argparse
import hashlib
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

from pydantic import ConfigDict, RootModel

from scripts.ci.unit_contracts import FileTiming, UnitCase, UnitCollection, UnitPlan, read_model
from scripts.ci.unit_reports import read_cases, timing_summary


class CalibrationCollection(RootModel[list[UnitCase] | UnitCollection]):
    model_config = ConfigDict(strict=True)


def build_plan(reports: list[Path], collection: Path, source_head: str) -> UnitPlan:
    recorded = read_model(collection, CalibrationCollection).root
    if isinstance(recorded, UnitCollection):
        if recorded.source_head != source_head:
            raise ValueError("Calibration source differs from its collection")
        expected = recorded.cases
    else:
        expected = recorded
    cases = [case for report in reports for case in read_cases(report)]
    timing_summary(cases)
    if Counter((case.classname, case.name) for case in expected) != Counter(case.identity for case in cases):
        raise ValueError("Calibration report does not match the full unit collection")
    owners = {(case.classname, case.name): case.file for case in expected}
    if len(owners) != len(expected):
        raise ValueError("Calibration unit identities are ambiguous")
    seconds: dict[str, Decimal] = defaultdict(Decimal)
    counts: Counter[str] = Counter()
    for case in cases:
        file = owners[case.identity]
        seconds[file] += case.seconds
        counts[file] += 1
    return UnitPlan(
        schema_version="tank-unit-plan-v2",
        shard_count=3,
        source_head=source_head,
        collection_sha256=hashlib.sha256(collection.read_bytes()).hexdigest(),
        reports_sha256=sorted(hashlib.sha256(report.read_bytes()).hexdigest() for report in reports),
        files=[FileTiming(file=file, cases=counts[file], seconds=seconds[file]) for file in sorted(counts)],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, nargs="+", required=True)
    parser.add_argument("--collection", type=Path, required=True)
    parser.add_argument("--source-head", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    plan = build_plan(arguments.report, arguments.collection, arguments.source_head)
    arguments.output.write_text(plan.model_dump_json(indent=2) + "\n")


if __name__ == "__main__":
    main()
