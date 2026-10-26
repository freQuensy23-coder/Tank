"""Admit complete successful unit JUnit observations and preserve timing limits."""

import xml.etree.ElementTree as ET
from decimal import Decimal
from math import ceil
from pathlib import Path

from scripts.ci.unit_contracts import PassedUnitCase, UnitSummary


def read_cases(path: Path) -> list[PassedUnitCase]:
    nodes = ET.parse(path).getroot().findall(".//testcase")
    if not nodes:
        raise ValueError("The unit report contains no test cases")
    if any(node.find(tag) is not None for node in nodes for tag in ("failure", "error", "skipped")):
        raise ValueError("Units require zero failures, errors and skips")
    return [
        PassedUnitCase(
            classname=node.attrib["classname"], name=node.attrib["name"], seconds=Decimal(node.attrib["time"])
        )
        for node in nodes
    ]


def timing_summary(cases: list[PassedUnitCase]) -> UnitSummary:
    if not cases:
        raise ValueError("Unit timing requires actual cases")
    durations = sorted(case.seconds for case in cases)
    p90 = durations[ceil(len(durations) * Decimal("0.90")) - 1]
    maximum = durations[-1]
    if p90 >= 1 or maximum >= 10:
        raise ValueError("Unit budget failed: every case must be <10s and p90 must be <1s")
    return UnitSummary(cases=len(cases), passed=len(cases), skipped=0, p90_seconds=p90, max_seconds=maximum)
