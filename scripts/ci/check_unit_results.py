"""Require both full unit partitions exactly once with the original timing policy."""

import argparse
import hashlib
import xml.etree.ElementTree as ET
from collections import Counter
from decimal import InvalidOperation
from pathlib import Path

from scripts.ci.unit_contracts import (
    PassedUnitCase,
    UnitCollection,
    UnitDeclaration,
    UnitPlan,
    UnitShard,
    UnitSummary,
    measured_assignments,
    parse_shard,
    read_model,
    shard_for,
    weight_coverage,
)
from scripts.ci.unit_reports import read_cases, timing_summary


def verify(
    bundles: list[Path], plan_path: Path, expected_source: str, shard: UnitShard | None
) -> UnitSummary:
    if not bundles:
        raise ValueError("Unit verification requires result bundles")
    plan = read_model(plan_path, UnitPlan)
    digest = hashlib.sha256(plan_path.read_bytes()).hexdigest()
    measured = measured_assignments(plan.files)
    declarations: list[UnitDeclaration] = []
    collections: list[UnitCollection] = []
    all_cases: list[PassedUnitCase] = []
    for bundle in bundles:
        collection = read_model(bundle / "unit-collection.json", UnitCollection)
        declaration = read_model(bundle / "unit-run.json", UnitDeclaration)
        if collection.source_head != expected_source or declaration.source_head != expected_source:
            raise ValueError("Unit artifacts belong to a different source commit")
        if declaration.collection_sha256 != collection.signature or declaration.plan_sha256 != digest:
            raise ValueError("Unit declaration does not bind its collection and measured plan")
        if declaration.coverage != weight_coverage(collection, plan):
            raise ValueError("Unit weight coverage metadata is inconsistent")
        cases = read_cases(bundle / "unit.xml")
        expected = Counter(
            case.identity
            for case in collection.cases
            if shard_for(case.file, measured) == declaration.shard.index
        )
        if not expected or Counter(case.identity for case in cases) != expected:
            raise ValueError("Unit report does not match the full selected case set exactly once")
        timing_summary(cases)
        declarations.append(declaration)
        collections.append(collection)
        all_cases.extend(cases)
    if any(collection.signature != collections[0].signature for collection in collections[1:]):
        raise ValueError("Unit lanes collected different full case sets")
    if shard is not None:
        if len(declarations) != 1 or declarations[0].shard != shard:
            raise ValueError("A unit shard check requires exactly its declared lane")
    else:
        if len(declarations) != 2 or {declaration.shard.index for declaration in declarations} != {0, 1}:
            raise ValueError("Unit aggregate requires both distinct lanes")
        expected = Counter(case.identity for case in collections[0].cases)
        if Counter(case.identity for case in all_cases) != expected:
            raise ValueError("Unit aggregate does not match the full case union exactly once")
    return timing_summary(all_cases)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, nargs="+", required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--shard")
    arguments = parser.parse_args()
    try:
        shard = None if arguments.shard is None else parse_shard(arguments.shard)
        result = verify(arguments.bundle, arguments.plan, arguments.source_sha, shard)
    except (ValueError, KeyError, OSError, ET.ParseError, InvalidOperation) as error:
        parser.error(str(error))
    print(result.model_dump_json())


if __name__ == "__main__":
    main()
