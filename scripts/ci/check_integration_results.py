"""Require the complete integration collection to pass with no skipped cases."""

import argparse
import json
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripts.ci.integration_shard import parse_shard, shard_for
else:
    from integration_shard import parse_shard, shard_for
from pydantic import BaseModel, ConfigDict, RootModel


class CollectedCase(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    classname: str
    name: str
    file: str


class Collection(RootModel[list[CollectedCase]]):
    pass


def verify(reports: list[Path], collections: list[Path], shard: str | None = None) -> dict[str, int]:
    expected = Collection.model_validate_json(collections[0].read_text()).root
    if not expected:
        raise ValueError("the expected integration collection is empty")
    if any(Collection.model_validate_json(path.read_text()).root != expected for path in collections[1:]):
        raise ValueError("integration shards collected different full case sets")
    if shard is not None:
        if len(reports) != 1 or len(collections) != 1:
            raise ValueError("a shard check requires exactly one report and collection")
        index, count = parse_shard(shard)
        expected = [case for case in expected if shard_for(case.file, count) == index]
    cases = [case for report in reports for case in ET.parse(report).getroot().findall(".//testcase")]
    if not cases or not expected:
        raise ValueError("the integration report or selected collection contains no cases")
    if any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
        raise ValueError("integration requires zero failures, errors and skips")
    expected_ids = Counter((case.classname, case.name) for case in expected)
    actual_ids = Counter((case.get("classname"), case.get("name")) for case in cases)
    if expected_ids != actual_ids:
        raise ValueError("integration report does not match the full collected case set")
    return {"cases": len(cases), "passed": len(cases), "skipped": 0}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, nargs="+")
    parser.add_argument("--collection", type=Path, required=True, nargs="+")
    parser.add_argument("--shard")
    args = parser.parse_args()
    try:
        result = verify(args.report, args.collection, args.shard)
    except (ValueError, KeyError, OSError, ET.ParseError) as error:
        parser.error(str(error))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
