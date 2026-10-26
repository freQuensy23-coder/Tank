"""Record the unsharded offline unit collection before selecting either lane."""

import subprocess
from pathlib import Path

import pytest
from _pytest.junitxml import bin_xml_escape, mangle_test_address

from scripts.ci.unit_contracts import UnitCase, UnitCollection


def source_head(root: Path) -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()


def case_from_item(item: pytest.Item) -> UnitCase:
    parts = mangle_test_address(item.nodeid)
    return UnitCase(
        classname=".".join(parts[:-1]), name=bin_xml_escape(parts[-1]), file=item.nodeid.split("::", 1)[0]
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--unit-collection-output", type=Path, default=Path(".test-results/unit-collection.json")
    )


def pytest_collection_finish(session: pytest.Session) -> None:
    if not session.config.option.collectonly:
        raise pytest.UsageError("collect_units requires --collect-only")
    collection = UnitCollection(
        schema_version="tank-unit-collection-v1",
        source_head=source_head(session.config.rootpath),
        cases=[case_from_item(item) for item in session.items],
    )
    target = session.config.getoption("--unit-collection-output")
    if not isinstance(target, Path):
        raise pytest.UsageError("Unit collection output must be a path")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(collection.model_dump_json(indent=2) + "\n")
