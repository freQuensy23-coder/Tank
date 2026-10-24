"""Record the full selected collection for the integration JUnit completeness gate."""

import json
from pathlib import Path

import pytest
from _pytest.junitxml import bin_xml_escape, mangle_test_address


def pytest_collection_finish(session: pytest.Session) -> None:
    if not session.config.option.collectonly:
        raise ValueError("collect_integration is for --collect-only")
    cases = []
    for item in session.items:
        parts = mangle_test_address(item.nodeid)
        cases.append(
            {
                "classname": ".".join(parts[:-1]),
                "name": bin_xml_escape(parts[-1]),
                "file": item.nodeid.split("::", 1)[0],
            }
        )
    target = Path(".test-results/integration-collection.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(cases, indent=2) + "\n")
