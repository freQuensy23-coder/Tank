"""Record the full selected collection for the integration JUnit completeness gate."""

from __future__ import annotations

import json
from pathlib import Path

from _pytest.junitxml import bin_xml_escape, mangle_test_address


def pytest_collection_finish(session) -> None:
    if not session.config.option.collectonly:
        raise ValueError("collect_integration is for --collect-only")
    cases = []
    for item in session.items:
        # Match the JUnit writer in the project's frozen pytest version.
        parts = mangle_test_address(item.nodeid)
        cases.append({"classname": ".".join(parts[:-1]), "name": bin_xml_escape(parts[-1])})
    target = Path(".test-results/integration-collection.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(cases, indent=2) + "\n")
