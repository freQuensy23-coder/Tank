"""Raw Go test2json event documents exercise the report boundary and its effects."""

import importlib.util
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pydantic import JsonValue

if TYPE_CHECKING:
    from scripts.ci import check_go_results as gate
else:
    source = Path(__file__).parents[1] / "scripts/ci/check_go_results.py"
    spec = importlib.util.spec_from_file_location("check_go_results", source)
    assert spec is not None
    assert spec.loader is not None
    gate = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = gate
    spec.loader.exec_module(gate)


def report(tmp_path: Path, events: list[dict[str, JsonValue]]) -> Path:
    path = tmp_path / "go.jsonl"
    path.write_text("".join(json.dumps(event) + "\n" for event in events))
    return path


def test_report_requires_each_started_execution_to_finish(tmp_path: Path) -> None:
    events: list[dict[str, JsonValue]] = [
        {"Action": "run", "Package": "tank", "Test": "TestState"},
        {"Action": "run", "Package": "tank", "Test": "TestState"},
        {"Action": "pass", "Package": "tank", "Test": "TestState"},
    ]
    with pytest.raises(ValueError, match="complete nonempty"):
        gate.verify(report(tmp_path, events))
    events.append({"Action": "pass", "Package": "tank", "Test": "TestState"})
    result = gate.verify(report(tmp_path, events))
    assert result.cases == 2
    assert result.passed == 2
    assert result.skipped == 0


@pytest.mark.parametrize("action", ["fail", "skip"])
def test_report_cannot_hide_unsuccessful_test(tmp_path: Path, action: str) -> None:
    events: list[dict[str, JsonValue]] = [
        {"Action": "run", "Package": "tank", "Test": "TestState"},
        {"Action": action, "Package": "tank", "Test": "TestState"},
    ]
    with pytest.raises(ValueError, match="zero failures and skips"):
        gate.verify(report(tmp_path, events))


@pytest.mark.parametrize(
    "event",
    [
        {"Action": "run", "Package": "tank"},
        {"Action": "unexpected", "Package": "tank", "Test": "TestState"},
        {"Action": 1, "Package": "tank", "Test": "TestState"},
    ],
)
def test_report_rejects_invalid_execution_evidence(tmp_path: Path, event: dict[str, JsonValue]) -> None:
    with pytest.raises(ValueError):
        gate.verify(report(tmp_path, [event]))
