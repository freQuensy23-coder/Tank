from pathlib import Path

import pytest
from pydantic import ValidationError
from source_support import fixture_versions

from tank.analysis.reaction_scheduling import (
    AssessmentFreeDiagnosticScheduling,
    diagnostic_call_catalog,
)
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import MemoryRepository


def test_assessment_free_capability_rejects_declared_assessment() -> None:
    catalog = diagnostic_call_catalog(
        "a" * 64,
        [{"call_id": "call-1", "tool": "get_item"}, {"call_id": "call-2", "tool": "set_assessment"}],
    )
    with pytest.raises(ValidationError, match="cannot contain an assessment"):
        AssessmentFreeDiagnosticScheduling(mode="ASSESSMENT_FREE_DIAGNOSTIC", catalog=catalog)


def test_catalog_identity_binds_all_captured_calls() -> None:
    first = diagnostic_call_catalog(
        "a" * 64, [{"call_id": "call-1", "tool": "get_item", "arguments": {"item_id": "one"}}]
    )
    changed = diagnostic_call_catalog(
        "a" * 64, [{"call_id": "call-1", "tool": "get_item", "arguments": {"item_id": "two"}}]
    )
    assert first.catalog_sha256 != changed.catalog_sha256
    assert first.calls == changed.calls
    with pytest.raises(ValidationError, match="repeats a call identity"):
        diagnostic_call_catalog(
            "a" * 64, [{"call_id": "same", "tool": "get_item"}, {"call_id": "same", "tool": "web_search"}]
        )


async def test_diagnostic_refuses_assessment_before_method_or_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = diagnostic_call_catalog("a" * 64, [{"call_id": "read-1", "tool": "get_item"}])
    scheduling = AssessmentFreeDiagnosticScheduling(mode="ASSESSMENT_FREE_DIAGNOSTIC", catalog=catalog)
    repo = MemoryRepository()
    await repo.open()
    await repo.put("evt.events", "event-1", {"event_id": "event-1", "statement": "Captured statement"})
    before = await repo.get("evt.events", "event-1")
    tools = EventToolService(
        repo, Settings(data_dir=tmp_path), None, None, versions=fixture_versions(), scheduler=scheduling
    )

    async def forbidden_assessment() -> None:
        raise AssertionError("Assessment mutation must not be reached")

    monkeypatch.setattr(tools, "set_assessment", forbidden_assessment)
    with pytest.raises(PermissionError, match="does not authorize assessment writes"):
        await tools.dispatch("set_assessment", {"event_id": "event-1"}, {})
    assert await repo.get("evt.events", "event-1") == before
    assert await repo.list("evt.ems_assessments") == []
    await repo.close()
