"""Actual owned DTO scalar flags retain boolean/integer identity."""

from dataclasses import dataclass

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue, ValidationError

from tank.agents.classification import JevClassifier
from tank.agents.jev import JevItem, JevState
from tank.agents.native_readback import NativeSessionMissing
from tank.agents.pipeline_contracts import JevScoreRecord
from tank.agents.tool_contracts import FailedToolResponse, NativeToolResult
from tank.contracts.repository import Collection, Repository
from tank.runtime_contracts import RevisedReactionJob
from tank.storage import MemoryRepository


@dataclass(frozen=True)
class ClassifierPreferences:
    opencode_api_key: str | None
    replay: bool


@dataclass(frozen=True)
class ClassifierSubject:
    item_id: str


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [False, True, 0.0, "0"])
async def test_actual_classifier_ledger_requires_integer_zero_web_searches(invalid: JsonValue) -> None:
    repo = MemoryRepository()
    collection = Collection("ops.llm_usage", JevScoreRecord)
    scores = Repository(repo, collection)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "answers": {"relevant": {"type": "noul", "noul": 0.8}},
                "usage": {},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        classifier = JevClassifier(
            scores,
            ClassifierPreferences(opencode_api_key=None, replay=False),
            http,
            versions=declared_versions_fixture(),
        )
        ledger = await classifier._score_record(
            ClassifierSubject("source"), "relevant", JevState(item=JevItem(source="Synthetic"))
        )
    assert type(ledger.web_searches) is int and ledger.web_searches == 0
    wire = collection.encode(ledger)
    wire["web_searches"] = invalid
    with pytest.raises(ValidationError):
        collection.decode(wire)


@pytest.mark.parametrize("invalid", [0, 0.0, "false", True])
def test_failed_tool_response_requires_an_actual_false_flag(invalid: JsonValue) -> None:
    error: dict[str, JsonValue] = {"type": "ValidationError", "reason": "Declared", "category": "VALIDATION"}
    assert FailedToolResponse.model_validate({"ok": False, "error": error}).ok is False
    with pytest.raises(ValidationError):
        FailedToolResponse.model_validate({"ok": invalid, "error": error})


@pytest.mark.parametrize("invalid", [1, 1.0, "true", False])
def test_actual_tool_error_and_revision_flags_require_boolean_true(invalid: JsonValue) -> None:
    tool: dict[str, JsonValue] = {
        "type": "user.custom_tool_result",
        "custom_tool_use_id": "call",
        "content": [],
    }
    assert NativeToolResult.model_validate(tool).is_error is None
    assert NativeToolResult.model_validate({**tool, "is_error": None}).is_error is None
    assert NativeToolResult.model_validate({**tool, "is_error": True}).is_error is True
    with pytest.raises(ValidationError):
        NativeToolResult.model_validate({**tool, "is_error": invalid})
    revision: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "POST_15M",
        "change_id": "change",
        "reason": "Declared",
    }
    assert (
        RevisedReactionJob.model_validate({**revision, "automatic_revision": True}).automatic_revision is True
    )
    with pytest.raises(ValidationError):
        RevisedReactionJob.model_validate({**revision, "automatic_revision": invalid})
    assert NativeSessionMissing(kind="MISSING", missing=True, session_id="session").missing is True
    with pytest.raises(ValidationError):
        NativeSessionMissing.model_validate({"kind": "MISSING", "missing": invalid, "session_id": "session"})
