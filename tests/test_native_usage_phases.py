"""Unused metadata cannot block a no-report span or an independently reported model."""

from pathlib import Path
from typing import Literal

import pytest
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture
from test_session_completion_lineage import completion_repo as completion_repo
from test_storage import repo as repo

from tank.agents.native_transport import NativeEventObserved
from tank.agents.native_usage import record_native_usage
from tank.contracts.native_usage import NativeUsageSession, NativeUsageSpan
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.storage import StorageBackend
from tank.time_precision import collection_clock


@pytest.mark.parametrize("reported_null", [False, True])
async def test_reported_span_model_does_not_require_an_unused_session_model_or_prompt(
    repo: StorageBackend, reported_null: bool
) -> None:
    record: dict[str, JsonValue] = {"session_id": "session"}
    if reported_null:
        record.update(model_id=None, prompt_version=None)
    session = NativeUsageSession.from_record(record)
    span = NativeUsageSpan.from_wire(
        {
            "id": "actual-span",
            "model": "actual-reported-model",
            "model_usage": {"input_tokens": 2, "output_tokens": 1},
        }
    )
    await record_native_usage(
        repo,
        session,
        span,
        versions=ResolvedVersionIdentity(config_version="synthetic-config", code_version="synthetic-code"),
        collected_at=collection_clock(),
    )
    current = await repo.get("ops.llm_usage", "session:actual-span")
    assert current is not None
    assert current["model_id"] == current["model"] == "actual-reported-model"
    assert current["model_id_basis"] == "NATIVE_SPAN_MODEL"
    assert current["reported_model_id"] == "actual-reported-model" and current["session_model_id"] is None
    assert current["prompt_version"] is None and current["session_config_version"] is None
    assert current["billing_basis"] == "SUBSCRIPTION" and current["cost_usd"] == "0"
    assert record == (
        {"session_id": "session", "model_id": None, "prompt_version": None}
        if reported_null
        else {"session_id": "session"}
    )


@pytest.mark.parametrize("phase", ["UNREPORTED", "EMPTY"])
async def test_actual_no_report_frame_progresses_without_unused_session_metadata(
    tmp_path: Path, completion_repo: StorageBackend, phase: Literal["UNREPORTED", "EMPTY"]
) -> None:
    repo = completion_repo
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "proposal_id": "proposal",
        "message_log": [],
        "active_message_id": None,
        "last_event_seq": 0,
        "agent_version_reason": "Synthetic retained session does not report its native agent version",
        "last_item_at_reason": "Retained proposal session has no source item timestamp",
        **collection_clock().fields("opened_at"),
    }
    await repo.put("ops.agent_sessions", "session", session)
    event: dict[str, JsonValue] = {
        "id": "span-no-report",
        "type": "span.model_request_end",
        **collection_clock().fields("processed_at"),
    }
    if phase == "EMPTY":
        event["model_usage"] = {}
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        first = await runtime.sessions.observe_native_event("session", event)
        duplicate = await runtime.sessions.observe_native_event("session", event)
        assert isinstance(first, NativeEventObserved) and isinstance(duplicate, NativeEventObserved)
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None and current["processed_events"] == ["span-no-report"]
    assert "model_id" not in current and "prompt_version" not in current
    assert "retry_count" not in current and current["message_log"] == []
    assert current["last_event_seq"] == 0
    assert await repo.list("ops.llm_usage") == [] and await repo.list("ops.jobs") == []


async def test_reported_usage_requires_one_actual_model_before_any_ledger_write(repo: StorageBackend) -> None:
    session = NativeUsageSession.from_record({"session_id": "session", "prompt_version": None})
    span = NativeUsageSpan.from_wire({"id": "span", "model_usage": {"input_tokens": 1}})
    with pytest.raises(ValueError, match="actual reported or recorded session model"):
        await record_native_usage(
            repo,
            session,
            span,
            versions=ResolvedVersionIdentity(
                config_version="synthetic-config", code_version="synthetic-code"
            ),
            collected_at=collection_clock(),
        )
    assert await repo.list("ops.llm_usage") == []
