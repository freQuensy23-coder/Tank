"""Message results preserve locally pending and observed acceptance phases."""

import pytest
from pydantic import JsonValue
from test_storage import repo as repo

from tank.agents.message_dispatch import (
    AcceptedMessageDispatch,
    IndexedMessageDispatch,
    PendingMessageDispatch,
    observe_message_dispatch,
)
from tank.storage import StorageBackend


def message_record() -> dict[str, JsonValue]:
    return {
        "message_id": "message",
        "retry_count": 0,
        "turn_status": "ACCEPTING",
        "body": {"type": "user.message", "content": [{"type": "text", "text": "Question"}]},
        "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "question"},
    }


def test_local_intent_is_pending_and_index_only_is_explicitly_unreported() -> None:
    journal: list[JsonValue] = [message_record()]
    pending = observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")
    assert isinstance(pending, PendingMessageDispatch) and pending.intent.attempt == 0
    index: list[JsonValue] = ["message"]
    indexed = observe_message_dispatch({"session_id": "session", "messages": index}, "message")
    assert isinstance(indexed, IndexedMessageDispatch)
    assert indexed.phase == "INDEXED_UNREPORTED"


@pytest.mark.parametrize("status", ["UNREPORTED", "EMPTY"])
def test_explicit_acceptance_does_not_require_unused_body_or_historical_budget(status: str) -> None:
    journal: list[JsonValue] = [
        {
            "message_id": "message",
            "initial_input_acceptance": {"status": status},
            "body": "Unconsumed historical text",
            "remote_event_id": None,
        }
    ]
    result = observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")
    assert isinstance(result, AcceptedMessageDispatch)
    assert result.acceptance.receipt.status == status and result.acceptance.sent_at is None


def test_unproved_or_contradictory_acceptance_is_not_reported_as_dispatched() -> None:
    with pytest.raises(ValueError, match="no actual"):
        observe_message_dispatch({"session_id": "session"}, "message")
    row = message_record()
    del row["retry_count"]
    journal: list[JsonValue] = [row]
    with pytest.raises(KeyError, match="retry_count"):
        observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")
    row.update(initial_input_acceptance={"status": "ACCEPTED", "event_id": "native"}, remote_event_id="other")
    with pytest.raises(ValueError, match="contradicts"):
        observe_message_dispatch({"session_id": "session", "message_log": journal}, "message")


def test_admitted_message_preserves_original_wire_digest_and_declared_presence() -> None:
    from tank.agents.message_dispatch import MessageDispatchRequest
    from tank.util import sha256

    payload: dict[str, JsonValue] = {
        "kind": "NEW_ITEM",
        "item_id": "item",
        "event_id": None,
    }
    request = MessageDispatchRequest.from_wire(payload)
    assert request.message_id == sha256(payload)
    assert request.original_wire == payload
    assert "session_id" not in request.original_wire
    assert request.message.kind == "NEW_ITEM"


def test_mutated_admitted_message_cannot_change_original_owner_or_identity() -> None:
    from pydantic import ValidationError

    from tank.agents.message_dispatch import MessageDispatchRequest
    from tank.runtime_contracts import UserQuestionMessage

    request = MessageDispatchRequest.from_wire(
        {
            "kind": "USER_QUESTION",
            "session_id": "session",
            "question_id": "question",
        }
    )
    changed = request.model_copy(
        update={
            "message": UserQuestionMessage(
                kind="USER_QUESTION",
                session_id="other",
                question_id="question",
            )
        }
    )
    with pytest.raises(ValidationError, match="command differs"):
        MessageDispatchRequest.model_validate(changed, by_name=True)
    request.original_wire["question_id"] = "other"
    with pytest.raises(ValidationError, match="identity differs"):
        MessageDispatchRequest.model_validate(request, by_name=True)


@pytest.mark.parametrize("explicit_null", [False, True])
@pytest.mark.parametrize("acceptance", ["UNREPORTED", "EMPTY", "ACCEPTED"])
async def test_actual_message_dispatch_preserves_wire_key_and_confirmed_phase_on_retry(
    repo: StorageBackend, explicit_null: bool, acceptance: str
) -> None:
    import httpx
    from agent_source_fixtures import declared_versions_fixture, source_record_fixture
    from native_input_fixtures import acknowledge_native_input
    from session_followup_fixtures import UnexpectedFollowupRefusals

    from tank.agents.context import CONTEXT_VERSION
    from tank.agents.message_dispatch import MessageDispatchRequest
    from tank.agents.sessions import SessionService
    from tank.settings import Settings
    from tank.time_precision import collection_clock
    from tank.util import sha256

    opened = collection_clock()
    await repo.put(
        "ingest.source_items",
        "item",
        source_record_fixture(
            {
                "item_id": "item",
                "text": "Authored input for original wire identity proof",
                "published_at": "2026-10-25T00:00:00Z",
                "published_prec": "EXACT",
            }
        ),
    )
    await repo.put(
        "ops.agent_sessions",
        "session",
        {
            "session_id": "session",
            "status": "OPEN",
            "runtime_status": "RUNNING",
            "model_context_version": CONTEXT_VERSION,
            "agent_version": "synthetic-version-1",
            "model_id": "synthetic-model",
            "prompt_version": "a" * 64,
            "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
            "opened_by_item": "item",
            **opened.fields("opened_at"),
            "replay_run_id": "run",
            "event_id": None,
            "last_event_seq": 0,
            "last_item_at_reason": "No input has been accepted in this authored session",
            "item_id": "item",
            "messages": [],
            "message_log": [],
        },
    )
    wire: dict[str, JsonValue] = {
        "kind": "NEW_ITEM",
        "item_id": "item",
        "event_id": None,
        "session_id": "session",
        "replay_run_id": "run",
        "replay_at": "2026-10-26T00:00:00Z",
    }
    if explicit_null:
        wire["replay_at_prec"] = None
    request = MessageDispatchRequest.from_wire(wire)
    posts: list[httpx.Request] = []

    def handle(native: httpx.Request) -> httpx.Response:
        assert native.method == "POST" and native.url.path == "/v1/sessions/session/events"
        posts.append(native)
        if acceptance == "ACCEPTED":
            return httpx.Response(200, json=acknowledge_native_input(native, "input"))
        return httpx.Response(200, json={} if acceptance == "UNREPORTED" else {"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, replay=True, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        try:
            first = await service.dispatch_message(request)
            await service.stop_followers()
            second = await service.dispatch_message(
                MessageDispatchRequest.model_validate_json(
                    request.model_dump_json(exclude_unset=True, by_alias=True)
                )
            )
            await service.stop_followers()
        finally:
            await service.close()
    assert isinstance(first, AcceptedMessageDispatch) and isinstance(second, AcceptedMessageDispatch)
    assert first.acceptance.receipt.status == second.acceptance.receipt.status == acceptance
    assert len(posts) == 1
    assert posts[0].headers["Idempotency-Key"] == "message:" + sha256(wire)
    assert first.message_id == second.message_id == sha256(wire)
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None
    journal = stored["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    assert journal[0]["payload"] == wire
    assert ("replay_at_prec" in wire) is explicit_null
