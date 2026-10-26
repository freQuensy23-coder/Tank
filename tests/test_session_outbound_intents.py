"""Initial input acceptance is phase-specific and cannot overwrite a changed intent."""

import copy

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from pydantic import JsonValue, ValidationError
from session_followup_fixtures import UnexpectedFollowupRefusals

from tank.agents.message_dispatch import AcceptedMessageDispatch, observe_message_dispatch
from tank.agents.native_inputs import NativeUserMessage, UnreportedNativeInput
from tank.agents.outbound_inputs import (
    AcceptedOutboundInput,
    LegacyUnreportedAcceptance,
    PendingOutboundInput,
    parse_outbound_input,
)
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock


def pending_record() -> dict[str, JsonValue]:
    return {
        "message_id": "message",
        "body": {"type": "user.message", "content": [{"type": "text", "text": "Declared initial input"}]},
        "payload": {"kind": "USER_QUESTION", "question_id": "question", "session_id": "session"},
        "turn_status": "ACCEPTING",
        "retry_count": 0,
        "remote_event_id": None,
    }


def session_record() -> dict[str, JsonValue]:
    return {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "PENDING",
        "active_message_id": "message",
        "message_log": [pending_record()],
        "messages": [],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", ["ACCEPTED", "UNREPORTED", "EMPTY"])
async def test_actual_initial_acceptance_preserves_reported_or_unreported_receipt_once(receipt: str) -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", session_record())
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        response: dict[str, JsonValue]
        if receipt == "ACCEPTED":
            response = acknowledge_native_input(request, "native-input")
        else:
            response = {} if receipt == "UNREPORTED" else {"data": []}
        return httpx.Response(200, json=response)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        await service.post_message_intent("session", "message")
        again = await service.post_message_intent("session", "message")
    outcome = observe_message_dispatch(again, "message")
    assert isinstance(outcome, AcceptedMessageDispatch) and outcome.acceptance.receipt.status == receipt
    journal = again["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    intent = journal[0]
    parsed = parse_outbound_input(intent)
    assert isinstance(parsed, AcceptedOutboundInput)
    assert parsed.receipt.status == receipt
    assert parsed.sent_at is not None and parsed.sent_at.value is not None
    assert len(requests) == 1
    assert intent["initial_input_acceptance"] == (
        {"status": "ACCEPTED", "event_id": "native-input"} if receipt == "ACCEPTED" else {"status": receipt}
    )
    assert intent["remote_event_id"] == ("native-input" if receipt == "ACCEPTED" else None)


@pytest.mark.asyncio
async def test_changed_durable_body_refuses_acceptance_commit_after_http_response() -> None:
    repo = MemoryRepository()
    original = session_record()
    await repo.put("ops.agent_sessions", "session", original)
    changed = copy.deepcopy(original)
    journal = changed["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    journal[0]["body"] = {"type": "user.message", "content": [{"type": "text", "text": "Newer intent"}]}

    async def handler(request: httpx.Request) -> httpx.Response:
        await repo.put("ops.agent_sessions", "session", changed)
        return httpx.Response(200, json=acknowledge_native_input(request, "native-input"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(ValueError, match="intent changed"):
            await service.post_message_intent("session", "message")
    assert await repo.get("ops.agent_sessions", "session") == changed


def test_retained_retry_identity_does_not_fabricate_the_initial_receipt() -> None:
    record = pending_record()
    assert isinstance(parse_outbound_input(record), PendingOutboundInput)
    record.update(remote_event_id="retry-input", retry_count=1, turn_status="RUNNING")
    parsed = parse_outbound_input(record)
    assert isinstance(parsed, AcceptedOutboundInput)
    assert isinstance(parsed.receipt, LegacyUnreportedAcceptance)
    assert parsed.event_id is None and parsed.sent_at is None


@pytest.mark.asyncio
async def test_new_service_recovers_lost_initial_response_with_same_durable_input_and_key() -> None:
    repo = MemoryRepository()
    original = session_record()
    await repo.put("ops.agent_sessions", "session", original)
    calls: list[tuple[str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST" and request.url.path == "/v1/sessions/session/events"
        calls.append((request.headers["idempotency-key"], request.content))
        if len(calls) == 1:
            raise httpx.ReadError("Synthetic lost acceptance response", request=request)
        return httpx.Response(200, json=acknowledge_native_input(request, "same-native-input"))

    configuration = Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        first = SessionService(
            repo,
            configuration,
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(httpx.ReadError):
            await first.post_message_intent("session", "message")
        assert await repo.get("ops.agent_sessions", "session") == original
        assert not first.tasks
        recovered = SessionService(
            repo,
            configuration,
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        await recovered.recover_outbound_inputs("session")
        assert not recovered.tasks
    assert len(calls) == 2 and calls[0] == calls[1]
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["messages"] == ["message"]
    journal = stored["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    phase = parse_outbound_input(journal[0])
    assert isinstance(phase, AcceptedOutboundInput) and phase.event_id == "same-native-input"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("retry_count", True),
        ("retry_count", 0.0),
        ("remote_event_id", True),
        ("initial_input_acceptance", None),
    ],
)
async def test_invalid_retained_intent_refuses_before_any_native_post(field: str, value: JsonValue) -> None:
    repo = MemoryRepository()
    original = session_record()
    journal = original["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    journal[0][field] = value
    await repo.put("ops.agent_sessions", "session", original)
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        raise AssertionError("Invalid intent must fail before native input posting")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(ValueError):
            await service.post_message_intent("session", "message")
    assert requests == []
    assert await repo.get("ops.agent_sessions", "session") == original


@pytest.mark.asyncio
@pytest.mark.parametrize("already_reported", [False, True])
async def test_concurrent_receipts_preserve_or_refine_actual_acknowledgment_without_losing_lineage(
    already_reported: bool,
) -> None:
    repo = MemoryRepository()
    original = session_record()
    await repo.put("ops.agent_sessions", "session", original)
    concurrent = copy.deepcopy(original)
    journal = concurrent["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    intent = journal[0]
    accepted_at = collection_clock()
    intent.update(
        **accepted_at.fields("sent_at"),
        remote_event_id="native-input" if already_reported else None,
        turn_status="PENDING",
        initial_input_acceptance=(
            {"status": "ACCEPTED", "event_id": "native-input"}
            if already_reported
            else {"status": "UNREPORTED"}
        ),
    )
    concurrent["messages"] = ["message"]

    async def handler(request: httpx.Request) -> httpx.Response:
        await repo.put("ops.agent_sessions", "session", concurrent)
        return httpx.Response(
            200, json={} if already_reported else acknowledge_native_input(request, "native-input")
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        result = await service.post_message_intent("session", "message")
    result_journal = result["message_log"]
    assert isinstance(result_journal, list) and isinstance(result_journal[0], dict)
    phase = parse_outbound_input(result_journal[0])
    assert isinstance(phase, AcceptedOutboundInput) and phase.event_id == "native-input"
    assert phase.sent_at == accepted_at
    assert result["messages"] == ["message"]
    if not already_reported:
        assert result_journal[0]["native_input_attempts"] == {"native-input": 0}


@pytest.mark.asyncio
async def test_outbound_row_identity_must_match_native_receiver_before_posting() -> None:
    repo = MemoryRepository()
    malformed = {**session_record(), "session_id": "different-session"}
    await repo.put("ops.agent_sessions", "session", malformed)
    calls: list[str] = []

    def forbidden(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise AssertionError("A different stored owner must not receive native input")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(ValueError, match="identity differs"):
            await service.post_message_intent("session", "message")
    assert calls == []
    assert await repo.get("ops.agent_sessions", "session") == malformed


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["", "session/with?delimiter"])
async def test_native_input_receiver_is_required_and_encoded_as_one_path_segment(identity: str) -> None:
    paths: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.raw_path)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            MemoryRepository(),
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        body = NativeUserMessage(type="user.message", content=[])
        if identity:
            observed = await service.post_input(identity, body, idem="message:declared-intent")
            assert observed == UnreportedNativeInput(status="UNREPORTED")
        else:
            with pytest.raises(ValidationError):
                await service.post_input(identity, body, idem="message:declared-intent")
    assert paths == ([b"/v1/sessions/session%2Fwith%3Fdelimiter/events"] if identity else [])


@pytest.mark.asyncio
async def test_recovery_refuses_unmatched_active_owner_before_posting_an_otherwise_valid_intent() -> None:
    repo = MemoryRepository()
    malformed = {**session_record(), "active_message_id": "missing-owner"}
    await repo.put("ops.agent_sessions", "session", malformed)
    calls: list[str] = []

    def forbidden(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        raise AssertionError("An inconsistent active pointer cannot authorize recovery")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(ValueError, match="authoritative journal owner"):
            await service.recover_outbound_inputs("session")
        with pytest.raises(ValueError, match="authoritative journal owner"):
            await service.post_message_intent("session", "message")
    assert calls == []
    assert await repo.get("ops.agent_sessions", "session") == malformed
