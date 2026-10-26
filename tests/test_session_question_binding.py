"""Native input must not borrow a different session's stored question."""

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_session_fixtures import native_session_record
from pydantic import JsonValue
from session_followup_fixtures import UnexpectedFollowupRefusals
from test_agents_session_audit import seed, settings

from tank.agents.sessions import SessionService
from tank.contracts.json import decode_unique_json
from tank.storage import MemoryRepository
from tank.util import canonical_json


@pytest.mark.asyncio
@pytest.mark.parametrize("question_session", ["s", "foreign"])
async def test_question_scope_is_checked_before_native_reads_or_input_effects(
    question_session: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = MemoryRepository()
    await seed(repo, runtime_status="IDLE")
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    await repo.put(
        "out.user_questions",
        "question",
        {
            "question_id": "question",
            "session_id": question_session,
            "status": "QUEUED",
            "text": "SYNTHETIC_PRIVATE_QUESTION",
            "reference": {"kind": "CARD", "text": "Reference"},
            "attachments": [],
        },
    )
    before = await repo.get("ops.agent_sessions", "s")
    requests: list[str] = []
    inputs: list[JsonValue] = []
    followers: list[str] = []

    def follow(session_id: str) -> None:
        followers.append(session_id)

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request.method + " " + request.url.path)
        if request.method == "GET":
            return httpx.Response(
                200,
                json=native_session_record(
                    "s",
                    agent_id="agent",
                    environment_id="live",
                    metadata={},
                    model_id="gpt-6.1-sol",
                ),
            )
        assert request.method == "POST" and request.url.path == "/v1/sessions/s/events"
        inputs.append(decode_unique_json(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            settings(),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        monkeypatch.setattr(service, "start_stream", follow)
        payload: dict[str, JsonValue] = {
            "kind": "USER_QUESTION",
            "session_id": "s",
            "question_id": "question",
        }
        if question_session == "foreign":
            try:
                await service.send_message(payload)
            except ValueError as error:
                assert "another session" in str(error)
            else:
                pytest.fail(
                    f"Foreign question crossed native boundary: requests={requests}, private_text_posted={'SYNTHETIC_PRIVATE_QUESTION' in canonical_json(inputs)}"
                )
            assert requests == [] and inputs == [] and followers == []
            assert await repo.get("ops.agent_sessions", "s") == before
        else:
            await service.send_message(payload)
            await service.send_message(payload)
            assert len(inputs) == 1 and "SYNTHETIC_PRIVATE_QUESTION" in canonical_json(inputs)
            assert followers == ["s", "s"]
            stored = await repo.get("ops.agent_sessions", "s")
            assert stored is not None
            journal = stored["message_log"]
            assert isinstance(journal, list) and len(journal) == 1


@pytest.mark.asyncio
async def test_retained_pending_question_reposts_original_owned_body_without_unused_question_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tank.agents.context import CONTEXT_VERSION
    from tank.util import sha256

    repo = MemoryRepository()
    payload: dict[str, JsonValue] = {"kind": "USER_QUESTION", "session_id": "s", "question_id": "gone"}
    key = sha256(payload)
    journal: list[JsonValue] = [
        {
            "message_id": key,
            "payload": payload,
            "turn_status": "ACCEPTING",
            "retry_count": 0,
            "model_context_version": CONTEXT_VERSION,
            "body": {"type": "user.message", "content": [{"type": "text", "text": "Retained original body"}]},
        }
    ]
    await seed(repo, runtime_status="PENDING", active_message_id=key, message_log=journal)
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    inputs: list[JsonValue] = []
    keys: list[str] = []
    followers: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json=native_session_record(
                    "s",
                    agent_id="agent",
                    environment_id="live",
                    metadata={},
                    model_id="gpt-6.1-sol",
                ),
            )
        inputs.append(decode_unique_json(request.content))
        keys.append(request.headers["idempotency-key"])
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            settings(),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        monkeypatch.setattr(service, "start_stream", followers.append)
        await service.send_message(payload)
    assert keys == ["message:" + key] and len(inputs) == 1
    assert "Retained original body" in canonical_json(inputs)
    assert await repo.get("out.user_questions", "gone") is None


@pytest.mark.asyncio
async def test_known_acceptance_does_not_reconstruct_unused_question_body_or_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tank.util import sha256

    repo = MemoryRepository()
    payload: dict[str, JsonValue] = {"kind": "USER_QUESTION", "session_id": "s", "question_id": "gone"}
    key = sha256(payload)
    journal: list[JsonValue] = [
        {
            "message_id": key,
            "body": "Opaque historical body",
            "initial_input_acceptance": {"status": "UNREPORTED"},
        }
    ]
    messages: list[JsonValue] = [key]
    await seed(repo, runtime_status="IDLE", active_message_id=None, message_log=journal, messages=messages)
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    methods: list[str] = []
    followers: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        assert request.method == "GET"
        return httpx.Response(
            200,
            json=native_session_record(
                "s",
                agent_id="agent",
                environment_id="live",
                metadata={},
                model_id="gpt-6.1-sol",
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            settings(),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        monkeypatch.setattr(service, "start_stream", followers.append)
        result = await service.send_message(payload)
    assert result is not None and result["message_log"] == journal
    assert methods == ["GET"] and followers == ["s"]
    assert await repo.get("out.user_questions", "gone") is None


@pytest.mark.asyncio
async def test_question_binding_is_rechecked_after_native_preflight_before_input_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = MemoryRepository()
    await seed(repo, runtime_status="IDLE")
    await repo.put("evt.events", "e", {"event_id": "e", "lifecycle": "TRACKED"})
    await repo.put(
        "out.user_questions",
        "question",
        {"question_id": "question", "session_id": "s", "text": "Owned text", "attachments": []},
    )
    methods: list[str] = []
    followers: list[str] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        assert request.method == "GET"
        async with repo.lock("question:question"):
            question = await repo.get("out.user_questions", "question")
            assert question is not None
            question["session_id"] = "foreign"
            await repo.put("out.user_questions", "question", question)
        return httpx.Response(
            200,
            json=native_session_record(
                "s",
                agent_id="agent",
                environment_id="live",
                metadata={},
                model_id="gpt-6.1-sol",
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            settings(),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        monkeypatch.setattr(service, "start_stream", followers.append)
        with pytest.raises(ValueError, match="another session"):
            await service.send_message(
                {"kind": "USER_QUESTION", "session_id": "s", "question_id": "question"}
            )
    assert methods == ["GET"] and followers == []
    current = await repo.get("ops.agent_sessions", "s")
    assert current is not None and current["message_log"] == []
