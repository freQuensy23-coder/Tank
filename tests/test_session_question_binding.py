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
