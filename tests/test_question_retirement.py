"""Pre-fix reproduction: a successful native input without an ID is retired as unsent."""

from typing import Literal

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from session_followup_fixtures import UnexpectedFollowupRefusals
from test_session_outbound_intents import session_record

from tank.agents.outbound_inputs import AcceptedOutboundInput, parse_outbound_input
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", ["ACCEPTED", "UNREPORTED", "EMPTY"])
async def test_candidate_initial_acceptance_without_reported_id_is_mistaken_for_unsent(
    receipt: Literal["ACCEPTED", "UNREPORTED", "EMPTY"],
) -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", session_record())
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        posts.append(request.headers["idempotency-key"])
        if receipt == "ACCEPTED":
            return httpx.Response(200, json=acknowledge_native_input(request, "reported-input"))
        return httpx.Response(200, json={} if receipt == "UNREPORTED" else {"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        accepted = await service.post_message_intent("session", "message")
        journal = accepted["message_log"]
        assert isinstance(journal, list) and isinstance(journal[0], dict)
        phase = parse_outbound_input(journal[0])
        assert isinstance(phase, AcceptedOutboundInput) and phase.receipt.status == receipt
        retired = await service.abandon_question("session", "question")
        await service.close()
    assert posts == ["message:message"]
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None
    if receipt == "ACCEPTED":
        assert retired is False and stored == accepted
    else:
        assert retired is True
        rows = stored["message_log"]
        assert isinstance(rows, list) and isinstance(rows[0], dict)
        assert rows[0]["turn_status"] == "FAILED" and rows[0]["answer_recorded"] is True
        assert "message" in stored["retired_message_ids"]
        assert rows[0]["initial_input_acceptance"] == {"status": receipt}
