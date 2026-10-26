"""Actual native acceptance cannot be retired as an unsent local question."""

import asyncio
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Literal, Self

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_input_fixtures import acknowledge_native_input
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from session_followup_fixtures import UnexpectedFollowupRefusals
from test_session_outbound_intents import session_record

from tank.agents.outbound_inputs import AcceptedOutboundInput, parse_outbound_input
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository, Repository
from tank.time_precision import collection_clock
from tank.util import JsonInput


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", ["ACCEPTED", "UNREPORTED", "EMPTY"])
async def test_initial_acceptance_without_reported_id_is_preserved_as_accepted(
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
    assert retired is False and stored == accepted


@pytest.mark.asyncio
async def test_retained_sent_at_without_initial_receipt_does_not_authorize_retirement() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", session_record())
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(request.headers["idempotency-key"])
        return httpx.Response(200, json={})

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
        journal[0].pop("initial_input_acceptance")
        await repo.put("ops.agent_sessions", "session", accepted)
        assert await service.abandon_question("session", "question") is False
        assert await repo.get("ops.agent_sessions", "session") == accepted
        await service.close()
    assert posts == ["message:message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", [None, "PENDING", "RUNNING"])
async def test_unproved_acceptance_does_not_become_an_unsent_intent(phase: str | None) -> None:
    from tank.agents.question_lifecycle import UnprovedQuestionRetirement

    repo = MemoryRepository()
    original = session_record()
    journal = original["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    if phase is None:
        journal[0].pop("turn_status")
    else:
        journal[0]["turn_status"] = phase
    await repo.put("ops.agent_sessions", "session", original)
    requests: list[httpx.Request] = []

    def refuse_http(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("Unknown acceptance cannot authorize native effects")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(UnprovedQuestionRetirement, match="INITIAL_ACCEPTANCE_NOT_RETAINED"):
            await service.abandon_question("session", "question")
        await service.close()
    assert await repo.get("ops.agent_sessions", "session") == original
    assert requests == [] and await repo.list("ops.jobs") == []


@pytest.mark.asyncio
async def test_cancelling_declared_pending_or_failed_intent_does_not_consume_unused_body_or_budget() -> None:
    repo = MemoryRepository()
    original = session_record()
    journal = original["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict)
    journal[0]["body"] = "Opaque retained body"
    journal[0]["retry_count"] = {"opaque": "unused budget"}
    await repo.put("ops.agent_sessions", "session", original)

    def refuse_http(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Local pending-intent retirement cannot post input")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        assert await service.abandon_question("session", "question") is True
        stored = await repo.get("ops.agent_sessions", "session")
        assert stored is not None
        rows = stored["message_log"]
        assert isinstance(rows, list) and isinstance(rows[0], dict)
        assert rows[0]["body"] == "Opaque retained body" and rows[0]["retry_count"] == {
            "opaque": "unused budget"
        }
        assert rows[0]["turn_status"] == "FAILED" and rows[0]["answer_recorded"] is True
        assert stored["active_message_id"] is None
        assert stored["retired_message_ids"] == ["message"]
        await service.close()


async def exercise_inflight_retirement(repo: Repository) -> None:
    original = session_record()
    original.update(
        proposal_id="synthetic-proposal",
        agent_version_reason="The authored session has no observed provider version",
        last_item_at_reason="The authored scout session has no source item",
        last_event_seq=0,
        **collection_clock().fields("opened_at"),
    )
    await repo.put("ops.agent_sessions", "session", original)
    posting = asyncio.Event()
    release = asyncio.Event()
    posts: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        posts.append(request.headers["idempotency-key"])
        posting.set()
        await release.wait()
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        recovering = asyncio.create_task(service.recover_outbound_inputs("session"))
        async with asyncio.timeout(1):
            await posting.wait()
        retiring = asyncio.create_task(service.abandon_question("session", "question"))
        try:
            await asyncio.sleep(0)
            assert not retiring.done()
            release.set()
            await recovering
            assert await retiring is False
        finally:
            release.set()
            await asyncio.gather(recovering, retiring, return_exceptions=True)
            await service.close()
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and "retired_message_ids" not in stored
    rows = stored["message_log"]
    assert isinstance(rows, list) and isinstance(rows[0], dict)
    assert rows[0]["turn_status"] == "PENDING" and rows[0]["initial_input_acceptance"] == {
        "status": "UNREPORTED"
    }
    assert posts == ["message:message"]


class AcceptanceCommitAckLoss:
    def __init__(self, store: Repository) -> None:
        self.store = store
        self.failed = False
        self.acceptance_written = False

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None:
        return await self.store.get(collection, key)

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        result = await self.store.put(collection, key, record, immutable=immutable)
        journal = record.get("message_log")
        if (
            collection == "ops.agent_sessions"
            and isinstance(journal, list)
            and any(isinstance(row, dict) and "initial_input_acceptance" in row for row in journal)
        ):
            self.acceptance_written = True
        return result

    @asynccontextmanager
    async def lock(self, name: str) -> AsyncIterator[Self]:
        async with self.store.lock(name):
            yield self
        if self.acceptance_written and not self.failed:
            self.failed = True
            raise RuntimeError("Synthetic committed acceptance acknowledgement lost")


async def exercise_committed_ack_loss(repo: Repository) -> None:
    original = session_record()
    original.update(
        proposal_id="synthetic-proposal",
        agent_version_reason="The authored session has no observed provider version",
        last_item_at_reason="The authored scout session has no source item",
        last_event_seq=0,
        **collection_clock().fields("opened_at"),
    )
    await repo.put("ops.agent_sessions", "session", original)
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(request.headers["idempotency-key"])
        return httpx.Response(200, json={"data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(
            AcceptanceCommitAckLoss(repo),
            Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises(RuntimeError, match="committed acceptance acknowledgement"):
            await service.recover_outbound_inputs("session")
        stored = await repo.get("ops.agent_sessions", "session")
        assert stored is not None
        assert await service.abandon_question("session", "question") is False
        await service.recover_outbound_inputs("session")
        assert await repo.get("ops.agent_sessions", "session") == stored
        await service.close()
    assert posts == ["message:message"]


@pytest.mark.asyncio
async def test_retirement_waits_for_the_actual_recovery_post_and_acceptance_commit() -> None:
    await exercise_inflight_retirement(MemoryRepository())


@pytest.mark.asyncio
async def test_lost_acceptance_commit_ack_does_not_retire_or_repost_the_accepted_question() -> None:
    await exercise_committed_ack_loss(MemoryRepository())


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["INFLIGHT", "COMMITTED_ACK_LOSS"])
async def test_actual_postgres_retirement_preserves_native_acceptance_through_owned_post_races(
    postgres_template: PostgresTemplate, mode: Literal["INFLIGHT", "COMMITTED_ACK_LOSS"]
) -> None:
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.migrate()
        if mode == "INFLIGHT":
            await exercise_inflight_retirement(repo)
        else:
            await exercise_committed_ack_loss(repo)


@pytest.mark.asyncio
async def test_accepted_question_retirement_does_not_require_unused_historical_body_or_budget() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", session_record())

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

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
        journal[0]["body"] = "Opaque retained body"
        journal[0]["retry_count"] = {"unknown": "unused prior budget"}
        await repo.put("ops.agent_sessions", "session", accepted)
        assert await service.abandon_question("session", "question") is False
        assert await repo.get("ops.agent_sessions", "session") == accepted
        await service.close()


@pytest.mark.asyncio
async def test_unbound_queued_question_cannot_invent_a_session_or_block_a_bound_answer() -> None:
    from test_session_answer_settlement import answer_history, question_session

    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", question_session())
    await repo.put(
        "out.user_questions",
        "question",
        {"question_id": "question", "session_id": "session", "status": "QUEUED"},
    )
    unbound: dict[str, JsonValue] = {"question_id": "unbound", "session_id": None, "status": "QUEUED"}
    await repo.put("out.user_questions", "unbound", unbound)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET" and request.url.path == "/v1/sessions/session/events"
        return httpx.Response(
            200, json={"data": answer_history("Actual bound answer")[::-1], "next_page": None}
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
        await service.reconcile_answers()
        await service.close()
    answered = await repo.get("out.user_questions", "question")
    assert answered is not None and answered["answer_text"] == "Actual bound answer"
    assert await repo.get("out.user_questions", "unbound") == unbound
    assert len(requests) == 1 and len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1
