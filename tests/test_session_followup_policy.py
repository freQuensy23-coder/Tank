"""Actual native preflight keeps lifecycle policy separate from refusal presentation."""

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from native_session_fixtures import native_session_record
from pydantic import JsonValue

from tank.agents.sessions import SessionService
from tank.contracts.session_followups import (
    FollowupClosure,
    FollowupRefusalTarget,
    NoSession,
    Terminated,
)
from tank.runtime_contracts import UserQuestionMessage
from tank.settings import Settings
from tank.storage import MemoryRepository


class RecordingRefusals:
    def __init__(self) -> None:
        self.calls: list[tuple[FollowupRefusalTarget, FollowupClosure]] = []
        self.fail_next = False

    async def record(self, target: FollowupRefusalTarget, closure: FollowupClosure) -> None:
        self.calls.append((target, closure))
        if self.fail_next:
            self.fail_next = False
            raise OSError("Synthetic refusal persistence unavailable")


def question() -> UserQuestionMessage:
    return UserQuestionMessage(kind="USER_QUESTION", session_id="session", question_id="question")


def options() -> Settings:
    return Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None)


@pytest.mark.asyncio
async def test_retained_missing_refuses_actual_question_without_native_or_input_effects() -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "remote_state": {"kind": "MISSING", "session_id": "session", "missing": True},
        "unrelated": {"retained": [False, 0, "raw"]},
    }
    await repo.put("ops.agent_sessions", "session", original)
    refusals = RecordingRefusals()

    def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Retained missing resource must not cause native I/O")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        service = SessionService(
            repo, options(), None, versions=declared_versions_fixture(), refusals=refusals, http=http
        )
        assert not await service.continuable(question())
    assert refusals.calls == [
        (
            FollowupRefusalTarget(question_id="question", session_id="session"),
            NoSession(status="NO_SESSION", reason="NATIVE_MISSING"),
        )
    ]
    assert await repo.get("ops.agent_sessions", "session") == original


@pytest.mark.asyncio
async def test_unknown_retained_facts_require_actual_native_preflight_before_continuation() -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {
        "session_id": "session",
        "termination_receipt": {},
        "remote_state": {},
        "unrelated": {"unknown": None},
    }
    await repo.put("ops.agent_sessions", "session", original)
    requests: list[httpx.Request] = []
    refusals = RecordingRefusals()

    def lookup(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET" and request.url.path == "/v1/sessions/session"
        return httpx.Response(
            200,
            json=native_session_record(
                "session",
                agent_id="agent",
                environment_id="environment",
                metadata={},
                model_id="gpt-6.1-sol",
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(lookup)) as http:
        service = SessionService(
            repo, options(), None, versions=declared_versions_fixture(), refusals=refusals, http=http
        )
        assert await service.continuable(question())
    assert len(requests) == 1 and refusals.calls == []
    assert await repo.get("ops.agent_sessions", "session") == original


@pytest.mark.asyncio
async def test_new_missing_observation_never_fabricates_termination_and_reuses_retained_fact() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", {"session_id": "session", "status": "OPEN"})
    requests: list[httpx.Request] = []
    refusals = RecordingRefusals()

    def missing(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(missing)) as http:
        service = SessionService(
            repo, options(), None, versions=declared_versions_fixture(), refusals=refusals, http=http
        )
        assert not await service.continuable(question())
        assert not await service.continuable(question())
    assert len(requests) == 1
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["status"] == "OPEN" and "termination_receipt" not in stored
    assert all(
        closure == NoSession(status="NO_SESSION", reason="NATIVE_MISSING") for _, closure in refusals.calls
    )


@pytest.mark.asyncio
async def test_termination_recorded_during_missing_readback_wins_after_actual_locked_recheck() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", {"session_id": "session", "status": "OPEN"})
    refusals = RecordingRefusals()

    async def racing_lookup(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        async with repo.lock("session:session"):
            await repo.put(
                "ops.agent_sessions",
                "session",
                {
                    "session_id": "session",
                    "status": "ENDED",
                    "termination_receipt": {"event_id": "termination"},
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(racing_lookup)) as http:
        service = SessionService(
            repo, options(), None, versions=declared_versions_fixture(), refusals=refusals, http=http
        )
        assert not await service.continuable(question())
    assert refusals.calls[0][1] == Terminated(status="TERMINATED", reason="TERMINATED")
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["termination_receipt"] == {"event_id": "termination"}


@pytest.mark.asyncio
async def test_refusal_persistence_failure_never_posts_and_can_reconcile_retained_missing_fact() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", {"session_id": "session", "status": "OPEN"})
    requests: list[httpx.Request] = []
    refusals = RecordingRefusals()
    refusals.fail_next = True

    def missing(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(missing)) as http:
        service = SessionService(
            repo, options(), None, versions=declared_versions_fixture(), refusals=refusals, http=http
        )
        with pytest.raises(OSError, match="refusal persistence"):
            await service.continuable(question())
        assert not await service.continuable(question())
    assert len(requests) == 1 and len(refusals.calls) == 2
    assert refusals.calls[0] == refusals.calls[1]


@pytest.mark.asyncio
async def test_foreign_persisted_session_cannot_authorize_question_refusal() -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {"session_id": "foreign", "status": "ENDED"}
    await repo.put("ops.agent_sessions", "session", original)
    refusals = RecordingRefusals()

    def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Foreign persistence identity cannot authorize native I/O")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        service = SessionService(
            repo, options(), None, versions=declared_versions_fixture(), refusals=refusals, http=http
        )
        with pytest.raises(ValueError, match="identity differs"):
            await service.continuable(question())
    assert refusals.calls == []
    assert await repo.get("ops.agent_sessions", "session") == original
