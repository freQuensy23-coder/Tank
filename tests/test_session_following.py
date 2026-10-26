"""Identity-bound native lifecycle observations never manufacture a running session."""

import asyncio

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from postgres_helpers import PostgresTemplate
from pydantic import ValidationError
from session_followup_fixtures import UnexpectedFollowupRefusals

from tank.agents.session_following import FollowerReady, FollowerUnavailable
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock


async def seed_follower(repo: MemoryRepository) -> None:
    await repo.put(
        "ops.agent_sessions",
        "session",
        {
            "session_id": "session",
            "status": "OPEN",
            "runtime_status": None,
            "message_log": [],
            "messages": [],
            "active_message_id": None,
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [("idle", "IDLE"), ("running", "RUNNING"), ("rescheduling", "PENDING"), ("terminated", "ENDED")],
)
async def test_follower_recovers_only_actual_reported_native_status(status: str, expected: str) -> None:
    repo = MemoryRepository()
    await seed_follower(repo)
    requests: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return httpx.Response(200, json={"id": "session", "status": status, "archived_at": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        result = await service.follower_state("session")
    assert isinstance(result, FollowerReady) and result.state.runtime_status == expected
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and "no_event" not in stored
    assert stored["status"] == ("ENDED" if status == "terminated" else "OPEN")
    assert requests == ["/v1/sessions/session"]


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable", ["missing", "archived"])
async def test_unavailable_follower_preserves_unknown_runtime_and_does_not_invent_termination(
    unavailable: str,
) -> None:
    repo = MemoryRepository()
    await seed_follower(repo)

    def handle(request: httpx.Request) -> httpx.Response:
        return (
            httpx.Response(404)
            if unavailable == "missing"
            else httpx.Response(
                200,
                json={
                    "id": "session",
                    "status": "idle",
                    "archived_at": "2026-10-26T00:00:00Z",
                },
            )
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        assert isinstance(await service.follower_state("session"), FollowerUnavailable)
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None and stored["status"] == "OPEN" and stored["runtime_status"] is None
    assert "termination_receipt" not in stored


@pytest.mark.asyncio
@pytest.mark.parametrize(("identity", "status"), [("other", "idle"), ("session", "unknown")])
async def test_invalid_follower_fact_refuses_before_local_state_changes(identity: str, status: str) -> None:
    repo = MemoryRepository()
    await seed_follower(repo)
    before = await repo.get("ops.agent_sessions", "session")

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": identity, "status": status, "archived_at": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        with pytest.raises((ValueError, ValidationError)):
            await service.follower_state("session")
    assert await repo.get("ops.agent_sessions", "session") == before


@pytest.mark.asyncio
async def test_missing_follower_cleans_owned_stream_and_preserves_retry_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = MemoryRepository()
    await seed_follower(repo)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def stream(session_id: str) -> None:
        assert session_id == "session"
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def handle(request: httpx.Request) -> httpx.Response:
        await started.wait()
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": [], "next_page": None})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        monkeypatch.setattr(service, "stream", stream)
        follower = asyncio.create_task(service.follow("session"))
        try:
            await asyncio.wait_for(cancelled.wait(), 1)
            assert not follower.done()
        finally:
            follower.cancel()
            await asyncio.gather(follower, return_exceptions=True)
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_later_local_event_state_wins_over_inflight_native_readback() -> None:
    repo = MemoryRepository()
    await seed_follower(repo)

    async def handle(request: httpx.Request) -> httpx.Response:
        current = await repo.get("ops.agent_sessions", "session")
        assert current is not None
        current["runtime_status"] = "RUNNING"
        await repo.put("ops.agent_sessions", "session", current)
        return httpx.Response(200, json={"id": "session", "status": "idle", "archived_at": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SessionService(
            repo,
            Settings(openma_url="http://native", _env_file=None),
            None,
            http=http,
            versions=declared_versions_fixture(),
            refusals=UnexpectedFollowupRefusals(),
        )
        result = await service.follower_state("session")
    assert isinstance(result, FollowerReady) and result.state.runtime_status == "RUNNING"
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None and "remote_state" not in current


def test_unreported_event_decision_is_not_an_observed_no_event_or_foreign_fact() -> None:
    from tank.agents.session_following import RecordedEventDecision

    assert (
        RecordedEventDecision.from_record(
            {"session_id": "session", "tool": "record_event"}, "session"
        ).is_event
        is None
    )
    assert (
        RecordedEventDecision.from_record(
            {"session_id": "session", "tool": "record_event", "result": {"is_event": False}}, "session"
        ).is_event
        is False
    )
    with pytest.raises(ValueError, match="different session"):
        RecordedEventDecision.from_record(
            {"session_id": "foreign", "tool": "record_event", "result": {"is_event": False}}, "session"
        )


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_missing_follower_records_observation_without_terminal_or_runtime_guess(
    postgres_template: PostgresTemplate,
) -> None:
    import os

    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.put(
            "ops.agent_sessions",
            "session",
            {
                "session_id": "session",
                "proposal_id": "synthetic-follower-proposal",
                "agent_version_reason": "The authored scout has no observed native agent version",
                "last_item_at_reason": "The authored scout has no source item",
                "last_event_seq": 0,
                **collection_clock().fields("opened_at"),
                "status": "OPEN",
                "runtime_status": None,
                "message_log": [],
                "messages": [],
                "active_message_id": None,
            },
        )

        def handle(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
            service = SessionService(
                repo,
                Settings(openma_url="http://native", _env_file=None),
                None,
                http=http,
                versions=declared_versions_fixture(),
                refusals=UnexpectedFollowupRefusals(),
            )
            result = await service.follower_state("session")
        assert isinstance(result, FollowerUnavailable)
        stored = await repo.get("ops.agent_sessions", "session")
        assert stored is not None and stored["status"] == "OPEN" and stored["runtime_status"] is None
        remote = stored["remote_state"]
        assert isinstance(remote, dict) and remote["kind"] == "MISSING" and remote["missing"] is True
        assert remote["checked_at"] is not None and "termination_receipt" not in stored
