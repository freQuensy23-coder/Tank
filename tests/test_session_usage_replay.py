"""Production 2026-11-11: an event whose usage was recorded but which was never marked processed."""

import asyncio

import httpx
import pytest
from test_agents import seed_session, settings

from tank.agents import SessionService
from tank.storage import MemoryRepository


class InterruptedRepository(MemoryRepository):
    """The next write of the session record fails, as when the follower is cut off mid-event."""

    def __init__(self):
        super().__init__()
        self.interrupt = False

    async def put(self, collection, key, record, **kwargs):
        if self.interrupt and collection == "ops.agent_sessions":
            self.interrupt = False
            raise RuntimeError("connection lost")
        return await super().put(collection, key, record, **kwargs)


def usage_event(event_id, at):
    return {
        "id": event_id,
        "type": "span.model_request_end",
        "processed_at": at,
        "model_usage": {"input_tokens": 10, "output_tokens": 2},
    }


HISTORY = [
    usage_event("oldest", "2026-11-11T15:32:09.993Z"),
    usage_event("stuck", "2026-11-11T15:32:33.244Z"),
    {"id": "later", "type": "session.status_terminated", "processed_at": "2026-11-11T18:21:40.551Z"},
]


def history_transport(requests=None):
    def handler(request):
        if requests is not None:
            requests.append(request)
        if request.url.path.endswith("/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, text="")
        return httpx.Response(200, json={"data": HISTORY, "next_page": None})

    return httpx.MockTransport(handler)


async def interrupt_stuck_event(repo, service):
    await service.process_event("ses_1", HISTORY[0])
    repo.interrupt = True
    with pytest.raises(RuntimeError):
        await service.process_event("ses_1", HISTORY[1])
    session = await repo.get("ops.agent_sessions", "ses_1")
    assert session["processed_events"] == ["oldest"]
    return await repo.get("ops.llm_usage", "ses_1:stuck")


@pytest.mark.asyncio
async def test_catch_up_reprocesses_event_whose_usage_is_already_recorded():
    repo = InterruptedRepository()
    opts, _ = settings()
    await seed_session(repo)
    async with httpx.AsyncClient(transport=history_transport()) as http:
        service = SessionService(repo, opts, None, http=http)
        recorded = await interrupt_stuck_event(repo, service)
        assert recorded
        service.seen_events.clear()
        await service.catch_up("ses_1")
    session = await repo.get("ops.agent_sessions", "ses_1")
    assert session["processed_events"] == ["oldest", "stuck", "later"]
    assert session["status"] == "ENDED"
    assert await repo.get("ops.llm_usage", "ses_1:stuck") == recorded
    assert len(await repo.list("ops.llm_usage")) == 2


@pytest.mark.asyncio
async def test_follower_recovers_a_session_stuck_behind_a_recorded_usage():
    repo = InterruptedRepository()
    opts, _ = settings()
    await seed_session(repo, runtime_status="RUNNING")
    async with httpx.AsyncClient(transport=history_transport()) as http:
        service = SessionService(repo, opts, None, http=http)
        await interrupt_stuck_event(repo, service)
        service.seen_events.clear()
        await asyncio.wait_for(service.follow_owned("ses_1"), timeout=5)
    session = await repo.get("ops.agent_sessions", "ses_1")
    assert session["status"] == "ENDED"
    assert session["processed_events"] == ["oldest", "stuck", "later"]
