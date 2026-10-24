"""Stream cost (live profile 2026-10-20): inert events cost no session read or write."""

import httpx
import pytest
from test_agents import seed_session, settings

from tank.agents import SessionService
from tank.agents.sessions import material_event
from tank.storage import MemoryRepository


class CountingRepository(MemoryRepository):
    def __init__(self) -> None:
        super().__init__()
        self.session_gets = self.session_puts = 0

    async def get(self, collection, key):
        if collection == "ops.agent_sessions":
            self.session_gets += 1
        return await super().get(collection, key)

    async def put(self, collection, key, record, **kwargs):
        if collection == "ops.agent_sessions":
            self.session_puts += 1
        return await super().put(collection, key, record, **kwargs)


def test_material_event_classification() -> None:
    assert not material_event({"type": "agent.message", "content": [{"type": "text", "text": "hi"}]})
    assert not material_event({"type": "agent.thinking"})
    assert not material_event({"type": "agent.tool_use", "name": "bash"})
    assert material_event({"type": "agent.message", "content": [{"type": "refusal"}]})
    assert material_event({"type": "agent.message", "finish_reason": "content-filter"})
    for kind in ("agent.custom_tool_use", "session.status_idle", "session.error", "span.model_request_end"):
        assert material_event({"type": kind})


@pytest.mark.asyncio
async def test_inert_events_skip_session_io_and_cursor_advances_with_next_material_write() -> None:
    repo = CountingRepository()
    opts, _ = settings()
    await seed_session(repo)
    repo.session_gets = repo.session_puts = 0
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    ) as http:
        service = SessionService(repo, opts, None, http=http)
        for seq in range(1, 51):
            await service.process_event(
                "ses_1", {"id": f"m{seq}", "type": "agent.message", "content": []}, seq
            )
        assert (repo.session_gets, repo.session_puts) == (0, 0)
        await service.process_event("ses_1", {"id": "run", "type": "session.status_running"}, 51)
        session = await repo.get("ops.agent_sessions", "ses_1")
        assert session["last_event_seq"] == 51
        assert session["processed_events"] == ["run"]
        puts = repo.session_puts

        await service.process_event("ses_1", {"id": "run", "type": "session.status_running"}, 51)
        assert repo.session_puts == puts

        await service.process_event("ses_1", {"id": "m52", "type": "agent.message", "content": []}, 52)
        await service.process_event("ses_1", {"id": "idle", "type": "session.status_idle"}, 53)
        assert (await repo.get("ops.agent_sessions", "ses_1"))["last_event_seq"] == 53


@pytest.mark.asyncio
async def test_history_copy_with_execution_id_records_the_receipt_the_stream_copy_lacked() -> None:
    repo = CountingRepository()
    opts, _ = settings()
    await seed_session(repo)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    ) as http:
        service = SessionService(repo, opts, None, http=http)
        idle = {"id": "idle-1", "type": "session.status_idle", "stop_reason": {"type": "end_turn"}}
        await service.process_event("ses_1", idle, 5)
        assert not (await repo.get("ops.agent_sessions", "ses_1")).get("native_execution_receipts")
        await service.process_event("ses_1", dict(idle, execution_id="exec-1"))
        receipts = (await repo.get("ops.agent_sessions", "ses_1"))["native_execution_receipts"]
        assert receipts["idle-1"]["execution_id"] == "exec-1"
        puts = repo.session_puts
        await service.process_event("ses_1", dict(idle, execution_id="exec-1"))
        assert repo.session_puts == puts
