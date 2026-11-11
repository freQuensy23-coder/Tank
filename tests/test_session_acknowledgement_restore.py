"""A message Tank sent keeps its OpenMA acknowledgement: a record without one gets it back from OpenMA.

OpenMA derives the accepted input's id from the Idempotency-Key, so posting the durable body again
under the key of the message's current attempt returns the id it accepted the first time without a
second execution, and delivers a message it never received.
"""

import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from test_agents_execution_barrier import config, idle, seeded
from test_session_supervisor_recovery import executions, stop_supervisor

from tank.agents.sessions import AdmissionDeferred, SessionService
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import iso, parse_time, utcnow


class NativeInputs:
    """OpenMA's event POST: one stored input per Idempotency-Key, executed once, replayed by id."""

    def __init__(self, held=()):
        self.accepted = {key: "input-" + key for key in held}
        self.executed = list(self.accepted)
        self.posts = []
        self.history = {}
        self.states = {}
        self.replies_without_id = 0

    def handler(self, request):
        sid = request.url.path.split("/")[3]
        if request.method == "POST":
            key = request.headers["Idempotency-Key"]
            self.posts.append((sid, key, json.loads(request.content)["events"]))
            if key not in self.accepted:
                self.accepted[key] = "input-" + key
                self.executed.append(key)
            if self.replies_without_id:
                self.replies_without_id -= 1
                return httpx.Response(200, json={})
            return httpx.Response(200, json={"data": [{"id": self.accepted[key]}]})
        if request.url.path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, text="")
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": self.history.get(sid, [])})
        if request.url.path.endswith("/executions"):
            return executions(request, self.states)
        raise AssertionError(request.url.path)

    def finish(self, sid, key):
        identity = self.accepted[key]
        self.history.setdefault(sid, []).append(idle("idle-" + identity, identity))
        self.states[(sid, identity)] = "COMPLETED"


async def unacknowledged(repo, *, turn_status="PENDING", sid="s", **changes):
    session = await seeded(repo, initial=None)
    sent_at = iso(utcnow() - timedelta(minutes=5))
    session["session_id"] = sid
    session["message_log"][0].update(
        turn_status=turn_status,
        requested_at=sent_at,
        sent_at=sent_at,
        sent_at_prec="COLLECTION_ONLY",
        **changes,
    )
    session["messages"] = []
    await repo.put("ops.agent_sessions", sid, session)
    return session


def one_slot():
    value = config()
    value.load_config = lambda name: {"max_running_sessions": 1} if name == "openma" else {}
    return value


@pytest.mark.parametrize("turn_status", ["PENDING", "ACCEPTING", "RUNNING", "DONE"])
async def test_sent_message_without_acknowledgement_gets_it_back_and_its_finished_turn_settles(
    turn_status,
):
    repo = MemoryRepository()
    seed = await unacknowledged(repo, turn_status=turn_status)
    native = NativeInputs(held=["message:message"])
    native.finish("s", "message:message")
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        try:
            assert service.running(await repo.get("ops.agent_sessions", "s"))
            await service.recover_outbound_inputs("s")
            await service.catch_up("s")
            assert (await service.completed_turn("s"))["complete"] is True
            await service.recover_outbound_inputs("s")
        finally:
            await service.close()
    session = await repo.get("ops.agent_sessions", "s")
    message = session["message_log"][0]
    assert message["remote_event_id"] == "input-message:message"
    assert message["native_input_attempts"] == {"input-message:message": 0}
    assert message["turn_status"] == "DONE"
    assert message["sent_at"] == seed["message_log"][0]["sent_at"]
    assert session.get("last_item_at") == seed.get("last_item_at")
    assert session["messages"] == ["message"]
    assert (session["status"], session["runtime_status"], session["active_message_id"]) == (
        "OPEN",
        "IDLE",
        None,
    )
    assert not service.running(session)
    assert native.posts == [("s", "message:message", [seed["message_log"][0]["body"]])]
    assert native.executed == ["message:message"]


async def test_message_accepted_with_a_reply_that_carries_no_id_is_acknowledged_by_the_follower():
    repo = MemoryRepository()
    session = await seeded(repo)
    session.update(messages=[], message_log=[], active_message_id=None, runtime_status="IDLE")
    await repo.put("ops.agent_sessions", "s", session)
    await repo.put("ingest.source_items", "item", {"item_id": "item", "text": "Source"})
    native = NativeInputs()
    native.replies_without_id = 1
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handler)) as http:
        service = SessionService(repo, config(), None, http=http)

        async def resolve(payload):
            return await repo.get("ops.agent_sessions", "s")

        service.resolve = resolve
        service.start_stream = lambda sid: None
        try:
            await service.send_message({"kind": "NEW_ITEM", "item_id": "item", "session_id": "s"})
            sent = await repo.get("ops.agent_sessions", "s")
            message = sent["message_log"][0]
            key = "message:" + message["message_id"]
            assert message["sent_at"] and message["remote_event_id"] is None
            assert service.running(sent)
            native.finish("s", key)
            await repo.put("ingest.source_items", "item", {"item_id": "item", "extraction_status": "DONE"})
            await service.follow_owned("s")
        finally:
            await service.close()
    session = await repo.get("ops.agent_sessions", "s")
    assert session["message_log"][0]["remote_event_id"] == "input-" + key
    assert session["message_log"][0]["sent_at"] == message["sent_at"]
    assert (session["runtime_status"], session["active_message_id"]) == ("IDLE", None)
    assert not service.running(session)
    assert native.posts == [("s", key, [message["body"]])] * 2
    assert native.executed == [key]


async def test_supervisor_restores_the_acknowledgement_and_the_freed_slot_admits_the_waiting_turn():
    repo = MemoryRepository()
    await unacknowledged(repo)
    await repo.put("ingest.source_items", "next", {"item_id": "next", "extraction_status": "PENDING"})
    job = await repo.enqueue("AGENT_MESSAGE", {"kind": "NEW_ITEM", "item_id": "next"}, "next", priority=1)
    later = collection_clock(utcnow() + timedelta(minutes=1))
    job = {**job, **collection_clock().fields("slot_waiting_since"), **later.fields("run_after")}
    await repo.put("ops.jobs", job["job_id"], job)
    native = NativeInputs(held=["message:message"])
    native.finish("s", "message:message")
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handler)) as http:
        service = SessionService(repo, one_slot(), None, http=http)
        running, waiting = await service.slot_state()
        assert running == {"s"} and [row["job_id"] for row in waiting] == [job["job_id"]]
        with pytest.raises(AdmissionDeferred):
            await service.admit(job)
        stop = asyncio.Event()
        task = asyncio.create_task(service.run(stop))
        try:
            async with asyncio.timeout(2):
                while "s" not in service.tasks:
                    await asyncio.sleep(0.01)
                await service.tasks["s"]
            session = await repo.get("ops.agent_sessions", "s")
            assert (session["runtime_status"], session["active_message_id"]) == ("IDLE", None)
            assert session["message_log"][0]["remote_event_id"] == "input-message:message"
            running, waiting = await service.slot_state()
            assert running == set() and [row["job_id"] for row in waiting] == [job["job_id"]]
            await service.wake_waiting()
            woken = await repo.get("ops.jobs", job["job_id"])
            assert parse_time(woken["run_after"]) <= utcnow()
            await service.admit(woken)
            assert (await repo.get("ops.jobs", job["job_id"]))["slot_admitted"] is True
            assert [(sid, key) for sid, key, _ in native.posts] == [("s", "message:message")]
            assert native.executed == ["message:message"]
        finally:
            await stop_supervisor(service, task, stop)


async def test_message_openma_never_received_is_delivered_once_and_its_turn_stays_running():
    repo = MemoryRepository()
    await unacknowledged(repo)
    native = NativeInputs()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        try:
            for _ in range(3):
                await service.recover_outbound_inputs("s")
            await service.catch_up("s")
            assert (await service.completed_turn("s"))["complete"] is False
        finally:
            await service.close()
    session = await repo.get("ops.agent_sessions", "s")
    assert session["message_log"][0]["remote_event_id"] == "input-message:message"
    assert session["message_log"][0]["turn_status"] == "PENDING"
    assert (session["runtime_status"], session["active_message_id"]) == ("RUNNING", "message")
    assert [key for _, key, _ in native.posts] == ["message:message"]
    assert native.executed == ["message:message"]


async def test_retried_turn_gets_the_acknowledgement_of_its_retry_not_of_the_superseded_attempt():
    repo = MemoryRepository()
    await unacknowledged(
        repo,
        turn_status="RUNNING",
        retry_count=1,
        native_input_attempts={"first-input": 0},
        superseded_native_input_ids=["first-input"],
    )
    session = await repo.get("ops.agent_sessions", "s")
    session["superseded_native_inputs"] = {
        "first-input": {
            "reason": "TURN_FAILED_AND_RETRIED",
            "failure_event_id": "failure",
            "replacement_input_id": None,
        }
    }
    await repo.put("ops.agent_sessions", "s", session)
    native = NativeInputs(held=["message:message", "turn-retry:s:message"])
    native.finish("s", "turn-retry:s:message")
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        try:
            await service.recover_outbound_inputs("s")
            await service.catch_up("s")
            assert (await service.completed_turn("s"))["complete"] is True
        finally:
            await service.close()
    session = await repo.get("ops.agent_sessions", "s")
    message = session["message_log"][0]
    assert message["remote_event_id"] == "input-turn-retry:s:message"
    assert message["native_input_attempts"] == {"first-input": 0, "input-turn-retry:s:message": 1}
    assert message["retry_count"] == 1
    assert (session["runtime_status"], session["active_message_id"]) == ("IDLE", None)
    assert [key for _, key, _ in native.posts] == ["turn-retry:s:message"]
    assert native.executed == ["message:message", "turn-retry:s:message"]


async def test_restore_posts_nothing_for_a_closed_session_or_a_retired_or_failed_turn():
    repo = MemoryRepository()
    for sid, status in (("failed-session", "FAILED"), ("ended-session", "ENDED")):
        closed = await unacknowledged(repo, sid=sid)
        await repo.put("ops.agent_sessions", sid, dict(closed, status=status))
    session = await unacknowledged(repo)
    for key, turn_status in (("retired", "PENDING"), ("failed", "FAILED")):
        session["message_log"].append(
            dict(session["message_log"][0], message_id=key, turn_status=turn_status)
        )
    session["retired_message_ids"] = ["retired"]
    await repo.put("ops.agent_sessions", "s", session)
    native = NativeInputs()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        try:
            for sid in ("s", "failed-session", "ended-session"):
                await service.recover_outbound_inputs(sid)
        finally:
            await service.close()
    assert [(sid, key) for sid, key, _ in native.posts] == [("s", "message:message")]
    session = await repo.get("ops.agent_sessions", "s")
    assert [row["remote_event_id"] for row in session["message_log"]] == [
        "input-message:message",
        None,
        None,
    ]
    for sid, status in (("failed-session", "FAILED"), ("ended-session", "ENDED")):
        closed = await repo.get("ops.agent_sessions", sid)
        assert closed["status"] == status and closed["message_log"][0]["remote_event_id"] is None


async def test_reply_without_an_id_leaves_the_message_unacknowledged_and_the_next_pass_asks_again():
    repo = MemoryRepository()
    seed = await unacknowledged(repo)
    replies = [{}, {"data": [{"id": "accepted-message"}]}]
    posts = []

    def handler(request):
        assert request.method == "POST"
        posts.append(request.headers["Idempotency-Key"])
        return httpx.Response(200, json=replies[len(posts) - 1])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config(), None, http=http)
        try:
            await service.recover_outbound_inputs("s")
            message = (await repo.get("ops.agent_sessions", "s"))["message_log"][0]
            assert message["remote_event_id"] is None and message.get("native_input_attempts") is None
            assert message["sent_at"] == seed["message_log"][0]["sent_at"]
            await service.recover_outbound_inputs("s")
        finally:
            await service.close()
    message = (await repo.get("ops.agent_sessions", "s"))["message_log"][0]
    assert message["remote_event_id"] == "accepted-message"
    assert posts == ["message:message", "message:message"]
