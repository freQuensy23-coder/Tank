import asyncio
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from tank.agents import SessionService
from tank.settings import threshold
from tank.storage import MemoryRepository
from tank.telegram_inbox import closed_reason
from tank.util import iso, parse_time, utcnow


def settings(keep_days=7, replay=False):
    config = {
        "openma": {"agent_id": "agt_1", "environment_id": "env_1", "prompt_version": "test"},
        "thresholds": {"session_keep_days": keep_days},
    }
    return SimpleNamespace(
        openma_api_key="secret",
        openma_url="http://oma",
        openma_model="gpt-6.1-sol",
        config_version="test",
        code_version="test",
        replay=replay,
        load_config=lambda name: config[name],
    )


async def seed(repo, *, age_days, event=None, no_event=False):
    """An idle session whose last message is age_days old, for a closed event or a rejected material."""
    last = iso(utcnow() - timedelta(days=age_days))
    if event is not None:
        await repo.put("evt.events", "evt_1", {"event_id": "evt_1", "lifecycle": event})
    await repo.put(
        "ops.agent_sessions",
        "ses_1",
        {
            "session_id": "ses_1",
            "event_id": "evt_1" if event is not None else None,
            "item_id": "itm_1",
            "no_event": no_event,
            "status": "OPEN",
            "runtime_status": "IDLE",
            "opened_at": iso(utcnow() - timedelta(days=age_days + 1)),
            "last_event_at": last,
            "messages": ["m0"],
            "message_log": [
                {
                    "message_id": "m0",
                    "payload": {"kind": "NEW_ITEM", "item_id": "itm_1"},
                    "requested_at": iso(utcnow() - timedelta(days=age_days + 1)),
                    "sent_at": iso(utcnow() - timedelta(days=age_days + 1)),
                    "turn_status": "DONE",
                }
            ],
        },
    )


async def finish(repo, config=None):
    posts = []

    def handler(request):
        posts.append((request.method, request.url.path))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config or settings(), None, http=http)

        async def complete(*args, **kwargs):
            return {"complete": True, "accepted_input_ids": []}

        service.completed_turn = complete
        service.execution_barrier = complete
        assert await service.finish_if_idle("ses_1")
    return posts, await repo.get("ops.agent_sessions", "ses_1")


@pytest.mark.parametrize(("event", "no_event"), [("CLOSED", False), ("INVALIDATED", False), (None, True)])
async def test_finished_session_is_not_archived_within_the_retention(event, no_event):
    repo = MemoryRepository()
    await seed(repo, age_days=0, event=event, no_event=no_event)
    posts, session = await finish(repo)
    assert posts == [] and session["status"] == "OPEN"
    last = parse_time(session["last_event_at"])
    assert parse_time(session["last_message_at"]) == last
    assert parse_time(session["archive_after"]) == last + timedelta(days=7)
    assert closed_reason(session) is None


@pytest.mark.parametrize(("event", "no_event"), [("CLOSED", False), (None, True)])
async def test_session_is_archived_only_after_the_retention_since_its_last_message(event, no_event):
    repo = MemoryRepository()
    await seed(repo, age_days=6.9, event=event, no_event=no_event)
    posts, session = await finish(repo)
    assert posts == [] and session["status"] == "OPEN"
    session["last_event_at"] = iso(utcnow() - timedelta(days=7, minutes=1))
    await repo.put("ops.agent_sessions", "ses_1", session)
    posts, session = await finish(repo)
    assert posts == [("POST", "/v1/sessions/ses_1/archive")]
    assert session["status"] == "ENDED" and session["archive_reason"] == "RETENTION"
    assert session["archive_keep_days"] == 7 and session.get("archived_at")
    assert closed_reason(session) == (
        "ARCHIVED",
        "Продолжить нельзя: сессия агента по этому сообщению закрыта после 7 дней без сообщений.",
    )


async def test_retention_comes_from_the_thresholds_config():
    repo = MemoryRepository()
    await seed(repo, age_days=2, no_event=True)
    posts, session = await finish(repo, settings(keep_days=3))
    assert posts == [] and parse_time(session["archive_after"]) == parse_time(
        session["last_event_at"]
    ) + timedelta(days=3)
    posts, session = await finish(repo, settings(keep_days=1))
    assert posts and session["archive_keep_days"] == 1
    assert closed_reason(session)[1].endswith("закрыта после 1 дня без сообщений.")
    assert threshold(SimpleNamespace(), "session_keep_days", 7) == 7
    config = yaml.safe_load((Path(__file__).parents[1] / "config" / "thresholds.yaml").read_text())
    assert config["session_keep_days"] == 7


async def test_a_reader_question_counts_as_the_last_message():
    repo = MemoryRepository()
    await seed(repo, age_days=30, no_event=True)
    asked = iso(utcnow() - timedelta(days=2))
    await repo.put(
        "out.user_questions", "q_1", {"question_id": "q_1", "session_id": "ses_1", "asked_at": asked}
    )
    async with httpx.AsyncClient() as http:
        service = SessionService(repo, settings(), None, http=http)
        assert await service.last_message_at(await repo.get("ops.agent_sessions", "ses_1")) == parse_time(
            asked
        )


@pytest.mark.parametrize("keeper", ["call", "watch", "job"])
async def test_open_call_armed_watch_or_pending_job_keeps_the_session_past_the_retention(keeper):
    repo = MemoryRepository()
    await seed(repo, age_days=30, event="CLOSED")
    if keeper == "call":
        await repo.put("ana.calls", "call_1", {"call_id": "call_1", "event_id": "evt_1", "status": "OPEN"})
    elif keeper == "watch":
        await repo.put(
            "ana.watch_conditions", "w_1", {"cond_id": "w_1", "event_id": "evt_1", "status": "ARMED"}
        )
    else:
        await repo.enqueue("AGENT_MESSAGE", {"kind": "CONDITION_FIRED", "event_id": "evt_1"}, "job_1")
    posts, session = await finish(repo)
    assert posts == [] and session["status"] == "OPEN" and not session.get("archive_after")


async def test_session_with_a_delivered_card_stays_open_for_follow_ups_past_the_retention():
    repo = MemoryRepository()
    await seed(repo, age_days=30, event="CLOSED")
    await repo.put("out.alerts", "a:1", {"alert_id": "a", "version": 1, "event_id": "evt_1"}, immutable=True)
    await repo.put(
        "out.deliveries",
        "d1",
        {"alert_id": "a", "version": 1, "channel": "telegram:alerts", "status": "SENT", "attempt": 1},
        immutable=True,
    )
    posts, session = await finish(repo)
    assert posts == [] and session["status"] == "OPEN" and session["followups_only"]


async def test_stale_retention_is_cleared_when_the_session_is_no_longer_archivable():
    repo = MemoryRepository()
    await seed(repo, age_days=0, event="CLOSED")
    _, session = await finish(repo)
    assert session["archive_after"]
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1", "lifecycle": "MONITORING"})
    _, session = await finish(repo)
    assert "archive_after" not in session and session["status"] == "OPEN"


async def test_replay_sessions_are_still_archived_at_turn_end():
    repo = MemoryRepository()
    await seed(repo, age_days=0, no_event=True)
    posts, session = await finish(repo, settings(replay=True))
    assert posts == [("POST", "/v1/sessions/ses_1/archive")]
    assert session["status"] == "ENDED" and session["archive_reason"] == "REPLAY"


async def test_sweep_skips_retained_sessions_until_their_retention_passes():
    repo = MemoryRepository()
    await seed(repo, age_days=0, no_event=True)
    _, session = await finish(repo)
    checked = []
    async with httpx.AsyncClient() as http:
        service = SessionService(repo, settings(), None, http=http)
        stop = asyncio.Event()

        async def record(sid):
            checked.append(sid)
            return True

        maintained = asyncio.Event()
        reconcile = service.reconcile_idle_sessions

        async def maintenance(sessions, tick):
            await reconcile(sessions, tick)
            maintained.set()

        async def tick():
            await maintained.wait()
            stop.set()

        async def nothing():
            return None

        service.finish_if_idle = record
        service.reconcile_idle_sessions = maintenance
        service.wake_waiting = tick
        service.reconcile_answers = nothing
        await service.run(stop)
        assert checked == []
        session["archive_after"] = iso(utcnow() - timedelta(seconds=1))
        await repo.put("ops.agent_sessions", "ses_1", session)
        stop.clear()
        maintained.clear()
        await service.run(stop)
    assert checked == ["ses_1"]


async def seed_item(repo, status, reason=None):
    await repo.put(
        "ingest.source_items",
        "itm_1",
        {"item_id": "itm_1", "extraction_status": status, "extraction_reason": reason},
    )


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ("SKIPPED", "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION"),
        ("DONE", None),
        ("FAILED", "AGENT_INCOMPLETE"),
    ],
)
async def test_eventless_session_whose_item_is_finished_is_archived_after_the_retention(status, reason):
    repo = MemoryRepository()
    await seed(repo, age_days=6.9)
    await seed_item(repo, status, reason)
    posts, session = await finish(repo)
    assert posts == [] and session["status"] == "OPEN"
    assert parse_time(session["archive_after"]) == parse_time(session["last_event_at"]) + timedelta(days=7)
    session["last_event_at"] = iso(utcnow() - timedelta(days=7, minutes=1))
    await repo.put("ops.agent_sessions", "ses_1", session)
    posts, session = await finish(repo)
    assert posts == [("POST", "/v1/sessions/ses_1/archive")]
    assert session["status"] == "ENDED" and session["archive_reason"] == "RETENTION"


@pytest.mark.parametrize("status", ["PENDING", "RUNNING", None])
async def test_eventless_session_whose_item_is_unfinished_stays_open(status):
    repo = MemoryRepository()
    await seed(repo, age_days=30)
    await seed_item(repo, status)
    posts, session = await finish(repo)
    assert posts == [] and session["status"] == "OPEN" and not session.get("archive_after")


async def test_scout_review_session_is_archived_after_the_retention():
    repo = MemoryRepository()
    await seed(repo, age_days=30)
    session = await repo.get("ops.agent_sessions", "ses_1")
    session.pop("item_id")
    session["proposal_id"] = "prp_1"
    session["message_log"][0]["payload"] = {"kind": "SCOUT_REVIEW", "proposal_id": "prp_1"}
    await repo.put("ops.agent_sessions", "ses_1", session)
    posts, session = await finish(repo)
    assert posts == [("POST", "/v1/sessions/ses_1/archive")]
    assert session["status"] == "ENDED" and session["archive_reason"] == "RETENTION"


async def finish_failed(repo, config=None):
    posts = []

    def handler(request):
        posts.append((request.method, request.url.path))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, config or settings(), None, http=http)
        finished = await service.finish_failed("ses_1")
    return finished, posts, await repo.get("ops.agent_sessions", "ses_1")


async def fail(repo):
    session = await repo.get("ops.agent_sessions", "ses_1")
    session.update(status="FAILED", terminal_failure={"event_type": "session.error"})
    await repo.put("ops.agent_sessions", "ses_1", session)


async def test_failed_session_is_archived_in_openma_after_the_retention_and_stays_failed():
    repo = MemoryRepository()
    await seed(repo, age_days=6.9, event="TRACKED")
    await fail(repo)
    finished, posts, session = await finish_failed(repo)
    assert finished and posts == [] and session["status"] == "FAILED"
    assert parse_time(session["archive_after"]) == parse_time(session["last_event_at"]) + timedelta(days=7)
    assert closed_reason(session) is None
    session["last_event_at"] = iso(utcnow() - timedelta(days=7, minutes=1))
    await repo.put("ops.agent_sessions", "ses_1", session)
    finished, posts, session = await finish_failed(repo)
    assert posts == [("POST", "/v1/sessions/ses_1/archive")]
    assert session["status"] == "FAILED" and session["archived_at"]
    assert session["archive_reason"] == "RETENTION" and session["terminal_failure"]
    assert closed_reason(session) == (
        "ARCHIVED",
        "Продолжить нельзя: сессия агента по этому сообщению закрыта после 7 дней без сообщений.",
    )
    finished, posts, _ = await finish_failed(repo)
    assert not finished and posts == []


async def test_failed_session_with_a_delivered_card_is_kept_for_follow_ups():
    repo = MemoryRepository()
    await seed(repo, age_days=30, event="TRACKED")
    await fail(repo)
    await repo.put("out.alerts", "a:1", {"alert_id": "a", "version": 1, "event_id": "evt_1"}, immutable=True)
    await repo.put(
        "out.deliveries",
        "d1",
        {"alert_id": "a", "version": 1, "channel": "telegram:alerts", "status": "SENT", "attempt": 1},
        immutable=True,
    )
    _, posts, session = await finish_failed(repo)
    assert posts == [] and session["status"] == "FAILED" and session["followups_only"]
    assert not session.get("archived_at")


async def test_sweep_archives_only_failed_sessions_that_are_not_kept_or_archived():
    repo = MemoryRepository()
    rows = {
        "due": {},
        "kept": {"archive_after": iso(utcnow() + timedelta(days=1))},
        "followups": {"followups_only": True},
        "archived": {"archived_at": iso(utcnow())},
    }
    for sid, extra in rows.items():
        await repo.put("ops.agent_sessions", sid, {"session_id": sid, "status": "FAILED", **extra})
    await repo.put(
        "ops.agent_sessions", "open", {"session_id": "open", "status": "OPEN", "runtime_status": "IDLE"}
    )
    checked = []
    async with httpx.AsyncClient() as http:
        service = SessionService(repo, settings(), None, http=http)

        async def record(sid):
            checked.append(sid)
            return True

        service.finish_failed = record
        await service.sweep_failed()
    assert checked == ["due"]
