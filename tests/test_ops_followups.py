from types import SimpleNamespace

import httpx
import pytest
from test_telegram_followups import (
    ALERTS,
    BOT,
    FakeTelegram,
    message,
    openma,
    run_inbox,
    seed,
    session_settings,
)

from tank.agents import SessionService
from tank.storage import MemoryRepository
from tank.telegram_inbox import (
    EARLY_ARCHIVED_TEXT,
    OPS_JEV_TEXT,
    OPS_NOT_RECEIVED_TEXT,
    OPS_UNMATCHED_TEXT,
    OPS_WAITING_TEXT,
    TelegramInbox,
    question_id_for,
    question_message,
    record_answer,
)

OPS = "-100600"
NOW = 1_800_000_100


def ops_reply(message_id, ref, text="Почему не опубликовали?", **extra):
    return message(
        message_id,
        date=NOW - 1,
        text=text,
        chat=int(OPS),
        chat_type="supergroup",
        reply_to_message={"message_id": ref, "from": {"id": BOT, "is_bot": True}},
        **extra,
    )


async def item(repo, item_id, *, decision="SEND", score=0.82, stages=()):
    await repo.put(
        "ingest.source_items",
        item_id,
        {
            "item_id": item_id,
            "source_id": "S03",
            "source_name": "The Block",
            "title": f"Заголовок {item_id}",
            "text": "Биржа объявила о делистинге токена.",
            "url": "https://example.com/news",
            "published_at": "2027-01-15T07:58:00Z",
            "observed_at": "2027-01-15T07:59:00Z",
            "relevance": {"decision": decision, "p_relevant": score, "cutoff": 0.3},
        },
    )
    for index, (stage, reason) in enumerate(stages):
        await repo.put(
            "out.ops_notices",
            f"progress_{item_id}_{index}",
            {
                "notice_id": f"progress_{item_id}_{index}",
                "kind": "ITEM_PROGRESS",
                "subject_id": item_id,
                "stage": stage,
                "reason": reason,
                "text": stage,
                "format_version": 2,
                "created_at": f"2027-01-15T08:0{index}:00Z",
            },
            immutable=True,
        )


async def ops_card(repo, item_id, ref):
    await repo.put(
        "out.ops_cards",
        "card_" + item_id,
        {
            "item_id": item_id,
            "chat_id": OPS,
            "message_ref": str(ref),
            "rendered_html": "<b>Ниже порога Alerts</b>\n<b>Делистинг токена</b>",
        },
    )


async def analyst_session(repo, session_id, event_id, item_id, *, status="OPEN", kind="NEW_ITEM"):
    await repo.put(
        "ops.agent_sessions",
        session_id,
        {
            "session_id": session_id,
            "model_context_version": 1,
            "event_id": event_id,
            "item_id": item_id if kind == "NEW_ITEM" else "itm_opening",
            "status": status,
            "runtime_status": "IDLE",
            "model_id": "gpt-6.1-sol",
            "prompt_version": "p" * 64,
            "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
            "messages": ["m0"],
            "message_log": [
                {
                    "message_id": "m0",
                    "payload": {"kind": kind, "item_id": item_id},
                    "remote_event_id": "in_0",
                    "turn_status": "DONE",
                }
            ],
            "last_event_seq": 0,
        },
    )


@pytest.mark.parametrize("kind", ["NEW_ITEM", "ITEM_ADDED"])
async def test_ops_reply_about_analyzed_unpublished_item_continues_its_session_and_answers_in_ops(kind):
    repo = MemoryRepository()
    await seed(repo)
    await item(repo, "itm_2", stages=[("AGENT_ACCEPTED", ""), ("AGENT_COMPLETED", "")])
    await analyst_session(repo, "ses_2", "evt_2", "itm_2", kind=kind)
    if kind == "ITEM_ADDED":
        await repo.put("evt.event_observations", "obs_2", {"item_id": "itm_2", "event_id": "evt_2"})
    await ops_card(repo, "itm_2", 700)
    telegram = FakeTelegram([{"update_id": 1, "message": ops_reply(30, 700)}])
    await run_inbox(repo, telegram, now=NOW)
    question_id = question_id_for(OPS, 30)
    question = await repo.get("out.user_questions", question_id)
    assert question["session_id"] == "ses_2" and question["chat_id"] == OPS
    assert question["reference"]["kind"] == "OPS_CARD" and question["reference"]["item_id"] == "itm_2"
    jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert [job["payload"] for job in jobs] == [
        {"kind": "USER_QUESTION", "session_id": "ses_2", "question_id": question_id}
    ]
    assert (await repo.get("ops.telegram_messages", f"{OPS}:30"))["status"] == "QUESTION"
    assert telegram.sent == []
    text = question_message(question)
    assert "Tank Ops" in text and "itm_2" in text and "Делистинг токена" in text
    posts = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma(posts, []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.start_stream = lambda sid: None
        await service.send_message(jobs[0]["payload"])
    assert posts[0][0] == "/v1/sessions/ses_2/events"
    assert "Почему не опубликовали?" in posts[0][1]["events"][0]["content"][0]["text"]
    await record_answer(repo, question_id, "Ниже порога MAJOR.", status="ANSWERED")
    answers = FakeTelegram()
    async with httpx.AsyncClient(transport=httpx.MockTransport(answers)) as http:
        inbox = TelegramInbox(
            repo, SimpleNamespace(telegram_token="test", telegram_alerts_chat=ALERTS), http=http
        )
        await inbox.answer({"question_id": question_id})
    assert [(row["chat_id"], row["text"]) for row in answers.sent] == [(OPS, "Ниже порога MAJOR.")]
    assert answers.sent[0]["reply_parameters"]["message_id"] == 30


async def answer_in_ops(repo, question_id):
    answers = FakeTelegram()
    async with httpx.AsyncClient(transport=httpx.MockTransport(answers)) as http:
        inbox = TelegramInbox(
            repo, SimpleNamespace(telegram_token="test", telegram_alerts_chat=ALERTS), http=http
        )
        await inbox.answer({"question_id": question_id})
    return answers.sent


async def test_ops_reply_about_published_item_continues_its_session_and_answers_in_ops():
    repo = MemoryRepository()
    await seed(repo)
    await item(repo, "itm_1", stages=[("ALERT_SENT", "")])
    await repo.put("evt.event_observations", "obs_1", {"item_id": "itm_1", "event_id": "evt_1"})
    await ops_card(repo, "itm_1", 702)
    telegram = FakeTelegram([{"update_id": 1, "message": ops_reply(33, 702)}])
    await run_inbox(repo, telegram, now=NOW)
    assert telegram.sent == []
    question_id = question_id_for(OPS, 33)
    question = await repo.get("out.user_questions", question_id)
    assert question["session_id"] == "ses_1" and question["chat_id"] == OPS
    assert question["reference"]["kind"] == "OPS_CARD" and question["reference"]["item_id"] == "itm_1"
    assert "Tank Ops" in question_message(question) and "не публиковалась" not in question_message(question)
    jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert [job["payload"] for job in jobs] == [
        {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
    ]
    posts = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma(posts, []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.start_stream = lambda sid: None
        await service.send_message(jobs[0]["payload"])
    assert posts[0][0] == "/v1/sessions/ses_1/events"
    await record_answer(repo, question_id, "Карточка вышла, токен AAVE.", status="ANSWERED")
    sent = await answer_in_ops(repo, question_id)
    assert [(row["chat_id"], row["text"]) for row in sent] == [(OPS, "Карточка вышла, токен AAVE.")]
    assert sent[0]["reply_parameters"]["message_id"] == 33


async def test_ops_reply_about_jev_filtered_item_states_only_the_reason():
    repo = MemoryRepository()
    await seed(repo)
    await item(
        repo,
        "itm_3",
        decision="SKIP",
        score=0.01,
        stages=[("RECEIVED", ""), ("JEV_REJECTED", "p_relevant=0.01")],
    )
    await repo.put(
        "out.deliveries",
        "d_ops",
        {
            "notice_id": "progress_itm_3_1",
            "channel": "telegram:ops",
            "chat_id": OPS,
            "status": "SENT",
            "attempt": 1,
            "message_ref": "701",
            "at": "2027-01-15T08:01:05Z",
        },
        immutable=True,
    )
    telegram = FakeTelegram([{"update_id": 1, "message": ops_reply(31, 701)}])
    await run_inbox(repo, telegram, now=NOW)
    assert [(row["chat_id"], row["text"]) for row in telegram.sent] == [(OPS, OPS_JEV_TEXT)]
    assert telegram.sent[0]["reply_parameters"]["message_id"] == 31
    for fact in ("релевантность", "порог", "UTC", "itm_3", "Заголовок"):
        assert fact not in telegram.sent[0]["text"]
    assert not await repo.list("out.user_questions")
    assert not await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    row = await repo.get("ops.telegram_messages", f"{OPS}:31")
    assert row["status"] == "NO_SESSION" and row["reason"] == "JEV_REJECTED"
    telegram.updates.append({"update_id": 2, "message": ops_reply(32, 1001, text="А какой был порог?")})
    await run_inbox(repo, telegram, now=NOW)
    assert [row["text"] for row in telegram.sent] == [OPS_JEV_TEXT, OPS_JEV_TEXT]
    assert telegram.sent[1]["reply_parameters"]["message_id"] == 32


@pytest.mark.parametrize(
    "stages,reason,text",
    [
        ([("RECEIVED", ""), ("AGENT_QUEUED", "")], "NOT_YET_RECEIVED", OPS_WAITING_TEXT),
        (
            [("RECEIVED", ""), ("PROCESSING_EXPIRED", "")],
            "PROCESSING_EXPIRED",
            OPS_NOT_RECEIVED_TEXT.format(stage="истёк срок обработки"),
        ),
    ],
)
async def test_ops_reply_about_item_that_never_reached_an_agent_states_why(stages, reason, text):
    repo = MemoryRepository()
    await seed(repo)
    await item(repo, "itm_5", stages=stages)
    await ops_card(repo, "itm_5", 705)
    telegram = FakeTelegram([{"update_id": 1, "message": ops_reply(36, 705)}])
    await run_inbox(repo, telegram, now=NOW)
    assert [row["text"] for row in telegram.sent] == [text]
    assert (await repo.get("ops.telegram_messages", f"{OPS}:36"))["reason"] == reason
    assert not await repo.list("out.user_questions")


@pytest.mark.parametrize("retention", [True, False])
async def test_ops_reply_about_archived_session_gets_only_the_reason(retention):
    repo = MemoryRepository()
    await seed(repo)
    await item(repo, "itm_4", stages=[("AGENT_REJECTED", "NO_EVENT")])
    await analyst_session(repo, "ses_4", None, "itm_4", status="ENDED")
    if retention:
        session = await repo.get("ops.agent_sessions", "ses_4")
        session.update(archive_reason="RETENTION", archive_keep_days=7)
        await repo.put("ops.agent_sessions", "ses_4", session)
    await ops_card(repo, "itm_4", 704)
    telegram = FakeTelegram([{"update_id": 1, "message": ops_reply(34, 704)}])
    await run_inbox(repo, telegram, now=NOW)
    assert telegram.sent == []
    question_id = question_id_for(OPS, 34)
    assert (await repo.get("out.user_questions", question_id))["session_id"] == "ses_4"
    posts = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma(posts, []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        payload = {"kind": "USER_QUESTION", "session_id": "ses_4", "question_id": question_id}
        assert await service.send_message(payload) is None
    assert posts == []
    expected = (
        "Продолжить нельзя: сессия агента по этому сообщению закрыта после 7 дней без сообщений."
        if retention
        else EARLY_ARCHIVED_TEXT
    )
    sent = await answer_in_ops(repo, question_id)
    assert [(row["chat_id"], row["text"]) for row in sent] == [(OPS, expected)]
    assert sent[0]["reply_parameters"]["message_id"] == 34


async def test_ops_messages_that_are_not_replies_to_tank_are_ignored():
    repo = MemoryRepository()
    await seed(repo)
    plain = message(40, date=NOW - 1, text="Всем привет", chat=int(OPS), chat_type="supergroup")
    human = message(
        41,
        date=NOW - 1,
        text="Согласен",
        chat=int(OPS),
        chat_type="supergroup",
        reply_to_message={"message_id": 40, "from": {"id": 43, "is_bot": False}},
    )
    unknown = ops_reply(42, 55)
    telegram = FakeTelegram(
        [
            {"update_id": 1, "message": plain},
            {"update_id": 2, "message": human},
            {"update_id": 3, "message": unknown},
        ]
    )
    await run_inbox(repo, telegram, now=NOW)
    for message_id in (40, 41):
        row = await repo.get("ops.telegram_messages", f"{OPS}:{message_id}")
        assert row["status"] == "IGNORED" and row["reason"] == "NOT_A_QUESTION"
    assert [row["text"] for row in telegram.sent] == [OPS_UNMATCHED_TEXT]
    assert not await repo.list("out.user_questions")


async def test_ops_follow_ups_survive_restart_and_redelivery_without_double_answers():
    repo = MemoryRepository()
    await seed(repo)
    await item(repo, "itm_2")
    await analyst_session(repo, "ses_2", "evt_2", "itm_2")
    await ops_card(repo, "itm_2", 700)
    await item(repo, "itm_3", decision="SKIP", score=0.01, stages=[("JEV_REJECTED", "")])
    await ops_card(repo, "itm_3", 701)
    updates = [
        {"update_id": 1, "message": ops_reply(30, 700)},
        {"update_id": 2, "message": ops_reply(31, 701)},
    ]
    telegram = FakeTelegram(updates)
    await run_inbox(repo, telegram, now=NOW)
    await repo.delete("ops.source_checkpoints", "telegram:bot-updates")
    reason = await repo.get("ops.telegram_messages", f"{OPS}:31")
    await repo.put("ops.telegram_messages", f"{OPS}:31", {**reason, "status": "RECEIVED"})
    await run_inbox(repo, telegram, now=NOW + 1)
    await run_inbox(repo, telegram, now=NOW + 2)
    assert len(telegram.sent) == 1
    assert (await repo.get("ops.telegram_messages", f"{OPS}:31"))["status"] == "NO_SESSION"
    assert [row["text"] for row in telegram.sent] == [OPS_JEV_TEXT]
    assert len(await repo.list("out.user_questions")) == 1
    assert len(await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})) == 1


async def test_alerts_follow_ups_are_unchanged_and_ops_needs_its_configured_chat():
    repo = MemoryRepository()
    await seed(repo)
    await ops_card(repo, "itm_1", 55)
    alerts_reply = message(
        9,
        date=NOW - 1,
        text="Почему не шорт?",
        chat=int(ALERTS),
        chat_type="supergroup",
        reply_to_message={"message_id": 55, "from": {"id": BOT, "is_bot": True}},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": alerts_reply}])
    await run_inbox(repo, telegram, now=NOW)
    question = await repo.get("out.user_questions", question_id_for(ALERTS, 9))
    assert question["reference"]["kind"] == "CARD" and question["chat_id"] == ALERTS
    assert "Tank Ops" not in question_message(question)
    assert telegram.sent == []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(FakeTelegram([{"update_id": 2, "message": ops_reply(30, 55)}]))
    ) as http:
        inbox = TelegramInbox(
            repo,
            SimpleNamespace(telegram_token="test", telegram_alerts_chat=ALERTS, telegram_ops_chat=None),
            http=http,
        )
        await inbox.poll(timeout=0)
        await inbox.sweep(now=NOW)
    row = await repo.get("ops.telegram_messages", f"{OPS}:30")
    assert row["status"] == "IGNORED" and row["reason"] == "UNAUTHORIZED"


@pytest.mark.parametrize("ref", [700, 1001])
async def test_reply_to_an_ops_answer_continues_the_same_session(ref):
    repo = MemoryRepository()
    await seed(repo)
    await item(repo, "itm_2")
    await analyst_session(repo, "ses_2", "evt_2", "itm_2")
    await ops_card(repo, "itm_2", 700)
    telegram = FakeTelegram([{"update_id": 1, "message": ops_reply(30, 700)}])
    await run_inbox(repo, telegram, now=NOW)
    await record_answer(repo, question_id_for(OPS, 30), "Ниже порога.", status="ANSWERED")
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(
            repo, SimpleNamespace(telegram_token="test", telegram_alerts_chat=ALERTS), http=http
        ).answer({"question_id": question_id_for(OPS, 30)})
    telegram.updates.append({"update_id": 2, "message": ops_reply(35, ref, text="А какие токены?")})
    await run_inbox(repo, telegram, now=NOW)
    follow = await repo.get("out.user_questions", question_id_for(OPS, 35))
    assert follow["session_id"] == "ses_2" and follow["chat_id"] == OPS
