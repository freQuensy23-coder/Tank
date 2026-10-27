import json

import httpx
import pytest
from test_telegram_followups import USER, FakeTelegram, ask, inbox_settings

from tank import telegram_inbox
from tank.storage import MemoryRepository
from tank.telegram_inbox import ANSWER_CHANNEL, TelegramInbox, chunks, formatted_chunks


async def answered_question(repo, markdown):
    question_id = await ask(repo)
    question = await repo.get("out.user_questions", question_id)
    question.update(status="ANSWERED", answer_status="ANSWERED", answer_text=markdown)
    await repo.put("out.user_questions", question_id, question)
    return question_id


async def legacy_first_chunk(repo, question_id, parts, outcome):
    base = {
        "channel": ANSWER_CHANNEL,
        "question_id": question_id,
        "session_id": "ses_1",
        "purpose": "ANSWER",
        "chat_id": str(USER),
        "chunk": 0,
        "chunks": len(parts),
        "attempt": 1,
        "text": parts[0],
    }
    await repo.put(
        "out.deliveries",
        "legacy-generated",
        {**base, "status": "GENERATED", "at": "2027-01-15T08:00:00Z"},
        immutable=True,
    )
    if outcome is not None:
        await repo.put(
            "out.deliveries",
            "legacy-outcome",
            {
                **base,
                "status": outcome,
                "at": "2027-01-15T08:00:01Z",
                **({"message_ref": "900", "telegram_date": 1_800_000_001} if outcome == "SENT" else {}),
            },
            immutable=True,
        )


def answer_body(text, reply_to, entities=()):
    return {
        "chat_id": str(USER),
        "text": text,
        "link_preview_options": {"is_disabled": True},
        "reply_parameters": {"message_id": reply_to, "allow_sending_without_reply": True},
        **({"entities": entities} if entities else {}),
    }


async def test_legacy_partial_answer_resumes_the_old_plain_chunks_without_losing_words():
    markdown = "**x** " * 1000
    repo = MemoryRepository()
    question_id = await answered_question(repo, markdown)
    old_parts = chunks(markdown)
    assert len(old_parts) == 2
    assert len(formatted_chunks(markdown)) == 1
    await legacy_first_chunk(repo, question_id, old_parts, "SENT")
    old_generated = await repo.get("out.deliveries", "legacy-generated")
    old_sent = await repo.get("out.deliveries", "legacy-outcome")
    telegram = FakeTelegram()

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == [answer_body(old_parts[1], 900)]
    delivered = old_sent["text"] + " " + " ".join(body["text"] for body in telegram.sent)
    assert delivered.split() == markdown.split()
    question = await repo.get("out.user_questions", question_id)
    assert question["status"] == "DELIVERED"
    assert question["answer_text"] == markdown
    assert await repo.get("out.deliveries", "legacy-generated") == old_generated
    assert await repo.get("out.deliveries", "legacy-outcome") == old_sent
    ledger = await repo.list("out.deliveries", filters={"question_id": question_id})
    sent = sorted((row["chunk"], row["text"]) for row in ledger if row["status"] == "SENT")
    assert sent == list(enumerate(old_parts))
    bodies = list(telegram.sent)

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == bodies
    assert await repo.list("out.deliveries", filters={"question_id": question_id}) == ledger


async def test_legacy_rejected_first_chunk_retries_the_entire_old_plain_plan():
    markdown = "**x** " * 1000
    repo = MemoryRepository()
    question_id = await answered_question(repo, markdown)
    old_parts = chunks(markdown)
    await legacy_first_chunk(repo, question_id, old_parts, "FAILED")
    old_failed = await repo.get("out.deliveries", "legacy-outcome")
    telegram = FakeTelegram()
    expected = [
        answer_body(part, 9 if index == 0 else 1000 + index) for index, part in enumerate(old_parts)
    ]

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == expected
    assert " ".join(body["text"] for body in telegram.sent).split() == markdown.split()
    assert (await repo.get("out.user_questions", question_id))["status"] == "DELIVERED"
    assert await repo.get("out.deliveries", "legacy-outcome") == old_failed
    ledger = await repo.list("out.deliveries", filters={"question_id": question_id})
    sent = sorted((row["chunk"], row["text"]) for row in ledger if row["status"] == "SENT")
    assert sent == list(enumerate(old_parts))

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == expected
    assert await repo.list("out.deliveries", filters={"question_id": question_id}) == ledger


async def test_legacy_ambiguous_generated_chunk_is_not_resent_or_recorded_as_sent():
    markdown = "**x** " * 1000
    repo = MemoryRepository()
    question_id = await answered_question(repo, markdown)
    old_parts = chunks(markdown)
    await legacy_first_chunk(repo, question_id, old_parts, None)
    old_generated = await repo.get("out.deliveries", "legacy-generated")
    telegram = FakeTelegram()

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == [answer_body(old_parts[1], 9)]
    assert await repo.get("out.deliveries", "legacy-generated") == old_generated
    ledger = await repo.list("out.deliveries", filters={"question_id": question_id})
    assert [row for row in ledger if row.get("chunk") == 0] == [old_generated]
    bodies = list(telegram.sent)

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == bodies
    assert await repo.list("out.deliveries", filters={"question_id": question_id}) == ledger


async def test_formatted_answer_retry_keeps_persisted_text_entities_and_unsent_chunks(monkeypatch):
    markdown = "**" + "x" * 9000 + "**"
    repo = MemoryRepository()
    question_id = await answered_question(repo, markdown)
    original_converter = formatted_chunks
    original_parts = original_converter(markdown)
    assert len(original_parts) >= 3
    expected = [
        answer_body(text, 9 if index == 0 else 1000 + index, entities)
        for index, (text, entities) in enumerate(original_parts)
    ]
    telegram = FakeTelegram()
    requests = []

    def reject_second_chunk_once(request):
        assert request.url.path.endswith("/sendMessage")
        requests.append(json.loads(request.content))
        if len(requests) == 2:
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 1}})
        return telegram(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject_second_chunk_once)) as http:
        with pytest.raises(RuntimeError, match="429"):
            await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert requests == expected[:2]
    assert telegram.sent == expected[:1]
    assert (await repo.get("out.user_questions", question_id))["status"] != "DELIVERED"
    first_ledger = await repo.list("out.deliveries", filters={"question_id": question_id})
    assert [(row["chunk"], row["text"]) for row in first_ledger if row["status"] == "SENT"] == [
        (0, original_parts[0][0])
    ]
    assert [(row["chunk"], row["text"]) for row in first_ledger if row["status"] == "FAILED"] == [
        (1, original_parts[1][0])
    ]

    def changed_converter(markdown, limit=997):
        return [
            (text, [{**entity, "type": "italic"} for entity in entities])
            for text, entities in original_converter(markdown, limit=limit)
        ]

    monkeypatch.setattr(telegram_inbox, "formatted_chunks", changed_converter)

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject_second_chunk_once)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert requests == expected[:2] + expected[1:]
    assert telegram.sent == expected
    assert "".join(body["text"] for body in telegram.sent) == "x" * 9000
    for body in telegram.sent:
        assert body["entities"] == [{"type": "bold", "offset": 0, "length": len(body["text"])}]
    question = await repo.get("out.user_questions", question_id)
    assert question["status"] == "DELIVERED"
    assert question["answer_text"] == markdown
    ledger = await repo.list("out.deliveries", filters={"question_id": question_id})
    sent = sorted((row["chunk"], row["text"]) for row in ledger if row["status"] == "SENT")
    assert sent == [(index, text) for index, (text, _) in enumerate(original_parts)]

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject_second_chunk_once)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert requests == expected[:2] + expected[1:]
    assert telegram.sent == expected
    assert await repo.list("out.deliveries", filters={"question_id": question_id}) == ledger
