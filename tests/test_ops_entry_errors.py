import asyncio
import html
import json
from types import SimpleNamespace

import httpx
import pytest
from test_sources import RSS
from test_sources import settings as source_settings

from tank.delivery import DeliveryService
from tank.ops_entry_errors import MESSAGE_LIMIT, report_entry_errors
from tank.sources import SourceService
from tank.storage import MemoryRepository

FEED_URL = "https://example.org/rss"
SOURCE = {"source_id": "S30", "name": "Fixture blog", "feeds": [{"url": FEED_URL, "kind": "rss"}]}


def broken(*titles):
    bad = b"".join(
        b"<item><title>"
        + title.encode()
        + b"</title><description>Body of "
        + title.encode()
        + b"</description></item>"
        for title in titles
    )
    return RSS.replace(b"<item><guid>", bad + b"<item><guid>")


async def entry_notices(repo):
    return await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})


async def entry_jobs(repo):
    return [
        job
        for job in await repo.list("ops.jobs", filters={"type": "DELIVER"})
        if job["payload"]["notice_id"].startswith("entry_errors_")
    ]


async def poll(repo, tmp_path, payloads, *, enabled=True, replay=False, source=SOURCE):
    settings = source_settings(tmp_path, [source])
    settings.ops_progress_enabled = enabled
    settings.replay = replay
    feed = iter(payloads)

    def handle(request):
        return httpx.Response(200, content=next(feed))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SourceService(repo, settings, http=http)
        results = [await service.poll_once(source["source_id"]) for _ in payloads]
    return results


async def test_first_poll_reports_unparsed_entry_once_with_identity_and_hashes(tmp_path):
    repo = MemoryRepository()
    results = await poll(repo, tmp_path, [broken("Bad entry without identity")])
    assert results[0]["items"] == 1
    [notice] = await entry_notices(repo)
    assert len(await entry_jobs(repo)) == 1
    checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
    [error] = checkpoint["entry_errors"]
    assert error["entry"]["title"] == "Bad entry without identity"
    text = notice["text"]
    assert text.startswith("TANK OPS · Записи источника не разобраны")
    assert "Источник: S30 · Fixture blog" in text and "Лента: " + FEED_URL in text
    assert "Запись без идентификатора и ссылки (ENTRY_WITHOUT_ID_OR_URL)" in text
    assert "«Bad entry without identity»" in text and "Фрагмент: Body of Bad entry" in text
    assert error["entry_sha256"] in text and checkpoint["payload_sha256"] in text
    assert "Обнаружено: " in text and notice["seen_at"] == checkpoint["fetched_at"]


async def test_repeat_polls_do_not_report_same_entry_and_new_entry_is_reported_alone(tmp_path):
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Stuck entry"), broken("Stuck entry"), broken("Stuck entry")])
    assert len(await entry_notices(repo)) == 1
    await poll(
        repo, tmp_path, [broken("Stuck entry", "Second broken"), broken("Second broken", "Stuck entry")]
    )
    notices = await entry_notices(repo)
    assert len(notices) == 2 and len(await entry_jobs(repo)) == 2
    latest = next(row for row in notices if "Second broken" in row["text"])
    assert "Stuck entry" not in latest["text"] and len(latest["entry_errors"]) == 1


async def test_burst_of_broken_entries_is_one_message_clipped_to_telegram_limit(tmp_path):
    repo = MemoryRepository()
    titles = [f"Broken entry {n} " + "x" * 150 for n in range(60)]
    await poll(repo, tmp_path, [broken(*titles)])
    [notice] = await entry_notices(repo)
    assert len(notice["entry_errors"]) == 60 and len(await entry_jobs(repo)) == 1
    assert len(html.escape(notice["text"])) <= MESSAGE_LIMIT
    assert "Новых записей с ошибкой: 60" in notice["text"]
    assert "полный список: out.ops_notices " + notice["notice_id"] in notice["text"]


@pytest.mark.parametrize("enabled,replay", [(False, False), (True, True)])
async def test_disabled_or_replay_reports_nothing(tmp_path, enabled, replay):
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Bad")], enabled=enabled, replay=replay)
    assert await entry_notices(repo) == [] and await entry_jobs(repo) == []
    checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
    assert checkpoint["entry_errors"]


async def test_restart_and_concurrent_reports_are_idempotent(tmp_path):
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Bad")])
    await poll(repo, tmp_path, [broken("Bad")])
    assert len(await entry_notices(repo)) == 1
    settings = SimpleNamespace(ops_progress_enabled=True)
    errors = [{"reason": "PENDING_PROPOSAL_UNAVAILABLE", "proposal_id": "0xabc", "payload_sha256": "p"}]
    source = {"source_id": "S24", "name": "Snapshot"}
    feed = {"url": "https://hub.snapshot.org/graphql"}
    with pytest.raises(ValueError, match="rollback"):
        async with repo.transaction():
            await report_entry_errors(repo, settings, source, feed, errors, "2026-10-24T12:00:00Z")
            raise ValueError("rollback")
    assert len(await entry_notices(repo)) == 1
    ids = await asyncio.gather(
        *(report_entry_errors(repo, settings, source, feed, errors, "2026-10-24T12:00:00Z") for _ in range(4))
    )
    assert len([notice_id for notice_id in ids if notice_id]) == 1
    assert len(await entry_notices(repo)) == 2 and len(await entry_jobs(repo)) == 2
    [snapshot] = [row for row in await entry_notices(repo) if row["source_id"] == "S24"]
    assert "предложение 0xabc" in snapshot["text"]


async def test_publisher_json_entry_error_is_reported_and_delivered_to_ops(tmp_path):
    repo = MemoryRepository()
    source = {
        "source_id": "S03",
        "name": "Federal Register",
        "feeds": [{"url": "https://example.org/index", "kind": "json"}],
    }
    data = {
        "results": [
            {"title": "Malformed original without identifier", "abstract": "Rule text"},
            {
                "document_number": "good",
                "title": "Valid release",
                "abstract": "Full visible body",
                "html_url": "https://example.org/good",
                "publication_date": "2026-10-18",
            },
        ],
        "next_page_url": None,
    }
    payload = json.dumps(data).encode()
    results = await poll(repo, tmp_path, [payload, payload], source=source)
    assert [result["items"] for result in results] == [1, 0]
    [notice] = await entry_notices(repo)
    assert "INVALID_PUBLISHER_ENTRY" in notice["text"]
    assert "«Malformed original without identifier»" in notice["text"]
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7, "date": 1}})

    settings = SimpleNamespace(
        ops_progress_enabled=True,
        telegram_token="test",
        telegram_alerts_chat="alerts",
        telegram_ops_chat="ops",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        delivery = DeliveryService(repo, settings, http=http)
        [job] = await entry_jobs(repo)
        await delivery.deliver(job["payload"])
        await delivery.deliver(job["payload"])
    assert len(requests) == 1 and requests[0]["chat_id"] == "ops"
    assert requests[0]["text"] == html.escape(notice["text"])
