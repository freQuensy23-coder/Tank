import asyncio
import html
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from delivery_support import delivery_job
from source_support import fixture_versions, source_definition, source_feed
from test_session_binding_concurrency import fixture_repo
from test_sources import RSS
from test_sources import settings as source_settings

from tank import ops_entry_errors
from tank.delivery import DeliveryService
from tank.ops_entry_errors import MESSAGE_LIMIT, report_entry_errors, units
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


async def poll(repo, tmp_path: Path, payloads, *, enabled=True, replay=False, source=SOURCE):
    settings = source_settings(tmp_path, [source])
    settings.ops_progress_enabled = enabled
    settings.replay = replay
    feed = iter(payloads)

    def handle(request: httpx.Request):
        return httpx.Response(200, content=next(feed))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SourceService(repo, settings, http=http, versions=fixture_versions())
        results = [await service.poll_once(source["source_id"]) for _ in payloads]
    return results


async def test_first_poll_reports_unparsed_entry_once_with_identity_and_hashes(tmp_path: Path) -> None:
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


async def test_repeat_polls_do_not_report_same_entry_and_new_entry_is_reported_alone(tmp_path: Path) -> None:
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


async def test_burst_of_broken_entries_is_one_message_clipped_to_telegram_limit(tmp_path: Path) -> None:
    repo = MemoryRepository()
    titles = [f"Broken entry {n} " + "\U0001f680" * 150 for n in range(60)]
    await poll(repo, tmp_path, [broken(*titles)])
    [notice] = await entry_notices(repo)
    assert len(notice["entry_errors"]) == 60 and len(await entry_jobs(repo)) == 1
    assert units(html.escape(notice["text"])) <= MESSAGE_LIMIT
    assert "Новых записей с ошибкой: 60" in notice["text"]
    assert "полный список: out.ops_notices " + notice["notice_id"] in notice["text"]


@pytest.mark.parametrize("enabled,replay", [(False, False), (True, True)])
async def test_disabled_or_replay_reports_nothing(tmp_path: Path, enabled, replay) -> None:
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Bad")], enabled=enabled, replay=replay)
    assert await entry_notices(repo) == [] and await entry_jobs(repo) == []
    checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
    assert checkpoint["entry_errors"]


async def test_restart_and_concurrent_reports_are_idempotent(tmp_path: Path) -> None:
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Bad")])
    await poll(repo, tmp_path, [broken("Bad")])
    assert len(await entry_notices(repo)) == 1
    settings = SimpleNamespace(ops_progress_enabled=True, replay=False)
    errors = [{"reason": "PENDING_PROPOSAL_UNAVAILABLE", "proposal_id": "0xabc", "payload_sha256": "a" * 64}]
    source = {"source_id": "S24", "name": "Snapshot"}
    feed = {"url": "https://hub.snapshot.org/graphql"}
    with pytest.raises(ValueError, match="rollback"):
        async with repo.transaction():
            await report_entry_errors(
                repo, settings, source_definition(source), source_feed(feed), errors, "2026-10-24T12:00:00Z"
            )
            raise ValueError("rollback")
    assert len(await entry_notices(repo)) == 1
    ids = await asyncio.gather(
        *(
            report_entry_errors(
                repo, settings, source_definition(source), source_feed(feed), errors, "2026-10-24T12:00:00Z"
            )
            for _ in range(4)
        )
    )
    assert len([notice_id for notice_id in ids if notice_id]) == 1
    assert len(await entry_notices(repo)) == 2 and len(await entry_jobs(repo)) == 2
    [snapshot] = [row for row in await entry_notices(repo) if row["source_id"] == "S24"]
    assert "предложение 0xabc" in snapshot["text"]


async def test_publisher_json_entry_error_is_reported_and_delivered_to_ops(tmp_path: Path) -> None:
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

    def respond(request: httpx.Request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7, "date": 1}})

    settings = SimpleNamespace(
        ops_progress_enabled=True,
        telegram_token="test",
        telegram_alerts_chat="alerts",
        telegram_ops_chat="ops",
        replay=False,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        delivery = DeliveryService(repo, settings, http=http)
        [job] = await entry_jobs(repo)
        await delivery.deliver(delivery_job(job["payload"]))
        await delivery.deliver(delivery_job(job["payload"]))
    assert len(requests) == 1 and requests[0]["chat_id"] == "ops"
    assert requests[0]["text"] == html.escape(notice["text"])


async def test_x_account_failures_are_keyed_by_account() -> None:
    repo = MemoryRepository()
    settings = SimpleNamespace(ops_progress_enabled=True, replay=False)
    source = {"source_id": "S29", "name": "X"}
    feed = {"url": "https://api.apify.com/v2/actors/test/run-sync", "kind": "x"}
    first = [
        {"account": "alice", "reason": "MISSING_FROM_RUN"},
        {"account": "bob", "reason": "MISSING_FROM_RUN"},
        {"account": None, "reason": "RUN_DEGRADED"},
    ]
    await report_entry_errors(
        repo, settings, source_definition(source), source_feed(feed), first, "2026-10-24T12:00:00Z"
    )
    [notice] = await entry_notices(repo)
    assert len(notice["entry_errors"]) == 3
    assert "аккаунт @alice" in notice["text"] and "аккаунт @bob" in notice["text"]
    assert "весь запуск сбора X" in notice["text"]
    again = first + [{"account": "carol", "reason": "MISSING_FROM_RUN"}]
    await report_entry_errors(
        repo, settings, source_definition(source), source_feed(feed), again, "2026-10-24T12:05:00Z"
    )
    latest = [row for row in await entry_notices(repo) if row["notice_id"] != notice["notice_id"]]
    assert [row["entry_errors"] for row in latest] == [[{"account": "carol", "reason": "MISSING_FROM_RUN"}]]


@pytest.mark.parametrize("second", ["broken", "recovered"])
async def test_failed_report_does_not_fail_acquisition_and_is_retried_next_poll(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, second
) -> None:
    repo = MemoryRepository()
    report = ops_entry_errors.report_entry_errors
    calls = []

    async def flaky(*args):
        calls.append(args)
        if len(calls) == 1:
            raise RuntimeError("database unavailable")
        return await report(*args)

    monkeypatch.setattr("tank.sources.polling.report_entry_errors", flaky)
    results = await poll(repo, tmp_path, [broken("Bad"), broken("Bad") if second == "broken" else RSS])
    assert [result["items"] for result in results] == [1, 0]
    [notice] = await entry_notices(repo)
    assert "«Bad»" in notice["text"] and len(await entry_jobs(repo)) == 1
    checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
    assert checkpoint["entry_error_reports"] == [] and not checkpoint.get("acquisition_error")
    assert bool(checkpoint["entry_errors"]) == (second == "broken")
    await poll(repo, tmp_path, [RSS])
    assert len(await entry_notices(repo)) == 1


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_entry_error_notices_persist_and_deduplicate_in_storage(backend, postgres_template) -> None:
    async with fixture_repo(backend, postgres_template) as repo:
        settings = SimpleNamespace(ops_progress_enabled=True, replay=False)
        source = {"source_id": "S30", "name": "Fixture blog"}
        errors = [{"reason": "ENTRY_WITHOUT_ID_OR_URL", "entry_sha256": "e" * 64, "payload_sha256": "a" * 64}]
        ids = await asyncio.gather(
            *(
                report_entry_errors(
                    repo,
                    settings,
                    source_definition(source),
                    source_feed({"url": FEED_URL}),
                    errors,
                    "2026-10-24T12:00:00Z",
                )
                for _ in range(3)
            )
        )
        assert len([notice_id for notice_id in ids if notice_id]) == 1
        assert len(await entry_notices(repo)) == 1
        assert len(await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERROR"})) == 1
        assert len(await entry_jobs(repo)) == 1
