import logging
from types import SimpleNamespace

import httpx
import pytest
from test_sources import RSS, Repo, settings
from test_x_diagnostics import diagnostics, failed_run
from test_xsensor import URL

from tank.scheduler import Scheduler
from tank.sources import SourceService
from tank.sources.polling import RECENT_READS, remember_reads
from tank.storage import MemoryRepository
from tank.util import utcnow


@pytest.fixture
def no_retry_wait(monkeypatch):
    import tank.sources as module

    async def no_wait(delay):
        return None

    monkeypatch.setattr(module.asyncio, "sleep", no_wait)


@pytest.mark.asyncio
async def test_chronic_feed_failure_is_logged_and_kept_in_recent_reads(tmp_path, caplog, no_retry_wait):
    fail = [True]

    def handle(request):
        return httpx.Response(503, content=b"down") if fail[0] else httpx.Response(200, content=RSS)

    sources = [{"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}]
    repo = Repo()
    caplog.set_level(logging.WARNING, logger="tank.sources.polling")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, sources), http=http)
        for _ in range(3):
            await svc.poll_once("S30")
        fail[0] = False
        await svc.poll_once("S30")
        await svc.close()
    state = await repo.get("ops.source_checkpoints", "S30:0")
    assert state["status"] == "HEALTHY"
    assert state["recent_reads"].count(False) == 3 and state["recent_reads"][:3] == [False] * 3
    failures = [r for r in caplog.records if "S30:0 read failed" in r.getMessage()]
    assert len(failures) == 3 and all(r.levelno == logging.WARNING for r in failures)
    assert "503" in failures[0].getMessage()


def test_recent_reads_keep_the_last_window_in_order():
    state = {"reads": {"ok": 2, "failed": 1}}
    remember_reads(state, [False] * 3)
    remember_reads(state, [True] * (RECENT_READS - 1))
    assert state["recent_reads"] == [False] * 1 + [True] * (RECENT_READS - 1)
    remember_reads(state, [])
    remember_reads(state, [False])
    assert state["recent_reads"] == [True] * (RECENT_READS - 1) + [False]
    assert state["reads"] == {"ok": 2, "failed": 1}


@pytest.mark.asyncio
async def test_detail_pass_keeps_early_failures_out_of_a_later_successful_window(tmp_path, no_retry_wait):
    links = "".join(f'<a href="/newsroom/r{i}">Release {i}</a>' for i in range(RECENT_READS + 10))

    def handle(request):
        if request.url.path == "/newsroom/":
            return httpx.Response(200, text=links)
        number = int(request.url.path.rsplit("r", 1)[1])
        if number < 5:
            return httpx.Response(404, text="<html>Publisher removed this article</html>")
        return httpx.Response(
            200, text=f"<article><h1>Release {number}</h1><p>Visible evidence number {number}.</p></article>"
        )

    sources = [
        {
            "source_id": "S20",
            "feeds": [
                {
                    "kind": "html",
                    "url": "https://publisher.example/newsroom/",
                    "article_pattern": "^/newsroom/r[0-9]+$",
                }
            ],
        }
    ]
    repo = Repo()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        svc = SourceService(repo, settings(tmp_path, sources), http=http)
        result = await svc.poll_once("S20")
        await svc.close()
    state = await repo.get("ops.source_checkpoints", "S20:0")
    assert len(result["errors"]) == 5 and state["detail_reads"] == {"ok": RECENT_READS + 5, "failed": 5}
    assert state["recent_reads"] == [True] * RECENT_READS


@pytest.mark.asyncio
async def test_failed_x_actor_run_is_a_failed_read(tmp_path, caplog):
    config = settings(tmp_path, [{"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]}])
    config.apify_api_token = "apify-secret"
    repo = MemoryRepository()
    caplog.set_level(logging.WARNING, logger="tank.sources.polling")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=failed_run(diagnostics())))
    ) as http:
        service = SourceService(repo, config, http=http)
        service.accounts = {"alice": {"handle": "alice", "role": "OTHER", "entity_id": "alice"}}
        await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "90"}})
        await service.poll_once("S29")
        await service.close()
    state = await repo.get("ops.source_checkpoints", "S29:0")
    assert state["reads"] == {"ok": 0, "failed": 1} and state["recent_reads"] == [False]
    assert state["status"] == "DEGRADED" and state["last_failure_at"]
    assert any("S29:0 read failed: the actor run failed" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_duplicate_merge_is_logged_once_per_external_id(tmp_path, caplog):
    republished = RSS.replace(
        b"</channel>",
        b"<item><guid>release-42-copy</guid><title>New protocol</title><link>https://example.org/a</link>"
        b"<pubDate>Fri, 16 Oct 2026 12:00:00 GMT</pubDate><description>Protocol deployment &amp; vote</description></item></channel>",
    )
    repo = Repo()
    source = {"source_id": "S30", "feeds": [{"url": "https://example.org/rss", "kind": "rss"}]}
    caplog.set_level(logging.WARNING, logger="tank.sources.polling")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=republished))
    ) as http:
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        await svc.poll_once("S30")
        await svc.poll_once("S30")
        await svc.close()
    [stored] = [v for (c, _), v in repo.rows.items() if c == "ingest.source_items"]
    merges = [r.getMessage() for r in caplog.records if "merged as a duplicate" in r.getMessage()]
    duplicate_id = (await repo.get("ops.source_checkpoints", "S30:0"))["duplicate_ids"][0]
    assert merges == [
        f"Source feed S30:0 entry {duplicate_id} merged as a duplicate into item {stored['item_id']}"
    ]


@pytest.mark.asyncio
async def test_heartbeat_lists_feeds_failing_their_recent_reads():
    repo = MemoryRepository()
    sources = [
        {"source_id": "S29", "feeds": [{"url": "https://example.org/x"}]},
        {"source_id": "S05", "feeds": [{"url": "https://example.org/a"}, {"url": "https://example.org/b"}]},
        {"source_id": "S01", "feeds": [{"url": "https://example.org/c"}]},
    ]
    await repo.put(
        "ops.source_checkpoints",
        "S29:0",
        {"status": "HEALTHY", "recent_reads": [False] * 27 + [True] * 23},
    )
    await repo.put(
        "ops.source_checkpoints", "S05:1", {"status": "HEALTHY", "recent_reads": [False, True] * 5}
    )
    await repo.put(
        "ops.source_checkpoints", "S01:0", {"status": "HEALTHY", "recent_reads": [False] * 4 + [True] * 46}
    )
    config = SimpleNamespace(
        load_config=lambda name: (
            {"sources": sources} if name == "sources" else {"source_failed_read_alarm": 0.1}
        )
    )
    now = utcnow()
    await Scheduler(repo, config, None, None).heartbeat(now)
    notice = await repo.get("out.ops_notices", f"heartbeat_{now.date()}")
    assert notice["failing_source_feeds"] == [
        {"feed": "S29:0", "failed": 27, "reads": 50},
        {"feed": "S05:1", "failed": 5, "reads": 10},
    ]
    assert notice["text"].endswith("; failing source feeds: S29:0 27/50, S05:1 5/10")


@pytest.mark.asyncio
async def test_heartbeat_without_failing_feeds_has_no_source_line():
    repo = MemoryRepository()
    sources = [{"source_id": "S01", "feeds": [{"url": "https://example.org/c"}]}]
    await repo.put("ops.source_checkpoints", "S01:0", {"recent_reads": [True] * 50})
    config = SimpleNamespace(load_config=lambda name: {"sources": sources} if name == "sources" else {})
    now = utcnow()
    await Scheduler(repo, config, None, None).heartbeat(now)
    notice = await repo.get("out.ops_notices", f"heartbeat_{now.date()}")
    assert notice["failing_source_feeds"] == [] and "failing source feeds" not in notice["text"]


def test_failing_feed_alarm_is_configured():
    from tank.settings import Settings, threshold

    assert threshold(Settings(), "source_failed_read_alarm", None) == 0.1
