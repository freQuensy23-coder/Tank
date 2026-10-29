import logging
from types import SimpleNamespace

import httpx
import pytest
from test_sources import RSS, Repo, settings

from tank.scheduler import Scheduler
from tank.sources import SourceService
from tank.sources.polling import RECENT_READS, count_read
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


def test_recent_reads_keep_only_the_last_window():
    state = {}
    count_read(state, "reads", ok=False, n=3)
    count_read(state, "detail_reads", ok=True, n=RECENT_READS + 7)
    count_read(state, "detail_reads", ok=False, n=0)
    assert state["recent_reads"] == [True] * RECENT_READS
    count_read(state, "reads", ok=False)
    assert len(state["recent_reads"]) == RECENT_READS and state["recent_reads"][-1] is False
    assert state["reads"] == {"ok": 0, "failed": 4}
    assert state["detail_reads"] == {"ok": RECENT_READS + 7, "failed": 0}


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
