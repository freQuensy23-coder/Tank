import asyncio
import os
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from test_sources import settings

from tank.sources import SourceService

pytestmark = [pytest.mark.integration, pytest.mark.postgres, pytest.mark.asyncio]
START = datetime(2026, 1, 1, tzinfo=UTC)


async def counts(repo):
    async with repo.transaction():
        rows = await (
            await repo._conn().execute(
                "SELECT bucket_start, source_mode, feed_kind, successful, failed FROM ops.source_poll_counts"
            )
        ).fetchall()
    return {
        (r["bucket_start"], r["source_mode"], r["feed_kind"]): (r["successful"], r["failed"]) for r in rows
    }


async def test_completed_polls_survive_resets_without_replaying_checkpoint_history(
    tmp_path, postgres_template, monkeypatch
):
    source = {
        "source_id": "S30",
        "mode": "REST",
        "feeds": [{"format": "RSS", "url": f"https://publisher.example/{n}"} for n in range(3)],
    }
    detail = {
        "source_id": "S20",
        "mode": "PAGE",
        "feeds": [
            {"kind": "html", "url": "https://publisher.example/news", "article_pattern": "^/news/item$"}
        ],
    }

    def respond(request):
        if request.url.path == "/news":
            return httpx.Response(200, text='<a href="/news/item">Release</a>')
        return httpx.Response(304 if request.url.path in {"/0", "/1"} else 404)

    async with (
        postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo,
        httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http,
    ):
        svc = SourceService(repo, settings(tmp_path, [source, detail]), http=http)
        now = START + timedelta(minutes=4, seconds=59)
        monkeypatch.setattr(svc.poller, "now", lambda: now)
        await repo.put(
            "ops.source_checkpoints", "S30:0", {"source_id": "S30", "reads": {"ok": 900, "failed": 70}}
        )
        assert await counts(repo) == {}
        outcomes = await asyncio.gather(svc.poll_once("S30"), svc.poll_once("S20"))
        assert all(len(outcome["errors"]) == 1 for outcome in outcomes)
        assert (await repo.get("ops.source_checkpoints", "S20:0"))["detail_reads"]["failed"] == 1
        first = {(START, "REST", "rss"): (2, 1), (START, "PAGE", "html"): (1, 0)}
        await svc.poll_once("S30", due_only=True)
        assert await counts(repo) == first
        await repo.put("ops.source_checkpoints", "S30:0", {"source_id": "S30"})
        assert await counts(repo) == first
        now = START + timedelta(minutes=15)
        restarted = SourceService(repo, settings(tmp_path, [dict(source, mode="NEW")]), http=http)
        monkeypatch.setattr(restarted.poller, "now", lambda: now)
        assert len((await restarted.poll_once("S30"))["errors"]) == 1
        assert await counts(repo) == {**first, (now, "NEW", "rss"): (2, 1)}
        assert (await repo.get("ops.source_checkpoints", "S30:0"))["reads"] == {"ok": 1, "failed": 0}
        await restarted.close()
        await svc.close()


@pytest.mark.parametrize("fault", ["missing", "locked"])
async def test_history_write_failure_does_not_change_feed_result(tmp_path, postgres_template, caplog, fault):
    source = {"source_id": "S30", "feeds": [{"kind": "rss", "url": "https://publisher.example/rss"}]}
    async with (
        postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo,
        httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(304))) as http,
    ):
        svc = SourceService(repo, settings(tmp_path, [source]), http=http)
        if fault == "missing":
            async with repo.transaction():
                await repo._conn().execute("DROP TABLE ops.source_poll_counts")
            result = await svc.poll_once("S30")
        else:
            async with repo.transaction():
                await repo._conn().execute("LOCK TABLE ops.source_poll_counts IN ACCESS EXCLUSIVE MODE")
                result = await asyncio.wait_for(svc.poll_once("S30"), timeout=5)
            assert await counts(repo) == {}
        assert result["status"] == "HEALTHY"
        assert (await repo.get("ops.source_checkpoints", "S30:0"))["reads"] == {"ok": 1, "failed": 0}
        assert "Feed poll history not recorded for S30:0" in caplog.text
        await svc.close()
