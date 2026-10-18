"""Actual HTTP capture producer → durable timer → immutable reaction revision."""

import asyncio
from datetime import timedelta

import httpx
import pytest

from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


@pytest.mark.asyncio
async def test_backfill_changes_append_reaction_revision_once_and_preserve_original(tmp_path):
    anchor = utcnow().replace(second=0, microsecond=0) - timedelta(hours=2)
    inst = {
        "instrument_id": "binance:spot:TUSD",
        "token_id": "test:T",
        "venue": "binance",
        "market_type": "SPOT",
        "venue_symbol": "TUSD",
        "quote_asset": "USD",
        "active": True,
        "contract_multiplier": "1",
        "tick_size": ".01",
        "lot_size": ".01",
    }

    class FixtureSettings(Settings):
        def load_config(self, name):
            if name == "registry":
                return {"tokens": [{"token_id": "test:T", "symbol": "T"}], "instruments": [inst]}
            if name == "semantics":
                return {"metrics": []}
            return super().load_config(name)

    settings = FixtureSettings(
        _env_file=None, data_dir=tmp_path, replay=True, config_version="fixture", code_version="fixture"
    )
    requests = []

    def external(req):
        requests.append(req.url.path)
        if req.url.path.endswith("aggTrades"):
            start = int(req.url.params["startTime"])
            end = int(req.url.params["endTime"])
            return httpx.Response(
                200,
                json=[
                    {
                        "a": ms // 60000,
                        "p": "100"
                        if ms < int((anchor - timedelta(minutes=30)).timestamp() * 1000)
                        else "110",
                        "q": "1",
                        "T": ms,
                        "m": False,
                    }
                    for ms in range(start, end, 60000)
                ],
            )
        if req.url.path.endswith("klines"):
            start = int(req.url.params["startTime"])
            end = int(req.url.params["endTime"])
            rows = []
            for ms in range(start, end, 60000):
                close = "100" if ms < int((anchor - timedelta(minutes=30)).timestamp() * 1000) else "110"
                rows.append([ms, close, close, close, close, "0", ms + 59999, "0", 0, "0", "0", "0"])
            return httpx.Response(200, json=rows)
        raise AssertionError("Unexpected external endpoint: " + req.url.path)

    repo = MemoryRepository()
    await repo.put(
        "evt.events", "e", {"event_id": "e", "known_to_us_at": iso(anchor), "public_at": iso(anchor)}
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(external)) as http:
        market = MarketService(repo, settings, http=http)
        scheduler = Scheduler(repo, settings, market, None)
        payload = {
            "event_id": "e",
            "token_id": "test:T",
            "checkpoint": "PRE_1H",
            "anchor": {"type": "T1", "ts": iso(anchor)},
            "retro": True,
        }
        original = await scheduler.reaction_capture(payload)
        assert original["r_raw"] is None and original["revision"] == 1
        # The actual producer acquires historical HTTP data and persists terminal capture state.
        await market._schedule_event_backfill("e", "test:T", iso(anchor))
        async with asyncio.timeout(10):
            while (await market.capture_readiness("e", "test:T", anchor=iso(anchor)))["status"] not in {
                "COMPLETE",
                "PARTIAL",
                "FAILED",
            }:
                await asyncio.sleep(0.005)
        state = await market.capture_readiness("e", "test:T", anchor=iso(anchor))
        assert state["status"] in {"COMPLETE", "PARTIAL"} and "klines" in requests[-1]
        await scheduler.detect_late_data(utcnow())
        jobs = [j async for j in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})]
        assert jobs and all(job["payload"]["automatic_revision"] for job in jobs)
        for job in jobs:
            await scheduler.reaction_capture(job["payload"])
        revised = await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:2")
        assert revised["revision"] == 2 and revised["r_raw"] is not None
        assert revised["rwp_id"] == original["rwp_id"] and revised["rwp_id"].startswith("RWP-")
        assert revised["reason"].startswith("Late capture completed:")
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:1") == original
        await scheduler.detect_late_data(utcnow())
        await scheduler.reaction_capture(jobs[0]["payload"])
        assert len([row async for row in repo.iter_all("out.reaction_checkpoints")]) == 2
        # A fresh capture change with identical semantic outputs is acknowledged without another revision.
        unchanged = dict(
            jobs[0]["payload"],
            change_id=jobs[0]["payload"]["change_id"] + ":same",
            reason="Late duplicate capture",
        )
        result = await scheduler.reaction_capture(unchanged)
        assert result["changed"] is False
        assert len([row async for row in repo.iter_all("out.reaction_checkpoints")]) == 2
        # Ordinary live late print follows the same actual ingest→change feed→revision path.
        late = {
            "a": int(anchor.timestamp() * 1000),
            "p": "120",
            "q": "1",
            "T": int(anchor.timestamp() * 1000) - 1,
            "m": False,
        }
        assert await market.ingest_trade(inst, late)
        await scheduler.detect_late_data(utcnow())
        previous_ids = {job["job_id"] for job in jobs}
        live_jobs = [
            job
            async for job in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})
            if job["job_id"] not in previous_ids
        ]
        assert len(live_jobs) == 1 and "CAP-LIVE-" in live_jobs[0]["payload"]["change_id"]
        runtime = object.__new__(Runtime)
        runtime.repo = repo
        runtime.handlers = {"REACTION_CAPTURE": scheduler.reaction_capture}
        runtime.active_jobs = set()
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            async with asyncio.timeout(5):
                while (await repo.get("ops.jobs", live_jobs[0]["job_id"]))["status"] != "DONE":
                    await asyncio.sleep(0.005)
            live = (await repo.get("ops.jobs", live_jobs[0]["job_id"]))["result"]
        finally:
            stop.set()
            await worker
        assert live["revision"] == 3 and live["data_fingerprint"] != revised["data_fingerprint"]
        assert live["rwp_id"] == original["rwp_id"]
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:1") == original
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:2") == revised
        # Exact duplicate frames do not create a new availability/revision watermark.
        assert not await market.ingest_trade(inst, late)
        await scheduler.detect_late_data(utcnow())
        assert (
            len([job async for job in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})])
            == len(jobs) + 1
        )
        await market.close()
        await scheduler.close()
