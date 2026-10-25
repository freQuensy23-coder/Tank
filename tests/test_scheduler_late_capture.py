"""External historical rows + actual Go late capture → immutable reaction revisions."""

import asyncio
import gzip
import json
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from source_support import fixture_versions

from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import iso, parse_time, utcnow


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.asyncio
async def test_backfill_changes_append_reaction_revision_once_and_preserve_original(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    parity_binary,
    request: pytest.FixtureRequest,
) -> None:
    import tank.scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "LATE_REVISION_DEFER_S", 0)
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

    async def go_capture(name, payload):
        path = tmp_path / (name + ".json.gz")
        with gzip.open(path, "wt") as stream:
            stream.write("\n".join(json.dumps(row) for row in payload) + "\n")
        args = [str(parity_binary), str(path)]
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await process.communicate()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        assert process.returncode == 0, stderr.decode()
        output = json.loads(stdout)
        assert not output["errors"], output["errors"]
        return output

    def external(req) -> None:
        raise AssertionError("Tank core must not fetch exchange data: " + str(req.url))

    async def persist_capture(output, *, observed, minute) -> None:
        for row in output["rows"]:
            await repo.put_market_batch(row["collection"], [row["row"]], keys=[row["key"]])

        bars = [
            {**bar, **collection_clock(observed).fields("recorded_at")}
            for bar in output["bars"]
            if bar["minute_start"] == minute
        ]
        assert len(bars) == 1 and bars[0]["complete"] is True
        if bars:
            await repo.put_market_batch(
                "mkt.flow_bars_1m",
                bars,
                keys=[bar["instrument_id"] + ":" + bar["minute_start"] for bar in bars],
            )

    repo = MemoryRepository()
    await repo.put(
        "evt.events",
        "e",
        {
            "event_id": "e",
            "known_to_us_at": iso(anchor),
            "public_at": iso(anchor),
            "public_at_prec": "EXACT",
            "public_at_prec_reason": None,
        },
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(external)) as http:
        market = MarketService(repo, settings, http=http, versions=fixture_versions())
        scheduler = Scheduler(
            repo,
            settings,
            market,
            None,
            thresholds=Settings(_env_file=None).thresholds(),
            versions=fixture_versions(),
        )
        payload = {
            "event_id": "e",
            "token_id": "test:T",
            "checkpoint": "PRE_1H",
            "anchor": {"type": "T1", "ts": iso(anchor), "ts_prec": "EXACT", "ts_prec_reason": None},
            "retro": True,
        }
        original = await scheduler.reaction_capture(payload)
        assert original["r_raw"] is None and original["revision"] == 1

        obligation = await market._schedule_event_backfill(
            "e", "test:T", chosen_boundary(anchor, precision="EXACT")
        )
        pending = await market.capture_readiness(
            "e", "test:T", anchor=chosen_boundary(anchor, precision="EXACT")
        )
        assert pending.status == "PENDING"
        key = pending.backfill_id
        assert key is not None
        await repo.put_market_batch(
            "mkt.backfill_runs", [{**obligation.model_dump(mode="json"), "status": "RUNNING"}], keys=[key]
        )
        observed = utcnow()

        historical = []
        for minute in range(-1440, 1):
            opened = anchor + timedelta(minutes=minute)
            close = "100" if opened < anchor - timedelta(minutes=30) else "110"
            historical.append(
                {
                    "instrument_id": inst["instrument_id"],
                    "token_id": "test:T",
                    "venue": "binance",
                    "market_type": "SPOT",
                    "open_time": iso(opened),
                    "open_time_prec": "BUCKET(60s)",
                    "open_time_prec_reason": None,
                    "interval_s": 60,
                    "o": close,
                    "h": close,
                    "l": close,
                    "c": close,
                    "volume_native": "1",
                    "quote_volume": close,
                    "quote_volume_usd": close,
                    "quote_asset": "USD",
                    "quote_usd_rate": "1",
                    "complete": True,
                    **collection_clock(observed).fields("received_ts"),
                    **collection_clock(observed).fields("recorded_at"),
                }
            )
        await repo.put_market_batch(
            "mkt.klines_1m",
            historical,
            keys=[row["instrument_id"] + ":" + row["open_time"] for row in historical],
        )

        history_bars = []
        for row in historical:
            end = parse_time(row["open_time"]) + timedelta(minutes=1)
            last_trade = {
                "trade_id": "historical-" + row["open_time"],
                "price": row["c"],
                "ts": iso(end - timedelta(microseconds=1)),
            }
            history_bars.append(
                {
                    "instrument_id": inst["instrument_id"],
                    "token_id": "test:T",
                    "venue": "binance",
                    "market_type": "SPOT",
                    "minute_start": row["open_time"],
                    "minute_start_prec": "BUCKET(60s)",
                    "minute_start_prec_reason": None,
                    "complete": True,
                    "n_buy": 1,
                    "n_sell": 0,
                    "buy_qty": "1",
                    "sell_qty": "0",
                    "buy_notional": row["c"],
                    "sell_notional": "0",
                    "quote_volume": row["c"],
                    "volume_usd": row["c"],
                    "o": row["c"],
                    "h": row["c"],
                    "l": row["c"],
                    "c": row["c"],
                    "sum_sq_qty": "1",
                    "trade_aggregation": "PER_FILL",
                    "last_trade_id": last_trade["trade_id"],
                    "last_trade": last_trade,
                    "reference_points": [{"boundary": iso(end), "complete": True, "last_trade": last_trade}],
                    **collection_clock(observed).fields("received_ts"),
                    **collection_clock(observed).fields("recorded_at"),
                }
            )
        await repo.put_market_batch(
            "mkt.flow_bars_1m",
            history_bars,
            keys=[row["instrument_id"] + ":" + row["minute_start"] for row in history_bars],
        )

        await repo.put_market_batch(
            "mkt.backfill_runs",
            [
                {
                    **await repo.get("mkt.backfill_runs", key),
                    **collection_clock(utcnow()).fields("finished_at"),
                    "status": "PARTIAL",
                    "reason": "ONLY_REACTION_WINDOW_CAPTURED",
                    "result": {"instruments": [], "benchmark_klines": []},
                }
            ],
            keys=[key],
        )
        state = await market.capture_readiness(
            "e", "test:T", anchor=chosen_boundary(anchor, precision="EXACT")
        )
        assert state.status in {"COMPLETE", "PARTIAL"}
        await scheduler.detect_late_data(utcnow())
        jobs = [j async for j in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})]
        assert jobs and all(job["payload"]["automatic_revision"] for job in jobs)
        for job in jobs:
            await scheduler.reaction_capture(job["payload"])

            await repo.put("ops.jobs", job["job_id"], {**job, "status": "DONE"})
        revised = await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:2")
        assert revised["revision"] == 2 and revised["r_raw"] is not None
        assert revised["r_raw"] == pytest.approx(0.1)
        assert revised["rwp_id"] == original["rwp_id"] and revised["rwp_id"].startswith("RWP-")
        assert revised["reason"].startswith("Late capture completed:")
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:1") == original
        await scheduler.detect_late_data(utcnow())
        await scheduler.reaction_capture(jobs[0]["payload"])
        assert len([row async for row in repo.iter_all("out.reaction_checkpoints")]) == 2

        unchanged = dict(
            jobs[0]["payload"],
            change_id=jobs[0]["payload"]["change_id"] + ":same",
            reason="Late duplicate capture",
        )
        result = await scheduler.reaction_capture(unchanged)
        assert result["changed"] is False
        assert len([row async for row in repo.iter_all("out.reaction_checkpoints")]) == 2

        late_observed = utcnow()
        frames = [
            {"recorded_at": iso(late_observed), "instruments": [inst]},
            {
                "t": iso(late_observed),
                "rest": "depth_snapshot",
                "instrument_id": inst["instrument_id"],
                "text": json.dumps({"lastUpdateId": 0, "bids": [["109", "10"]], "asks": [["121", "10"]]}),
            },
        ]
        for trade_id, offset in enumerate((-180000, -120000, -1), start=100):
            ms = int(anchor.timestamp() * 1000) + offset
            trade = {
                "e": "aggTrade",
                "s": "TUSD",
                "E": ms,
                "a": trade_id,
                "p": "120" if offset == -1 else "110",
                "q": "1",
                "T": ms,
                "m": False,
            }
            frames.append(
                {
                    "t": iso(late_observed),
                    "endpoint": "wss://data-stream.binance.vision/stream",
                    "text": json.dumps({"stream": "tusd@aggTrade", "data": trade}),
                }
            )
        late = await go_capture("late", frames)
        assert len(late["trades"]) == 3 and late["bars"]
        late_minute = iso(anchor - timedelta(minutes=1))
        late_bar = next(bar for bar in late["bars"] if bar["minute_start"] == late_minute)
        assert late_bar["complete"] is True and late_bar["last_trade"]["price"] == "120"
        await persist_capture(late, observed=late_observed, minute=late_minute)
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
        assert live["r_raw"] == pytest.approx(0.2)
        assert live["rwp_id"] == original["rwp_id"]
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:1") == original
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:2") == revised

        duplicate = await go_capture("duplicate", frames)
        assert duplicate == late
        await persist_capture(duplicate, observed=late_observed, minute=late_minute)
        await scheduler.detect_late_data(utcnow())
        assert (
            len([job async for job in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})])
            == len(jobs) + 1
        )
        request.node.user_properties.extend(
            [
                ("revision2_r_raw", revised["r_raw"]),
                ("revision3_r_raw", live["r_raw"]),
                ("late_go_bar_complete", late_bar["complete"]),
                ("late_go_last_price", late_bar["last_trade"]["price"]),
                ("normalized_trades", len(late["trades"])),
            ]
        )
        await market.close()
        await scheduler.close()
