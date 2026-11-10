"""Reaction pack: BTC/ETH benchmark values (lead ruling on 03 §8.1) and coalesced late revisions (03 §8.6)."""

from datetime import timedelta
from decimal import Decimal

import pytest

import tank.analysis.truth as truth_module
from tank.clickhouse_market import load_snapshot
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


def klines(token, iid, end, minutes=180):
    return [
        {
            "instrument_id": iid,
            "token_id": token,
            "market_type": "SPOT",
            "open_time": iso(end - timedelta(minutes=minutes - i)),
            "interval_s": 60,
            "complete": True,
            "c": str(100 + i),
            "quote_volume_usd": "1000",
        }
        for i in range(minutes)
    ]


class KlineMarket:
    instruments = {
        "binance:spot:BTCUSDT": {"token_id": "bitcoin:BTC"},
        "binance:perp:BTCUSDT": {"token_id": "bitcoin:BTC"},
    }

    def __init__(self, end):
        self.end = end
        self.changes = []
        self.snapshots = 0

    async def snapshot(self, token_id, cutoff, *, anchor=None):
        self.snapshots += 1
        return {"token_id": token_id, "klines": klines(token_id, "binance:spot:BTCUSDT", self.end)}

    async def capture_changes(self, since=None, end=None):
        for change in self.changes:
            yield change


async def seed(repo, anchor, analysis_set):
    await repo.put(
        "evt.events", "e", {"event_id": "e", "public_at": iso(anchor), "known_to_us_at": iso(anchor)}
    )
    await repo.put(
        "evt.beneficiary_maps", "e:1", {"event_id": "e", "map_version": 1, "analysis_set": analysis_set}
    )


@pytest.mark.integration
@pytest.mark.parametrize("in_analysis_set", [False, True])
async def test_benchmark_checkpoint_uses_base_klines_only_outside_analysis_set(
    tmp_path, monkeypatch, market_clickhouse, in_analysis_set
):
    end = utcnow().replace(second=0, microsecond=0)
    anchor = end - timedelta(hours=2)
    repo = MemoryRepository()
    repo.clickhouse = market_clickhouse
    await seed(repo, anchor, ["test:T", "bitcoin:BTC"] if in_analysis_set else ["test:T"])
    monkeypatch.setattr(truth_module, "compute_market_truth", lambda snapshot, config=None: {})
    market = KlineMarket(end)
    await load_snapshot(market_clickhouse, {"klines": klines("bitcoin:BTC", "binance:spot:BTCUSDT", end)})
    scheduler = Scheduler(repo, Settings(_env_file=None, data_dir=tmp_path), market, None)
    payload = {
        "event_id": "e",
        "token_id": "bitcoin:BTC",
        "checkpoint": "POST_1H",
        "anchor": {"type": "T1", "ts": iso(anchor), "ts_prec": "EXACT"},
    }
    data = (await scheduler._measure_reaction(payload))["checkpoint_data"]
    assert {"oi_native_start", "oi_native_end", "oi_venues"} <= set(data)
    if in_analysis_set:
        assert "benchmark" not in data and data["spot_method"] == "NONE"
    else:
        assert data["spot_method"] == "NOT_APPLICABLE" and data["spot_cvd"] is None
        assert market.snapshots == 0
        assert data["benchmark"]["r_raw_venue"] == "binance:spot:BTCUSDT"

        assert data["r_raw"] == pytest.approx((100 + 119) / (100 + 59) - 1)
    assert await scheduler.analysis_tokens(await repo.get("evt.events", "e")) >= {
        "bitcoin:BTC",
        "ethereum:ETH",
    }


class StatementClient:
    def __init__(self, row):
        self.row, self.sent = row, []

    async def rows(self, query, params=None):
        self.sent.append((query, params))
        return [self.row]


async def benchmark_scheduler(tmp_path, repo):
    end = utcnow().replace(second=0, microsecond=0)
    anchor = end - timedelta(hours=2)
    await seed(repo, anchor, ["test:T"])
    scheduler = Scheduler(repo, Settings(_env_file=None, data_dir=tmp_path), KlineMarket(end), None)
    payload = {
        "event_id": "e",
        "token_id": "bitcoin:BTC",
        "checkpoint": "POST_1H",
        "anchor": {"type": "T1", "ts": iso(anchor), "ts_prec": "EXACT"},
    }
    return scheduler, payload, anchor


async def test_benchmark_checkpoint_sends_the_spot_instruments_and_maps_the_returned_scalars(tmp_path):
    repo = MemoryRepository()
    repo.clickhouse = StatementClient(
        {
            "venue": "binance:spot:BTCUSDT",
            "quote_volume_24h": 120000.0,
            "price_start": "159.50",
            "price_start_observed_at": "2026-11-09T15:55:00.000000Z",
            "price_end": "219",
            "price_end_observed_at": "2026-11-09T16:55:00.000000Z",
            "r_raw": 0.37,
            "venues": 1,
        }
    )
    scheduler, payload, anchor = await benchmark_scheduler(tmp_path, repo)
    measured = await scheduler._measure_reaction(payload)
    data = measured["checkpoint_data"]
    ((query, params),) = repo.clickhouse.sent
    assert "mkt.klines_1m" in query
    assert params["instrument_ids"] == ("['binance:spot:BTCUSDT']", "Array(String)")
    assert params["window_start"][0] == anchor.strftime("%Y-%m-%d %H:%M:%S.%f")
    assert data["r_raw"] == 0.37 and data["spot_method"] == "NOT_APPLICABLE"
    assert data["benchmark"]["r_raw_venue"] == "binance:spot:BTCUSDT"
    assert data["benchmark"]["price_start"] == Decimal("159.50")
    assert data["benchmark"]["price_end_observed_at"] == "2026-11-09T16:55:00Z"
    assert measured["truth"] == {"benchmark": data["benchmark"]}


async def test_benchmark_checkpoint_without_clickhouse_is_an_error(tmp_path):
    scheduler, payload, _ = await benchmark_scheduler(tmp_path, MemoryRepository())
    with pytest.raises(RuntimeError, match="stored in ClickHouse only"):
        await scheduler._measure_reaction(payload)


async def test_late_changes_keep_one_queued_revision_per_checkpoint(tmp_path):
    end = utcnow().replace(second=0, microsecond=0)
    anchor = end - timedelta(hours=2)
    repo = MemoryRepository()
    await seed(repo, anchor, ["test:T"])
    for label in ("PRE_1H", "POST_1H"):
        await repo.put(
            "out.reaction_checkpoints",
            f"e:test:T:{label}:1",
            {
                "event_id": "e",
                "token_id": "test:T",
                "checkpoint": label,
                "revision": 1,
                "captured_at": iso(end - timedelta(minutes=30)),
                "anchor": {"type": "T1", "ts": iso(anchor)},
            },
        )
    market = KlineMarket(end)
    scheduler = Scheduler(repo, Settings(_env_file=None, data_dir=tmp_path), market, None)
    for n in range(3):
        market.changes = [
            {
                "event_id": "e",
                "token_id": "test:T",
                "anchor": iso(anchor),
                "observed_at": iso(end - timedelta(minutes=10 - n)),
                "change_id": f"CAP-LIVE-{n}",
                "status": "AVAILABLE",
            }
        ]
        await scheduler.detect_late_data(end + timedelta(minutes=n))
    jobs = [j async for j in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})]
    assert sorted(j["payload"]["checkpoint"] for j in jobs) == ["POST_1H", "PRE_1H"]

    for job in jobs:
        await repo.put("ops.jobs", job["job_id"], {**job, "status": "DONE"})
    market.changes = [dict(market.changes[0], change_id="CAP-LIVE-9", observed_at=iso(end))]
    await scheduler.detect_late_data(end + timedelta(minutes=9))
    jobs = [
        j async for j in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE", "status": "QUEUED"})
    ]
    assert sorted(j["payload"]["checkpoint"] for j in jobs) == ["POST_1H", "PRE_1H"]
