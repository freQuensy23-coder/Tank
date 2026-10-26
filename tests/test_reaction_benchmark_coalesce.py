"""Reaction pack: BTC/ETH benchmark values (lead ruling on 03 §8.1) and coalesced late revisions (03 §8.6)."""

from datetime import timedelta
from pathlib import Path

import pytest
from source_support import fixture_versions

from tank.contracts.scheduler_commands import InitialReactionCommand, reaction_command_from_wire
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, parse_time, utcnow


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

    def __init__(self, end) -> None:
        self.end = end
        self.changes = []
        self.snapshots = 0

    async def snapshot(self, token_id, cutoff, *, anchor=None, scope=None):
        self.snapshots += 1
        return {"token_id": token_id, "klines": klines(token_id, "binance:spot:BTCUSDT", self.end)}

    async def capture_changes(self, since=None, end=None):
        for change in self.changes:
            observed = parse_time(change["observed_at"])
            if since is not None and observed < parse_time(since):
                continue
            if end is not None and observed >= parse_time(end):
                continue
            yield change


async def seed(repo, anchor, analysis_set) -> None:
    await repo.put(
        "evt.events", "e", {"event_id": "e", "public_at": iso(anchor), "known_to_us_at": iso(anchor)}
    )
    await repo.put(
        "evt.beneficiary_maps", "e:1", {"event_id": "e", "map_version": 1, "analysis_set": analysis_set}
    )


@pytest.mark.parametrize("in_analysis_set", [False, True])
async def test_benchmark_checkpoint_uses_base_klines_only_outside_analysis_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, in_analysis_set
) -> None:
    end = utcnow().replace(second=0, microsecond=0)
    anchor = end - timedelta(hours=2)
    repo = MemoryRepository()
    await seed(repo, anchor, ["test:T", "bitcoin:BTC"] if in_analysis_set else ["test:T"])
    market = KlineMarket(end)
    for row in klines("bitcoin:BTC", "binance:spot:BTCUSDT", end):
        row = {k: v for k, v in row.items() if k != "token_id"}
        await repo.put("mkt.klines_1m", row["instrument_id"] + ":" + row["open_time"], row)
    scheduler = Scheduler(
        repo, Settings(_env_file=None, data_dir=tmp_path), market, None, versions=fixture_versions()
    )
    payload = {
        "event_id": "e",
        "token_id": "bitcoin:BTC",
        "checkpoint": "POST_1H",
        "anchor": {"type": "T1", "ts": iso(anchor), "ts_prec": "EXACT"},
    }
    command = reaction_command_from_wire(payload)
    assert isinstance(command, InitialReactionCommand)
    data = (await scheduler._measure_reaction(command))["checkpoint_data"]
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


async def test_late_changes_keep_one_queued_revision_per_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    scheduler = Scheduler(
        repo, Settings(_env_file=None, data_dir=tmp_path), market, None, versions=fixture_versions()
    )
    for n in range(3):
        market.changes = [
            {
                "event_id": "e",
                "token_id": "test:T",
                "anchor": iso(anchor),
                "observed_at": iso(end + timedelta(minutes=n)),
                "change_id": f"CAP-LIVE-{n}",
                "status": "AVAILABLE",
            }
        ]
        await scheduler.detect_late_data(end + timedelta(minutes=n))
    jobs = [j async for j in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})]
    assert sorted(j["payload"]["checkpoint"] for j in jobs) == ["POST_1H", "PRE_1H"]

    import tank.storage as storage_module

    advanced = end + timedelta(hours=7)
    monkeypatch.setattr(storage_module, "utcnow", lambda: advanced)
    assert await repo.claim_job() is None
    for job in jobs:
        terminal = await repo.get("ops.jobs", job["job_id"])
        assert terminal is not None and terminal["status"] == "EXPIRED"
    market.changes = [dict(market.changes[0], change_id="CAP-LIVE-9", observed_at=iso(advanced))]
    await scheduler.detect_late_data(advanced)
    jobs = [
        j async for j in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE", "status": "QUEUED"})
    ]
    assert sorted(j["payload"]["checkpoint"] for j in jobs) == ["POST_1H", "PRE_1H"]
