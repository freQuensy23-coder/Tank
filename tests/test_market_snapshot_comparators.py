"""Full snapshots retain exact sector returns without dense unrelated histories."""

from copy import deepcopy
from datetime import timedelta
from decimal import Decimal

import pytest
import yaml
from source_support import fixture_http_admission, fixture_versions

from tank.analysis.truth import relative_truth
from tank.market.service import MarketService
from tank.util import iso, parse_time


@pytest.mark.asyncio
@pytest.mark.parametrize("target_gap", [False, True])
@pytest.mark.parametrize("long_event", [False, True])
@pytest.mark.parametrize("invalid_peer_close", [False, True])
async def test_comparator_projection_matches_dense_reader_with_sparse_boundaries_and_venue_ranking(
    setup,
    target_gap,
    long_event,
    invalid_peer_close,
) -> None:
    repo, settings, target = setup
    path = settings.config_dir / "registry.yaml"
    registry = yaml.safe_load(path.read_text())
    for symbol in ("PEER1", "PEER2"):
        token = "test:" + symbol
        registry["tokens"].append({"token_id": token, "symbol": symbol, "sector": "A"})
        registry["instruments"].append(
            {**target, "instrument_id": "binance:spot:" + symbol, "token_id": token, "venue_symbol": symbol}
        )
    registry["instruments"].append(
        {
            **target,
            "instrument_id": "okx:spot:PEER1",
            "token_id": "test:PEER1",
            "venue": "okx",
            "venue_symbol": "PEER1",
        }
    )
    path.write_text(yaml.safe_dump(registry, sort_keys=False))
    end = parse_time("2026-10-16T12:00:00Z")
    start = end - (timedelta(days=60) if long_event else timedelta(hours=4, seconds=17))
    offsets = [-2900, -1441, -1440, -242, -241, -240, -61, -60, -16, -15, -2, -1, 0]
    rows = []
    for index, inst in enumerate(registry["instruments"]):
        selected = offsets if inst["venue"] == "binance" else offsets[-5:]
        if target_gap and inst["instrument_id"] == target["instrument_id"]:
            selected = [-3001, -2941, -2901]
        for n, offset in enumerate(selected):
            value = str(100 + (index + 1) * n)
            rows.append(
                {
                    "instrument_id": inst["instrument_id"],
                    "token_id": inst["token_id"],
                    "open_time": iso(end + timedelta(minutes=offset)),
                    "open_time_prec": "BUCKET(60s)",
                    "interval_s": 60,
                    "o": value,
                    "h": value,
                    "l": value,
                    "c": value,
                    "complete": True,
                    "volume_native": "10",
                }
            )
    if invalid_peer_close:
        for row in rows:
            if row["instrument_id"] == "binance:spot:PEER1":
                age = (end - parse_time(row["open_time"])).total_seconds()
                if age == 60:
                    row["c"] = "0"
                elif age == 120:
                    row["c"] = None
                elif age == 900:
                    row["complete"] = False
    await repo.put_market_batch("mkt.klines_1m", rows)
    flow = [
        {
            "instrument_id": target["instrument_id"],
            "token_id": target["token_id"],
            "minute_start": iso(end - timedelta(minutes=m)),
            "minute_start_prec": "BUCKET(60s)",
            "complete": True,
            "buy_qty": "2",
            "sell_qty": "1",
            "sum_sq_qty": "1",
            "buy_notional": "200",
            "sell_notional": "100",
        }
        for m in range(1, 1441)
    ]
    await repo.put_market_batch("mkt.flow_bars_1m", flow)
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    try:
        current = await service.snapshot(target["token_id"], end, anchor={"type": "T1", "ts": iso(start)})
        if long_event:
            for kind, field in (("price", "stat_price_series"), ("volume", "stat_volume_series")):
                independently_read = await service._condition_kline_series(
                    target,
                    end - timedelta(days=90),
                    end,
                    current["assembled_at"],
                    kind=kind,
                    coverage_count=current["series_coverage_counts"][target["instrument_id"]],
                    fixed_window=True,
                )
                assert current[field][target["instrument_id"]] == independently_read
        dense = deepcopy(current)
        dense.pop("stat_price_series")
        dense.pop("series_coverage_counts")
        for inst in registry["instruments"]:
            prior = await service._range(
                "mkt.klines_1m",
                "open_time",
                start=end - timedelta(days=90),
                end=end,
                filters={"instrument_id": inst["instrument_id"]},
            )
            dense["klines"] = [r for r in dense["klines"] if r["instrument_id"] != inst["instrument_id"]]
            dense["klines"].extend(
                r for r in prior if parse_time(r["open_time"]) + timedelta(minutes=1) <= end
            )
        expected = relative_truth(dense, start, end)
        actual = relative_truth(current, start, end)
        assert actual == expected
        compared = actual["windows"]["W4H"]
        if invalid_peer_close:
            valid_peer_closes = [
                parse_time(row["open_time"]) + timedelta(seconds=row["interval_s"])
                for row in rows
                if row["instrument_id"] == "binance:spot:PEER1"
                and row["complete"] is True
                and row["c"] is not None
                and Decimal(row["c"]) > 0
                and parse_time(row["open_time"]) + timedelta(seconds=row["interval_s"]) <= end
            ]
            assert max(valid_peer_closes) == end - timedelta(minutes=15)
            assert compared["r_raw"] is not None
            assert compared["r_sector_adj"] is None
            assert compared["sector_reason"] == "NOT_ESTIMABLE"
        else:
            assert compared["r_sector_adj"] is not None
            assert compared["sector_reason"] is None
        if target_gap:
            assert actual["close_1h_price"] is not None
        peers = {
            k: v for k, v in current["stat_price_series"].items() if v["token_id"].startswith("test:PEER")
        }
        assert set(peers) == {"binance:spot:PEER1", "binance:spot:PEER2"}
        assert sum(len(v["times_us"]) for v in peers.values()) <= 12
        assert len([r for r in current["klines"] if r["token_id"].startswith("test:PEER")]) == 3
    finally:
        await service.close()
