"""Full snapshots retain the volume series of the target and the registry peers of its sector."""

from datetime import timedelta

import pytest
import yaml

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
):
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
    service = MarketService(repo, settings)
    try:
        current = await service.snapshot(target["token_id"], end, anchor={"type": "T1", "ts": iso(start)})
        assert "stat_price_series" not in current
        if long_event:
            independently_read = await service._condition_kline_series(
                target,
                end - timedelta(days=90),
                end,
                current["assembled_at"],
                kind="volume",
                coverage_count=current["series_coverage_counts"][target["instrument_id"]],
                fixed_window=True,
            )
            assert current["stat_volume_series"][target["instrument_id"]] == independently_read
        counts = current["series_coverage_counts"]
        assert {"binance:spot:PEER1", "binance:spot:PEER2"} <= set(counts)
        assert {
            i["instrument_id"] for i in current["instruments"] if i["token_id"].startswith("test:PEER")
        } == {
            "binance:spot:PEER1",
            "binance:spot:PEER2",
            "okx:spot:PEER1",
        }
    finally:
        await service.close()
