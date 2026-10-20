import json
from datetime import UTC, datetime, timedelta

from tank.market.loader_feed import LoaderFeed, dir_name

INSTRUMENT = {
    "instrument_id": "binance:spot:WLDUSDT",
    "token_id": "world:WLD",
    "venue": "binance",
    "market_type": "SPOT",
    "venue_symbol": "WLDUSDT",
    "quote_asset": "USDT",
}
T0 = datetime(2026, 10, 20, 21, 0, tzinfo=UTC)


def agg(a, at, qty, maker):
    ms = int(at.timestamp() * 1000)
    return {"ts_ms": ms, "id": str(a), "raw": {"a": a, "p": "0.56", "q": qty, "f": a, "l": a, "T": ms, "m": maker}}


def write(root, trades, *, sealed, gaps=(), extra=b""):
    d = root / dir_name(INSTRUMENT["instrument_id"])
    d.mkdir(parents=True)
    body = b"".join(json.dumps(t).encode() + b"\n" for t in trades)
    (d / "2026-10-20T21.ndjson").write_bytes(body + extra)
    (d / "WATERMARK.json").write_text(
        json.dumps(
            {
                "covered_from": "2026-10-20T21:00:00Z",
                "sealed_through": sealed,
                "segments": {"2026-10-20T21": len(body)},
                "gaps": list(gaps),
            }
        )
    )


def test_sealed_minutes_become_complete_bars_and_unpublished_bytes_are_ignored(tmp_path):
    trades = [
        agg(1, T0 + timedelta(seconds=5), "10", False),  # taker buy
        agg(2, T0 + timedelta(seconds=50), "4", True),  # taker sell
        agg(3, T0 + timedelta(minutes=2, seconds=1), "7", False),
    ]
    unpublished = json.dumps(agg(4, T0 + timedelta(minutes=2, seconds=30), "999", False)).encode() + b"\n"
    write(tmp_path, trades, sealed="2026-10-20T21:03:00Z", extra=unpublished)
    rows = dict(LoaderFeed(str(tmp_path)).read(INSTRUMENT, T0 + timedelta(minutes=5)))
    assert [k[1] for k in rows] == ["2026-10-20T21:00:00Z", "2026-10-20T21:01:00Z", "2026-10-20T21:02:00Z"]
    first, empty, third = rows.values()
    assert first["complete"] and first["capture_method"] == "TRADES_LOADER" and first["backfill_verified"]
    assert str(first["buy_qty"]) == "10" and str(first["sell_qty"]) == "4"
    assert empty["complete"] and empty["n_buy"] == empty["n_sell"] == 0  # a sealed minute with no trades is a valid zero
    assert str(third["buy_qty"]) == "7"  # the unpublished 999 is not read


def test_minutes_overlapping_a_loader_gap_stay_incomplete(tmp_path):
    trades = [agg(1, T0 + timedelta(seconds=5), "1", False), agg(9, T0 + timedelta(minutes=1, seconds=5), "1", False)]
    gap = {"start": "2026-10-20T21:00:05Z", "end": "2026-10-20T21:01:05Z", "reason": "BINANCE_AGGTRADE_ID_GAP"}
    write(tmp_path, trades, sealed="2026-10-20T21:02:00Z", gaps=[gap])
    rows = dict(LoaderFeed(str(tmp_path)).read(INSTRUMENT, T0 + timedelta(minutes=5)))
    assert all(not r["complete"] and r["gap_reason"] == "LOADER_GAP" for r in rows.values())


def test_nothing_is_read_past_sealed_through_and_reads_resume(tmp_path):
    write(tmp_path, [agg(1, T0 + timedelta(seconds=5), "1", False)], sealed="2026-10-20T21:01:00Z")
    feed = LoaderFeed(str(tmp_path))
    assert [k[1] for k, _ in feed.read(INSTRUMENT, T0 + timedelta(minutes=5))] == ["2026-10-20T21:00:00Z"]
    assert feed.read(INSTRUMENT, T0 + timedelta(minutes=5)) == []


def test_missing_loader_output_reads_nothing(tmp_path):
    assert LoaderFeed(str(tmp_path)).read(INSTRUMENT, T0) == []
