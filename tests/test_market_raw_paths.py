"""Hourly raw Parquet of HLD §7.2: layout, journal durability, sealing, and a lock-free capture path."""

# ruff: noqa: F811
import asyncio
from decimal import Decimal
import json
import time
from datetime import timedelta
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
from test_market_service import setup  # noqa: F401

from tank.market import normalize
from tank.market.archive import BookArchive, TradeArchive
from tank.market.service import MarketService
from tank.util import iso, parse_time, utcnow

SPOT = {
    "instrument_id": "binance:spot:ABCUSDT",
    "token_id": "test:ABC",
    "venue": "binance",
    "market_type": "SPOT",
    "venue_symbol": "ABCUSDT",
    "quote_asset": "USDT",
    "contract_multiplier": "1",
}
PERP = dict(SPOT, instrument_id="binance:perp_linear:ABCUSDT", market_type="PERP_LINEAR")
HOUR = parse_time("2026-10-17T12:00:00Z")


def print_(tid, at, inst=SPOT):
    return normalize.trade(
        inst, {"a": tid, "p": "2", "q": "1", "m": False, "T": int(parse_time(at).timestamp() * 1000)}
    )


def journals(canonical):
    return sorted(Path(canonical).parent.glob("." + Path(canonical).stem + ".*.arrows"))


def test_paths_follow_hld_layout_and_books_share_spot_and_perp(tmp_path):
    trades = TradeArchive(tmp_path, instruments=[SPOT, PERP])
    books = BookArchive(tmp_path, instruments=[SPOT, PERP])
    at = HOUR + timedelta(minutes=5)
    assert trades.path(SPOT["instrument_id"], at) == tmp_path / "raw/trades/binance/spot/ABCUSDT/2026-10-17/12.parquet"
    assert trades.path(PERP["instrument_id"], at) == (
        tmp_path / "raw/trades/binance/perp_linear/ABCUSDT/2026-10-17/12.parquet"
    )
    assert books.path(SPOT["instrument_id"], at) == books.path(PERP["instrument_id"], at) == (
        tmp_path / "raw/books/binance/ABCUSDT/2026-10-17/12.parquet"
    )


def test_append_touches_no_disk_and_deduplicates(tmp_path):
    archive = TradeArchive(tmp_path, instruments=[SPOT])
    row = print_(1, HOUR + timedelta(seconds=5))
    assert archive.append(row) and not archive.append(dict(row))
    assert not (tmp_path / "raw").exists()
    buffer = archive.take()
    assert [len(rows) for rows in buffer.values()] == [1] and archive.pending() == 0


def test_journal_survives_crash_and_seal_merges_restart_without_duplicates(tmp_path):
    first = TradeArchive(tmp_path, instruments=[SPOT])
    for tid in (1, 2):
        first.append(print_(tid, HOUR + timedelta(seconds=tid)))
    first.write(first.take(), now=HOUR + timedelta(minutes=10))
    canonical = first.path(SPOT["instrument_id"], HOUR)
    assert not canonical.exists() and len(journals(canonical)) == 1
    # Crash: the first process never closes its journal. The next one restarts in the hour.
    second = TradeArchive(tmp_path, instruments=[SPOT], grace_s=0)
    assert [r["trade_id"] for r in second.read_hour(canonical)] == ["1", "2"]
    for tid in (2, 3):
        second.append(print_(tid, HOUR + timedelta(seconds=tid)))
    result = second.write(second.take(), now=HOUR + timedelta(minutes=30))
    assert result["sealed"] == [] and len(journals(canonical)) == 2  # the hour is not over yet
    result = second.write({}, now=HOUR + timedelta(hours=1, seconds=1))
    assert result["sealed"] == [str(canonical)]
    assert not journals(canonical)
    table = pq.read_table(canonical)
    assert sorted(table.column("trade_id").to_pylist()) == ["1", "2", "3"]


def test_sealing_waits_for_quiet_journal_and_orphans_of_past_hours_are_sealed(tmp_path):
    archive = TradeArchive(tmp_path, instruments=[SPOT], grace_s=3600)
    archive.append(print_(1, HOUR))
    after = HOUR + timedelta(hours=2)
    assert archive.write(archive.take(), now=after)["sealed"] == []  # written just now: not quiet
    canonical = archive.path(SPOT["instrument_id"], HOUR)
    archive._journals[canonical]["touched"] = time.monotonic() - 3601
    assert archive.write({}, now=after)["sealed"] == [str(canonical)]
    # A journal left by a previous process for a finished hour is sealed on the first write.
    late = TradeArchive(tmp_path, instruments=[SPOT], grace_s=3600)
    late.append(print_(9, HOUR + timedelta(hours=1)))
    late.write(late.take(), now=after)
    restarted = TradeArchive(tmp_path, instruments=[SPOT], grace_s=3600)
    sealed = restarted.write({}, now=after)["sealed"]
    assert sealed == [str(late.path(SPOT["instrument_id"], HOUR + timedelta(hours=1)))]


def test_torn_journal_tail_keeps_every_complete_batch(tmp_path):
    archive = TradeArchive(tmp_path, instruments=[SPOT], grace_s=0)
    for tid in (1, 2):
        archive.append(print_(tid, HOUR + timedelta(seconds=tid)))
        archive.write(archive.take(), now=HOUR)
    canonical = archive.path(SPOT["instrument_id"], HOUR)
    (journal,) = journals(canonical)
    size = journal.stat().st_size
    with journal.open("r+b") as handle:  # a crash cut the last batch short
        handle.truncate(size - 40)
    restarted = TradeArchive(tmp_path, instruments=[SPOT], grace_s=0)
    assert [r["trade_id"] for r in restarted.read_hour(canonical)] == ["1"]
    restarted.write({}, now=HOUR + timedelta(hours=2))
    assert pq.read_table(canonical).column("trade_id").to_pylist() == ["1"]


def test_late_rows_for_a_sealed_hour_are_merged_into_a_new_file(tmp_path):
    archive = TradeArchive(tmp_path, instruments=[SPOT], grace_s=0)
    archive.append(print_(1, HOUR))
    archive.write(archive.take(), now=HOUR + timedelta(hours=2))
    canonical = archive.path(SPOT["instrument_id"], HOUR)
    assert pq.read_table(canonical).num_rows == 1
    archive.append(print_(2, HOUR + timedelta(minutes=59)))  # REST backfill of an old hour
    archive.append(dict(print_(1, HOUR), capture_source="REST"))  # already sealed: dropped on merge
    archive.write(archive.take(), now=HOUR + timedelta(hours=2))
    assert sorted(pq.read_table(canonical).column("trade_id").to_pylist()) == ["1", "2"]
    assert not journals(canonical)


def test_books_of_spot_and_perp_share_one_hour_file_and_name_their_instrument(tmp_path):
    books = BookArchive(tmp_path, instruments=[SPOT, PERP], grace_s=0)
    for inst in (SPOT, PERP):
        books.add(inst["instrument_id"], "BOOK_SUMMARY", {"mid": "2", "who": inst["market_type"]}, HOUR)
    books.write(books.take(), now=HOUR + timedelta(hours=2))
    table = pq.read_table(books.path(SPOT["instrument_id"], HOUR)).to_pylist()
    assert sorted(r["instrument_id"] for r in table) == sorted([SPOT["instrument_id"], PERP["instrument_id"]])
    assert {json.loads(r["payload_json"])["who"] for r in table} == {"SPOT", "PERP_LINEAR"}


@pytest.mark.asyncio
async def test_capture_never_waits_for_the_archive_writer(setup, monkeypatch):
    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        await service._ensure_market_restore()
        release = asyncio.Event()
        original = service.trade_archive.write
        loop = asyncio.get_running_loop()

        def slow_write(buffer, **kwargs):
            asyncio.run_coroutine_threadsafe(release.wait(), loop).result(5)
            return original(buffer, **kwargs)

        monkeypatch.setattr(service.trade_archive, "write", slow_write)
        flush = asyncio.create_task(service.flush_market(raw=True))
        await asyncio.sleep(0.05)
        now = int(utcnow().timestamp() * 1000)
        started = time.monotonic()
        for number in range(1, 501):
            raw = {"a": number, "p": "2", "q": "1", "T": now + number, "m": False}
            assert await asyncio.wait_for(service.ingest_trade(inst, raw), 0.5)
        assert time.monotonic() - started < 2 and not flush.done()  # writer still busy, capture went on
        release.set()
        await flush
        await service.flush_market(raw=True)
        canonical = service.trade_archive.path(inst["instrument_id"], iso(utcnow()))
        rows = await asyncio.to_thread(service.trade_archive.read_hour, canonical)
        assert len(rows) == 500


@pytest.mark.asyncio
async def test_failed_raw_write_keeps_rows_and_typed_rows_wait_for_raw(setup, monkeypatch):
    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        now = int(utcnow().timestamp() * 1000)
        await service.ingest_trade(inst, {"a": 1, "p": "2", "q": "1", "T": now, "m": False})
        await service._flush_flow()
        original = service.trade_archive.write
        monkeypatch.setattr(service.trade_archive, "write", lambda *a, **k: (_ for _ in ()).throw(OSError("full")))
        with pytest.raises(OSError):
            await service.flush_market(raw=True)
        assert not await repo.list("mkt.flow_bars_1m")  # no derived bar without its print on disk
        assert service.trade_archive.pending() == 1
        monkeypatch.setattr(service.trade_archive, "write", original)
        await service.flush_market(raw=True)
        canonical = service.trade_archive.path(inst["instrument_id"], iso(utcnow()))
        assert len(await asyncio.to_thread(service.trade_archive.read_hour, canonical)) == 1
        assert len(await repo.list("mkt.flow_bars_1m")) == 1


@pytest.mark.asyncio
async def test_restart_dedups_old_hour_prints_and_rebuilds_late_minute(setup):
    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        minute = utcnow().replace(second=0, microsecond=0) - timedelta(minutes=10)
        base = int(minute.timestamp() * 1000)
        for number in range(1, 11):
            await service.ingest_trade(inst, {"a": number, "p": "2", "q": "3", "T": base + number, "m": False})
        await service._flush_flow()
        await service.flush_market(raw=True)
        restarted = MarketService(repo, settings, http=http)
        assert not await restarted.ingest_trade(inst, {"a": 3, "p": "2", "q": "3", "T": base + 3, "m": False})
        assert await restarted.ingest_trade(inst, {"a": 11, "p": "3", "q": "1", "T": base + 11, "m": True})
        await restarted._flush_flow()
        await restarted.flush_market(raw=True)
        (bar,) = await repo.list("mkt.flow_bars_1m", filters={"instrument_id": inst["instrument_id"]})
        assert bar["buy_qty"] == "30" and bar["sell_qty"] == "1" and bar["n_buy"] == 10
        assert bar["complete"] is False  # the restart broke stream continuity in that minute


def test_file_retention_by_path_keeps_event_days_and_recent_hours(tmp_path):
    import os

    from tank.market.archive import prune_market_files

    now = parse_time("2026-12-01T00:00:00Z")
    old, event_day, recent = "2026-10-01", "2026-10-02", "2026-11-20"
    for day in (old, event_day, recent):
        (tmp_path / f"raw/trades/binance/spot/ABCUSDT/{day}").mkdir(parents=True)
        (tmp_path / f"raw/trades/binance/spot/ABCUSDT/{day}/12.parquet").write_bytes(b"x")
        (tmp_path / f"raw/books/binance/ABCUSDT/{day}").mkdir(parents=True)
    cache = tmp_path / "archive/binance/spot/klines"
    cache.mkdir(parents=True)
    stale, fresh = cache / "ABC-1m-2026-01-01.zip", cache / "ABC-1m-2026-11-01.zip"
    for path in (stale, fresh):
        path.write_bytes(b"z")
    os.utime(stale, (now.timestamp() - 200 * 86400,) * 2)
    os.utime(fresh, (now.timestamp() - 10 * 86400,) * 2)
    windows = [("EVT-1", "2026-10-02T23:00:00Z", "2026-10-02T23:59:00Z")]
    removed = prune_market_files(tmp_path, now, windows)
    assert sorted(removed) == sorted(
        [
            str(tmp_path / f"raw/trades/binance/spot/ABCUSDT/{old}"),
            str(tmp_path / f"raw/books/binance/ABCUSDT/{old}"),
            str(stale),
        ]
    )
    assert (tmp_path / f"raw/trades/binance/spot/ABCUSDT/{event_day}/12.parquet").exists()
    assert (tmp_path / f"raw/books/binance/ABCUSDT/{recent}").exists() and fresh.exists()


@pytest.mark.asyncio
async def test_repeated_rest_histories_are_processed_once_and_stream_context_once_per_10s(setup):
    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        rows = [{"fundingTime": 1_700_000_000_000 + n * 28_800_000} for n in range(3)]
        assert service._fresh("i", "settled", rows, lambda r: r["fundingTime"]) == (None, rows)
        newer = rows + [{"fundingTime": rows[-1]["fundingTime"] + 28_800_000}]
        predecessor, fresh = service._fresh("i", "settled", newer, lambda r: r["fundingTime"])
        assert fresh == newer[-1:] and predecessor == parse_time(rows[-1]["fundingTime"])
        assert service._fresh("i", "settled", newer, lambda r: r["fundingTime"])[1] == []
        assert service.context_due("i", "mark") and not service.context_due("i", "mark")
        assert service.context_due("i", "funding")  # each context kind has its own cadence


def test_checksum_books_verify_once_per_second_or_when_a_summary_is_due(monkeypatch):
    from tank.market.books import OrderBook

    book = OrderBook(dict(SPOT, instrument_id="kraken:spot:ABC/USD", venue="kraken", venue_symbol="ABC/USD"))
    calls = []
    monkeypatch.setattr(book, "verify", lambda expected: calls.append(expected) or True)
    book.snapshot([["1", "1"]], [["2", "1"]], sequence=0, ts=iso(HOUR), checksum=1)
    assert calls == [1] and book.verified
    book.update([["1", "2"]], [], sequence=1, ts=iso(HOUR), checksum=2)
    assert calls == [1, 2] and book.verified  # first update after the snapshot interval
    book.update([["1", "3"]], [], sequence=2, ts=iso(HOUR), checksum=3)
    assert calls == [1, 2] and not book.verified  # within 1 s: not summarized from this state
    book.update([["1", "4"]], [], sequence=3, ts=iso(HOUR), checksum=4, verify=True)
    assert calls == [1, 2, 4] and book.verified  # a due summary forces verification


def test_book_price_index_stays_in_step_with_levels_and_kraken_depth():
    import random

    from tank.market.books import OrderBook

    rng = random.Random(7)
    book = OrderBook(dict(SPOT, instrument_id="kraken:spot:ABC/USD", venue="kraken", venue_symbol="ABC/USD"))
    bids = [[f"{99 - i * 0.01:.2f}", "1"] for i in range(1200)]
    asks = [[f"{101 + i * 0.01:.2f}", "1"] for i in range(10)]
    book.snapshot(bids, asks, sequence=0, ts=iso(HOUR))
    for n in range(500):
        price = f"{rng.uniform(80, 99.99):.2f}" if n % 2 else f"{rng.uniform(101, 110):.2f}"
        side = [[price, rng.choice(["0", "2"])]]
        book.update(side if n % 2 else [], [] if n % 2 else side, sequence=n + 1, ts=iso(HOUR))
        assert book._prices["bid"] == sorted(book.bids) and book._prices["ask"] == sorted(book.asks)
        assert len(book.bids) <= 1000 and len(book.asks) <= 1000  # Kraken depth is trimmed
    summary = book.summary()
    assert summary["best_bid"] == str(max(book.bids)) and summary["best_ask"] == str(min(book.asks))
    edge = Decimal(summary["mid"]) * Decimal(".98")
    assert [row[0] for row in summary["bids"]] == [str(p) for p in sorted(book.bids, reverse=True) if p >= edge]
