"""Literal HLD7.2 paths retain frozen bytes across restart and late publication."""

import json

# ruff: noqa: F811
from datetime import timedelta
from pathlib import Path

import pytest
from test_market_service import setup  # noqa: F401

from tank.market import normalize
from tank.market.archive import BookArchive, RawArchive, TradeArchive
from tank.util import parse_time


def instrument(market="SPOT"):
    return {
        "instrument_id": "opaque-id-" + market,
        "token_id": "test:ABC",
        "venue": "binance",
        "market_type": market,
        "venue_symbol": "ABCUSD",
        "quote_asset": "USD",
        "contract_multiplier": "1",
    }


def trade(inst, number, minute):
    return normalize.trade(
        inst, {"a": number, "p": "12", "q": "2", "m": False, "T": int(minute.timestamp() * 1000) + number}
    )


def test_hour_preload_cannot_mutate_state_during_durable_flush(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, minute))
    writing, release, loading = Event(), Event(), Event()
    write_parquet = raw._write_parquet

    def paused_write(*args):
        writing.set()
        assert release.wait(5)
        return write_parquet(*args)

    def preload():
        loading.set()
        return raw._state(inst["instrument_id"], minute + timedelta(hours=1))

    monkeypatch.setattr(raw, "_write_parquet", paused_write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        flushing = pool.submit(raw.flush)
        assert writing.wait(5)
        pending = pool.submit(preload)
        assert loading.wait(5)
        try:
            # MarketService preloads hours in a worker while the periodic writer
            # is persisting another hour. It must obey the same archive lock.
            from concurrent.futures import TimeoutError

            with pytest.raises(TimeoutError):
                pending.result(timeout=0.1)
        finally:
            release.set()
        first = flushing.result(timeout=5)
        pending.result(timeout=5)
    raw.append(trade(inst, 2, minute + timedelta(hours=1)))
    second = raw.flush()
    rows = [
        row
        for manifest in first + second
        for row in RawArchive.read_verified(manifest["path"], manifest["sha256"])
    ]
    assert sorted(row["trade_id"] for row in rows) == ["1", "2"]


def test_canonical_trade_hour_versions_survive_restart_and_late_write(tmp_path):
    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    assert raw.append(trade(inst, 1, minute))
    first = raw.flush(seal_before=minute + timedelta(hours=2))[0]
    canonical = tmp_path / "raw/trades/binance/spot/ABCUSD/2026-10-17/12.parquet"
    frozen = Path(first["path"])
    original = frozen.read_bytes()
    assert canonical.exists() and frozen.parent.name == ".versions"
    assert canonical.stat().st_ino == frozen.stat().st_ino
    resumed = TradeArchive(tmp_path, instruments=[inst])
    assert not resumed.append(trade(inst, 1, minute))
    assert resumed.append(trade(inst, 2, minute))
    second = resumed.flush(seal_before=minute + timedelta(hours=2))[0]
    assert frozen.read_bytes() == original
    assert first["path"] != second["path"]
    assert len(RawArchive.read_verified(first["path"], first["sha256"])) == 1
    assert len(RawArchive.read_verified(second["path"], second["sha256"])) == 2
    assert canonical.stat().st_ino == Path(second["path"]).stat().st_ino


def test_legacy_trade_prefix_import_preserves_all_original_evidence(tmp_path):
    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    # Historical JSONL is read-only compatibility evidence, not produced by
    # today's Parquet-only writer.
    import hashlib

    from tank.util import canonical_json, iso

    point = trade(inst, 1, minute)
    point.pop("venue_symbol")
    old_journal, old_index = TradeArchive(tmp_path)._legacy_paths(inst["instrument_id"], minute)
    old_journal.parent.mkdir(parents=True)
    payload = (canonical_json(point) + "\n").encode()
    old_journal.write_bytes(payload)
    index = {
        "instrument_id": inst["instrument_id"],
        "hour_start": iso(minute),
        "journal_path": str(old_journal),
        "chunks": {},
        "coverage": [],
        "row_count": 1,
        "byte_length": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    index["index_sha256"] = TradeArchive._index_hash(index)
    old_index.write_text(json.dumps(index))
    original = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    current = TradeArchive(tmp_path, instruments=[inst])
    assert not current.append(trade(inst, 1, minute))
    assert current.append(trade(inst, 2, minute))
    new = current.flush(seal_before=minute + timedelta(hours=2))[0]
    assert all(path.read_bytes() == contents for path, contents in original.items())
    assert list(tmp_path.rglob("*.jsonl")) == [old_journal]
    assert new["integrity"]["legacy_origin"]["journal_path"] == str(old_journal)
    assert len(RawArchive.read_verified(new["path"], new["sha256"])) == 2


def test_mixed_market_books_share_physical_hour_with_exact_instrument_scope(tmp_path):
    spot, perp = instrument(), instrument("PERP_LINEAR")
    raw = BookArchive(tmp_path, instruments=[spot, perp])
    stamp = "2026-10-17T12:00:10Z"
    entries = [
        (i["instrument_id"], "BOOK_SUMMARY", {"market_type": i["market_type"], "mid": "12"}, stamp)
        for i in (spot, perp)
    ]
    opened = raw.write_batch(entries)
    assert len(opened) == 2
    assert len({r["journal_path"] for r in opened}) == 1
    assert all(r["row_count"] == 2 and r["integrity"]["filtered_instrument_row_count"] == 1 for r in opened)
    sealed = raw.flush_all()
    canonical = tmp_path / "raw/books/binance/ABCUSD/2026-10-17/12.parquet"
    assert canonical.exists()
    assert len({r["path"] for r in sealed}) == 1
    frozen = sealed[0]
    original = Path(frozen["path"]).read_bytes()
    rows = RawArchive.read_verified(frozen["path"], frozen["sha256"], instrument_id=spot["instrument_id"])
    assert len(rows) == 1 and rows[0]["market_type"] == "SPOT"
    assert json.loads(rows[0]["payload_json"])["mid"] == "12"
    restarted = BookArchive(tmp_path, instruments=[spot, perp])
    repeated = restarted.write_batch(entries)
    assert not repeated
    restarted.write_batch(
        [
            (
                perp["instrument_id"],
                "BOOK_SNAPSHOT",
                {"market_type": "PERP_LINEAR", "bids": [["11", "2"]], "asks": [["13", "2"]]},
                stamp,
            )
        ]
    )
    revised = restarted.flush_all()
    assert Path(frozen["path"]).read_bytes() == original
    assert all(r["row_count"] == 3 for r in revised)
    assert sorted(r["integrity"]["filtered_instrument_row_count"] for r in revised) == [1, 2]


@pytest.mark.asyncio
async def test_actual_service_book_summary_snapshot_durable_before_typed_batch(setup):
    from unittest.mock import AsyncMock

    import httpx

    from tank.market.books import OrderBook
    from tank.market.service import MarketService

    repo, settings, inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        service._independent_book = AsyncMock(return_value={"second_path_confirmed": True})
        book = OrderBook(inst)
        service.books[inst["instrument_id"]] = book
        book.snapshot([["11", "2"]], [["13", "3"]], sequence=1, ts="2026-10-17T12:00:00Z")
        await service._save_book(inst)
        # Provider snapshots arriving inside the 10s summary cadence remain
        # coalesced in memory; they do not cause per-frame Parquet publication.
        book.snapshot([["11", "4"]], [["13", "3"]], sequence=2, ts="2026-10-17T12:00:01Z")
        await service._save_book(inst)
        assert len(service._book_raw_pending) == 2
        calls = []
        original = repo.put_market_batch

        async def batch(collection, rows, **kwargs):
            if collection == "mkt.book_summaries":
                journals = list(settings.data_dir.glob("raw/books/**/*.jsonl"))
                assert journals and journals[0].stat().st_size > 0
                calls.append(collection)
            return await original(collection, rows, **kwargs)

        repo.put_market_batch = batch
        await service.flush_market()
        assert calls == ["mkt.book_summaries"]
        manifests = service.book_archive.flush_all()
        assert len(manifests) == 1 and manifests[0]["row_count"] == 2
        rows = RawArchive.read_verified(
            manifests[0]["path"], manifests[0]["sha256"], instrument_id=inst["instrument_id"]
        )
        summary = json.loads(next(r["payload_json"] for r in rows if r["kind"] == "BOOK_SUMMARY"))
        snapshot = json.loads(next(r["payload_json"] for r in rows if r["kind"] == "BOOK_SNAPSHOT"))
        assert "bids" not in summary and "asks" not in summary
        assert snapshot["bids"] == [["11", "2"]] and snapshot["sequence"] == 1
        assert all(r["market_type"] == "SPOT" for r in rows)
        if service._background:
            import asyncio

            await asyncio.gather(*service._background)


def test_active_trade_batches_are_parquet_only_and_prefix_replay_is_frozen(tmp_path, monkeypatch):
    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    written_counts = []
    write = raw._write_parquet

    def tracked(rows, directory, stem):
        written_counts.append(len(rows))
        return write(rows, directory, stem)

    monkeypatch.setattr(raw, "_write_parquet", tracked)
    for number in range(1, 101):
        raw.append(trade(inst, number, minute))
    assert not written_counts  # Ingestion is memory-only, not one write/frame.
    raw.coverage(inst["instrument_id"], minute, minute + timedelta(minutes=1), complete=True)
    first = raw.flush()[0]
    first_bytes = Path(first["path"]).read_bytes()
    assert first["path"].endswith(".parquet") and written_counts == [100]
    assert not list(tmp_path.rglob("*.jsonl"))
    restarted = TradeArchive(tmp_path, instruments=[inst])
    assert not restarted.append(trade(inst, 1, minute))
    restarted.append(trade(inst, 101, minute))
    second = restarted.flush()[0]
    assert len(TradeArchive.read_verified(second["path"], second["sha256"])) == 1
    assert Path(first["path"]).read_bytes() == first_bytes
    frozen = TradeArchive.rebuild_window(
        [first], inst["instrument_id"], minute, minute + timedelta(minutes=1), instrument=inst
    )
    latest = TradeArchive.rebuild_window(
        [first, second], inst["instrument_id"], minute, minute + timedelta(minutes=1), instrument=inst
    )
    assert frozen["flow_bars"][0]["n_buy"] == 100
    assert latest["flow_bars"][0]["n_buy"] == 101
    assert len(restarted.recover_minute(inst["instrument_id"], minute, cause="AUDIT")["rows"]) == 101


def test_corrupted_parquet_batch_is_rejected_on_restart_and_minute_recovery(tmp_path):
    from tank.market.archive import ArchiveIntegrityError

    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, minute))
    manifest = raw.flush()[0]
    path = Path(manifest["path"])
    path.write_bytes(path.read_bytes() + b"corruption")
    with pytest.raises(ArchiveIntegrityError, match="checksum"):
        raw.recover_minute(inst["instrument_id"], minute, cause="AUDIT")
    with pytest.raises(ArchiveIntegrityError, match="checksum"):
        TradeArchive(tmp_path, instruments=[inst]).append(trade(inst, 2, minute))


def test_failed_trade_index_publication_retries_without_losing_pending_rows(tmp_path, monkeypatch):
    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, minute))
    persist = raw._persist_index

    def fail(state):
        raise OSError("simulated index fsync failure")

    monkeypatch.setattr(raw, "_persist_index", fail)
    with pytest.raises(OSError, match="fsync"):
        raw.flush()
    monkeypatch.setattr(raw, "_persist_index", persist)
    manifest = raw.flush()[0]
    assert manifest["row_count"] == 1
    restarted = TradeArchive(tmp_path, instruments=[inst])
    assert len(restarted.recover_minute(inst["instrument_id"], minute, cause="AUDIT")["rows"]) == 1
    assert not restarted.append(trade(inst, 1, minute))


def test_multi_instrument_flush_retry_preserves_prior_successful_manifests(tmp_path, monkeypatch):
    spot, perp = instrument(), instrument("PERP_LINEAR")
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[spot, perp])
    for inst in (spot, perp):
        raw.append(trade(inst, 1, minute))
    persist = raw._persist_index
    first = True

    def fail_second(state):
        nonlocal first
        if first:
            first = False
            return persist(state)
        raise OSError("second instrument index failure")

    monkeypatch.setattr(raw, "_persist_index", fail_second)
    with pytest.raises(OSError):
        raw.flush()
    monkeypatch.setattr(raw, "_persist_index", persist)
    manifests = raw.flush()
    assert {r["instrument_id"] for r in manifests} == {spot["instrument_id"], perp["instrument_id"]}
    assert all(len(raw.read_verified(r["path"], r["sha256"])) == 1 for r in manifests)


def test_missing_same_minute_batch_cannot_certify_complete_flow(tmp_path):
    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, minute))
    raw.coverage(inst["instrument_id"], minute, minute + timedelta(minutes=1), complete=True)
    first = raw.flush()[0]
    raw.append(trade(inst, 2, minute))
    second = raw.flush()[0]
    partial = TradeArchive.rebuild_window(
        [second], inst["instrument_id"], minute, minute + timedelta(minutes=1), instrument=inst
    )
    complete = TradeArchive.rebuild_window(
        [first, second], inst["instrument_id"], minute, minute + timedelta(minutes=1), instrument=inst
    )
    assert partial["complete"] is False
    assert partial["flow_bars"][0]["complete"] is False
    assert complete["complete"] is True and complete["flow_bars"][0]["n_buy"] == 2
    sealed = raw.flush(seal_before=minute + timedelta(hours=1))[0]
    full = TradeArchive.rebuild_window(
        [sealed], inst["instrument_id"], minute, minute + timedelta(minutes=1), instrument=inst
    )
    assert full["complete"] is True and full["flow_bars"][0]["n_buy"] == 2


def test_sealed_replay_projection_preserves_missing_batch_failure(tmp_path):
    from types import SimpleNamespace

    from tank.replay_evidence import seal_replay_evidence
    from tank.util import iso

    inst = instrument()
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path / "raw", instruments=[inst])
    raw.append(trade(inst, 1, minute))
    raw.coverage(inst["instrument_id"], minute, minute + timedelta(minutes=1), complete=True)
    first = raw.flush()[0]
    raw.append(trade(inst, 2, minute))
    second = raw.flush()[0]
    for label, manifests, expected in [("partial", [second], False), ("all", [first, second], True)]:
        inputs = SimpleNamespace(
            assembled_at="2099-01-01T00:00:00Z",
            cutoff=iso(minute + timedelta(minutes=1)),
            config={},
            registry={"instruments": [inst]},
            _records=[{"collection": "mkt.raw_archives", "record": r} for r in manifests],
        )
        projected = seal_replay_evidence(inputs, "RPL-" + label, tmp_path / label, input_sha256="f" * 64)
        output = TradeArchive.rebuild_window(
            projected["rebuild_manifests"],
            inst["instrument_id"],
            minute,
            minute + timedelta(minutes=1),
            instrument=inst,
        )
        assert output["complete"] is expected
        assert output["flow_bars"][0]["n_buy"] == (1 if label == "partial" else 2)


def test_operation_batch_reuse_matches_uncached_minutes_and_does_not_share_mutable_rows(
    tmp_path, monkeypatch
):
    from tank.util import iso

    inst = instrument()
    start = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    for number in range(3):
        raw.append(trade(inst, number + 1, start + timedelta(minutes=number)))
    raw.coverage(inst["instrument_id"], start, start + timedelta(minutes=3), complete=True)
    point = {
        "boundary": iso(start + timedelta(seconds=30)),
        "complete": True,
        "last_trade": {"ts": iso(start + timedelta(milliseconds=1)), "price": "12"},
    }
    raw.reference_points(inst["instrument_id"], [point])
    raw.flush()
    expected = [
        raw.recover_minute(inst["instrument_id"], start + timedelta(minutes=n), cause="AUDIT")
        for n in range(3)
    ]
    reads = []
    read = raw.read_verified

    def counted(*args, **kwargs):
        reads.append(str(args[0]))
        return read(*args, **kwargs)

    monkeypatch.setattr(raw, "read_verified", counted)
    operation = {}
    actual = [
        raw.recover_minute(
            inst["instrument_id"], start + timedelta(minutes=n), cause="AUDIT", verified_batch=operation
        )
        for n in range(3)
    ]
    assert actual == expected and len(reads) == 1
    assert set(operation) == {"key", "rows"} and len(operation["key"]) == 7
    actual[0]["rows"][0]["price"] = "caller mutation"
    again = raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    assert again["rows"] == expected[0]["rows"] and len(reads) == 1
    # A fresh operation has no global reuse, and must verify the file again.
    raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch={})
    assert len(reads) == 2


def test_operation_batch_reuse_observes_current_coverage_references_pending_and_late_batch(tmp_path):
    from tank.util import iso

    inst = instrument()
    start = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, start))
    raw.coverage(inst["instrument_id"], start, start + timedelta(minutes=1), complete=True)
    raw.flush()
    operation = {}
    raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    raw.coverage(
        inst["instrument_id"], start, start + timedelta(minutes=1), complete=False, reason="REST_TRADE_ID_GAP"
    )
    raw.reference_points(
        inst["instrument_id"],
        [
            {
                "boundary": iso(start + timedelta(seconds=30)),
                "complete": False,
                "coverage_reason": "REST_TRADE_ID_GAP",
            }
        ],
    )
    raw.append(trade(inst, 2, start))
    pending = raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    assert pending == raw.recover_minute(inst["instrument_id"], start, cause="AUDIT")
    assert len(pending["rows"]) == 2 and pending["coverage"][-1]["complete"] is False
    assert pending["reference_points"][-1]["complete"] is False
    pending["rows"][-1]["price"] = "caller mutation of pending row"
    raw.flush()
    late = raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    assert late == raw.recover_minute(inst["instrument_id"], start, cause="AUDIT")
    assert [r["trade_id"] for r in late["rows"]] == ["1", "2"]
    assert all(r["price"] == "12" for r in late["rows"])
    assert len(operation["rows"]) == 1  # Only the last batch remains retained.


def test_operation_batch_reuse_reverifies_changed_same_size_file_with_restored_mtime(tmp_path):
    import os

    from tank.market.archive import ArchiveIntegrityError

    inst = instrument()
    start = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, start))
    manifest = raw.flush()[0]
    operation = {}
    raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    path = Path(manifest["path"])
    stat = path.stat()
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 1
    path.write_bytes(data)
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert path.stat().st_size == stat.st_size and path.stat().st_mtime_ns == stat.st_mtime_ns
    with pytest.raises(ArchiveIntegrityError, match="checksum"):
        raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    assert operation == {}


def test_operation_batch_reuse_reverifies_metadata_change_and_new_expected_hash(tmp_path, monkeypatch):
    import os

    from tank.market.archive import ArchiveIntegrityError
    from tank.util import iso

    inst = instrument()
    start = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, start))
    manifest = raw.flush()[0]
    reads = []
    read = raw.read_verified

    def counted(*args, **kwargs):
        reads.append(args[1])
        return read(*args, **kwargs)

    monkeypatch.setattr(raw, "read_verified", counted)
    operation = {}
    expected = raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    path = Path(manifest["path"])
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1))
    assert (
        raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation) == expected
    )
    assert len(reads) == 2
    raw._state(inst["instrument_id"], start)["index"]["chunks"][iso(start)][0]["sha256"] = "0" * 64
    with pytest.raises(ArchiveIntegrityError, match="checksum"):
        raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    assert reads[-1] == "0" * 64 and operation == {}


def test_operation_batch_reuse_refuses_file_change_during_verified_read(tmp_path, monkeypatch):
    import os

    from tank.market.archive import ArchiveIntegrityError

    inst = instrument()
    start = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path, instruments=[inst])
    raw.append(trade(inst, 1, start))
    raw.flush()
    read = raw.read_verified

    def changed_after_read(path, checksum):
        result = read(path, checksum)
        stat = Path(path).stat()
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1))
        return result

    monkeypatch.setattr(raw, "read_verified", changed_after_read)
    operation = {}
    with pytest.raises(ArchiveIntegrityError, match="changed during verification"):
        raw.recover_minute(inst["instrument_id"], start, cause="AUDIT", verified_batch=operation)
    assert operation == {}
