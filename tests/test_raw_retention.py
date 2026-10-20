"""Real temporary market files exercise TTL, reference retention and reader boundaries."""

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from tank.market import normalize
from tank.market.archive import BookArchive, RawArchive, TradeArchive
from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import iso, parse_time


def instrument():
    return {
        "instrument_id": "binance:spot:ABC",
        "token_id": "test:ABC",
        "venue": "binance",
        "market_type": "SPOT",
        "venue_symbol": "ABCUSD",
        "quote_asset": "USD",
        "contract_multiplier": "1",
    }


def settings(root):
    return SimpleNamespace(data_dir=root, load_config=lambda name: {"instruments": [instrument()]})


async def save(repo, manifests):
    for row in manifests:
        await repo.put("mkt.raw_archives", row["archive_id"], row)


def old_trade(root, stamp, monkeypatch, *, seal=True):
    import tank.market.archive as module

    actual_iso = iso
    monkeypatch.setattr(module, "iso", lambda value=None: actual_iso(stamp if value is None else value))
    raw = TradeArchive(root, instruments=[instrument()])
    raw.append(
        normalize.trade(
            instrument(), {"a": 1, "p": "12", "q": "2", "m": False, "T": int(stamp.timestamp() * 1000)}
        )
    )
    manifests = raw.flush(seal_before=stamp + timedelta(hours=2) if seal else None)
    return raw, manifests


@pytest.mark.asyncio
async def test_scheduled_ttl_deletes_canonical_trade_book_dependencies_without_raw_backup(
    monkeypatch, tmp_path
):
    import json

    import tank.scheduler as module

    now = parse_time("2026-10-19T12:00:00Z")
    old = now - timedelta(days=70)
    repo = MemoryRepository()
    raw, manifests = old_trade(tmp_path, old, monkeypatch)
    await save(repo, manifests)
    trade_manifest = manifests[-1]
    assert RawArchive.read_verified(trade_manifest["path"], trade_manifest["sha256"])
    book = BookArchive(tmp_path, instruments=[instrument()])
    book.write(instrument()["instrument_id"], "BOOK_SUMMARY", {"mid": "12"}, received_ts=old)
    await save(repo, book.flush_all())
    original_rows = await repo.list("mkt.raw_archives")
    files = list((tmp_path / "raw").rglob("*"))
    files = [path for path in files if path.is_file()]
    assert any(path.suffix == ".jsonl" for path in files)
    assert any(path.parent.name == ".batches" for path in files)
    assert any(".index" in path.name for path in files)
    monkeypatch.setattr(
        module, "verify_backup", lambda *a, **k: pytest.fail("Raw files are not in the planned backup")
    )
    removed = await Scheduler(repo, settings(tmp_path), None, None).prune_raw_files(
        now, tmp_path / "no-raw-backup"
    )
    assert len(removed) == 2
    assert all(not path.exists() for path in files)
    assert await repo.list("mkt.raw_archives") == original_rows  # immutable provenance survives TTL
    recovered = TradeArchive(tmp_path, instruments=[instrument()]).recover_minute(
        instrument()["instrument_id"], old, cause="REPLAY"
    )
    assert recovered["rows"] == [] and recovered["coverage"] == []
    with pytest.raises(FileNotFoundError):
        RawArchive.read_verified(trade_manifest["path"], trade_manifest["sha256"])
    audit = [
        json.loads(line)
        for line in (tmp_path / "retention/removed-market-files.jsonl").read_text().splitlines()
    ]
    assert [r["status"] for r in audit].count("REMOVAL_INTENT") == 2
    assert [r["status"] for r in audit].count("REMOVAL_COMPLETE") == 2
    assert (
        await Scheduler(repo, settings(tmp_path), None, None).prune_raw_files(now, tmp_path / "absent") == []
    )


@pytest.mark.asyncio
async def test_ttl_keeps_major_days_bundle_references_current_and_incomplete_hours(monkeypatch, tmp_path):
    now = parse_time("2026-10-19T12:00:00Z")
    keep = []
    for name, age, seal in [
        ("major", 60, True),
        ("bundle", 50, True),
        ("new", 2, True),
        ("recent_receipt", 80, True),
    ]:
        root = tmp_path / name
        _, manifests = old_trade(root, now - timedelta(days=age), monkeypatch, seal=seal)
        # Each root is an isolated synthetic data volume; validate it independently.
        own = MemoryRepository()
        if name == "recent_receipt":
            for row in manifests:
                row["received_ts"] = iso(now)
        await save(own, manifests)
        if name == "major":
            own.preserved_market_windows = lambda: _windows(now - timedelta(days=age))
        if name == "bundle":
            await own.put(
                "out.evidence_bundles",
                "permanent",
                {
                    "payload": {
                        "input": {"integrity": {"index_path": manifests[-1]["integrity"]["index_path"]}}
                    }
                },
                immutable=True,
            )
        files = [p for p in root.rglob("*") if p.is_file()]
        before = {p: p.read_bytes() for p in files}
        assert (
            await Scheduler(own, settings(root), None, None).prune_raw_files(now, tmp_path / "no-backup")
            == []
        )
        assert all(p.read_bytes() == value for p, value in before.items())
        keep += files
    assert keep


async def _windows(stamp):
    return [("another-token:BENCHMARK", stamp - timedelta(minutes=1), stamp + timedelta(minutes=1))]


@pytest.mark.asyncio
async def test_archive_uses_180_day_cache_ttl_and_symlink_sidecar_is_never_followed(tmp_path):
    now = parse_time("2026-10-19T12:00:00Z")
    repo = MemoryRepository()
    paths = {}
    for label, age in [("expired", 200), ("within_archive_ttl", 70), ("symlink", 210)]:
        day = (now - timedelta(days=age)).date()
        path = tmp_path / "archive/binance/spot/aggTrades" / f"ABCUSD-aggTrades-{day}.zip"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"synthetic archive cache")
        sidecar = path.with_suffix(".zip.CHECKSUM")
        if label == "symlink":
            target = tmp_path / "outside-checksum"
            target.write_text("never touch")
            sidecar.symlink_to(target)
        else:
            sidecar.write_text("retained provider checksum")
        await repo.put(
            "mkt.raw_archives",
            label,
            {
                "archive_id": label,
                "path": str(path),
                "token_id": "test:ABC",
                "kind": "aggTrades",
                "received_ts": iso(now - timedelta(days=age)),
            },
        )
        paths[label] = path
    removed = await Scheduler(repo, settings(tmp_path), None, None).prune_raw_files(now, tmp_path / "absent")
    assert [r["archive_id"] for r in removed] == ["expired"]
    assert not paths["expired"].exists() and not paths["expired"].with_suffix(".zip.CHECKSUM").exists()
    assert paths["within_archive_ttl"].exists() and paths["symlink"].exists()
    assert (tmp_path / "outside-checksum").read_text() == "never touch"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition", ["uncommitted_producer", "incomplete_index", "permanent_metadata_reference"]
)
async def test_retention_refuses_current_or_incomplete_dependency_family(condition, monkeypatch, tmp_path):
    import tank.market.archive as archive_module

    now = parse_time("2026-10-19T12:00:00Z")
    old = now - timedelta(days=70)
    repo = MemoryRepository()
    _, manifests = old_trade(tmp_path, old, monkeypatch)
    if condition == "uncommitted_producer":
        # Actual producer publishes a newer hour version/index before DB persistence.
        monkeypatch.setattr(archive_module, "iso", lambda value=None: iso(now if value is None else value))
        writer = TradeArchive(tmp_path, instruments=[instrument()])
        writer.append(
            normalize.trade(
                instrument(), {"a": 2, "p": "12", "q": "2", "m": False, "T": int(old.timestamp() * 1000) + 2}
            )
        )
        writer.flush(seal_before=old + timedelta(hours=2))
    elif condition == "incomplete_index":
        canonical = Path(manifests[-1]["integrity"]["canonical_parquet_path"])
        canonical.with_suffix(".index.json").write_text('{"incomplete":')
    else:
        permanent = tmp_path / "bundles/permanent.json.zst"
        permanent.parent.mkdir()
        permanent.write_bytes(b"immutable bundle")
        manifests[-1]["integrity"]["index_path"] = str(permanent)
    await save(repo, manifests)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    assert (
        await Scheduler(repo, settings(tmp_path), None, None).prune_raw_files(now, tmp_path / "absent") == []
    )
    assert all(p.read_bytes() == value for p, value in before.items())


@pytest.mark.asyncio
async def test_retention_durable_intent_failure_never_deletes_files(monkeypatch, tmp_path):
    import os

    now = parse_time("2026-10-19T12:00:00Z")
    repo = MemoryRepository()
    _, manifests = old_trade(tmp_path, now - timedelta(days=70), monkeypatch)
    await save(repo, manifests)
    original = {p: p.read_bytes() for p in (tmp_path / "raw").rglob("*") if p.is_file()}

    def fail(_):
        raise OSError("synthetic audit fsync failure")

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError, match="synthetic audit"):
        await Scheduler(repo, settings(tmp_path), None, None).prune_raw_files(now, tmp_path / "absent")
    assert all(p.read_bytes() == value for p, value in original.items())


@pytest.mark.asyncio
async def test_retention_does_not_follow_symlinked_market_parent(tmp_path):
    now = parse_time("2026-10-19T12:00:00Z")
    data = tmp_path / "data"
    root = data / "archive/binance"
    root.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    old = now - timedelta(days=200)
    target = outside / f"ABCUSD-aggTrades-{old.date()}.zip"
    target.write_bytes(b"must stay")
    (root / "spot").symlink_to(outside, target_is_directory=True)
    repo = MemoryRepository()
    await repo.put(
        "mkt.raw_archives",
        "link",
        {
            "archive_id": "link",
            "path": str(root / "spot" / target.name),
            "token_id": "test:ABC",
            "received_ts": iso(old),
        },
    )
    assert await Scheduler(repo, settings(data), None, None).prune_raw_files(now, tmp_path / "absent") == []
    assert target.read_bytes() == b"must stay"


@pytest.mark.asyncio
async def test_expired_unsealed_trade_and_frame_journals_are_not_retained_forever(monkeypatch, tmp_path):
    now = parse_time("2026-10-19T12:00:00Z")
    old = now - timedelta(days=50)
    repo = MemoryRepository()
    _, manifests = old_trade(tmp_path, old, monkeypatch, seal=False)
    assert all(row["status"] == "OPEN" for row in manifests)
    await save(repo, manifests)
    frames = RawArchive(tmp_path)
    await save(
        repo,
        [
            frames.write(
                instrument()["instrument_id"],
                "websocket",
                {"T": int(old.timestamp() * 1000)},
                received_ts=old,
            )
        ],
    )
    files = [p for p in (tmp_path / "raw").rglob("*") if p.is_file()]
    assert await Scheduler(repo, settings(tmp_path), None, None).prune_raw_files(now, tmp_path / "absent")
    assert all(not path.exists() for path in files)
