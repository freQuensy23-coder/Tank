"""Durable raw Parquet and verified Binance historical archive ingestion."""

import csv
import hashlib
import io
import itertools
import zipfile
from pathlib import Path

from tank.util import canonical_json, iso, new_id, parse_time


class ArchiveIntegrityError(ValueError):
    pass


class RawArchive:
    """Hourly Parquet segments with an fsynced append journal for current-hour durability.

    OPEN manifests refer to a checksummed byte prefix of the journal. SEALED manifests
    refer to immutable Parquet. The journal remains alongside Parquet for frozen prefix
    references until the same raw-retention deadline.
    """

    def __init__(self, root):
        import threading

        self.root = Path(root) / "market" / "raw"
        self._lock = threading.RLock()
        self._states = {}
        self._completed = []

    def _state(self, instrument_id, kind, ts):
        import json

        directory = (
            self.root
            / ts[:10]
            / "".join(c if c.isalnum() or c in "-_." else "_" for c in instrument_id)
            / kind
        )
        directory.mkdir(parents=True, exist_ok=True)
        stem = ts[:13].replace(":", "-")
        journal = directory / (stem + ".jsonl")
        parquet = directory / (stem + ".parquet")
        key = str(journal)
        state = self._states.get(key)
        if state:
            return state
        # Resume an unsealed journal; never lose a prefix after a process restart.
        if not journal.exists() and parquet.exists():
            import pyarrow.parquet as pq

            with journal.open("wb") as handle:
                for row in pq.read_table(parquet).to_pylist():
                    handle.write((canonical_json(row) + "\n").encode())
        hasher = hashlib.sha256()
        count = 0
        length = 0
        if journal.exists():
            with journal.open("rb") as handle:
                for line in handle:
                    json.loads(line)
                    hasher.update(line)
                    count += 1
                    length += len(line)
        state = {
            "instrument_id": instrument_id,
            "kind": kind,
            "hour": ts[:13],
            "journal": journal,
            "parquet": parquet,
            "hasher": hasher,
            "row_count": count,
            "byte_length": length,
            "received_ts": ts,
        }
        self._states[key] = state
        return state

    @staticmethod
    def _file_sha256(path):
        digest = hashlib.sha256()
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _manifest(self, state, sealed=False):
        path = state["sealed_path"] if sealed else state["journal"]
        digest = state["sealed_sha256"] if sealed else state["hasher"].hexdigest()
        return {
            "archive_id": "RAW-"
            + hashlib.sha256(str(path if sealed else state["parquet"]).encode()).hexdigest(),
            "instrument_id": state["instrument_id"],
            "kind": state["kind"],
            "path": str(path),
            "parquet_path": str(state["sealed_path"] if sealed else state["parquet"]),
            "journal_path": str(state["journal"]),
            "sha256": digest,
            "byte_length": path.stat().st_size if sealed else state["byte_length"],
            "checksum_scope": "FILE" if sealed else "JOURNAL_PREFIX",
            "row_count": state["row_count"],
            "status": "SEALED" if sealed else "OPEN",
            "hour_start": state["hour"] + ":00:00Z",
            "received_ts": state["received_ts"],
        }

    def write(self, instrument_id, kind, payload, *, received_ts=None):
        import os

        ts = iso(received_ts)
        with self._lock:
            for key, state in list(self._states.items()):
                if state["hour"] < ts[:13]:
                    self._seal(state)
                    self._states.pop(key)
            state = self._state(instrument_id, kind, ts)
            row = {
                "instrument_id": instrument_id,
                "kind": kind,
                "received_ts": ts,
                "payload_json": canonical_json(payload),
            }
            line = (canonical_json(row) + "\n").encode()
            with state["journal"].open("ab") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
            state["hasher"].update(line)
            state["row_count"] += 1
            state["byte_length"] += len(line)
            state["received_ts"] = ts
            return self._manifest(state)

    def _seal(self, state):
        import json
        import os

        import pyarrow as pa
        import pyarrow.parquet as pq

        temp = state["parquet"].with_suffix(".tmp")
        schema = pa.schema(
            [(name, pa.string()) for name in ("instrument_id", "kind", "received_ts", "payload_json")]
        )
        with pq.ParquetWriter(temp, schema, compression="zstd") as writer:
            rows = []
            with state["journal"].open("rb") as handle:
                for line in handle:
                    rows.append(json.loads(line))
                    if len(rows) >= 10000:
                        writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                        rows = []
            if rows:
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
        with temp.open("rb") as handle:
            os.fsync(handle.fileno())
        digest = self._file_sha256(temp)
        sealed_path = state["parquet"].with_name(state["parquet"].stem + "-" + digest + ".parquet")
        if sealed_path.exists():
            if self._file_sha256(sealed_path) != digest:
                raise ArchiveIntegrityError("existing sealed segment checksum mismatch")
            temp.unlink()
        else:
            temp.replace(sealed_path)
        state["sealed_path"] = sealed_path
        state["sealed_sha256"] = digest
        self._completed.append(self._manifest(state, sealed=True))

    def flush_expired(self, now=None):
        """Seal previous UTC hours even when a quiet source sends no new frame."""
        current_hour = iso(now)[:13]
        with self._lock:
            for key, state in list(self._states.items()):
                if state["hour"] < current_hour:
                    self._seal(state)
                    del self._states[key]
            return self.drain_completed()

    def flush_all(self):
        with self._lock:
            for state in self._states.values():
                self._seal(state)
            self._states.clear()
            return self.drain_completed()

    def drain_completed(self):
        with self._lock:
            completed = self._completed
            self._completed = []
            return completed

    @staticmethod
    def read_verified(path, checksum, *, byte_length=None):
        import json

        import pyarrow.parquet as pq

        with Path(path).open("rb") as handle:
            data = handle.read(byte_length) if byte_length is not None else handle.read()
        if hashlib.sha256(data).hexdigest() != checksum:
            raise ArchiveIntegrityError("raw archive checksum mismatch")
        if str(path).endswith(".jsonl"):
            return [json.loads(line) for line in data.splitlines()]
        return pq.read_table(io.BytesIO(data)).to_pylist()


def verify_binance_zip(payload, checksum_text, *, filename, kind, expected_unit=None, daily=True):
    parts = checksum_text.strip().split()
    if len(parts) != 2 or parts[1].lstrip("*") != filename:
        raise ArchiveIntegrityError("checksum filename mismatch")
    if hashlib.sha256(payload).hexdigest().lower() != parts[0].lower():
        raise ArchiveIntegrityError("archive checksum mismatch")
    with zipfile.ZipFile(io.BytesIO(payload)) as z:
        members = [n for n in z.namelist() if not n.endswith("/")]
        if len(members) != 1:
            raise ArchiveIntegrityError("archive must contain one CSV")
        rows = list(csv.reader(io.StringIO(z.read(members[0]).decode("utf-8-sig"))))
    if not rows:
        raise ArchiveIntegrityError("empty archive")
    header = None
    try:
        int(rows[0][0])
    except ValueError:
        header = rows.pop(0)
    if not rows:
        raise ArchiveIntegrityError("archive has no data rows")
    if kind == "metrics":
        if header is None:
            raise ArchiveIntegrityError("metrics header absent")
        try:
            index = header.index("create_time")
        except ValueError:
            raise ArchiveIntegrityError("metrics timestamp column absent")
        stamps = [int(parse_time(r[index]).timestamp() * 1000) for r in rows]
        unit = "MS"
        step = 300000
        expected = 288
    elif kind == "fundingRate":
        stamps = [int(r[0]) for r in rows]
        unit = "MS"
        step = None
        expected = None
    else:
        index = 5 if kind == "aggTrades" else 0
        stamps = [int(r[index]) for r in rows]
        unit = "US" if stamps[0] > 10**14 else "MS"
        step = 60000000 if unit == "US" else 60000
        expected = 1440
    if expected_unit is not None and unit != expected_unit:
        raise ArchiveIntegrityError(f"timestamp unit {unit} != semantics {expected_unit}")
    gaps = []
    duplicates = []
    values = [int(r[0]) for r in rows] if kind == "aggTrades" else stamps
    increment = 1 if kind == "aggTrades" else step
    for previous, current in itertools.pairwise(values):
        if current == previous:
            duplicates.append(current)
        elif increment is not None and current != previous + increment:
            gaps.append({"after": previous, "before": current})
    if daily and kind not in ("aggTrades", "fundingRate") and len(rows) != expected:
        gaps.append({"expected_rows": expected, "actual_rows": len(rows)})
    return {
        "rows": rows,
        "header": header,
        "timestamp_unit": unit,
        "gaps": gaps,
        "duplicates": duplicates,
        "complete": not gaps and not duplicates,
        "sha256": parts[0],
    }


async def fetch_archive(
    repo, settings, symbol, date, *, market_type="SPOT", kind="klines", http=None, _scheduled_retry=False
):
    """Download, verify and import a single Binance daily/monthly dataset."""
    import httpx

    from .normalize import funding, kline, oi, trade

    own = http is None
    client = http or httpx.AsyncClient(timeout=90)
    futures = market_type.upper() != "SPOT"
    prefix = "futures/um" if futures else "spot"
    monthly = kind == "fundingRate"
    period = "monthly" if monthly else "daily"
    date = date[:7] if monthly else date
    filename = f"{symbol}-1m-{date}.zip" if kind == "klines" else f"{symbol}-{kind}-{date}.zip"
    url = (
        f"https://data.binance.vision/data/{prefix}/{period}/{kind}/{symbol}/"
        + ("1m/" if kind == "klines" else "")
        + filename
    )
    archive_id = "ARCH-" + hashlib.sha256(url.encode()).hexdigest()
    previous = await repo.get("mkt.raw_archives", archive_id)
    if previous and previous.get("import_complete"):
        return {
            "archive_id": archive_id,
            "path": previous["path"],
            "source_url": url,
            "rows": previous["row_count"],
            "complete": previous["integrity"]["complete"],
            "gaps": previous["integrity"]["gaps"],
            "duplicates": previous["integrity"]["duplicates"],
            "timestamp_unit": previous["integrity"]["timestamp_unit"],
        }
    registry = settings.load_config("registry")
    instrument = next(
        (
            dict(i)
            for i in registry["instruments"]
            if i["venue"] == "binance"
            and i["venue_symbol"] == symbol
            and (i["market_type"] != "SPOT") == futures
        ),
        None,
    )
    if instrument is None:
        raise ValueError("archive instrument not present in registry")
    try:
        for attempt in range(1):
            response = await client.get(url)
            response.raise_for_status()
            checksum = await client.get(url + ".CHECKSUM")
            checksum.raise_for_status()
            expected = "MS" if futures or date < "2025-01" else "US"
            try:
                verified = verify_binance_zip(
                    response.content,
                    checksum.text,
                    filename=filename,
                    kind=kind,
                    expected_unit=expected if kind in ("klines", "aggTrades") else None,
                    daily=not monthly,
                )
                if not verified["complete"]:
                    raise ArchiveIntegrityError("archive continuity incomplete")
                break
            except ArchiveIntegrityError as exc:
                from datetime import timedelta

                delay = settings.load_config("thresholds").get("archive_retry_s", 3600)
                rejected_at = iso()
                retry = {
                    "review_id": "archive-retry:" + archive_id,
                    "subject": "ARCHIVE_RETRY",
                    "label": "FAILED" if _scheduled_retry else "PENDING",
                    "at": rejected_at,
                    "retry_at": iso(parse_time(rejected_at) + timedelta(seconds=delay)),
                    "args": {"symbol": symbol, "date": date, "market_type": market_type, "kind": kind},
                    "reason": str(exc),
                    "attempts": 2 if _scheduled_retry else 1,
                }
                if await repo.get("qa.reviews", retry["review_id"]) is not None:
                    retry["review_id"] += ":" + new_id("REV")
                await repo.put("qa.reviews", retry["review_id"], retry)
                raise
        directory = Path(settings.data_dir) / "market" / "binance" / prefix / kind
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / filename
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(response.content)
        tmp.replace(path)
        path.with_suffix(".zip.CHECKSUM").write_text(checksum.text)
        provenance = {"config_version": settings.config_version, "code_version": settings.code_version}
        archive_id = "ARCH-" + hashlib.sha256(url.encode()).hexdigest()
        await repo.put(
            "mkt.raw_archives",
            archive_id,
            dict(
                archive_id=archive_id,
                instrument_id=instrument["instrument_id"],
                kind=kind,
                path=str(path),
                sha256=verified["sha256"],
                received_ts=iso(),
                source_url=url,
                integrity={k: v for k, v in verified.items() if k not in ("rows", "header")},
                **provenance,
            ),
        )
        rows = verified["rows"]
        header = verified["header"]
        count = 0
        flow = None
        if kind == "aggTrades":
            from .flow import FlowAccumulator

            flow = FlowAccumulator()
        for raw in rows:
            if kind == "klines":
                record = kline(instrument, raw)
                record["complete"] = bool(record["complete"] and verified["complete"])
                collection = "mkt.klines_1m"
                key = instrument["instrument_id"] + ":" + record["open_time"]
            elif kind == "metrics":
                source = dict(zip(header, raw))
                record = oi(
                    instrument,
                    {
                        "sumOpenInterest": source["sum_open_interest"],
                        "sumOpenInterestValue": source["sum_open_interest_value"],
                        "timestamp": iso(source["create_time"]),
                    },
                )
                collection = "mkt.oi_obs"
                key = instrument["instrument_id"] + ":" + record["metric_key"] + ":" + record["ts"]
            elif kind == "aggTrades":
                source = dict(zip(("a", "p", "q", "f", "l", "T", "m"), raw[:7]))
                source["m"] = str(source["m"]).lower() == "true"
                record = trade(instrument, source)
                collection = "mkt.trades"
                key = instrument["instrument_id"] + ":" + record["trade_id"]
                # Archive quantities remain quote/native; historic FX must be provided separately.
            elif kind == "fundingRate":
                source = (
                    dict(zip(header, raw))
                    if header
                    else dict(zip(("calc_time", "funding_interval_hours", "last_funding_rate"), raw))
                )
                record = funding(
                    instrument,
                    {"fundingTime": source["calc_time"], "fundingRate": source["last_funding_rate"]},
                    kind="SETTLED",
                    interval_h=source["funding_interval_hours"],
                )
                collection = "mkt.funding_obs"
                key = instrument["instrument_id"] + ":SETTLED:" + record["funding_time"]
            else:
                raise ValueError("unsupported archive kind " + kind)
            record.update(source_archive=archive_id, **provenance)
            # Existing live evidence has earlier collection time and possibly verified historic FX.
            existing = await repo.get(collection, key)
            if existing is None:
                record["recorded_at"] = iso()
                await repo.put(collection, key, record, immutable=collection == "mkt.trades")
            if flow is not None:
                flow.add(existing or record)
            count += 1
        if flow is not None:
            for bar_key in flow.bars:
                bar = flow.record(bar_key, complete=verified["complete"])
                bar.update(source_archive=archive_id, received_ts=iso(), recorded_at=iso(), **provenance)
                if not verified["complete"]:
                    bar["gap_reason"] = "ARCHIVE_INTEGRITY_GAP"
                storage_key = bar_key[0] + ":" + bar_key[1]
                old = await repo.get("mkt.flow_bars_1m", storage_key)
                if old is None or not old.get("complete"):
                    await repo.put("mkt.flow_bars_1m", storage_key, bar)
        archive_record = await repo.get("mkt.raw_archives", archive_id)
        archive_record.update(import_complete=True, row_count=count)
        await repo.put("mkt.raw_archives", archive_id, archive_record)
        return {
            "archive_id": archive_id,
            "path": str(path),
            "source_url": url,
            "rows": count,
            "complete": verified["complete"],
            "gaps": verified["gaps"],
            "duplicates": verified["duplicates"],
            "timestamp_unit": verified["timestamp_unit"],
        }
    finally:
        if own:
            await client.aclose()


class ArchiveClient:
    def __init__(self, repo, settings, *, http=None):
        self.repo = repo
        self.settings = settings
        self.http = http

    async def fetch(self, symbol, date, **kwargs):
        return await fetch_archive(self.repo, self.settings, symbol, date, http=self.http, **kwargs)
