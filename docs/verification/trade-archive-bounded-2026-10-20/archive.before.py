"""Durable raw Parquet and verified Binance historical archive ingestion."""

import csv
import hashlib
import io
import itertools
import zipfile
from pathlib import Path

from tank.time_precision import collection_clock
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

        self.root = Path(root) / "raw" / "market_frames"
        self._lock = threading.RLock()
        self._states = {}
        self._completed = []

    def _layout(self, instrument_id, kind, ts):
        directory = (
            self.root
            / ts[:10]
            / "".join(c if c.isalnum() or c in "-_." else "_" for c in instrument_id)
            / kind
        )
        return directory, ts[:13].replace(":", "-")

    def _state(self, instrument_id, kind, ts):
        import json

        directory, stem = self._layout(instrument_id, kind, ts)
        directory.mkdir(parents=True, exist_ok=True)
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
        counts, seen = {}, set()
        if journal.exists():
            with journal.open("rb") as handle:
                for line in handle:
                    row = json.loads(line)
                    counts[row["instrument_id"]] = counts.get(row["instrument_id"], 0) + 1
                    seen.add(hashlib.sha256(line).hexdigest())
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
            "instrument_counts": counts,
            "seen": seen,
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
            "hour_start_prec": "BUCKET(1h)",
            **collection_clock(state["received_ts"]).fields("received_ts"),
        }

    def write(self, instrument_id, kind, payload, *, received_ts=None):
        return self.write_batch([(instrument_id, kind, payload, received_ts)])[0]

    def write_batch(self, entries):
        """One fsync per journal per capture flush, never one per live frame."""
        import os
        from collections import defaultdict

        grouped = defaultdict(list)
        states = {}
        with self._lock:
            newest = None
            for instrument_id, kind, payload, received_ts in entries:
                ts = iso(received_ts)
                newest = max(newest, ts) if newest else ts
                state = self._state(instrument_id, kind, ts)
                key = str(state["journal"])
                record = {
                    "instrument_id": instrument_id,
                    "kind": kind,
                    "market_type": payload.get("market_type") if isinstance(payload, dict) else None,
                    **collection_clock(ts).fields("received_ts"),
                    "payload_json": canonical_json(payload),
                }
                line = (canonical_json(record) + "\n").encode()
                digest = hashlib.sha256(line).hexdigest()
                if getattr(self, "deduplicate", False) and (digest in state["seen"] or line in grouped[key]):
                    continue
                grouped[key].append(line)
                state["received_ts"] = ts
                states[key] = state
            manifests = []
            for key, lines in grouped.items():
                state = states[key]
                if state["journal"].exists() and state["journal"].stat().st_size != state["byte_length"]:
                    self._states.pop(key, None)
                    state = self._state(state["instrument_id"], state["kind"], state["received_ts"])
                if getattr(self, "deduplicate", False):
                    lines = [line for line in lines if hashlib.sha256(line).hexdigest() not in state["seen"]]
                block = b"".join(lines)
                with state["journal"].open("ab") as handle:
                    handle.write(block)
                    handle.flush()
                    os.fsync(handle.fileno())
                state["hasher"].update(block)
                state["row_count"] += len(lines)
                for line in lines:
                    import json

                    owner = json.loads(line)["instrument_id"]
                    state["instrument_counts"][owner] = state["instrument_counts"].get(owner, 0) + 1
                    state["seen"].add(hashlib.sha256(line).hexdigest())
                state["byte_length"] += len(block)
                manifests.append(self._manifest(state))
            if newest:
                for key, state in list(self._states.items()):
                    if state["hour"] < newest[:13]:
                        self._seal(state)
                        self._states.pop(key)
            return manifests

    @staticmethod
    def _version_path(canonical, digest):
        return canonical.with_name(canonical.stem + "-" + digest + ".parquet")

    def _publish(self, canonical, version):
        # Supplementary market-frame paths retain their previous immutable layout.
        return None

    def _seal(self, state):
        import json
        import os

        import pyarrow as pa
        import pyarrow.parquet as pq

        temp = state["parquet"].with_suffix(".tmp")
        schema = pa.schema(
            [
                (name, pa.string())
                for name in (
                    "instrument_id",
                    "market_type",
                    "kind",
                    "received_ts",
                    "received_ts_prec",
                    "received_ts_prec_reason",
                    "payload_json",
                )
            ]
        )
        actual_hash, actual_count, actual_length = hashlib.sha256(), 0, 0
        with pq.ParquetWriter(temp, schema, compression="zstd") as writer:
            rows = []
            with state["journal"].open("rb") as handle:
                for line in handle:
                    row = json.loads(line)
                    rows.append(row)
                    actual_hash.update(line)
                    actual_count += 1
                    actual_length += len(line)
                    state["received_ts"] = row["received_ts"]
                    if len(rows) >= 10000:
                        writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                        rows = []
            if rows:
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
        with temp.open("rb") as handle:
            os.fsync(handle.fileno())
        digest = self._file_sha256(temp)
        sealed_path = self._version_path(state["parquet"], digest)
        sealed_path.parent.mkdir(parents=True, exist_ok=True)
        if sealed_path.exists():
            if self._file_sha256(sealed_path) != digest:
                raise ArchiveIntegrityError("existing sealed segment checksum mismatch")
            temp.unlink()
        else:
            temp.replace(sealed_path)
        self._publish(state["parquet"], sealed_path)
        state.update(hasher=actual_hash, row_count=actual_count, byte_length=actual_length)
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
    def read_verified(path, checksum, *, byte_length=None, instrument_id=None):
        import json

        import pyarrow.parquet as pq

        with Path(path).open("rb") as handle:
            data = handle.read(byte_length) if byte_length is not None else handle.read()
        if hashlib.sha256(data).hexdigest() != checksum:
            raise ArchiveIntegrityError("raw archive checksum mismatch")
        if str(path).endswith(".jsonl"):
            rows = [json.loads(line) for line in data.splitlines()]
        else:
            rows = pq.read_table(io.BytesIO(data)).to_pylist()
        return [row for row in rows if instrument_id is None or row.get("instrument_id") == instrument_id]


class BookArchive(RawArchive):
    """Registry-bound shared venue/symbol UTC-hour raw book evidence."""

    deduplicate = True

    def __init__(self, root, *, instruments):
        super().__init__(root)
        self.root = Path(root) / "raw" / "books"
        self.instruments = {i["instrument_id"]: dict(i) for i in instruments}

    def _layout(self, instrument_id, kind, ts):
        from urllib.parse import quote

        instrument = self.instruments[instrument_id]
        venue, symbol = str(instrument["venue"]).lower(), str(instrument["venue_symbol"])
        if any(not value or value in (".", "..") for value in (venue, symbol)):
            raise ArchiveIntegrityError("raw book registry path identity is incomplete")
        return self.root / quote(venue, safe="") / quote(symbol, safe="") / ts[:10], ts[11:13]

    def _state(self, instrument_id, kind, ts):
        return super()._state(instrument_id, "BOOKS", ts)

    @staticmethod
    def _version_path(canonical, digest):
        return canonical.parent / ".versions" / (canonical.stem + "." + digest + ".parquet")

    def _publish(self, canonical, version):
        import os

        temporary = canonical.with_suffix(".publish.tmp")
        temporary.unlink(missing_ok=True)
        os.link(version, temporary)
        temporary.replace(canonical)
        for directory in (version.parent, canonical.parent):
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _manifest(self, state, sealed=False):
        result = super()._manifest(state, sealed)
        result["parquet_path"] = str(state["parquet"])
        result["integrity"] = {
            "physical_scope": "VENUE_SYMBOL_UTC_HOUR_ALL_MARKET_TYPES",
            "instrument_row_counts": dict(state["instrument_counts"]),
            "canonical_parquet_path": str(state["parquet"]),
        }
        return result

    @staticmethod
    def _expand(manifests):
        expanded = []
        for manifest in manifests:
            if manifest.get("integrity", {}).get("row_scope"):
                expanded.append(manifest)
                continue
            for iid, count in manifest["integrity"]["instrument_row_counts"].items():
                point = dict(
                    manifest,
                    instrument_id=iid,
                    archive_id=manifest["archive_id"] + ":" + hashlib.sha256(iid.encode()).hexdigest(),
                )
                point["integrity"] = dict(
                    manifest["integrity"],
                    row_scope={"instrument_id": iid},
                    filtered_instrument_row_count=count,
                )
                expanded.append(point)
        return expanded

    def write_batch(self, entries):
        return self._expand(super().write_batch(entries))

    def drain_completed(self):
        return self._expand(super().drain_completed())


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
        csv_bytes = z.read(members[0])
        rows = list(csv.reader(io.StringIO(csv_bytes.decode("utf-8-sig"))))
    if not rows:
        raise ArchiveIntegrityError("empty archive")
    header = None
    try:
        int(rows[0][0])
    except ValueError:
        header = rows.pop(0)
    if not rows:
        raise ArchiveIntegrityError("archive has no data rows")
    normalization = {}
    if kind == "metrics":
        if header is None:
            raise ArchiveIntegrityError("metrics header absent")
        try:
            index = header.index("create_time")
        except ValueError:
            raise ArchiveIntegrityError("metrics timestamp column absent")
        stamps = [int(parse_time(r[index]).timestamp() * 1000) for r in rows]
        # Official metrics CSVs may be physically unordered. Preserve the ZIP
        # and CSV evidence, and validate the actual chronological buckets.
        ordered = sorted(zip(stamps, rows, strict=True), key=lambda item: item[0])
        normalization = {
            "order_changed": stamps != [stamp for stamp, _ in ordered],
            "normalization": {
                "method": "METRICS_STABLE_ASCENDING_CREATE_TIME",
                "original_csv_sha256": hashlib.sha256(csv_bytes).hexdigest(),
                "original_timestamp_order_sha256": hashlib.sha256(
                    canonical_json(stamps).encode()
                ).hexdigest(),
                "rows_preserved": len(rows),
            },
        }
        stamps = [stamp for stamp, _ in ordered]
        rows = [row for _, row in ordered]
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
        **normalization,
    }


async def fetch_archive(
    repo,
    settings,
    symbol,
    date,
    *,
    market_type="SPOT",
    kind="klines",
    http=None,
    _scheduled_retry=False,
    market=None,
):
    """Download, verify and import a single Binance daily/monthly dataset."""
    import httpx

    from .normalize import funding, kline, oi, trade

    if market is not None:
        await market.flush_market()
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
        for _attempt in range(1):
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
        directory = Path(settings.data_dir) / "archive" / "binance" / prefix / kind
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / filename
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(response.content)
        tmp.replace(path)
        path.with_suffix(".zip.CHECKSUM").write_text(checksum.text)
        provenance = {"config_version": settings.config_version, "code_version": settings.code_version}
        archive_id = "ARCH-" + hashlib.sha256(url.encode()).hexdigest()
        archive_record = dict(
            archive_id=archive_id,
            instrument_id=instrument["instrument_id"],
            kind=kind,
            path=str(path),
            sha256=verified["sha256"],
            **collection_clock().fields("received_ts"),
            source_url=url,
            integrity={k: v for k, v in verified.items() if k not in ("rows", "header")},
            **provenance,
        )
        rows = verified["rows"]
        header = verified["header"]
        count = 0
        flow = None
        batches = {}

        async def batch(collection, key, record):
            values = batches.setdefault(collection, {})
            values[key] = record
            if len(values) >= 1000:
                if collection == "mkt.flow_bars_1m":
                    for manifest in trades.flush():
                        manifest.update(token_id=instrument["token_id"], **provenance)
                        await batch("mkt.raw_archives", manifest["archive_id"], manifest)
                await repo.put_market_batch(collection, list(values.values()), keys=list(values))
                values.clear()

        if kind == "aggTrades":
            from .flow import FlowAccumulator

            flow = FlowAccumulator()
            if market is not None:
                flow.boundaries = market.flow.boundaries
                flow.references = market.flow.references
            trades = (
                market.trade_archive
                if market is not None
                else TradeArchive(settings.data_dir, instruments=[instrument])
            )
            previous_minute = None
            next_minute = parse_time(date).replace(hour=0, minute=0, second=0, microsecond=0)

        async def emit_bar(key):
            minute = parse_time(key[1])
            from datetime import timedelta

            flow.prove_range(
                instrument["instrument_id"],
                minute,
                minute + timedelta(minutes=1),
                complete=verified["complete"],
                source="VERIFIED_BINANCE_DAILY_ARCHIVE",
            )
            bar = flow.record(key, complete=verified["complete"])
            trades.reference_points(instrument["instrument_id"], bar.get("reference_points", []))
            if market is not None:
                for point in bar.get("reference_points", []):
                    if point.get("complete") and point.get("last_trade"):
                        market.flow.references[instrument["instrument_id"], point["boundary"]] = point
            bar.update(
                source_archive=archive_id,
                **collection_clock().fields("received_ts"),
                **collection_clock().fields("recorded_at"),
                **provenance,
            )
            await batch("mkt.flow_bars_1m", key[0] + ":" + key[1], bar)
            flow.bars.pop(key)

        for raw in rows:
            if kind == "klines":
                record = kline(instrument, raw)
                record["complete"] = bool(record["complete"] and verified["complete"])
                collection = "mkt.klines_1m"
                key = instrument["instrument_id"] + ":" + record["open_time"]
            elif kind == "metrics":
                source = dict(zip(header, raw, strict=False))
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
                source = dict(zip(("a", "p", "q", "f", "l", "T", "m"), raw[:7], strict=False))
                source["m"] = str(source["m"]).lower() == "true"
                record = trade(instrument, source, source="ARCHIVE")
                collection = None
                key = instrument["instrument_id"] + ":" + record["trade_id"]
                # Archive quantities remain quote/native; historic FX must be provided separately.
            elif kind == "fundingRate":
                source = (
                    dict(zip(header, raw, strict=False))
                    if header
                    else dict(
                        zip(("calc_time", "funding_interval_hours", "last_funding_rate"), raw, strict=False)
                    )
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
            record.update(collection_clock().fields("recorded_at"))
            if flow is not None:
                minute = iso(parse_time(record["ts"]).replace(second=0, microsecond=0))
                from datetime import timedelta

                if previous_minute is not None and minute != previous_minute:
                    await emit_bar((instrument["instrument_id"], previous_minute))
                    next_minute = parse_time(previous_minute) + timedelta(minutes=1)
                if minute != previous_minute:
                    while next_minute < parse_time(minute):
                        flow.empty_minute(instrument, next_minute)
                        await emit_bar((instrument["instrument_id"], iso(next_minute)))
                        next_minute += timedelta(minutes=1)
                trades.append(record)
                flow.add(record, continuity=False)
                previous_minute = minute
                if count and count % 1000 == 0:
                    for manifest in trades.flush(seal_before=record["ts"]):
                        manifest.update(token_id=instrument["token_id"], **provenance)
                        await batch("mkt.raw_archives", manifest["archive_id"], manifest)
            else:
                await batch(collection, key, record)
            count += 1
        if flow is not None:
            from datetime import timedelta

            if previous_minute is not None:
                await emit_bar((instrument["instrument_id"], previous_minute))
            day = parse_time(date)
            next_minute = (
                parse_time(previous_minute) + timedelta(minutes=1) if previous_minute is not None else day
            )
            while next_minute < day + timedelta(days=1):
                flow.empty_minute(instrument, next_minute)
                await emit_bar((instrument["instrument_id"], iso(next_minute)))
                next_minute += timedelta(minutes=1)
            trades.coverage(
                instrument["instrument_id"],
                day,
                day + timedelta(days=1),
                complete=verified["complete"],
                source="VERIFIED_BINANCE_DAILY_ARCHIVE",
            )
            for manifest in trades.flush(seal_before=day + timedelta(days=1)):
                manifest.update(token_id=instrument["token_id"], **provenance)
                await batch("mkt.raw_archives", manifest["archive_id"], manifest)
        archive_record.update(import_complete=True, row_count=count)
        await batch("mkt.raw_archives", archive_id, archive_record)
        for collection, values in batches.items():
            if values:
                await repo.put_market_batch(collection, list(values.values()), keys=list(values))
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


class TradeArchive:
    """Normalized hourly trade evidence; PostgreSQL contains only aggregates.

    Immutable Parquet batches are fsynced once per flush, with checksummed minute
    references. Immutable Parquet versions seal completed hours. Recovery reads are
    explicit capture audits; ordinary Market Truth never calls this reader.
    """

    RECOVERY_CAUSES = frozenset({"REPLAY", "AUDIT", "RESTART_RECOVERY", "LATE_BAR_RECOVERY"})

    def __init__(self, root, *, instruments=()):
        import threading

        self.root = Path(root) / "raw" / "trades"
        self._states = {}
        self._ready_manifests = []
        self.instruments = {i["instrument_id"]: dict(i) for i in instruments}
        self._lock = threading.RLock()

    def _legacy_paths(self, instrument_id, timestamp):
        stamp = iso(timestamp)
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in instrument_id)
        directory = self.root / stamp[:10] / safe
        stem = stamp[:13].replace(":", "-")
        return directory / (stem + ".jsonl"), directory / (stem + ".index.json")

    def _paths(self, instrument_id, timestamp):
        from urllib.parse import quote

        instrument = self.instruments.get(instrument_id)
        if instrument is None:
            # Read compatibility for pre-HLD-layout captured evidence. New live
            # producers bind registry identity before any normalized trade append.
            legacy, index = self._legacy_paths(instrument_id, timestamp)
            return legacy.with_suffix(".parquet"), index.with_name(index.stem + ".parquet-batches.json")
        stamp = iso(timestamp)
        components = [str(instrument[k]) for k in ("venue", "market_type", "venue_symbol")]
        if any(not value or value in (".", "..") for value in components):
            raise ArchiveIntegrityError("raw trade registry path identity is incomplete")
        venue, market, symbol = [quote(v, safe="") for v in components]
        directory = self.root / venue.lower() / market.lower() / symbol / stamp[:10]
        stem = stamp[11:13]
        return directory / (stem + ".parquet"), directory / (stem + ".index.json")

    @staticmethod
    def _index_hash(index):

        return hashlib.sha256(
            canonical_json({k: v for k, v in index.items() if k != "index_sha256"}).encode()
        ).hexdigest()

    @staticmethod
    def _sync_directory(path):
        import os

        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _state(self, instrument_id, timestamp):
        # MarketService also preloads hours from worker threads. Protect the
        # lookup, cold-state eviction and installation as one operation, just
        # like append/recovery/flush; callers already holding the RLock may reenter.
        with self._lock:
            return self._load_state(instrument_id, timestamp)

    def _load_state(self, instrument_id, timestamp):
        import json

        canonical, index_path = self._paths(instrument_id, timestamp)
        key = str(canonical)
        alternate_index = index_path.with_name(index_path.stem + ".parquet-batches.json")
        if alternate_index.exists():
            index_path = alternate_index
        cold = [
            k
            for k, value in self._states.items()
            if k != key
            and value["index"]["instrument_id"] == instrument_id
            and not value["dirty"]
            and value["index"].get("sealed_row_count") == value["index"]["row_count"]
        ]
        for old in cold[:-2]:
            self._states.pop(old)
        if key in self._states:
            return self._states[key]
        index = {
            "format": "PARQUET_BATCHES_V1",
            "instrument_id": instrument_id,
            "hour_start": iso(timestamp)[:13] + ":00:00Z",
            "chunks": {},
            "coverage": [],
            "row_count": 0,
            "byte_length": 0,
        }
        pending = {}
        if index_path.exists():
            existing = json.loads(index_path.read_text())
            if self._index_hash(existing) != existing.get("index_sha256"):
                raise ArchiveIntegrityError("normalized trade index checksum mismatch")
            if existing.get("format") == "PARQUET_BATCHES_V1":
                index = existing
            else:
                # Legacy metadata/JSONL is read-only. Import its verified prefix
                # in memory; the next durability flush writes only Parquet.
                legacy_path = Path(existing["journal_path"])
                rows = RawArchive.read_verified(
                    legacy_path, existing["sha256"], byte_length=existing["byte_length"]
                )
                pending = {str(row["trade_id"]): row for row in rows}
                index.update(
                    coverage=existing.get("coverage", []),
                    reference_points=existing.get("reference_points", {}),
                    legacy_origin={
                        "journal_path": str(legacy_path),
                        "index_path": str(index_path),
                        "sha256": existing["sha256"],
                    },
                )
                index_path = index_path.with_name(index_path.stem + ".parquet-batches.json")
        else:
            legacy_path, legacy_index = self._legacy_paths(instrument_id, timestamp)
            if legacy_path.exists():
                if not legacy_index.exists():
                    raise ArchiveIntegrityError("legacy normalized trade journal has no verified index")
                old = json.loads(legacy_index.read_text())
                if self._index_hash(old) != old.get("index_sha256"):
                    raise ArchiveIntegrityError("legacy normalized trade index checksum mismatch")
                rows = RawArchive.read_verified(legacy_path, old["sha256"], byte_length=old["byte_length"])
                pending = {str(row["trade_id"]): row for row in rows}
                index.update(
                    coverage=old.get("coverage", []),
                    reference_points=old.get("reference_points", {}),
                    legacy_origin={
                        "journal_path": str(legacy_path),
                        "index_path": str(legacy_index),
                        "sha256": old["sha256"],
                    },
                )
        seen = set(pending)
        checked = set()
        for chunks in index["chunks"].values():
            for chunk in chunks:
                if chunk["path"] in checked:
                    continue
                checked.add(chunk["path"])
                rows = RawArchive.read_verified(chunk["path"], chunk["sha256"])
                seen.update(str(row["trade_id"]) for row in rows)
        state = {
            "index": index,
            "index_path": index_path,
            "journal": canonical,
            "seen": seen,
            "pending": pending,
            "dirty": bool(pending),
        }
        self._states[key] = state
        return state

    def append(self, row):
        """Deduplicate within the actual hourly evidence, including after restart."""
        with self._lock:
            if row["instrument_id"] not in self.instruments and all(
                row.get(k) for k in ("venue", "market_type", "venue_symbol")
            ):
                self.instruments[row["instrument_id"]] = {
                    k: row[k] for k in ("instrument_id", "venue", "market_type", "venue_symbol")
                }
            state = self._state(row["instrument_id"], row["ts"])
            identity = str(row["trade_id"])
            if identity in state["seen"]:
                return False
            state["seen"].add(identity)
            state["pending"][identity] = row
            state["dirty"] = True
            return True

    def minute_provenance(self, instrument_id, minute):
        """Copy committed references without exposing mutable writer state."""
        from copy import deepcopy

        with self._lock:
            state = self._states.get(str(self._paths(instrument_id, minute)[0]))
            if state is None:
                return None
            return {
                "chunks": deepcopy(state["index"]["chunks"].get(iso(minute), [])),
                "index_path": str(state.get("index_ref_path", state["index_path"])),
                "index_sha256": state["index"].get("index_sha256"),
            }

    def coverage(
        self,
        instrument_id,
        start,
        end,
        *,
        complete,
        reason=None,
        source="CAPTURE_CONTINUITY",
        observed_at=None,
    ):
        from datetime import timedelta

        cursor = parse_time(start).replace(minute=0, second=0, microsecond=0)
        while cursor < parse_time(end):
            with self._lock:
                state = self._state(instrument_id, cursor)
                point = {
                    "start": iso(start),
                    "end": iso(end),
                    "complete": bool(complete),
                    "reason": reason,
                    "source": source,
                    **collection_clock(observed_at).fields("observed_at"),
                }
                history = state["index"]["coverage"]
                if (
                    history
                    and all(
                        history[-1].get(k) == point.get(k) for k in ("start", "complete", "reason", "source")
                    )
                    and parse_time(history[-1]["end"]) <= parse_time(end)
                ):
                    history[-1] = point
                else:
                    history.append(point)
                state["dirty"] = True
            cursor += timedelta(hours=1)

    def _freeze_index(self, state):
        """A frozen manifest must never point at subsequently replaced index bytes."""
        import json
        import os

        index = state["index"]
        path = state["index_path"].with_name(state["index_path"].stem + "-" + index["index_sha256"] + ".json")
        if path.exists():
            if self._index_hash(json.loads(path.read_text())) != index["index_sha256"]:
                raise ArchiveIntegrityError("existing normalized index version mismatch")
        else:
            with path.open("x") as handle:
                json.dump(index, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
        state["index_ref_path"] = path
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return path

    def reference_points(self, instrument_id, points):
        """Persist bounded requested-boundary summaries for capture recovery."""
        with self._lock:
            for point in points:
                state = self._state(instrument_id, point["boundary"])
                references = state["index"].setdefault("reference_points", {})
                previous = references.get(point["boundary"], {})

                def content(row):
                    return {
                        key: value
                        for key, value in row.items()
                        if key not in ("observed_at", "observed_at_prec", "observed_at_prec_reason")
                    }

                if content(previous) != content(point):
                    references[point["boundary"]] = point
                    state["dirty"] = True

    @staticmethod
    def _write_parquet(rows, directory, stem):
        import os

        import pyarrow as pa
        import pyarrow.parquet as pq

        absent = []
        cursor = directory
        while not cursor.exists():
            absent.append(cursor)
            cursor = cursor.parent
        directory.mkdir(parents=True, exist_ok=True)
        for created in reversed(absent):
            TradeArchive._sync_directory(created.parent)
        fields = sorted(
            {"instrument_id", "trade_id", "ts", "price", "qty_native", "side"}.union(*(set(r) for r in rows))
        )
        schema = pa.schema([(field, pa.string()) for field in fields])
        values = [{f: None if row.get(f) is None else str(row[f]) for f in fields} for row in rows]
        temporary = directory / (stem + ".tmp.parquet")
        pq.write_table(pa.Table.from_pylist(values, schema=schema), temporary, compression="zstd")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        digest = RawArchive._file_sha256(temporary)
        path = directory / (stem + "." + digest + ".parquet")
        if path.exists():
            if RawArchive._file_sha256(path) != digest:
                raise ArchiveIntegrityError("existing normalized Parquet checksum mismatch")
            temporary.unlink()
        else:
            temporary.replace(path)
        TradeArchive._sync_directory(directory)
        return path, digest

    def _persist_index(self, state):
        import json
        import os

        index = state["index"]
        index["index_sha256"] = self._index_hash(index)
        path = state["index_path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        with temporary.open("w") as handle:
            json.dump(index, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        self._sync_directory(path.parent)
        self._freeze_index(state)

    def flush(self, *, seal_before=None, seal_exclude=()):
        from collections import defaultdict

        with self._lock:
            manifests = self._ready_manifests
            for state in self._states.values():
                index = state["index"]
                should_seal = (
                    seal_before
                    and index["instrument_id"] not in seal_exclude
                    and (index["instrument_id"], index["hour_start"]) not in seal_exclude
                    and parse_time(index["hour_start"])
                    < parse_time(seal_before).replace(minute=0, second=0, microsecond=0)
                    and (index.get("sealed_row_count") != index["row_count"] or bool(state["pending"]))
                )
                if not state["dirty"] and not should_seal:
                    continue
                from copy import deepcopy

                prior_index = deepcopy(index)
                prior_pending = dict(state["pending"])
                try:
                    rows = sorted(state["pending"].values(), key=lambda r: (r["ts"], str(r["trade_id"])))
                    # One immutable Parquet write per flush, never a per-frame rewrite
                    # of all preceding rows. Empty batches pin coverage-only changes.
                    path, digest = self._write_parquet(
                        rows, state["journal"].parent / ".batches", state["journal"].stem
                    )
                    groups = defaultdict(int)
                    for row in rows:
                        groups[iso(parse_time(row["ts"]).replace(second=0, microsecond=0))] += 1
                    for minute, count in groups.items():
                        index["chunks"].setdefault(minute, []).append(
                            {"path": str(path), "sha256": digest, "row_count": count}
                        )
                    index["row_count"] += len(rows)
                    index["byte_length"] += path.stat().st_size if rows else 0
                    index["received_ts"] = iso()
                    index["last_batch"] = {"path": str(path), "sha256": digest}
                    state["pending"].clear()
                    state["dirty"] = False
                    count = len(rows)
                    if should_seal:
                        path, digest = self._seal(state)
                        count = index["row_count"]
                    self._persist_index(state)
                    manifests.append(
                        {
                            "archive_id": "TRADES-"
                            + hashlib.sha256(
                                (str(path) + digest + index["index_sha256"]).encode()
                            ).hexdigest(),
                            "instrument_id": index["instrument_id"],
                            "kind": "NORMALIZED_TRADES",
                            "path": str(path),
                            "sha256": digest,
                            "row_count": count,
                            "byte_length": path.stat().st_size,
                            "hour_start": index["hour_start"],
                            "hour_start_prec": "BUCKET(1h)",
                            "checksum_scope": "FILE",
                            "status": "SEALED" if should_seal else "OPEN",
                            "integrity": {
                                "canonical_parquet_path": str(state["journal"]),
                                "required_batch_sha256": [digest]
                                if should_seal
                                else sorted({c["sha256"] for cs in index["chunks"].values() for c in cs}),
                                "contained_batch_sha256": sorted(
                                    {c["sha256"] for cs in index["chunks"].values() for c in cs}
                                )
                                if should_seal
                                else [],
                                "legacy_origin": index.get("legacy_origin"),
                                "index_path": str(state["index_ref_path"]),
                                "index_sha256": index["index_sha256"],
                                "coverage": list(index["coverage"]),
                                "reference_points": list(index.get("reference_points", {}).values()),
                            },
                            **collection_clock(index["received_ts"]).fields("received_ts"),
                        }
                    )
                except BaseException:
                    state["index"] = prior_index
                    state["pending"] = prior_pending
                    state["dirty"] = True
                    raise
            self._ready_manifests = []
            return manifests

    def _seal(self, state):
        import os

        import pyarrow as pa
        import pyarrow.parquet as pq

        index = state["index"]
        chunks = {}
        for minute_chunks in index["chunks"].values():
            for chunk in minute_chunks:
                chunks[chunk["path"]] = chunk
        fields = {"instrument_id", "trade_id", "ts", "price", "qty_native", "side"}
        for chunk in chunks.values():
            if RawArchive._file_sha256(chunk["path"]) != chunk["sha256"]:
                raise ArchiveIntegrityError("normalized Parquet batch checksum mismatch")
            fields.update(pq.read_schema(chunk["path"]).names)
        schema = pa.schema([(field, pa.string()) for field in sorted(fields)])
        canonical = state["journal"]
        versions = canonical.parent / ".versions"
        versions.mkdir(parents=True, exist_ok=True)
        temporary = versions / (canonical.stem + ".tmp.parquet")
        # Hour compaction is bounded to Arrow batches; no full-hour Python list
        # or repeated per-frame rewrite. Immutable batch files remain frozen refs.
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            for chunk in chunks.values():
                for batch in pq.ParquetFile(chunk["path"]).iter_batches(batch_size=10000):
                    table = pa.Table.from_batches([batch])
                    writer.write_table(
                        pa.Table.from_arrays(
                            [
                                table[field]
                                if field in table.column_names
                                else pa.nulls(table.num_rows, type=pa.string())
                                for field in schema.names
                            ],
                            schema=schema,
                        )
                    )
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        digest = RawArchive._file_sha256(temporary)
        path = versions / (canonical.stem + "." + digest + ".parquet")
        if path.exists():
            if RawArchive._file_sha256(path) != digest:
                raise ArchiveIntegrityError("existing normalized Parquet version checksum mismatch")
            temporary.unlink()
        else:
            temporary.replace(path)
        self._sync_directory(versions)
        publication = canonical.with_suffix(".publish.tmp")
        publication.unlink(missing_ok=True)
        os.link(path, publication)
        publication.replace(canonical)
        self._sync_directory(canonical.parent)
        index.update(
            canonical_parquet_path=str(canonical),
            sealed_path=str(path),
            sealed_sha256=digest,
            sealed_row_count=index["row_count"],
        )
        return path, digest

    def recover_minute(self, instrument_id, minute, *, cause, verified_batch=None):
        from datetime import timedelta

        if cause not in self.RECOVERY_CAUSES:
            raise ValueError("Raw trade reads require an explicit recovery/replay/audit cause")
        minute = parse_time(minute).replace(second=0, microsecond=0)
        with self._lock:
            state = self._state(instrument_id, minute)
            index = state["index"]
            rows = {}
            chunks = index["chunks"].get(iso(minute), [])
            for chunk in chunks:
                if verified_batch is None:
                    decoded = self.read_verified(chunk["path"], chunk["sha256"])
                else:
                    path = Path(chunk["path"]).resolve()

                    def identity():
                        stat = path.stat()
                        return (
                            str(path),
                            chunk["sha256"],
                            stat.st_dev,
                            stat.st_ino,
                            stat.st_size,
                            stat.st_mtime_ns,
                            stat.st_ctime_ns,
                        )

                    key = identity()
                    if verified_batch.get("key") == key:
                        decoded = verified_batch["rows"]
                    else:
                        # The caller owns this one-batch operation-local reuse.
                        # Discard the old batch before reading another; never
                        # cache current coverage, references or pending prints.
                        verified_batch.clear()
                        decoded = self.read_verified(path, chunk["sha256"])
                        if identity() != key:
                            raise ArchiveIntegrityError(
                                "normalized Parquet batch changed during verification"
                            )
                        verified_batch.update(key=key, rows=decoded)
                for row in decoded:
                    if minute <= parse_time(row["ts"]) < minute + timedelta(minutes=1):
                        rows[str(row["trade_id"])] = dict(row)
            for row in state["pending"].values():
                if minute <= parse_time(row["ts"]) < minute + timedelta(minutes=1):
                    rows[str(row["trade_id"])] = dict(row)
            return {
                "rows": sorted(
                    rows.values(),
                    key=lambda row: (
                        row["ts"],
                        (0, int(row["trade_id"]))
                        if str(row["trade_id"]).isdigit()
                        else (1, str(row["trade_id"])),
                    ),
                ),
                "reference_points": list(index.get("reference_points", {}).values()),
                "coverage": [
                    p
                    for p in index["coverage"]
                    if parse_time(p["start"]) < minute + timedelta(minutes=1)
                    and parse_time(p["end"]) > minute
                ],
                "provenance": {"cause": cause, "index_sha256": index.get("index_sha256"), "chunks": chunks},
            }

    @staticmethod
    def read_verified(path, checksum, *, byte_length=None):
        return RawArchive.read_verified(path, checksum, byte_length=byte_length)

    @staticmethod
    def prefix_available(manifest, manifests):
        """Coverage belongs to the frozen raw prefix, not any selected sub-batch."""
        available = set()
        for point in manifests:
            if point.get("instrument_id") == manifest.get("instrument_id"):
                available.add(point.get("sha256"))
                available.update((point.get("integrity") or {}).get("contained_batch_sha256", []))
        required = (manifest.get("integrity") or {}).get("required_batch_sha256", [])
        return set(required).issubset(available)

    @classmethod
    def validated_coverage(cls, manifest, manifests):
        valid = cls.prefix_available(manifest, manifests)
        return [
            dict(proof, **({"complete": False, "reason": "INCOMPLETE_RAW_BATCH_PREFIX"} if not valid else {}))
            for proof in (manifest.get("integrity") or {}).get("coverage", [])
        ]

    @classmethod
    def rebuild_window(
        cls, manifests, instrument_id, start, end, *, purpose="REPLAY", boundaries=(), instrument=None
    ):
        """Explicit replay/audit from frozen refs; never discovers live files/APIs."""
        from datetime import timedelta

        from .flow import FlowAccumulator

        if purpose not in ("REPLAY", "AUDIT"):
            raise ValueError("Window regeneration is restricted to explicit replay/audit")
        start, end = parse_time(start), parse_time(end)
        manifests = list(manifests)
        rows = {}
        proofs = []
        references = []
        for manifest in manifests:
            if manifest.get("instrument_id") != instrument_id or manifest.get("kind") != "NORMALIZED_TRADES":
                continue
            decoded = cls.read_verified(
                manifest["path"],
                manifest["sha256"],
                byte_length=manifest.get("byte_length")
                if manifest.get("checksum_scope") == "JOURNAL_PREFIX"
                else None,
            )
            for row in decoded:
                if row.get("instrument_id") == instrument_id:
                    identity = str(row["trade_id"])
                    previous = rows.get(identity)
                    if previous and any(
                        previous.get(field) != row.get(field)
                        for field in ("ts", "price", "qty_native", "side")
                    ):
                        raise ArchiveIntegrityError("Conflicting immutable normalized trade identity")
                    rows[identity] = row
            for proof in cls.validated_coverage(manifest, manifests):
                lo, hi = (
                    max(start.replace(second=0, microsecond=0), parse_time(proof["start"])),
                    min(end, parse_time(proof["end"])),
                )
                if lo < hi:
                    proofs.append(dict(proof, start=iso(lo), end=iso(hi)))
            references.append({key: manifest[key] for key in ("path", "sha256", "checksum_scope")})
        proofs.sort(key=lambda proof: proof.get("observed_at") or "")
        accumulator = FlowAccumulator()
        accumulator.register_boundaries(instrument_id, [start, end, *boundaries])
        for manifest in manifests:
            if manifest.get("instrument_id") == instrument_id:
                for point in (manifest.get("integrity") or {}).get("reference_points", []):
                    if parse_time(point["boundary"]) <= end and (
                        not point.get("last_trade")
                        or parse_time(point["last_trade"]["ts"]) < parse_time(point["boundary"])
                    ):
                        accumulator.references[instrument_id, point["boundary"]] = dict(
                            point,
                            **(
                                {"complete": False, "coverage_reason": "INCOMPLETE_RAW_BATCH_PREFIX"}
                                if not cls.prefix_available(manifest, manifests)
                                else {}
                            ),
                        )
        for proof in proofs:
            accumulator.prove_range(
                instrument_id,
                proof["start"],
                proof["end"],
                complete=proof["complete"],
                source="VERIFIED_REPLAY_EVIDENCE",
                reason=proof.get("reason"),
                observed_at=proof.get("observed_at"),
            )
        for row in sorted(rows.values(), key=lambda point: (point["ts"], str(point["trade_id"]))):
            if parse_time(row["ts"]) < end:
                accumulator.add(row, continuity=False)
        if instrument is not None:
            if instrument["instrument_id"] != instrument_id:
                raise ValueError("Replay instrument metadata identity mismatch")
            cursor = start.replace(second=0, microsecond=0)
            while cursor < end:
                accumulator.empty_minute(instrument, cursor)
                cursor += timedelta(minutes=1)
        result = []
        for key in sorted(accumulator.bars):
            minute = parse_time(key[1])
            if minute < end and minute + timedelta(minutes=1) > start:
                result.append(
                    accumulator.record(
                        key, complete=accumulator.covered(minute, minute + timedelta(minutes=1), proofs)
                    )
                )
        return {
            "flow_bars": [
                bar for bar in result if parse_time(bar["minute_start"]) + timedelta(minutes=1) <= end
            ],
            "flow_segments": [
                dict(segment, instrument_id=instrument_id)
                for bar in result
                for segment in bar.get("boundary_segments", [])
                if start <= parse_time(segment["start"]) and parse_time(segment["end"]) <= end
            ],
            "reference_points": [
                dict(point, instrument_id=instrument_id)
                for bar in result
                for point in bar.get("reference_points", [])
                if parse_time(point["boundary"]) <= end
            ],
            "complete": bool(result) and accumulator.covered(start, end, proofs),
            "provenance": {"purpose": purpose, "raw_refs": references},
        }
