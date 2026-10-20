"""Hourly raw Parquet (HLD §7.2) and verified Binance historical archive ingestion."""

import csv
import hashlib
import io
import itertools
import os
import threading
import time
import zipfile
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from urllib.parse import quote

from tank.time_precision import collection_clock
from tank.util import canonical_json, iso, new_id, parse_time, utcnow


class ArchiveIntegrityError(ValueError):
    pass


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


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _component(value):
    value = str(value or "")
    if value in ("", ".", ".."):
        raise ArchiveIntegrityError("raw archive path identity is incomplete")
    return quote(value, safe="")


class HourlyParquet:
    """One Parquet file per UTC hour, written off the capture path.

    The event loop only calls `_add`: rows go to an in-memory buffer, with no lock
    and no disk access. `take` hands the buffer over and `write` runs in a worker
    thread, one call at a time. `write` appends each hour's rows to that hour's
    Arrow IPC journal (`.{hh}.<segment>.arrows`) with one fsync, so a restart keeps
    every flushed row, then seals each finished hour whose journal has been quiet
    for `grace_s` into `{hh}.parquet`. Sealing merges an earlier file of the same
    hour (late backfill rows), drops duplicate rows and replaces the file atomically.
    """

    def __init__(self, root, *, grace_s=120):
        self.root = Path(root)
        self.grace_s = grace_s
        self._buffer = defaultdict(list)
        self._inflight = {}  # taken by the writer, not yet durable
        self._journals = {}
        self._orphans = set()
        self._recovered = False
        self._segment = f"{os.getpid()}-{time.time_ns()}"
        self._sequence = itertools.count()
        # Held only by worker threads (seal publication vs. readers), never by capture.
        self._publish = threading.Lock()

    def _identity(self, row):
        raise NotImplementedError

    @staticmethod
    def _encode(row):
        """Worker thread: final row fields (subclasses serialize nested payloads here)."""
        return row

    def _add(self, canonical, row):
        self._buffer[canonical].append(row)

    def pending(self):
        return sum(len(rows) for rows in self._buffer.values())

    def take(self):
        buffer, self._buffer = self._buffer, defaultdict(list)
        self._inflight = dict(buffer)
        return self._inflight

    def written(self):
        """Event loop: the taken buffer is durable."""
        self._inflight = {}

    def restore(self, buffer):
        """Put back a buffer whose write failed, ahead of rows added since."""
        self._inflight = {}
        for canonical, rows in buffer.items():
            self._buffer[canonical][:0] = rows

    @staticmethod
    def hour_of(canonical):
        canonical = Path(canonical)
        return parse_time(f"{canonical.parent.name}T{canonical.stem}:00:00Z")

    @staticmethod
    def _journal_paths(canonical):
        return sorted(canonical.parent.glob("." + canonical.stem + ".*.arrows"))

    def _recover(self):
        if self.root.exists():
            for journal in self.root.rglob(".*.arrows"):
                self._orphans.add(journal.parent / (journal.name.split(".")[1] + ".parquet"))
        self._recovered = True

    def _open(self, canonical, names):
        import pyarrow as pa

        directory = canonical.parent
        absent = []
        cursor = directory
        while not cursor.exists():
            absent.append(cursor)
            cursor = cursor.parent
        directory.mkdir(parents=True, exist_ok=True)
        for created in reversed(absent):
            _sync_directory(created.parent)
        path = directory / f".{canonical.stem}.{self._segment}.{next(self._sequence)}.arrows"
        handle = path.open("xb")
        schema = pa.schema([(name, pa.string()) for name in names])
        state = {
            "path": path,
            "handle": handle,
            "writer": pa.ipc.new_stream(handle, schema),
            "names": names,
            "touched": time.monotonic(),
        }
        _sync_directory(directory)
        self._journals[canonical] = state
        return state

    def _close(self, canonical):
        state = self._journals.pop(canonical, None)
        if state is not None:
            state["writer"].close()
            state["handle"].flush()
            os.fsync(state["handle"].fileno())
            state["handle"].close()

    def write(self, buffer, *, now=None):
        """Worker thread: append and fsync each hour's journal, then seal quiet hours."""
        import pyarrow as pa

        if not self._recovered:
            self._recover()
        rows_written = 0
        for canonical, rows in buffer.items():
            if not rows:
                continue
            rows = [self._encode(row) for row in rows]
            names = sorted(set().union(*(row.keys() for row in rows)))
            state = self._journals.get(canonical)
            if state is not None and not set(names) <= set(state["names"]):
                self._close(canonical)
                state = None
            if state is None:
                state = self._open(canonical, names)
            table = pa.table(
                {
                    name: pa.array(
                        [None if row.get(name) is None else str(row[name]) for row in rows], pa.string()
                    )
                    for name in state["names"]
                }
            )
            state["writer"].write_table(table)
            state["handle"].flush()
            os.fsync(state["handle"].fileno())
            state["touched"] = time.monotonic()
            rows_written += len(rows)
        sealed = self.seal_due(now)
        return {"rows": rows_written, "sealed": sealed, "open_journals": len(self._journals)}

    def seal_due(self, now=None):
        now = parse_time(now) if now is not None else utcnow()
        quiet = time.monotonic() - self.grace_s
        due = [
            canonical
            for canonical, state in self._journals.items()
            if self.hour_of(canonical) + timedelta(hours=1) <= now and state["touched"] <= quiet
        ]
        due += [
            canonical
            for canonical in self._orphans
            if canonical not in self._journals and self.hour_of(canonical) + timedelta(hours=1) <= now
        ]
        sealed = []
        for canonical in sorted(set(due)):
            self.seal(canonical)
            sealed.append(str(canonical))
        return sealed

    def _batches(self, canonical, journals, columns=None):
        import pyarrow as pa
        import pyarrow.parquet as pq

        if canonical.exists():
            parquet = pq.ParquetFile(canonical)
            present = [c for c in columns if c in parquet.schema_arrow.names] if columns else None
            yield from parquet.iter_batches(batch_size=10000, columns=present)
        for journal in journals:
            try:
                with journal.open("rb") as handle:
                    reader = pa.ipc.open_stream(handle)
                    while True:
                        try:
                            batch = reader.read_next_batch()
                        except StopIteration:
                            break
                        except (pa.ArrowInvalid, OSError):
                            break  # torn tail of a journal cut by a crash: earlier batches stand
                        if columns:
                            batch = batch.select([c for c in columns if c in batch.schema.names])
                        yield batch
            except (pa.ArrowInvalid, OSError):
                continue  # journal created but its schema header never reached disk

    def seal(self, canonical):
        import pyarrow as pa
        import pyarrow.parquet as pq

        canonical = Path(canonical)
        self._close(canonical)
        self._orphans.discard(canonical)
        with self._publish:
            journals = self._journal_paths(canonical)
            if not journals:
                return None
            names = set(pq.read_schema(canonical).names) if canonical.exists() else set()
            for batch in self._batches(canonical, journals):
                names.update(batch.schema.names)
            schema = pa.schema([(name, pa.string()) for name in sorted(names)])
            temporary = canonical.with_name("." + canonical.stem + ".parquet.tmp")
            seen = set()
            count = 0
            with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
                for batch in self._batches(canonical, journals):
                    columns = [
                        batch.column(name) if name in batch.schema.names else pa.nulls(batch.num_rows, pa.string())
                        for name in schema.names
                    ]
                    table = pa.Table.from_arrays(columns, schema=schema)
                    keep = []
                    for row in zip(*(table.column(name).to_pylist() for name in self.identity_fields), strict=True):
                        key = self._identity(row)
                        keep.append(key not in seen)
                        seen.add(key)
                    if not all(keep):
                        table = table.filter(pa.array(keep))
                    if table.num_rows:
                        writer.write_table(table)
                        count += table.num_rows
            with temporary.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary, canonical)
            _sync_directory(canonical.parent)
            for journal in journals:
                journal.unlink()
            _sync_directory(canonical.parent)
            return count

    def read_hour(self, canonical, columns=None):
        """Worker thread: every durable row of one hour (sealed file and journals)."""
        canonical = Path(canonical)
        with self._publish:
            rows = []
            for batch in self._batches(canonical, self._journal_paths(canonical), columns):
                rows.extend(batch.to_pylist())
        seen, unique = set(), []
        for row in rows:
            key = self._identity(tuple(row.get(name) for name in self.identity_fields))
            if key not in seen:
                seen.add(key)
                unique.append(row)
        return unique

    def seal_finished(self, now=None):
        """Seal every journal of a finished hour, quiet or not (end of a batch job or process)."""
        if not self._recovered:
            self._recover()
        now = parse_time(now) if now is not None else utcnow()
        sealed = []
        for canonical in sorted(set(self._journals) | self._orphans):
            if self.hour_of(canonical) + timedelta(hours=1) <= now:
                self.seal(canonical)
                sealed.append(str(canonical))
        return sealed

    def close(self):
        self.seal_finished()
        for canonical in list(self._journals):
            self._close(canonical)


class TradeArchive(HourlyParquet):
    """Hot-tier normalized trades: `raw/trades/{venue}/{market}/{symbol}/{yyyy-mm-dd}/{hh}.parquet`.

    PostgreSQL holds only the 1m bars (03 §14); these files are read back only to
    rebuild a minute for a late or restarted bar, a REST backfill, replay or audit.
    """

    identity_fields = ("instrument_id", "trade_id")
    FLOW_COLUMNS = (
        "instrument_id",
        "token_id",
        "venue",
        "market_type",
        "quote_asset",
        "trade_id",
        "ts",
        "ts_prec",
        "ts_prec_reason",
        "price",
        "qty_native",
        "side",
        "notional_usd",
        "trade_aggregation",
        "sequence",
    )
    COVERAGE_HOURS = 30

    def __init__(self, root, *, instruments=(), grace_s=120):
        super().__init__(Path(root) / "raw" / "trades", grace_s=grace_s)
        self.instruments = {i["instrument_id"]: dict(i) for i in instruments}
        self._seen = {}
        self._minutes = {}
        self._loaded = set()
        self._touched = {}
        self._paths = {}
        self._born = utcnow().replace(minute=0, second=0, microsecond=0)
        self._coverage = defaultdict(list)

    @staticmethod
    def _identity(row):
        return tuple(row)

    def path(self, instrument_id, timestamp):
        stamp = timestamp if isinstance(timestamp, str) and timestamp.endswith("Z") else iso(timestamp)
        key = (instrument_id, stamp[:13])
        cached = self._paths.get(key)
        if cached is not None:
            return cached
        instrument = self.instruments.get(instrument_id)
        if instrument is None:
            raise ArchiveIntegrityError("raw trade instrument is not in the registry: " + str(instrument_id))
        venue, market, symbol = (
            _component(instrument[k]) for k in ("venue", "market_type", "venue_symbol")
        )
        path = self.root / venue.lower() / market.lower() / symbol / stamp[:10] / (stamp[11:13] + ".parquet")
        if len(self._paths) > 100000:
            self._paths.clear()
        self._paths[key] = path
        return path

    def canonical(self, row):
        """Event loop: the hour file of a print, registering a new instrument from the row."""
        iid = row["instrument_id"]
        if iid not in self.instruments and all(row.get(k) for k in ("venue", "market_type", "venue_symbol")):
            self.instruments[iid] = {k: row[k] for k in ("instrument_id", "venue", "market_type", "venue_symbol")}
        return self.path(iid, row["ts"])

    def needs_load(self, canonical):
        """Event loop: whether an earlier process may have left prints of this hour on disk."""
        if canonical in self._loaded:
            return False
        if self.hour_of(canonical) > self._born:
            self._loaded.add(canonical)
            return False
        return True

    def load_ids(self, canonical):
        """Worker thread: identities and minutes of the prints already on disk for one hour."""
        return [
            (str(row["trade_id"]), iso(parse_time(row["ts"]).replace(second=0, microsecond=0)))
            for row in self.read_hour(canonical, columns=self.identity_fields + ("ts",))
        ]

    def mark_loaded(self, canonical, ids):
        seen = self._seen.setdefault(canonical, set())
        minutes = self._minutes.setdefault(canonical, set())
        for identity, minute in ids:
            seen.add(identity)
            minutes.add(minute)
        self._loaded.add(canonical)

    def append(self, row):
        """Event loop: buffer one normalized trade; False when this capture already has it.

        A print of an hour an earlier process wrote is deduplicated only after that
        hour's identities were loaded (`needs_load` / `load_ids` / `mark_loaded`).
        """
        canonical = self.canonical(row)
        seen = self._seen.setdefault(canonical, set())
        identity = str(row["trade_id"])
        if identity in seen:
            return False
        seen.add(identity)
        self._minutes.setdefault(canonical, set()).add(iso(parse_time(row["ts"]).replace(second=0, microsecond=0)))
        self._touched[canonical] = time.monotonic()
        self._add(canonical, row)
        return True

    def remember(self, row):
        """Event loop: a print already on disk (restart warm-up), so it is not taken twice."""
        canonical = self.path(row["instrument_id"], row["ts"])
        self._seen.setdefault(canonical, set()).add(str(row["trade_id"]))
        self._minutes.setdefault(canonical, set()).add(iso(parse_time(row["ts"]).replace(second=0, microsecond=0)))

    def recent(self, instrument_id, since):
        """Worker thread: durable prints at or after `since`, oldest first."""
        import pyarrow as pa
        import pyarrow.compute as pc

        if instrument_id not in self.instruments:
            return []
        since = parse_time(since)
        floor = pa.scalar(iso(since - timedelta(seconds=1)))
        hour, now, rows = since.replace(minute=0, second=0, microsecond=0), utcnow(), {}
        while hour <= now:
            canonical = self.path(instrument_id, hour)
            if canonical.parent.exists():
                with self._publish:
                    for batch in self._batches(canonical, self._journal_paths(canonical), self.FLOW_COLUMNS):
                        if "ts" in batch.schema.names:
                            batch = batch.filter(pc.greater_equal(batch.column("ts"), floor))
                        for row in batch.to_pylist():
                            if row.get("instrument_id") == instrument_id and parse_time(row["ts"]) >= since:
                                rows.setdefault(str(row["trade_id"]), row)
            hour += timedelta(hours=1)
        return sorted(rows.values(), key=lambda r: (parse_time(r["ts"]), str(r["trade_id"])))

    def take(self):
        # Identities are kept for the current and previous hour only: live prints of
        # older hours are rare and are deduplicated again when their hour is sealed.
        horizon = utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        idle = time.monotonic() - 600  # a backfill still writing an old hour keeps its identities
        for canonical in [
            c for c in self._seen if self.hour_of(c) < horizon and self._touched.get(c, 0) < idle
        ]:
            self._touched.pop(canonical, None)
            self._seen.pop(canonical, None)
            self._minutes.pop(canonical, None)
            self._loaded.discard(canonical)
        return super().take()

    def minute_archived(self, instrument_id, minute):
        """Whether this process already archived prints of that minute."""
        if instrument_id not in self.instruments:
            return False
        stamp = iso(parse_time(minute).replace(second=0, microsecond=0))
        return stamp in self._minutes.get(self.path(instrument_id, stamp), ())

    def buffered(self, instrument_id, minute):
        """Event loop: prints of that minute not yet durable (buffered or being written)."""
        if instrument_id not in self.instruments:
            return []
        lo = parse_time(minute).replace(second=0, microsecond=0)
        hi = lo + timedelta(minutes=1)
        canonical = self.path(instrument_id, lo)
        return [
            row
            for rows in (self._inflight.get(canonical, ()), self._buffer.get(canonical, ()))
            for row in rows
            if row["instrument_id"] == instrument_id and lo <= parse_time(row["ts"]) < hi
        ]

    def read_minutes(self, instrument_id, hour):
        """Worker thread: durable prints of one hour, grouped by minute."""
        if instrument_id not in self.instruments:
            return {}
        grouped = defaultdict(list)
        for row in self.read_hour(self.path(instrument_id, hour), columns=self.FLOW_COLUMNS):
            if row.get("instrument_id") == instrument_id:
                grouped[iso(parse_time(row["ts"]).replace(second=0, microsecond=0))].append(row)
        return dict(grouped)

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
        """Event loop: record which spans of an instrument's prints are known complete.

        Kept in memory for the current process only: bars already written before a
        restart carry their own completeness, and minutes spanning a restart are
        rebuilt from the REST backfill of 03 §5.2.
        """
        point = {
            "start": iso(start),
            "end": iso(end),
            "complete": bool(complete),
            "reason": reason,
            "source": source,
            **collection_clock(observed_at).fields("observed_at"),
        }
        history = self._coverage[instrument_id]
        last = history[-1] if history else None
        if (
            last
            and all(last.get(k) == point.get(k) for k in ("complete", "reason", "source"))
            and parse_time(last["start"]) <= parse_time(start) <= parse_time(last["end"])
        ):
            # Contiguous proof of the same kind: extend it (one entry per unbroken span).
            if parse_time(end) > parse_time(last["end"]):
                last.update(end=point["end"], **{k: v for k, v in point.items() if k.startswith("observed_at")})
        else:
            history.append(point)
        horizon = utcnow() - timedelta(hours=self.COVERAGE_HOURS)
        if len(history) > 64 and parse_time(history[0]["end"]) < horizon:
            self._coverage[instrument_id] = [p for p in history if parse_time(p["end"]) >= horizon]

    def coverage_for(self, instrument_id, minute):
        lo = parse_time(minute).replace(second=0, microsecond=0)
        hi = lo + timedelta(minutes=1)
        return [
            dict(point)
            for point in self._coverage.get(instrument_id, ())
            if parse_time(point["start"]) < hi and parse_time(point["end"]) > lo
        ]


    def frozen_manifest(self, instrument_id, hour):
        """A replay reference to one sealed hour file: its path, checksum and known coverage."""
        canonical = self.path(instrument_id, hour)
        digest = hashlib.sha256()
        with canonical.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest = digest.hexdigest()
        lo = parse_time(hour).replace(minute=0, second=0, microsecond=0)
        hi = lo + timedelta(hours=1)
        coverage = [
            dict(point, start=iso(max(lo, parse_time(point["start"]))), end=iso(min(hi, parse_time(point["end"]))))
            for point in self._coverage.get(instrument_id, ())
            if parse_time(point["start"]) < hi and parse_time(point["end"]) > lo
        ]
        return {
            "archive_id": "TRADES-" + digest,
            "instrument_id": instrument_id,
            "kind": "NORMALIZED_TRADES",
            "path": str(canonical),
            "sha256": digest,
            "byte_length": canonical.stat().st_size,
            "checksum_scope": "FILE",
            "status": "SEALED",
            "hour_start": iso(lo),
            "hour_start_prec": "BUCKET(1h)",
            "integrity": {
                "required_batch_sha256": [digest],
                "contained_batch_sha256": [digest],
                "coverage": coverage,
                "reference_points": [],
            },
        }

    # Replay and audit over explicitly frozen file references (tank.replay).
    @staticmethod
    def read_verified(path, checksum, *, byte_length=None):
        return read_verified(path, checksum, byte_length=byte_length)

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


class BookArchive(HourlyParquet):
    """10 s book summaries and snapshots: `raw/books/{venue}/{symbol}/{yyyy-mm-dd}/{hh}.parquet`.

    Spot and perpetual books of one venue symbol share the file; every row names
    its instrument.
    """

    identity_fields = ("instrument_id", "kind", "received_ts", "payload_json")

    def __init__(self, root, *, instruments, grace_s=120):
        super().__init__(Path(root) / "raw" / "books", grace_s=grace_s)
        self.instruments = {i["instrument_id"]: dict(i) for i in instruments}

    @staticmethod
    def _identity(row):
        return row[:3] + (hashlib.sha256((row[3] or "").encode()).digest(),)

    def path(self, instrument_id, timestamp):
        instrument = self.instruments[instrument_id]
        stamp = iso(timestamp)
        return (
            self.root
            / _component(str(instrument["venue"]).lower())
            / _component(instrument["venue_symbol"])
            / stamp[:10]
            / (stamp[11:13] + ".parquet")
        )

    def add(self, instrument_id, kind, payload, received_ts):
        """Event loop: buffer one book row; `payload` must not change afterwards."""
        ts = iso(received_ts)
        self._add(
            self.path(instrument_id, ts),
            {
                "instrument_id": instrument_id,
                "market_type": self.instruments[instrument_id].get("market_type"),
                "kind": kind,
                **collection_clock(ts).fields("received_ts"),
                "payload": payload,
            },
        )

    @staticmethod
    def _encode(row):
        if "payload" not in row:
            return row
        row = dict(row)
        row["payload_json"] = canonical_json(row.pop("payload"))
        return row



def prune_market_files(data_dir, now, windows, *, hot_days=30, archive_days=180, protected=()):
    """Raw market file retention of HLD §7.2, found by path alone.

    `raw/trades/{venue}/{market}/{symbol}/{day}/` and `raw/books/{venue}/{symbol}/{day}/`
    go after `hot_days` unless the UTC day overlaps an event window in `windows`
    (`(event_id, start, end)` of MAJOR+ events, kept permanently). Downloaded
    `archive/binance/...` files are a cache: they go `archive_days` after download.
    Nothing inside or containing a `protected` path is touched. Returns the removed paths.
    """
    import shutil

    root = Path(data_dir)
    protected = [Path(p).resolve() for p in protected]

    def guarded(path):
        path = path.resolve()
        return any(path.is_relative_to(p) or p.is_relative_to(path) for p in protected)

    now = parse_time(now)
    hot_cutoff = (now - timedelta(days=hot_days)).date()
    kept_days = set()
    for _, start, end in windows:
        day, last = parse_time(start).date(), parse_time(end).date()
        while day <= last:
            kept_days.add(day)
            day += timedelta(days=1)
    removed = []
    for pattern in ("raw/trades/*/*/*/*", "raw/books/*/*/*", "raw/market_frames/*"):
        for directory in sorted(root.glob(pattern)):
            try:
                day = parse_time(directory.name + "T00:00:00Z").date()
            except ValueError:
                continue
            if directory.is_symlink() or not directory.is_dir() or guarded(directory):
                continue
            if day < hot_cutoff and day not in kept_days:
                shutil.rmtree(directory)
                removed.append(str(directory))
    archive_cutoff = (now - timedelta(days=archive_days)).timestamp()
    for path in sorted((root / "archive" / "binance").rglob("*")):
        if path.is_file() and not path.is_symlink() and not guarded(path) and path.stat().st_mtime < archive_cutoff:
            path.unlink()
            removed.append(str(path))
    return removed

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


_IMPORTED = {
    # kind -> (collection, time field, rows of one complete daily file)
    "klines": ("mkt.klines_1m", "open_time", 1440),
    "metrics": ("mkt.oi_obs", "ts", 288),
    "aggTrades": ("mkt.flow_bars_1m", "minute_start", 1440),
}


async def _imported_rows(repo, instrument, kind, date, archive_id):
    """Rows a verified daily file already left in PostgreSQL, or None if not imported."""
    if kind not in _IMPORTED:
        return None  # monthly funding: one small idempotent upsert
    collection, field, expected = _IMPORTED[kind]
    start = parse_time(date)
    count = 0
    async for row in repo.iter_range(
        collection,
        field,
        start=start,
        end=start + timedelta(days=1) - timedelta(microseconds=1),
        filters={"instrument_id": instrument["instrument_id"]},
        batch_size=2000,
    ):
        if row.get("source_archive") == archive_id:
            count += 1
    return count if count >= expected else None


async def fetch_archive(
    repo,
    settings,
    symbol,
    date,
    *,
    market_type="SPOT",
    kind="klines",
    http=None,
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
    imported = await _imported_rows(repo, instrument, kind, date, archive_id)
    if imported is not None:
        return {"archive_id": archive_id, "source_url": url, "rows": imported, "complete": True, "imported": True}
    try:
        # A rejected file leaves nothing behind: the next daily bootstrap or event
        # backfill fetches it again (lead ruling 2026-10-20 18:30, no retry records).
        response = await client.get(url)
        response.raise_for_status()
        checksum = await client.get(url + ".CHECKSUM")
        checksum.raise_for_status()
        expected = "MS" if futures or date < "2025-01" else "US"
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
        directory = Path(settings.data_dir) / "archive" / "binance" / prefix / kind
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / filename
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(response.content)
        tmp.replace(path)
        path.with_suffix(".zip.CHECKSUM").write_text(checksum.text)
        provenance = {"config_version": settings.config_version, "code_version": settings.code_version}
        rows = verified["rows"]
        header = verified["header"]
        count = 0
        flow = None
        batches = {}

        async def write_trades():
            # Raw prints are durable before any bar derived from them reaches PostgreSQL.
            import asyncio

            if market is not None:
                await market.flush_market(raw=True)
            else:
                await asyncio.to_thread(trades.write, trades.take())

        async def batch(collection, key, record):
            values = batches.setdefault(collection, {})
            values[key] = record
            if len(values) >= 1000:
                if collection == "mkt.flow_bars_1m":
                    await write_trades()
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
                record = trade(instrument, source, source="ARCHIVE")
                collection = None
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
            await write_trades()
            if market is None:
                import asyncio

                await asyncio.to_thread(trades.close)
        for collection, values in batches.items():
            if values:
                await repo.put_market_batch(collection, list(values.values()), keys=list(values))
        if market is not None and kind == "klines":
            # The service counts whole past days once; a newly imported day is counted again.
            market._kline_day_counts.pop((instrument["instrument_id"], parse_time(date)), None)
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
