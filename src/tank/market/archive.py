"""Hourly raw Parquet (HLD §7.2)."""

import hashlib
import io
import itertools
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from urllib.parse import quote

from tank.time_precision import collection_clock
from tank.util import canonical_json, iso, parse_time, utcnow

log = logging.getLogger(__name__)


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
        # Journal appends and seals of this archive never interleave (worker threads only).
        self._io = threading.RLock()

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

    def write(self, buffer, *, now=None, seal_limit=None):
        """Worker thread: append and fsync each hour's journal, then seal at most `seal_limit`
        quiet hours (all with None, none with 0: the service seals in its own pass so a seal
        backlog never holds up journal writes)."""
        with self._io:
            return self._write(buffer, now=now, seal_limit=seal_limit)

    def _write(self, buffer, *, now, seal_limit):
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
        sealed = self.seal_due(now, limit=seal_limit) if seal_limit != 0 else []
        return {"rows": rows_written, "sealed": sealed, "open_journals": len(self._journals)}

    def seal_due(self, now=None, *, limit=None):
        """Worker thread: seal finished quiet hours, oldest first, at most `limit` of them;
        each seal holds the archive only for that hour, so writes interleave."""
        if not self._recovered:
            with self._io:
                self._recover()
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
        for canonical in sorted(set(due))[:limit]:
            try:
                with self._io:
                    self.seal(canonical)
            except Exception as exc:  # noqa: BLE001 - one bad hour must not stop writes or other seals
                # Journals stay on disk and the hour is retried on the next write.
                self._orphans.add(Path(canonical))
                log.warning("seal of %s failed: %s", canonical, type(exc).__name__)
                continue
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
                            # A batch cut mid-write can parse yet point outside its buffers.
                            batch.validate(full=True)
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
                    # Every stored column is text; a journal or file from another writer
                    # (e.g. an all-null column) is cast, never rejected.
                    columns = [
                        batch.column(name).cast(pa.string())
                        if name in batch.schema.names
                        else pa.nulls(batch.num_rows, pa.string())
                        for name in schema.names
                    ]
                    table = pa.Table.from_arrays(columns, schema=schema)
                    keep = []
                    for row in zip(
                        *(table.column(name).to_pylist() for name in self.identity_fields), strict=True
                    ):
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
        venue, market, symbol = (_component(instrument[k]) for k in ("venue", "market_type", "venue_symbol"))
        path = self.root / venue.lower() / market.lower() / symbol / stamp[:10] / (stamp[11:13] + ".parquet")
        if len(self._paths) > 100000:
            self._paths.clear()
        self._paths[key] = path
        return path

    def canonical(self, row):
        """Event loop: the hour file of a print, registering a new instrument from the row."""
        iid = row["instrument_id"]
        if iid not in self.instruments and all(row.get(k) for k in ("venue", "market_type", "venue_symbol")):
            self.instruments[iid] = {
                k: row[k] for k in ("instrument_id", "venue", "market_type", "venue_symbol")
            }
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
        self._minutes.setdefault(canonical, set()).add(
            iso(parse_time(row["ts"]).replace(second=0, microsecond=0))
        )
        self._touched[canonical] = time.monotonic()
        self._add(canonical, row)
        return True

    def remember(self, row):
        """Event loop: a print already on disk (restart warm-up), so it is not taken twice."""
        canonical = self.path(row["instrument_id"], row["ts"])
        self._seen.setdefault(canonical, set()).add(str(row["trade_id"]))
        self._minutes.setdefault(canonical, set()).add(
            iso(parse_time(row["ts"]).replace(second=0, microsecond=0))
        )

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
                last.update(
                    end=point["end"], **{k: v for k, v in point.items() if k.startswith("observed_at")}
                )
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
            dict(
                point,
                start=iso(max(lo, parse_time(point["start"]))),
                end=iso(min(hi, parse_time(point["end"]))),
            )
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
        if (
            path.is_file()
            and not path.is_symlink()
            and not guarded(path)
            and path.stat().st_mtime < archive_cutoff
        ):
            path.unlink()
            removed.append(str(path))
    return removed
