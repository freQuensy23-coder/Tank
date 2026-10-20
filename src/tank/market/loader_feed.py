"""Read the trades_loader's files and turn its sealed minutes into complete flow bars.

The loader (trades_loader/, Go) is the only writer of those files; Tank only reads them.
A minute is complete when the loader has sealed it (every trade of the minute was read
over REST with continuous IDs) and no recorded loader gap overlaps it. Trades are
normalized by the same `normalize.trade` the stream path uses, from the venue's own
REST objects, and bars are built by the same FlowAccumulator.
"""

import json
import os
from datetime import UTC, datetime, timedelta

from tank.util import iso, parse_time

from . import normalize as norm
from .flow import FlowAccumulator

SOURCE = "TRADES_LOADER"


def dir_name(instrument_id):
    return instrument_id.replace(":", "__").replace("/", "-")


def _minute(at):
    return at.replace(second=0, microsecond=0)


class LoaderFeed:
    def __init__(self, root, *, lookback=timedelta(hours=26), chunk=timedelta(hours=2)):
        self.root = root
        self.lookback = lookback
        self.chunk = chunk
        self.next = {}  # instrument_id -> first minute not yet turned into bars

    def watermark(self, instrument_id):
        path = os.path.join(self.root, dir_name(instrument_id), "WATERMARK.json")
        try:
            with open(path, "rb") as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    def read(self, instrument, now):
        """Bars for the next run of sealed minutes of one instrument (blocking; run in a thread)."""
        iid = instrument["instrument_id"]
        wm = self.watermark(iid)
        if not wm or not wm.get("segments"):
            return []
        covered, sealed = parse_time(wm["covered_from"]), parse_time(wm["sealed_through"])
        start = self.next.get(iid) or max(covered, _minute(now - self.lookback))
        end = min(sealed, start + self.chunk)
        if end <= start:
            return []
        gaps = [(parse_time(g["start"]), parse_time(g["end"])) for g in wm.get("gaps") or []]
        by_minute = {}
        hour = start.replace(minute=0)
        while hour < end:
            size = wm["segments"].get(hour.strftime("%Y-%m-%dT%H"))
            if size:
                with open(os.path.join(self.root, dir_name(iid), hour.strftime("%Y-%m-%dT%H") + ".ndjson"), "rb") as f:
                    data = f.read(size)
                for line in data.splitlines():
                    t = json.loads(line)
                    minute = _minute(datetime.fromtimestamp(t["ts_ms"] / 1000, UTC))
                    if start <= minute < end:
                        by_minute.setdefault(minute, []).append(t["raw"])
            hour += timedelta(hours=1)
        acc = FlowAccumulator()
        rows = []
        minute = start
        while minute < end:
            stop = minute + timedelta(minutes=1)
            gap = any(lo < stop and hi > minute for lo, hi in gaps)
            raws = by_minute.get(minute, [])
            for raw in raws:
                acc.add(norm.trade(instrument, raw, source="REST"), continuity=False, emit_record=False)
            if not raws:
                acc.empty_minute(instrument, minute)
            key = (iid, iso(minute))
            acc.prove_range(iid, minute, stop, complete=not gap, source=SOURCE, reason="LOADER_GAP" if gap else None)
            row = acc.record(key, complete=not gap)
            row.update(capture_method=SOURCE, backfill_verified=not gap)
            if gap:
                row["gap_reason"] = "LOADER_GAP"
            rows.append((key, row))
            acc.bars.pop(key, None)
            minute = stop
        self.next[iid] = end
        return rows

