"""Records of the series statements (analysis/sql/ticker_volumes.sql, lead_lag.sql and series_health.sql) for tests of
the rules that read them, made from the rows a test writes down.

A test that describes a market with rows (`flow_bars`, `klines`, `books`, `oi`, `funding`, `liquidations`, `tickers`)
gets the records the statements return for them: the latest ticker volume of every instrument, the lead-lag bars of
every instrument against the normal range of the relative records, and per series, instrument and key the latest row
and the latest row fit to be freshness evidence. The statements themselves are tested against a real ClickHouse server
in tests/test_truth_series_sql.py."""

from datetime import timedelta
from decimal import Decimal

from tank.analysis import truth, truth_sql
from tank.util import parse_time

TIME_FIELDS = (
    "minute_start",
    "open_time",
    "exchange_ts",
    "ts",
    "as_of",
    "settlement_time",
    "funding_time",
    "observed_at",
)
KEYS = {
    "flow_bars": (),
    "klines": (),
    "books": ("metric_key",),
    "oi": ("metric_key",),
    "funding": ("metric_key", "kind"),
    "liquidations": ("metric_key",),
    "tickers": ("kind",),
}


def _number(value):
    if isinstance(value, dict):
        value = value.get("value")
    return None if value is None else float(value)


def _field(row):
    return next(name for name in TIME_FIELDS if row.get(name) is not None)


def _interval(row, collection):
    return int(row.get("interval_s", 60)) if collection in truth.BAR_SERIES else 0


def _visible(row, assembled):
    for name in ("recorded_at", "received_ts"):
        if assembled and row.get(name) and parse_time(row[name]) > assembled:
            return False
    return True


def _rows(snapshot, collection, since, end, assembled):
    token = snapshot.get("token_id")
    owners = {
        i["instrument_id"]: i["token_id"]
        for i in snapshot.get("instruments", [])
        if i.get("instrument_id") and i.get("token_id")
    }
    return [
        r
        for r in snapshot.get(collection, [])
        if r.get("instrument_id")
        and r.get("token_id", owners.get(r["instrument_id"], token)) == token
        and truth._time(r) is not None
        and since <= truth._time(r) <= end
        and _visible(r, assembled)
    ]


def _fit(row, collection):
    if collection == "klines":
        return row.get("complete") is True and bool(_number(row.get("c", row.get("close"))))
    if collection == "flow_bars":
        return row.get("complete") is True
    if collection == "books":
        covered = row.get("range_covered_pct")
        return (
            row.get("integrity_ok") is True
            and bool(_number(row.get("mid")))
            and isinstance(covered, dict)
            and all(_number(covered.get(side)) is not None for side in ("bid", "ask"))
        )
    if collection == "oi":
        return row.get("oi_native") is not None
    if collection == "funding":
        return _number(row.get("rate")) is not None and bool(_number(row.get("interval_h")))
    return False


def _health(snapshot, end, assembled, look, near):
    records = []
    for collection in KEYS:
        since = end - (near if collection in ("books", "tickers") else look)
        groups = {}
        for row in _rows(snapshot, collection, since, end, assembled):
            if (
                collection in truth.BAR_SERIES
                and truth._time(row) + timedelta(seconds=_interval(row, collection)) > end
            ):
                continue
            key = (row["instrument_id"], *(row.get(name) for name in KEYS[collection]))
            groups.setdefault(key, []).append(row)
        for key, rows in groups.items():
            names = dict(zip(KEYS[collection], key[1:], strict=True))
            chosen = [r for r in rows if collection not in truth.BAR_SERIES or r.get("complete") is True]
            last = max(chosen, key=truth._time) if chosen else None
            fit = [r for r in rows if _fit(r, collection)]

            def stamp(r, collection=collection):
                return truth._time(r) + timedelta(seconds=_interval(r, collection))

            field = _field(last) if last else None
            usd = [
                r
                for r in rows
                if collection == "klines"
                and r.get("quote_volume_usd") is not None
                and truth._time(r) >= end - timedelta(days=1)
            ]
            records.append(
                truth_sql.SeriesHealth(
                    series=collection,
                    instrument_id=key[0],
                    metric_key=names.get("metric_key"),
                    kind=names.get("kind"),
                    rows=len(rows),
                    last_at=truth._time(last) if last else None,
                    last_interval_s=_interval(last, collection) if last else 0,
                    last_prec=last.get(field + "_prec") if last else None,
                    last_prec_reason=last.get(field + "_prec_reason") if last else None,
                    last_timestamp_precision=(last.get("timestamp_precision") or last.get("precision"))
                    if last
                    else None,
                    last_bucket_size_s=_number(last.get(field + "_bucket_size_s")) if last else None,
                    last_bucket_anchor=last.get(field + "_bucket_anchor") if last else None,
                    last_interval_h=_number(last.get("interval_h")) if last else None,
                    valid_at=stamp(max(fit, key=truth._time)) if fit else None,
                    usd_volume=float(sum(Decimal(str(r["quote_volume_usd"])) for r in usd)) if usd else None,
                )
            )
    return records


def _tickers(snapshot, end, assembled, near):
    latest = {}
    for row in _rows(snapshot, "tickers", end - near, end, assembled):
        iid = row["instrument_id"]
        if iid not in latest or truth._time(row) > truth._time(latest[iid]):
            latest[iid] = row
    return [
        truth_sql.TickerVolume(
            instrument_id=iid,
            observed_at=truth._time(row),
            volume_native=(
                None
                if row.get("volume_native", row.get("volume")) is None
                else Decimal(str(row.get("volume_native", row.get("volume"))))
            ),
        )
        for iid, row in sorted(latest.items())
    ]


def _lead_lag(snapshot, start, end, assembled, normal):
    if normal is None:
        return []
    groups = {}
    for row in _rows(snapshot, "klines", start, end, assembled):
        if truth._time(row) < end and truth._time(row) + timedelta(seconds=_interval(row, "klines")) <= end:
            groups.setdefault(row["instrument_id"], []).append(row)
    records = []
    for iid, rows in sorted(groups.items()):
        rows.sort(key=truth._time)
        usable = all(r.get("complete") is True for r in rows) and all(
            (truth._time(b) - truth._time(a)).total_seconds() == 60
            for a, b in zip(rows, rows[1:], strict=False)
        )
        opened = _number(rows[0].get("o"))
        exit_at = None
        if opened:
            for row in rows:
                close = _number(row.get("c"))
                if close is not None and (close / opened - 1 < normal[0] or close / opened - 1 > normal[1]):
                    exit_at = truth._time(row)
                    break
        records.append(
            truth_sql.LeadLagBars(
                instrument_id=iid, bars=len(rows), usable=int(usable), first_exit_at=exit_at
            )
        )
    return records


def series_evidence(
    snapshot, *, end=None, start=None, normal=None, tickers=True, health=True, lead_lag=True, config=None
):
    """The `SeriesEvidence` a snapshot of hand-written rows would have."""
    end = end or parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    anchor_at = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = start or (parse_time(anchor_at) if anchor_at else end - timedelta(hours=4))
    normal = normal or truth.horizon_range(
        snapshot.get("relative_sql") or truth_sql.NO_RELATIVE,
        "W15M",
        float((config or {}).get("history_min_days", 7)),
    )
    assembled = parse_time(snapshot["assembled_at"]) if snapshot.get("assembled_at") else None
    look = end - truth.recent_history_start(end, anchor_at, config or {})
    near = timedelta(seconds=truth.NEAR_SERIES_S)
    return truth_sql.SeriesEvidence(
        tickers=tuple(_tickers(snapshot, end, assembled, near)) if tickers else (),
        lead_lag=tuple(_lead_lag(snapshot, start, end, assembled, normal)) if lead_lag else (),
        health=tuple(_health(snapshot, end, assembled, look, near)) if health else (),
    )


def with_series(snapshot, **kwargs):
    """The snapshot with the series evidence of its rows as `series_sql`."""
    return dict(snapshot, series_sql=series_evidence(snapshot, **kwargs))


def data_confidence(snapshot, spot, derivatives, end):
    """`truth.data_confidence` of a snapshot of hand-written rows."""
    return truth.data_confidence(
        snapshot if "series_sql" in snapshot else with_series(snapshot, end=end), spot, derivatives, end
    )


def available_field_freshness(snapshot, end):
    """`truth.available_field_freshness` of a snapshot of hand-written rows."""
    return truth.available_field_freshness(
        snapshot if "series_sql" in snapshot else with_series(snapshot, end=end), end
    )


def volumes(tickers, end=None):
    """The venue to native volume mapping `truth.spot_truth` reads, from hand-written ticker rows at or before
    `end`: the volume of the latest row of each venue."""
    latest = {}
    for row in tickers:
        venue = row.get("venue") or str(row["instrument_id"]).split(":")[0]
        stamp = truth._time(row)
        if stamp is None or end is not None and stamp > end:
            continue
        if venue not in latest or stamp > latest[venue][0]:
            latest[venue] = (stamp, row)
    return {
        venue: None
        if row.get("volume_native", row.get("volume")) is None
        else Decimal(str(row.get("volume_native", row.get("volume"))))
        for venue, (_, row) in latest.items()
    }
