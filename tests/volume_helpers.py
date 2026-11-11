"""Hand-built records of the volume statements (analysis/sql/window_volumes.sql, volume_normal.sql and
volume_buckets.sql) for tests of the rules that read them."""

from datetime import timedelta

from tank.analysis import truth_sql
from tank.util import parse_time

WINDOWS = ("W15M", "W1H", "W4H", "W24H", "EVENT")
NATIVE = dict(value=600.0, previous=300.0, normal_low=300.0, normal_high=300.0)
USD = dict(current_usd=30000.0, previous_usd=15000.0)
NO_USD = dict(current_usd=None, previous_usd=None)


def evidence(instruments, *, windows=WINDOWS, native=None, usd=None, buckets=()):
    """Evidence for the instruments of a token. `native` maps a market (SPOT or PERP) to changes of its
    native volume record, `usd` to changes of the USD volume record of each of its instruments; a market
    mapped to None has no native volume (no record) or no USD volume."""
    native, usd = native or {}, usd or {}
    return truth_sql.VolumeEvidence(
        instruments=tuple(sorted(instruments)),
        normals=tuple(
            truth_sql.VolumeNormal(
                market=market, window=window, samples=1000, **(NATIVE | native.get(market, {}))
            )
            for market in ("SPOT", "PERP")
            if native.get(market, {}) is not None
            for window in windows
        ),
        windows=tuple(
            truth_sql.WindowVolume(
                window=window,
                instrument_id=iid,
                **(NO_USD if usd.get(market) is None and market in usd else USD | usd.get(market, {})),
            )
            for iid in sorted(instruments)
            for market in ["SPOT" if ":spot:" in iid else "PERP"]
            for window in windows
        ),
        buckets=tuple(buckets),
    )


def buckets(end, venues, *, volume=1_000_000.0, count=96, **changes):
    """The 15-minute buckets of the 24 hours before `end` of the venues, all covered."""
    first = parse_time(end) - timedelta(minutes=15 * count)
    values = dict(minutes=15, incomplete=0, missing=0, notional=volume) | changes
    return [
        truth_sql.VolumeBucket(bucket_start=first + timedelta(minutes=15 * i), venue=venue, **values)
        for i in range(count)
        for venue in venues
    ]
