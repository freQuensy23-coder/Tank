"""Hand-built records of the open-interest statements (analysis/sql/oi_history.sql and oi_endpoints.sql) for
tests of the rules that read them."""

from datetime import timedelta

from tank.analysis import truth_sql
from tank.util import parse_time

LENGTHS = dict(W15M=900, W1H=3600, W4H=14400, W24H=86400, EVENT=14400)
ENDPOINT = dict(
    oi_native_start=100.0,
    oi_native_end=108.0,
    oi_usd_start=1000.0,
    oi_usd_end=1080.0,
    end_mark=None,
    end_ts_prec="EXACT",
    end_timestamp_precision="EXACT",
    end_ts_bucket_size_s=None,
    basis_bps=None,
    basis_start_bps=None,
    excluded=0,
    excluded_at=None,
    excluded_oi_native=None,
    excluded_oi_usd=None,
    excluded_ts_prec=None,
    excluded_timestamp_precision=None,
    excluded_ts_bucket_size_s=None,
    excluded_ts_bucket_anchor=None,
)
HISTORY = dict(
    observations=2000,
    first_observation=parse_time("2026-08-01T00:00:00Z"),
    last_observation=parse_time("2026-10-16T00:00:00Z"),
    native_samples=1000,
    native_low=-0.03,
    native_high=0.035,
    native_first_end=parse_time("2026-08-01T00:00:00Z"),
    native_last_end=parse_time("2026-10-16T00:00:00Z"),
    usd_samples=1000,
    usd_low=-0.04,
    usd_high=0.05,
    usd_first_end=parse_time("2026-08-01T00:00:00Z"),
    usd_last_end=parse_time("2026-10-16T00:00:00Z"),
    pair_tolerance_s=60.0,
    basis_samples=0,
    basis_low=None,
    basis_high=None,
    basis_first=None,
    basis_last=None,
)
NO_HISTORY = dict(
    observations=0,
    first_observation=None,
    last_observation=None,
    native_samples=0,
    native_low=None,
    native_high=None,
    native_first_end=None,
    native_last_end=None,
    usd_samples=0,
    usd_low=None,
    usd_high=None,
    usd_first_end=None,
    usd_last_end=None,
    basis_samples=0,
)


def evidence(
    end,
    instruments,
    *,
    lengths=None,
    cohort=(),
    venue=None,
    composite=None,
    venue_history=None,
    composite_history=None,
    history=True,
):
    """Evidence for the perp instruments of a token at the cutoff `end`. Every window has, per instrument, an
    endpoint (100 to 108 native, 1000 to 1080 USD, observed exactly at the bounds) and a history of
    -3% .. +3.5% native and -4% .. +5% USD spanning 76 days. `cohort` names the instruments of the composite
    (none: no composite). `venue` and `composite` map a window name (or `None` for every window) to changes of
    the endpoint records, `venue_history` and `composite_history` likewise of the history records; a window
    mapped to `None` has no record. The value of `venue` may also map an instrument to changes."""
    end = parse_time(end)
    lengths = lengths or LENGTHS
    venue, composite = venue or {}, composite or {}
    venue_history, composite_history = venue_history or {}, composite_history or {}

    def changes(table, window, iid=None):
        if window in table and table[window] is None:
            return None
        found = {**table.get(None, {}), **table.get(window, {})}
        return {**found, **found.get(iid, {})} if iid else found

    endpoints, histories = [], []
    for window, seconds in lengths.items():
        start = end - timedelta(seconds=seconds)
        for series, iid in enumerate(instruments, 1):
            found = changes(venue, window, iid)
            if found is not None:
                found = {k: v for k, v in found.items() if k not in instruments}
                endpoints.append(
                    truth_sql.OiEndpoint(
                        window=window,
                        series=series,
                        instrument_id=iid,
                        cohort="",
                        **(dict(start_observed_at=start, end_observed_at=end) | ENDPOINT | found),
                    )
                )
            found = changes(venue_history, window, iid)
            if history and found is not None:
                found = {k: v for k, v in found.items() if k not in instruments}
                histories.append(
                    truth_sql.OiHistory(
                        window=window, series=series, instrument_id=iid, cohort="", **(HISTORY | found)
                    )
                )
        if cohort:
            found = changes(composite, window)
            if found is not None:
                endpoints.append(
                    truth_sql.OiEndpoint(
                        window=window,
                        series=0,
                        instrument_id="composite",
                        cohort=",".join(cohort),
                        **(
                            dict(start_observed_at=start, end_observed_at=end)
                            | ENDPOINT
                            | dict(end_ts_prec=None, end_timestamp_precision=None)
                            | found
                        ),
                    )
                )
            found = changes(composite_history, window)
            if history and found is not None:
                histories.append(
                    truth_sql.OiHistory(
                        window=window,
                        series=0,
                        instrument_id="composite",
                        cohort=",".join(cohort),
                        **(HISTORY | dict(pair_tolerance_s=0.0) | found),
                    )
                )
    return truth_sql.OiEvidence(
        instruments=tuple(instruments), history=tuple(histories), endpoints=tuple(endpoints)
    )


STEADY = dict(oi_native_start=1000.0, oi_native_end=1000.0, oi_usd_start=100000.0, oi_usd_end=100000.0)
FLAT = dict(native_low=0.0, native_high=0.0, usd_low=0.0, usd_high=0.0)
NO_USD = dict(oi_usd_start=None, oi_usd_end=None)
NO_USD_RANGE = dict(usd_samples=0, usd_low=None, usd_high=None)


def steady(end, instruments, *, usd=True, **changes):
    """Evidence of open interest that did not move (1000 native, 100,000 USD) in any window, against a history
    in which it never moved either, the instruments being the composite's cohort; `usd=False` leaves out
    every USD value and range."""
    endpoint = STEADY if usd else STEADY | NO_USD
    history = FLAT if usd else FLAT | NO_USD_RANGE
    return evidence(
        end,
        instruments,
        cohort=instruments,
        venue={None: endpoint},
        composite={None: endpoint},
        venue_history={None: history},
        composite_history={None: history},
        **changes,
    )
