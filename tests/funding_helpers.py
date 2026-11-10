"""Hand-built records of the funding and liquidation statements (analysis/sql/funding.sql, funding_dispersion.sql,
funding_confirmations.sql and liquidations.sql) for tests of the rules that read them."""

from datetime import timedelta
from decimal import Decimal

from tank.analysis import truth_sql
from tank.util import parse_time

LENGTHS = dict(W15M=900, W1H=3600, W4H=14400, W24H=86400, EVENT=14400)
HISTORY = dict(samples=28, normal_low=0.0001, normal_high=0.0001)
DISPERSION = dict(samples=100, normal_low=0.0, normal_high=0.0)


def rate(value, interval=8):
    """The columns of a settled rate `value` per `interval` hours."""
    return dict(
        settled_rate=Decimal(str(value)),
        settled_interval_h=Decimal(str(interval)),
        settled_eq=float(value) * 8 / interval,
    )


def estimate(value, interval=8, at=None):
    """The columns of a current estimate `value` per `interval` hours observed at `at`."""
    return dict(
        estimate_rate=Decimal(str(value)),
        estimate_interval_h=Decimal(str(interval)),
        estimate_eq=float(value) * 8 / interval,
        estimate_at=parse_time(at) if at else None,
    )


def start(value, interval=8):
    """The columns of the settled rate at the window start."""
    return dict(start_rate=Decimal(str(value)), start_interval_h=Decimal(str(interval)))


def evidence(
    end,
    instruments,
    *,
    lengths=None,
    venue=None,
    dispersion=None,
    confirmations=(),
    liquidations=None,
    state=True,
):
    """Evidence for the perp instruments of a token at the cutoff `end`. Every window has, per instrument, a
    settled rate of 0.01% per 8 hours observed at `end`, the same rate at the window start and a history of 28
    settlements from ten days before to 8 hours before the window start in which it never differed, and, with
    `state`, a dispersion history of 100 samples over the same span in which the venues never differed. `venue`
    maps a window name (or `None` for every window) to changes of the venue records, in which an instrument
    maps to changes of its own, and `dispersion` likewise to changes of the dispersion record; a window mapped
    to `None` has no record. `liquidations` maps a window name (or `None`) to the records of the instruments
    that have liquidations, each the changes of an all-zero record."""
    end = parse_time(end)
    lengths = lengths or LENGTHS
    venue, dispersion, liquidations = venue or {}, dispersion or {}, liquidations or {}

    def changes(table, window, iid=None):
        if window in table and table[window] is None:
            return None
        found = {**table.get(None, {}), **table.get(window, {})}
        return {**found, **found.get(iid, {})} if iid else found

    venues, spreads, records = [], [], []
    for window, seconds in lengths.items():
        begin = end - timedelta(seconds=seconds)
        history = dict(history_first=begin - timedelta(days=10), history_last=begin - timedelta(hours=8))
        for iid in instruments:
            found = changes(venue, window, iid)
            if found is not None:
                found = {k: v for k, v in found.items() if k not in instruments}
                venues.append(
                    truth_sql.FundingVenue(
                        **(
                            dict(
                                window=window,
                                instrument_id=iid,
                                settled_at=end,
                                estimate_rate=None,
                                estimate_interval_h=None,
                                estimate_at=None,
                                estimate_eq=None,
                                start_at=begin,
                            )
                            | rate(0.0001)
                            | start(0.0001)
                            | HISTORY
                            | history
                            | found
                        )
                    )
                )
            found = changes(liquidations, window, iid)
            if found:
                found = {k: v for k, v in found.items() if k not in instruments}
                records.append(
                    truth_sql.LiquidationWindow(
                        **(
                            {
                                f: 0 if f.endswith(("count", "missing")) or f == "observations" else None
                                for f in truth_sql.LiquidationWindow.__dataclass_fields__
                            }
                            | dict(window=window, instrument_id=iid)
                            | found
                        )
                    )
                )
        found = changes(dispersion, window)
        if state and found is not None and len(instruments) > 1:
            spreads.append(truth_sql.FundingDispersion(window=window, **(DISPERSION | history | found)))
    return truth_sql.FundingEvidence(
        instruments=tuple(instruments),
        venues=tuple(venues),
        dispersion=tuple(spreads),
        confirmations=tuple(confirmations),
        liquidations=tuple(records),
    )


def confirmation(iid, value, at, confirmed=1):
    """A confirmed estimate of `iid`: `value` per 8 hours observed at `at`."""
    return truth_sql.FundingConfirmation(
        instrument_id=iid, rate_8h_eq=float(value), as_of=parse_time(at), confirmed=confirmed
    )


STEADY = rate(0) | start(0) | dict(normal_low=0.0, normal_high=0.0)


def steady(end, instruments, **changes):
    """Evidence of funding that was never anything but 0 (a settled rate of 0 now and at every window start,
    against a history in which it never differed)."""
    return evidence(end, instruments, venue={None: STEADY}, **changes)
