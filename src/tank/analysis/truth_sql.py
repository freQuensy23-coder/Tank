"""Market Truth statements (03 §11.1): hand-written ClickHouse statements and their typed results.

Every number derived from the stored market series is produced by one `.sql` file of
`analysis/sql/`: it reads `mkt.*` itself and returns aggregates whose size does not depend on the
window length. `fetch` sends the statement with its server-side parameters and decodes each result
row into the frozen record class of that statement; the rules over those scalars stay in
`analysis.truth`. A record field is a result column of the same name, `None` where the column is
NULL; a statement and its record must agree on the column names exactly.
"""

import re
import types
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from functools import cache
from pathlib import Path

from tank.clickhouse_market import _literal
from tank.util import parse_time

STATEMENTS = Path(__file__).with_name("sql")
PARAMETER = re.compile(r"\{(\w+):((?:[^{}()]|\([^()]*\))+)\}")
DECODERS = {str: str, int: int, float: float, Decimal: Decimal, datetime: parse_time}


@dataclass(frozen=True)
class Record:
    """One result row of the statement named by `statement`."""

    statement = ""

    @classmethod
    def decode(cls, row):
        spec = {field.name: field.type for field in fields(cls)}
        if row.keys() != spec.keys():
            raise ValueError(f"{cls.statement}: columns {sorted(row)} are not the fields {sorted(spec)}")
        values = {}
        for name, kind in spec.items():
            nullable = isinstance(kind, types.UnionType)
            kind = next(part for part in kind.__args__ if part is not type(None)) if nullable else kind
            value = row[name]
            if value is None:
                if not nullable:
                    raise ValueError(f"{cls.statement}.{name} is NULL")
                values[name] = None
            else:
                values[name] = DECODERS[kind](value)
        return cls(**values)


@cache
def load_statement(name):
    """The text of `sql/<name>.sql` and the ClickHouse type of each of its parameters."""
    text = (STATEMENTS / f"{name}.sql").read_text()
    return text.strip() + "\n", dict(PARAMETER.findall(text))


async def fetch(ch, record, **values):
    """Run the statement of `record` with these parameter values; one decoded record per row."""
    query, kinds = load_statement(record.statement)
    if values.keys() != kinds.keys():
        raise TypeError(f"{record.statement} takes {sorted(kinds)}, not {sorted(values)}")
    params = {name: (_literal(values[name]), kind) for name, kind in kinds.items()}
    return [record.decode(row) for row in await ch.rows(query, params)]


@dataclass(frozen=True)
class BenchmarkReaction(Record):
    """Raw return of a benchmark on its largest spot instrument (benchmark_reaction.sql).

    `venues` counts the spot instruments with volume in the 24 hours before the window end; no
    venue and no price are None."""

    statement = "benchmark_reaction"
    venue: str | None
    quote_volume_24h: float
    price_start: Decimal | None
    price_start_observed_at: datetime | None
    price_end: Decimal | None
    price_end_observed_at: datetime | None
    r_raw: float | None
    venues: int


async def benchmark_reaction(ch, instrument_ids, window_start, window_end):
    (record,) = await fetch(
        ch,
        BenchmarkReaction,
        instrument_ids=list(instrument_ids),
        window_start=window_start,
        window_end=window_end,
    )
    return record


@dataclass(frozen=True)
class ReferencePrice(Record):
    """What the reference venue, the anchor price and the entry price of a token are chosen from, one
    row per spot instrument (reference_prices.sql).

    The close columns are the last complete one-minute bar closed at or before the cutoff and the last
    one that closed on a whole hour; `flow_volume_native` is the native volume of the 24 hours before the
    cutoff when their flow coverage is complete, otherwise None with `flow_detail`; the trades are the
    stored JSON of the last trade before the anchor and before the cutoff."""

    statement = "reference_prices"
    instrument_id: str
    last_close: Decimal | None
    last_close_time: datetime | None
    last_close_prec: str | None
    last_close_prec_reason: str | None
    hourly_close: Decimal | None
    hourly_close_time: datetime | None
    hourly_close_prec: str | None
    hourly_close_prec_reason: str | None
    flow_volume_native: float | None
    flow_detail: str | None
    anchor_trade: str | None
    entry_trade: str | None


@dataclass(frozen=True)
class SectorReturn(Record):
    """The return of one peer instrument over one horizon, None when a bound has no fresh price
    (sector_returns.sql)."""

    statement = "sector_returns"
    instrument_id: str
    name: str
    peer_return: float | None


@dataclass(frozen=True)
class RelativeHorizon(Record):
    """The price-relative numbers of one horizon (relative_baselines.sql).

    Returns are of the token, BTC and ETH and the BTC- and ETH-adjusted ones; `measure` is what the
    history is compared with and `percentile` the share of the history not above it; the history is
    described by its sample count, its quantiles and the first and last end it reaches. The BTC context
    and the realised volatility of the reference are the same on every row."""

    statement = "relative_baselines"
    name: str
    length_s: float
    beta_btc_pairs: int
    beta_btc_ols: float | None
    beta_btc: float
    beta_eth_pairs: int
    beta_eth_ols: float | None
    beta_eth: float
    raw_return: float | None
    btc_return: float | None
    eth_return: float | None
    btc_adjusted_return: float | None
    eth_adjusted_return: float | None
    measure: float | None
    sample_count: int
    history_q025: float | None
    history_q25: float | None
    history_q75: float | None
    history_q975: float | None
    percentile: float | None
    history_first_end: datetime | None
    history_last_end: datetime | None
    price_first_close: datetime | None
    price_last_close: datetime | None
    history_resolution_s: float | None
    btc_first_close: datetime | None
    btc_last_close: datetime | None
    btc_realized_vol_24h: float | None
    btc_return_24h_q025: float | None
    btc_return_24h_q25: float | None
    btc_return_24h_q975: float | None
    btc_realized_vol_24h_q025: float | None
    btc_realized_vol_24h_q975: float | None
    btc_return_24h_samples: int
    btc_realized_vol_24h_samples: int
    ref_realized_vol_24h: float | None
    ref_realized_vol_24h_q025: float | None
    ref_realized_vol_24h_q975: float | None
    ref_realized_vol_24h_samples: int
    ref_hourly_first: datetime | None
    ref_hourly_last: datetime | None


@dataclass(frozen=True)
class RelativeEvidence:
    """Everything the price-relative rules of a snapshot read from ClickHouse: the instruments chosen
    from the registry (reference venue, BTC, ETH), the statement rows by instrument and horizon."""

    ref: str | None = None
    btc: str | None = None
    eth: str | None = None
    references: tuple[ReferencePrice, ...] = ()
    horizons: tuple[RelativeHorizon, ...] = ()
    peers: tuple[SectorReturn, ...] = ()


NO_RELATIVE = RelativeEvidence()


@dataclass(frozen=True)
class WindowVolume(Record):
    """The observed USD volume of one instrument in the current window and in the window before it, None
    where the window is not covered (window_volumes.sql)."""

    statement = "window_volumes"
    window: str
    instrument_id: str
    current_usd: float | None
    previous_usd: float | None


@dataclass(frozen=True)
class VolumeNormal(Record):
    """The native volume of a market (SPOT or PERP) in the current window and the window before it, and
    the 0.025 and 0.975 quantiles of the totals of its 90-day history of windows of the same length, None
    where a total is not covered or the history spans less than the minimum (volume_normal.sql)."""

    statement = "volume_normal"
    market: str
    window: str
    value: float | None
    previous: float | None
    samples: int
    normal_low: float | None
    normal_high: float | None


@dataclass(frozen=True)
class VolumeBucket(Record):
    """One venue's flow bars of one 15-minute bucket of the last 24 hours (volume_buckets.sql)."""

    statement = "volume_buckets"
    bucket_start: datetime
    venue: str
    minutes: int
    incomplete: int
    missing: int
    notional: float


@dataclass(frozen=True)
class VolumeEvidence:
    """Everything the volume rules of a snapshot read from ClickHouse: the instruments of the token by
    market and the statement rows."""

    instruments: tuple[str, ...] = ()
    windows: tuple[WindowVolume, ...] = ()
    normals: tuple[VolumeNormal, ...] = ()
    buckets: tuple[VolumeBucket, ...] = ()


NO_VOLUME = VolumeEvidence()


@dataclass(frozen=True)
class OiHistory(Record):
    """The 90-day history behind the normal ranges of the open-interest change and of the basis of one window
    (oi_history.sql): one row per perp venue (`series` its 1-based position among the instruments) and one for
    the synchronised composite (series 0, `cohort` the instruments it sums).

    `observations` is the number of observations before the window start with the first and the last of them;
    a change range is its sample count, its 0.025 and 0.975 quantiles and the first and the last window end it
    reaches, the basis range the same over the basis of the observations; None where there is none. The pairing
    tolerance is the venue's, 0 for the composite."""

    statement = "oi_history"
    window: str
    series: int
    instrument_id: str
    cohort: str
    observations: int
    first_observation: datetime | None
    last_observation: datetime | None
    native_samples: int
    native_low: float | None
    native_high: float | None
    native_first_end: datetime | None
    native_last_end: datetime | None
    usd_samples: int
    usd_low: float | None
    usd_high: float | None
    usd_first_end: datetime | None
    usd_last_end: datetime | None
    pair_tolerance_s: float
    basis_samples: int
    basis_low: float | None
    basis_high: float | None
    basis_first: datetime | None
    basis_last: datetime | None


@dataclass(frozen=True)
class OiEndpoint(Record):
    """The open-interest endpoints of one window (oi_endpoints.sql): per perp venue (`series` its 1-based
    position among the instruments) the observations nearest to the window start and to the cutoff, and for the
    synchronised composite (series 0, `cohort` the instruments it sums) the composite at the times nearest to
    them; None where there is none or the cohort has no composite.

    A venue row also carries the own mark of its end observation and the declared precision of its timestamp,
    the basis of its latest observation and of the one at the window start, and `excluded` observations near the
    bounds whose bucket edge is unresolved with the latest of them."""

    statement = "oi_endpoints"
    window: str
    series: int
    instrument_id: str
    cohort: str
    start_observed_at: datetime | None
    oi_native_start: float | None
    oi_usd_start: float | None
    end_observed_at: datetime | None
    oi_native_end: float | None
    oi_usd_end: float | None
    end_mark: float | None
    end_ts_prec: str | None
    end_timestamp_precision: str | None
    end_ts_bucket_size_s: float | None
    basis_bps: float | None
    basis_start_bps: float | None
    excluded: int
    excluded_at: datetime | None
    excluded_oi_native: float | None
    excluded_oi_usd: float | None
    excluded_ts_prec: str | None
    excluded_timestamp_precision: str | None
    excluded_ts_bucket_size_s: float | None
    excluded_ts_bucket_anchor: str | None


@dataclass(frozen=True)
class OiEvidence:
    """Everything the open-interest rules of a snapshot read from ClickHouse: the perp instruments of the token
    and the statement rows. `history` is empty where only the endpoints were read."""

    instruments: tuple[str, ...] = ()
    history: tuple[OiHistory, ...] = ()
    endpoints: tuple[OiEndpoint, ...] = ()


NO_OI = OiEvidence()


@dataclass(frozen=True)
class FundingVenue(Record):
    """The funding of one perp venue and its normal range in one window (funding.sql).

    The rates and intervals are the decimal text of the stored row; `*_eq` is rate x 8 / interval in float,
    None where the rate is missing or the interval is missing or 0. The settled and the estimate columns are of
    the latest SETTLED and CURRENT_ESTIMATE row, the `start_*` columns of the latest SETTLED row at or before the
    window start. The history is the settled 8-hour-equivalent rates before the window start: its sample count,
    the first and the last time and the 0.025 and 0.975 quantiles; None where there is none."""

    statement = "funding"
    window: str
    instrument_id: str
    settled_rate: Decimal | None
    settled_interval_h: Decimal | None
    settled_at: datetime | None
    settled_eq: float | None
    estimate_rate: Decimal | None
    estimate_interval_h: Decimal | None
    estimate_at: datetime | None
    estimate_eq: float | None
    start_rate: Decimal | None
    start_interval_h: Decimal | None
    start_at: datetime | None
    samples: int
    history_first: datetime | None
    history_last: datetime | None
    normal_low: float | None
    normal_high: float | None


NO_FUNDING_VENUE = FundingVenue(
    window="",
    instrument_id="",
    settled_rate=None,
    settled_interval_h=None,
    settled_at=None,
    settled_eq=None,
    estimate_rate=None,
    estimate_interval_h=None,
    estimate_at=None,
    estimate_eq=None,
    start_rate=None,
    start_interval_h=None,
    start_at=None,
    samples=0,
    history_first=None,
    history_last=None,
    normal_low=None,
    normal_high=None,
)


@dataclass(frozen=True)
class FundingDispersion(Record):
    """The history behind the normal range of the funding dispersion across venues in one window
    (funding_dispersion.sql): the sample count, the first and the last time and the 0.025 and 0.975 quantiles;
    None where there is none."""

    statement = "funding_dispersion"
    window: str
    samples: int
    history_first: datetime | None
    history_last: datetime | None
    normal_low: float | None
    normal_high: float | None


@dataclass(frozen=True)
class FundingConfirmation(Record):
    """The latest funding estimate of a perp venue confirmed by an independent second path: its time, its
    8-hour-equivalent rate (None when the latest confirmed observations have none) and how many observations
    were confirmed (funding_confirmations.sql)."""

    statement = "funding_confirmations"
    instrument_id: str
    rate_8h_eq: float | None
    as_of: datetime | None
    confirmed: int


@dataclass(frozen=True)
class LiquidationWindow(Record):
    """The liquidations of a perp venue in one window (liquidations.sql): the rows of each side and of the
    first half of the window with the sum of their USD value and the number of rows without one, the native
    quantity of each side likewise, and the USD sum and missing count of all the rows. A sum is None where no
    row has the value."""

    statement = "liquidations"
    window: str
    instrument_id: str
    observations: int
    long_count: int
    long_usd: float | None
    long_usd_missing: int
    long_native: float | None
    long_native_missing: int
    short_count: int
    short_usd: float | None
    short_usd_missing: int
    short_native: float | None
    short_native_missing: int
    total_usd: float | None
    total_usd_missing: int
    first_half_count: int
    first_half_usd: float | None
    first_half_usd_missing: int


NO_LIQUIDATIONS = LiquidationWindow(
    window="",
    instrument_id="",
    observations=0,
    long_count=0,
    long_usd=None,
    long_usd_missing=0,
    long_native=None,
    long_native_missing=0,
    short_count=0,
    short_usd=None,
    short_usd_missing=0,
    short_native=None,
    short_native_missing=0,
    total_usd=None,
    total_usd_missing=0,
    first_half_count=0,
    first_half_usd=None,
    first_half_usd_missing=0,
)


@dataclass(frozen=True)
class FundingEvidence:
    """Everything the funding and liquidation rules of a snapshot read from ClickHouse: the perp instruments of
    the token and the statement rows. `dispersion`, `confirmations` and `liquidations` are empty where only the
    funding of the venues was read."""

    instruments: tuple[str, ...] = ()
    venues: tuple[FundingVenue, ...] = ()
    dispersion: tuple[FundingDispersion, ...] = ()
    confirmations: tuple[FundingConfirmation, ...] = ()
    liquidations: tuple[LiquidationWindow, ...] = ()


NO_FUNDING = FundingEvidence()
