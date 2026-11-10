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
