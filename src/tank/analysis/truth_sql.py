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
