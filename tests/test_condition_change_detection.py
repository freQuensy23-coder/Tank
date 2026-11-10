"""Per-minute watch conditions learn that their inputs changed from the database heads.

One token with two spot instruments and one perpetual, the BTC and ETH benchmarks, and the
metric families armed in production. A long-lived service is evaluated minute after minute
while rows arrive, and every result is compared with a service that has never seen the data:
equal values prove no change was missed, and the bytes handed to SHA-256 prove no row list is
serialised to find that out.
"""

import hashlib
from datetime import timedelta
from math import sin
from pathlib import Path

import httpx
import pytest

from tank.analysis.truth import MarketTruthService
from tank.market.service import RANGE_TAIL, MarketService, distinct_rows
from tank.market_schema import MARKET_COLUMNS
from tank.market_storage import _prepare_records, decode, fingerprint_mark
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import iso, parse_time, sha256, utcnow

TOKEN = "uniswap:UNI"
SPOTS = ("binance:spot:UNIUSDT", "coinbase:spot:UNI-USD")
PERP = "binance:perp_linear:UNIUSDT"
BENCHMARKS = ("bitcoin:BTC", "ethereum:ETH")
GROUPS = {
    "relative": (
        "T",
        ["mt.move_class", "mt.divergence_state", "price.repricing_band", "price.vs_anchor_pct"],
    ),
    "spot": (
        "H",
        ["spot.cohort.direction", "spot.cohort.cwb_sell", "spot.cohort.cwb_buy", "spot.classification"],
    ),
    "liquidity": ("T", ["dc.class", "liq.depth_p1_vs_pre", "liq.spread_ratio"]),
    "derivatives": (
        "T",
        ["deriv.native_oi_chg_pct", "deriv.native_oi_vs_normal", "deriv.oi_class", "deriv.funding_vs_normal"],
    ),
    "close": ("T", ["price.close_vs_level"]),
}
HOURS, BOOK_HOURS, OI_DAYS, ANCHOR_HOURS = 4, 3, 10, 2
SIZES = {"close": {"flow_hours": 26}}
OPAQUE = ("request_key", "assembled_at", "reused")
GAPS = (72, 60, 20, 1)
HISTORIES = {}


def wave(seed, minute):
    return 1 + 0.05 * sin((minute + 400 * seed) / 900) + 0.01 * sin((minute + seed) / 37)


class Stand:
    """Stored-format rows of the stand, numbered in minutes from `first`."""

    def __init__(self, settings, first):
        registry = settings.load_config("registry")
        self.first = first
        self.versions = {"config_version": settings.config_version, "code_version": settings.code_version}
        chosen = {*SPOTS, PERP}
        self.instruments = {}
        for instrument in registry["instruments"]:
            token = instrument["token_id"]
            if instrument["instrument_id"] in chosen or (
                token in BENCHMARKS
                and instrument["market_type"] == "SPOT"
                and instrument.get("active", True)
                and not any(held["token_id"] == token for held in self.instruments.values())
            ):
                self.instruments[instrument["instrument_id"]] = instrument

    def common(self, iid):
        return {"instrument_id": iid, "token_id": self.instruments[iid]["token_id"], **self.versions}

    def price(self, iid, at):
        seed = list(self.instruments).index(iid)
        return (20 + 3 * seed) * wave(seed, (at - self.first).total_seconds() / 60)

    def kline(self, iid, opened, recorded=None, scale=1.0):
        o, c = self.price(iid, opened), self.price(iid, opened + timedelta(minutes=1)) * scale
        return {
            **self.common(iid),
            "venue": self.instruments[iid]["venue"],
            "market_type": self.instruments[iid]["market_type"],
            "open_time": iso(opened),
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "complete": True,
            "o": f"{o:.6f}",
            "c": f"{c:.6f}",
            "h": f"{max(o, c) * 1.0004:.6f}",
            "l": f"{min(o, c) * 0.9996:.6f}",
            "volume_native": "1100.0000",
            "quote_volume": f"{1100 * o:.4f}",
            "quote_volume_usd": f"{1100 * o:.4f}",
            "quote_asset": self.instruments[iid]["quote_asset"],
            "n_trades": 120,
            "recorded_at": recorded or iso(opened + timedelta(seconds=62)),
            "recorded_at_prec": "COLLECTION_ONLY",
        }

    def flow(self, iid, started, recorded=None, scale=1.0):
        minute = (started - self.first).total_seconds() / 60
        o, c = self.price(iid, started), self.price(iid, started + timedelta(minutes=1))
        buy, sell = (500 + 200 * sin(minute / 240)) * scale, 520 + 150 * sin(minute / 310)
        return {
            **self.common(iid),
            "venue": self.instruments[iid]["venue"],
            "market_type": self.instruments[iid]["market_type"],
            "minute_start": iso(started),
            "minute_start_prec": "BUCKET(60s)",
            "complete": True,
            "o": f"{o:.6f}",
            "c": f"{c:.6f}",
            "h": f"{max(o, c) * 1.0004:.6f}",
            "l": f"{min(o, c) * 0.9996:.6f}",
            "low": f"{min(o, c) * 0.9996:.6f}",
            "vwap": f"{(o + c) / 2:.6f}",
            "buy_qty": f"{buy:.4f}",
            "sell_qty": f"{sell:.4f}",
            "sum_sq_qty": f"{(buy * buy + sell * sell) / 60:.4f}",
            "buy_usd": f"{buy * o:.4f}",
            "sell_usd": f"{sell * o:.4f}",
            "buy_notional": f"{buy * o:.4f}",
            "sell_notional": f"{sell * o:.4f}",
            "volume_usd": f"{(buy + sell) * o:.4f}",
            "quote_volume": f"{(buy + sell) * o:.4f}",
            "quote_asset": self.instruments[iid]["quote_asset"],
            "n_buy": 60,
            "n_sell": 55,
            "size_buckets": {},
            "trade_aggregation": "PER_FILL",
            "capture_method": "WS",
            "recorded_at": recorded or iso(started + timedelta(seconds=63)),
            "recorded_at_prec": "COLLECTION_ONLY",
        }

    def book(self, iid, at, recorded=None, scale=1.0):
        mid = self.price(iid, at)
        deep = 450000 * scale * (1 + 0.1 * sin((at - self.first).total_seconds() / 700))
        side = {
            "p0_5": {"native": f"{deep / mid / 3:.4f}", "usd": f"{deep / 3:.2f}"},
            "p1": {"native": f"{deep / mid:.4f}", "usd": f"{deep:.2f}"},
            "p2": {"native": f"{deep * 1.8 / mid:.4f}", "usd": f"{deep * 1.8:.2f}"},
        }
        instrument = self.instruments[iid]
        venue = instrument["venue"] + (".spot" if instrument["market_type"] == "SPOT" else ".usdm")
        return {
            **self.common(iid),
            "exchange_ts": iso(at),
            "exchange_ts_prec": "EXACT",
            "received_ts": iso(at + timedelta(milliseconds=200)),
            "received_ts_prec": "COLLECTION_ONLY",
            "recorded_at": recorded or iso(at + timedelta(milliseconds=500)),
            "recorded_at_prec": "COLLECTION_ONLY",
            "metric_key": venue + ".book.depth",
            "mid": f"{mid:.6f}",
            "best_bid": f"{mid * 0.99995:.6f}",
            "best_ask": f"{mid * 1.00005:.6f}",
            "spread_bps": "1",
            "depth": {"bid": side, "ask": side},
            "raw_levels_range_pct": "2",
            "raw_levels_scope": "WITHIN_TWO_PERCENT_OF_MID",
            "quote_asset": instrument["quote_asset"],
            "quote_usd_rate": "1",
            "range_covered_pct": {"bid": "2", "ask": "2"},
            "levels_received": 400,
            "integrity_ok": True,
            "integrity_method": "SEQUENCE",
            "bids": [[f"{mid * (1 - 0.001 * n):.6f}", "1500"] for n in range(1, 4)],
            "asks": [[f"{mid * (1 + 0.001 * n):.6f}", "1500"] for n in range(1, 4)],
        }

    def oi(self, at, recorded=None, scale=1.0):
        minute = (at - self.first).total_seconds() / 60
        mark = self.price(PERP, at)
        native = 5_000_000 * (1 + 0.1 * sin(minute / 4000) + 0.004 * sin(minute / 13)) * scale
        return {
            **self.common(PERP),
            "ts": iso(at),
            "ts_prec": "EXACT",
            "metric_key": "binance.usdm.openInterest",
            "oi_native": f"{native:.2f}",
            "oi_usd": f"{native * mark:.2f}",
            "raw_value": f"{native:.2f}",
            "raw_unit": "BASE",
            "sidedness": "SINGLE_SIDED",
            "mark_price": f"{mark:.6f}",
            "received_ts": iso(at + timedelta(seconds=1)),
            "received_ts_prec": "COLLECTION_ONLY",
            "recorded_at": recorded or iso(at + timedelta(seconds=2)),
            "recorded_at_prec": "COLLECTION_ONLY",
        }

    def funding(self, at, kind, recorded=None, rate=None):
        minute = (at - self.first).total_seconds() / 60
        rate = rate or f"{0.0001 * (1 + 0.3 * sin(minute / 700)):.8f}"
        row = {
            **self.common(PERP),
            "kind": kind,
            "metric_key": "binance.usdm.funding",
            "as_of": iso(at),
            "as_of_prec": "EXACT",
            "rate": rate,
            "interval_h": 8,
            "rate_8h_eq": rate,
            "received_ts": iso(at + timedelta(seconds=1)),
            "received_ts_prec": "COLLECTION_ONLY",
            "recorded_at": recorded or iso(at + timedelta(seconds=2)),
            "recorded_at_prec": "COLLECTION_ONLY",
        }
        if kind == "SETTLED":
            return row | {"funding_time": iso(at), "funding_time_prec": "EXACT"}
        return row | {"capture_source": "PUBLIC_REST"}

    def ticker(self, iid, at, recorded=None):
        mid = self.price(iid, at)
        return {
            **self.common(iid),
            "ts": iso(at),
            "ts_prec": "EXACT",
            "kind": "TICKER_24H",
            "last": f"{mid:.6f}",
            "mark_price": f"{mid:.6f}",
            "quote_asset": self.instruments[iid]["quote_asset"],
            "quote_volume": "2500000",
            "volume_native": f"{2500000 / mid:.4f}",
            "volume_usd": "2500000",
            "capture_source": "PUBLIC_REST",
            "received_ts": iso(at + timedelta(seconds=1)),
            "received_ts_prec": "COLLECTION_ONLY",
            "recorded_at": recorded or iso(at + timedelta(seconds=2)),
            "recorded_at_prec": "COLLECTION_ONLY",
        }

    def minute(self, start, recorded, *, books=2):
        """Every row one captured minute adds, with `books` order-book summaries of each
        instrument."""
        target = [iid for iid in self.instruments if iid in (*SPOTS, PERP)]
        return {
            "mkt.klines_1m": [self.kline(iid, start, recorded) for iid in self.instruments],
            "mkt.flow_bars_1m": [self.flow(iid, start, recorded) for iid in target],
            "mkt.book_summaries": [
                self.book(iid, start + timedelta(seconds=60 / books * n), recorded)
                for iid in target
                for n in range(books)
            ],
            "mkt.tickers_24h": [self.ticker(iid, start + timedelta(seconds=59), recorded) for iid in target],
            "mkt.oi_obs": [self.oi(start, recorded)],
            "mkt.funding_obs": [self.funding(start, "CURRENT_ESTIMATE", recorded)],
        }

    def old(self, at, recorded, scale, *, books=2):
        """The rows of one past minute as a write made now: a late arrival, or with
        `scale` a correction."""
        changed = scale != 1.0
        target = [iid for iid in self.instruments if iid in (*SPOTS, PERP)]
        return {
            "mkt.klines_1m": [self.kline(iid, at, recorded, scale) for iid in target],
            "mkt.flow_bars_1m": [self.flow(iid, at, recorded, 3.0 if changed else 1.0) for iid in target],
            "mkt.book_summaries": [
                self.book(iid, at + timedelta(seconds=60 / books * n), recorded, 0.5 if changed else 1.0)
                for iid in target
                for n in range(books)
            ],
            "mkt.oi_obs": [self.oi(at, recorded, 1.05 if changed else 1.0)],
        }


async def write(repo, batches):
    for collection, rows in batches.items():
        await repo.put_market_batch(collection, rows)


def history(stand, cutoff, *, books, hours, flow_hours, book_hours):
    """The stored rows of one stand size by collection, each with its storage key and the
    minute it belongs to (None for rows no case holds back). They are built once for a size
    and stored as a batch write stores them, without its search of the collection for an
    earlier version of each row."""
    size = (cutoff, books, hours, flow_hours, book_hours)
    if size in HISTORIES:
        return HISTORIES[size]
    target = [iid for iid in stand.instruments if iid in (*SPOTS, PERP)]
    minutes = [cutoff - timedelta(minutes=m) for m in range(max(hours, flow_hours) * 60, 0, -1)]
    batches = {
        "mkt.klines_1m": [
            (at, stand.kline(iid, at)) for iid in stand.instruments for at in minutes[-hours * 60 :]
        ],
        "mkt.flow_bars_1m": [(at, stand.flow(iid, at)) for iid in target for at in minutes],
        "mkt.book_summaries": [
            (at, stand.book(iid, at + timedelta(seconds=60 / books * n)))
            for iid in target
            for at in minutes[-book_hours * 60 :]
            for n in range(books)
        ],
        "mkt.tickers_24h": [
            (None, stand.ticker(iid, cutoff - timedelta(minutes=m))) for iid in target for m in (61, 31, 1)
        ],
        "mkt.oi_obs": [
            (at, stand.oi(at))
            for at in (cutoff - timedelta(minutes=m) for m in range(OI_DAYS * 1440, 0, -1))
            if cutoff - at <= timedelta(hours=3) or (at - stand.first) % timedelta(minutes=30) == timedelta(0)
        ],
        "mkt.funding_obs": [
            (None, stand.funding(stand.first + timedelta(hours=8 * n), "SETTLED")) for n in range(OI_DAYS * 3)
        ]
        + [(None, stand.funding(at, "CURRENT_ESTIMATE")) for at in minutes[-180:]],
    }
    stored = HISTORIES[size] = {}
    for collection, rows in batches.items():
        prepared = _prepare_records(
            collection, [row for _, row in rows], None, MARKET_COLUMNS[collection], False
        )
        stored[collection] = [
            (at, key, decode(collection, row))
            for (at, _), (key, row) in zip(rows, prepared.items(), strict=True)
        ]
    return stored


async def seed(
    repo,
    stand,
    cutoff,
    *,
    books=2,
    hours=HOURS,
    flow_hours=HOURS,
    book_hours=BOOK_HOURS,
    anchor_hours=ANCHOR_HOURS,
    held_back=(),
):
    """`hours` of candles up to `cutoff`, `flow_hours` of flow, `book_hours` of order books
    with `books` summaries a minute at that capture cadence, open interest and funding of
    OI_DAYS, and the event `anchor_hours` before the cutoff, without the minutes `held_back`
    (counted back from the cutoff)."""
    missing = {cutoff - timedelta(minutes=gap) for gap in held_back}
    rows = history(stand, cutoff, books=books, hours=hours, flow_hours=flow_hours, book_hours=book_hours)
    for collection, entries in rows.items():
        repo._data.setdefault(collection, {}).update(
            {key: dict(row) for at, key, row in entries if at not in missing}
        )
    for token_id, tier in ((TOKEN, "HOT"), *((token, "BASE") for token in BENCHMARKS)):
        await repo.put(
            "ops.capture_tier_transitions",
            "STAND-" + token_id,
            {
                "transition_id": "STAND-" + token_id,
                **stand.versions,
                "transition_seq": 1,
                "token_id": token_id,
                "tier": tier,
                "effective_at": iso(stand.first),
                "effective_at_prec": "COLLECTION_ONLY",
                "reason": "STAND",
                "event_ids": [],
                "capture_cadences": {
                    "oi_funding_s": 60 if tier == "HOT" else 300,
                    "kline_s": 60,
                    "book_s": 60 // books,
                    "supply_by_kind": {},
                },
            },
        )
    await repo.put(
        "evt.events",
        "EVT-STAND",
        {
            "event_id": "EVT-STAND",
            "public_at": iso(cutoff - timedelta(hours=anchor_hours)),
            "public_at_prec": "EXACT",
        },
    )


def comparable(value):
    """`value` without the opaque dependency keys, the assembly clock and the note that a
    held result was reused."""
    if isinstance(value, dict):
        return {
            k: comparable(v)
            for k, v in value.items()
            if "dependency" not in k and "fingerprint" not in k and k not in OPAQUE
        }
    if isinstance(value, list):
        return [comparable(v) for v in value]
    return value


async def evaluate(truth, cutoff, group):
    lane, metrics = GROUPS[group]
    stamp = (
        collection_clock(cutoff)
        if lane == "T"
        else chosen_boundary(cutoff.replace(minute=0, second=0, microsecond=0), precision="EXACT")
    )
    requirements = [{"metric": metric, "window": "W1H"} for metric in metrics]
    return await truth.condition_values("EVT-STAND", TOKEN, stamp, requirements)


@pytest.fixture
def stand(tmp_path):
    settings = Settings(
        _env_file=None,
        config_dir=Path(__file__).resolve().parents[1] / "config",
        data_dir=tmp_path / "data",
        replay=True,
    )
    cutoff = utcnow().replace(minute=59, second=0, microsecond=0) - timedelta(hours=3)
    return MemoryRepository(), settings, Stand(settings, cutoff - timedelta(days=OI_DAYS)), cutoff


@pytest.fixture
def hashed(monkeypatch):
    """The number of bytes handed to a SHA-256 or BLAKE2 digest, by any route."""
    spent = [0]

    class Counting:
        def __init__(self, inner):
            self.inner = inner

        def update(self, data):
            spent[0] += len(data)
            self.inner.update(data)

        def copy(self):
            return Counting(self.inner.copy())

        def __getattr__(self, name):
            return getattr(self.inner, name)

    def counting(real):
        def constructor(data=b"", **kwargs):
            spent[0] += len(data)
            return Counting(real(data, **kwargs))

        return constructor

    for name in ("sha256", "blake2b"):
        monkeypatch.setattr(hashlib, name, counting(getattr(hashlib, name)))
    return spent


def key(result):
    return result["_condition_evaluation"]["request_key"]


def arrivals(stand, cutoff, change, books):
    """The writes of one change and the cutoff of the evaluation that follows it: a new
    minute, the held-back minutes arriving late, a correction of those minutes, or a
    correction of rows days and hours back that only the baselines read."""
    now = iso()
    if change == "new":
        return [stand.minute(cutoff, None, books=books)], cutoff + timedelta(minutes=1)
    if change == "ancient":
        target = [iid for iid in stand.instruments if iid in (*SPOTS, PERP)]
        settled = stand.first + timedelta(hours=24)
        return [
            {
                "mkt.oi_obs": [stand.oi(cutoff - timedelta(days=5), now, 1.3)],
                "mkt.funding_obs": [stand.funding(settled, "SETTLED", now, "0.00090000")],
                "mkt.book_summaries": [
                    stand.book(iid, cutoff - timedelta(minutes=150), now, 0.5) for iid in target
                ],
            }
        ], cutoff
    scale = 1.0 if change == "late" else 1.02
    return [stand.old(cutoff - timedelta(minutes=gap), now, scale, books=books) for gap in GAPS], cutoff


def services(repo, settings, http, rows):
    """A fresh service whose registry holds the instruments of the stand only."""
    market = MarketService(repo, settings, http=http)
    market.instruments = {iid: market.instruments[iid] for iid in rows.instruments}
    return MarketTruthService(repo, settings, market)


CASES = [(group, change) for group in GROUPS for change in ("new", "late", "correction")]
CASES += [("derivatives", "ancient"), ("liquidity", "ancient")]


@pytest.mark.parametrize(("group", "change"), CASES)
async def test_a_long_lived_service_answers_as_one_that_never_saw_the_data(stand, group, change):
    repo, settings, rows, cutoff = stand
    size = SIZES.get(group, {})
    await seed(repo, rows, cutoff, **size, held_back=GAPS if change == "late" else ())
    async with httpx.AsyncClient() as http:
        warm = services(repo, settings, http, rows)
        before = await evaluate(warm, cutoff, group)
        assert key(await evaluate(warm, cutoff, group)) == key(before)
        batches, then = arrivals(rows, cutoff, change, size.get("books", 2))
        for batch in batches:
            await write(repo, batch)
        after = await evaluate(warm, then, group)
        cold = await evaluate(services(repo, settings, http, rows), then, group)
        assert comparable(after) == comparable(cold)
        assert key(after) == key(cold) != key(before)
        assert comparable(after) != comparable(before)
        again = await evaluate(warm, then, group)
        assert key(again) == key(after) and comparable(again) == comparable(after)


async def spent(settings, rows, cutoff, hashed, group, **size):
    """Bytes hashed by a service that already holds the history: by the evaluation of the
    next minute with its new rows, and by one more evaluation with nothing new."""
    repo = MemoryRepository()
    await seed(repo, rows, cutoff, anchor_hours=1, book_hours=2, **size)
    async with httpx.AsyncClient() as http:
        truth = services(repo, settings, http, rows)
        await evaluate(truth, cutoff, group)
        await write(repo, rows.minute(cutoff, None, books=size["books"]))
        start = hashed[0]
        await evaluate(truth, cutoff + timedelta(minutes=1), group)
        moved = hashed[0]
        await evaluate(truth, cutoff + timedelta(minutes=1), group)
        return moved - start, hashed[0] - moved


@pytest.mark.parametrize("group", GROUPS)
async def test_hashing_does_not_grow_with_the_rows_in_the_window(stand, hashed, group):
    _, settings, rows, cutoff = stand
    flow = SIZES.get(group, {}).get("flow_hours", 4)
    few = await spent(settings, rows, cutoff, hashed, group, books=1, hours=2, flow_hours=flow // 2)
    many = await spent(settings, rows, cutoff, hashed, group, books=6, hours=4, flow_hours=flow)
    assert all(heavy <= light + 1024 for light, heavy in zip(few, many, strict=True))
    assert max(many) < 65536


def test_repeats_are_dropped_as_a_mapping_keyed_by_content_drops_them():
    stored = {"semantic_fingerprint": "ab" * 32, "recorded_at": "2026-10-18T00:01:00Z", "mid": "1"}
    stored["received_ts"] = "2026-10-18T00:00:59Z"
    bare = {"exchange_ts": "2026-10-18T00:00:00Z", "mid": "3"}
    rows = [
        stored,
        bare,
        dict(stored),
        {**stored, "recorded_at": "2026-10-18T00:02:00Z"},
        {**stored, "mid": "2"},
        {**stored, "received_ts": None},
        dict(bare),
        {**bare, "mid": "4"},
        {**stored, "mid": "2"},
    ]
    kept, expected = distinct_rows(rows), list({sha256(row): row for row in rows}.values())
    assert len(kept) == 6 and kept == expected
    assert all(mine is theirs for mine, theirs in zip(kept, expected, strict=True))


class Recording(MemoryRepository):
    """A memory repository that notes the bounds of every range read."""

    def __init__(self):
        super().__init__()
        self.reads = []

    def iter_range(self, collection, time_field, *, start=None, end=None, **options):
        self.reads.append((parse_time(start), parse_time(end)))
        return super().iter_range(collection, time_field, start=start, end=end, **options)


async def test_a_window_reads_only_what_its_head_says_moved(stand):
    _, settings, rows, cutoff = stand
    repo, collection, field, iid = Recording(), "mkt.flow_bars_1m", "minute_start", SPOTS[0]
    filters = {"instrument_id": iid}
    opened = cutoff - timedelta(hours=2)
    minutes = [opened + timedelta(minutes=m) for m in range(120) if m != 60]
    await repo.put_market_batch(collection, [rows.flow(iid, at) for at in minutes])
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)

        async def window(start, upper):
            del repo.reads[:]
            held, head = await market._condition_range(collection, field, start, upper, iid)
            done = list(repo.reads)
            stored = MemoryRepository.iter_range(
                repo, collection, field, start=start, end=upper, filters=filters
            )
            assert held == [row async for row in stored]
            digest = 0
            for row in held:
                digest ^= fingerprint_mark(row["semantic_fingerprint"], row["recorded_at"])
            assert head[2] == {"rows": len(held), "unknown": 0, "digest": f"{digest:016x}"}
            return head, done

        first, done = await window(opened, cutoff)
        assert done == [(opened, cutoff)] and first[2]["rows"] == 119
        assert await window(opened, cutoff) == (first, [])

        await repo.put_market_batch(collection, [rows.flow(iid, cutoff)])
        start, upper = opened + timedelta(minutes=1), cutoff + timedelta(minutes=1)
        moved, done = await window(start, upper)
        assert done == [(cutoff - RANGE_TAIL, upper)] and moved[2]["rows"] == 119

        await repo.put_market_batch(collection, [rows.flow(iid, opened + timedelta(minutes=60), iso())])
        late, done = await window(start, upper)
        assert done == [(upper - RANGE_TAIL, upper), (start, upper)] and late[2]["rows"] == 120

        await repo.put_market_batch(collection, [rows.flow(iid, opened + timedelta(minutes=45), iso(), 3.0)])
        corrected, done = await window(start, upper)
        assert done == [(upper - RANGE_TAIL, upper), (start, upper)]
        assert corrected[2]["rows"] == 120 and corrected[2]["digest"] != late[2]["digest"]
        assert await window(start, upper) == (corrected, [])

        key, row = next(
            (key, row)
            for key, row in repo._data[collection].items()
            if row[field] == iso(opened + timedelta(minutes=30))
        )
        repo._data[collection][key] = {**row, "recorded_at": iso(cutoff + timedelta(hours=1))}
        stamped, done = await window(start, upper)
        assert done == [(upper - RANGE_TAIL, upper), (start, upper)]
        assert stamped[2]["rows"] == 120 and stamped[2]["digest"] != corrected[2]["digest"]
