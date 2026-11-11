"""Supply statements (03 §0, §5.6) against a real PostgreSQL server (TANK_TEST_DATABASE_URL): supply_inventory.sql,
supply_lending.sql and supply_chains.sql, what the assessment and a condition send, and the rules over their
records."""

import os
from contextlib import asynccontextmanager
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
import pytest_asyncio
from supply_helpers import supply_evidence, window_supply

from tank.analysis import truth, truth_sql
from tank.market.service import MarketService
from tank.storage import MemoryRepository
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
ASSEMBLED = END + timedelta(minutes=30)
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
CONFIG = {"historical_lookback_days": 12, "history_min_days": 7}
WINDOWS = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400, "EVENT": 79500}


@pytest_asyncio.fixture
async def repo(postgres_template):
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.fail("TANK_TEST_DATABASE_URL must point to an isolated tank_test* PostgreSQL database")
    async with postgres_template.repository(url) as repository:
        yield repository


def row(kind, at, *, token="T", available=True, recorded=None, **fields):
    return {
        "token_id": token,
        "kind": kind,
        "ts": iso(at),
        "recorded_at": iso(recorded or at),
        "received_ts": iso(recorded or at),
        "available": available,
        **fields,
    }


def balance(venue, quantity, *, complete=True, contract="0xT", chainid=1):
    return {
        "venue": venue,
        "chainid": chainid,
        "contract": contract,
        "wallet_list_commit": "pinned",
        "qty_native": None if quantity is None else str(quantity),
        "complete": complete,
    }


def inventory(at, quantities, **fields):
    return row("EXCHANGE_INVENTORY", at, balances=[balance(v, q) for v, q in quantities.items()], **fields)


def chain(at, series, name="Ethereum", **fields):
    return row(
        "CHAIN_LIQUIDITY",
        at,
        chain=name,
        series=[{"ts": iso(t), "supply_usd": str(v)} for t, v in series],
        **fields,
    )


def lending(at, pools, **fields):
    return row(
        "LENDING",
        at,
        pools=[{"pool_id": p, "utilization": str(u), "borrow_apy": "7"} for p, u in pools.items()],
        **fields,
    )


async def store(repo, rows):
    keys = [f"{r['token_id']}:{r['kind']}:{r.get('chain')}:{r['ts']}:{n}" for n, r in enumerate(rows)]
    await repo.put_market_batch("mkt.supply_obs", rows, keys=keys)


def base(token="T"):
    return dict(token_id=token, cutoff=iso(END), assembled_at=iso(ASSEMBLED))


async def ask(repo, windows=None, token="T", **changes):
    return await truth.supply_evidence(repo, base(token) | changes, END, CONFIG, windows or WINDOWS)


def percentile(values, p):
    ordered = sorted(values)
    k = (len(ordered) - 1) * p
    i = int(k)
    return ordered[i] + (ordered[min(i + 1, len(ordered) - 1)] - ordered[i]) * (k - i)


def week_of_inventory(*, end_quantities=None, current_extra=()):
    """Eight days of 6-hour observations, binance growing by 10 each and okx constant; the last is at the cutoff."""
    rows = []
    for k in range(33):
        at = END - (32 - k) * 6 * HOUR
        quantities = {"binance": 100 + 10 * k, "okx": 200}
        rows.append(inventory(at, quantities))
    rows[-1]["balances"] += list(current_extra)
    return rows


def day_changes(starts_before=None):
    totals = [300 + 10 * k for k in range(33)]
    return [
        ((totals[k] - totals[k - 4]) / totals[k - 4], END - (32 - k) * 6 * HOUR)
        for k in range(4, 33)
        if starts_before is None or END - (32 - k) * 6 * HOUR < starts_before
    ]


@pytest.mark.integration
@pytest.mark.postgres
async def test_the_inventory_change_is_the_cohort_sum_a_day_apart_and_its_history_is_cut_at_the_window_start(
    repo,
):
    await store(
        repo,
        week_of_inventory(
            current_extra=[balance("kraken", 5, complete=False), balance("bybit", None), {"venue": "x"}]
        ),
    )
    result = await ask(repo)
    event = next(r for r in result.inventory if r.window == "EVENT")
    assert [r.window for r in result.inventory] == list(WINDOWS)
    assert event.current_at == END and event.previous_at == END - DAY
    assert event.first_at == END - 8 * DAY
    assert (event.observed_balances, event.complete_balances, event.comparable_balances) == (5, 2, 2)
    assert (event.base, event.total, event.change) == (Decimal(580), Decimal(620), Decimal(40))
    assert event.pct == pytest.approx(40 / 580, rel=1e-12)
    assert (
        event.venues == ("binance", "okx") and event.chainids == (1, 1) and event.contracts == ("0xT", "0xT")
    )
    assert event.qty_native == (Decimal(420), Decimal(200))
    assert event.chg_native == (Decimal(40), Decimal(0))
    assert event.chg_pct == (pytest.approx(40 / 380, rel=1e-12), pytest.approx(0.0, abs=1e-15))
    for record in result.inventory:
        start = END - timedelta(seconds=WINDOWS[record.window])
        expected = [x for x, at in day_changes() if at < start]
        assert record.samples == len(expected), record.window
        assert record.normal_low == pytest.approx(percentile(expected, 0.025), rel=1e-12)
        assert record.normal_high == pytest.approx(percentile(expected, 0.975), rel=1e-12)
    assert len({r.samples for r in result.inventory}) > 1


@pytest.mark.integration
@pytest.mark.postgres
async def test_an_inventory_without_an_observation_a_day_before_has_no_cohort_and_no_change(repo):
    rows = [
        inventory(END - 31 * HOUR, {"binance": 100}),
        inventory(END - HOUR, {"binance": 120}),
        inventory(END, {"binance": 130}),
    ]
    await store(repo, rows)
    result = await ask(repo, {"W1H": 3600})
    (found,) = result.inventory
    assert found.previous_at is None and found.comparable_balances == 0 and found.complete_balances == 1
    assert found.change is None and found.pct is None and found.base is None and found.total is None
    assert found.venues == () and found.samples == 0 and found.normal_low is None


@pytest.mark.integration
@pytest.mark.postgres
async def test_the_observation_a_day_before_counts_only_within_six_hours_and_when_available(repo):
    await store(
        repo,
        [
            inventory(END - DAY - 7 * HOUR, {"binance": 100}),
            inventory(END - DAY - 5 * HOUR, {"binance": 110}),
            inventory(END, {"binance": 121}),
        ],
    )
    (found,) = (await ask(repo, {"W1H": 3600})).inventory
    assert found.previous_at == END - DAY - 5 * HOUR and found.change == Decimal(11)
    assert found.pct == pytest.approx(0.1, rel=1e-12)
    await store(repo, [row("EXCHANGE_INVENTORY", END - DAY, available=False, reason="SOURCE_FAILED")])
    (found,) = (await ask(repo, {"W1H": 3600})).inventory
    assert found.previous_at == END - DAY and found.comparable_balances == 0 and found.change is None


@pytest.mark.integration
@pytest.mark.postgres
async def test_a_balance_key_is_venue_chain_contract_and_wallet_list_and_the_last_balance_of_a_key_wins(repo):
    now = inventory(END, {"binance": 100})
    now["balances"] += [balance("binance", 300), balance("binance", 50, contract="0xU"), balance("okx", 70)]
    old = inventory(END - DAY, {"binance": 100})
    old["balances"] += [balance("binance", 150, contract="0xU"), balance("okx", 60, chainid=56)]
    await store(repo, [now, old])
    (found,) = (await ask(repo, {"W1H": 3600})).inventory
    assert found.complete_balances == 3 and found.comparable_balances == 2
    assert found.venues == ("binance", "binance") and found.contracts == ("0xT", "0xU")
    assert found.qty_native == (Decimal(300), Decimal(50)) and found.chg_native == (
        Decimal(200),
        Decimal(-100),
    )
    assert (found.base, found.total, found.change) == (Decimal(250), Decimal(350), Decimal(100))


@pytest.mark.integration
@pytest.mark.postgres
async def test_cohort_keys_are_ordered_by_their_python_text_so_that_chain_137_comes_before_chain_56(repo):
    now = inventory(END, {})
    now["balances"] = [
        balance("okx", 10, chainid=1),
        balance("okx", 10, chainid=137),
        balance("okx", 10, chainid=56),
    ]
    old = dict(now, ts=iso(END - DAY), balances=[balance("okx", 5, chainid=c) for c in (1, 137, 56)])
    await store(repo, [now, old])
    (found,) = (await ask(repo, {"W1H": 3600})).inventory
    assert found.chainids == (1, 137, 56)


@pytest.mark.integration
@pytest.mark.postgres
async def test_rows_after_the_cutoff_or_recorded_or_received_after_assembly_do_not_exist(repo):
    rows = week_of_inventory()
    rows.append(inventory(END + timedelta(minutes=1), {"binance": 999, "okx": 200}))
    rows.append(inventory(END - HOUR, {"binance": 999, "okx": 200}, recorded=ASSEMBLED + HOUR))
    late = inventory(END - 2 * HOUR, {"binance": 999, "okx": 200})
    late["received_ts"] = iso(ASSEMBLED + HOUR)
    rows.append(late)
    rows.append(inventory(END - 13 * DAY, {"binance": 1, "okx": 1}))
    await store(repo, rows)
    (found,) = (await ask(repo, {"W1H": 3600})).inventory
    assert found.current_at == END and found.change == Decimal(40)
    assert found.first_at == END - 8 * DAY
    other = await ask(repo, {"W1H": 3600}, assembled_at=iso(ASSEMBLED + 2 * HOUR))
    assert other.inventory[0].current_at == END
    assert other.inventory[0].samples != found.samples


@pytest.mark.integration
@pytest.mark.postgres
async def test_another_token_and_a_lookback_shorter_than_the_rows_are_not_read(repo):
    await store(repo, week_of_inventory() + [inventory(END, {"binance": 1}, token="U")])
    assert (await ask(repo, {"W1H": 3600}, token="U")).inventory[0].change is None
    short = await truth.supply_evidence(repo, base(), END, {"historical_lookback_days": 2}, {"W1H": 3600})
    assert short.inventory[0].first_at == END - 2 * DAY
    assert short.inventory[0].samples == len(
        [1 for _, at in day_changes() if at - DAY >= END - 2 * DAY and at < END - HOUR]
    )


@pytest.mark.integration
@pytest.mark.postgres
async def test_an_inventory_without_rows_gives_one_empty_row_per_window(repo):
    result = await ask(repo)
    assert [r.window for r in result.inventory] == list(WINDOWS)
    assert all(r.current_at is None and r.complete_balances == 0 and r.venues == () for r in result.inventory)
    assert result.pools == () and result.chains == ()


@pytest.mark.integration
@pytest.mark.postgres
async def test_a_pool_change_is_its_utilization_minus_the_one_a_day_before_within_half_an_hour(repo):
    await store(
        repo,
        [
            lending(END, {"P1": 0.5, "P2": 0.1, "P3": 0.3}),
            lending(END + timedelta(seconds=1), {"P1": 0.9}),
            lending(END - DAY - timedelta(minutes=10), {"P1": 0.4, "P3": 0.45}),
            row("LENDING", END - DAY, available=False, reason="SOURCE_FAILED"),
            lending(END - DAY - timedelta(minutes=40), {"P2": 0.0}),
        ],
    )
    result = await ask(repo)
    changes = {r.pool_id: r.chg_24h for r in result.pools}
    assert changes == {"P1": pytest.approx(0.1, abs=1e-12), "P2": None, "P3": pytest.approx(-0.15, abs=1e-12)}


@pytest.mark.integration
@pytest.mark.postgres
async def test_a_pool_has_no_change_without_an_earlier_observation_and_a_utilization_keeps_its_digits(repo):
    long = "0.2746941874292319864314918252580811741686683058788972744543460443345319480568702416653638245758656793"
    await store(repo, [lending(END, {"P1": long})])
    (found,) = (await ask(repo)).pools
    assert found.pool_id == "P1" and found.chg_24h is None
    await store(repo, [lending(END - DAY, {"P1": "0.25"})])
    (found,) = (await ask(repo)).pools
    assert found.chg_24h == float(long) - 0.25


@pytest.mark.integration
@pytest.mark.postgres
async def test_a_chain_has_its_supply_at_the_cutoff_and_the_changes_over_a_day_and_a_week(repo):
    series = [
        (END - 8 * DAY, 90),
        (END - 7 * DAY, 100),
        (END - DAY, "110.50"),
        (END - HOUR, 114),
        (END, 115),
        (END + DAY, 99999),
    ]
    await store(repo, [chain(END - HOUR, series)])
    (found,) = (await ask(repo)).chains
    assert found.chain == "Ethereum" and found.observed_at == END - HOUR and found.available is True
    assert found.supply_usd == Decimal(115) and found.supply_chg_24h == Decimal("4.50")
    assert found.supply_chg_7d == Decimal(15)


@pytest.mark.integration
@pytest.mark.postgres
async def test_a_point_older_than_a_day_before_its_instant_is_no_point_and_the_later_of_two_of_one_instant_wins(
    repo,
):
    series = [
        (END - 3 * DAY, 100),
        (END - 2 * DAY, 105),
        (END - DAY, 7),
        (END - DAY, 8),
        (END - 2 * HOUR, 120),
    ]
    await store(repo, [chain(END, series)])
    (found,) = (await ask(repo)).chains
    assert found.supply_usd == Decimal(120)
    assert found.supply_chg_24h == Decimal(112)
    assert found.supply_chg_7d is None


@pytest.mark.integration
@pytest.mark.postgres
async def test_each_chain_has_its_latest_observation_and_an_unavailable_one_has_a_reason_and_no_numbers(repo):
    await store(
        repo,
        [
            chain(END - 2 * HOUR, [(END, 1)], "Base"),
            chain(END - HOUR, [(END, 2)], "Base"),
            chain(END - 3 * HOUR, [(END, 3)], "Arbitrum"),
            row(
                "CHAIN_LIQUIDITY",
                END - 90 * timedelta(minutes=1),
                chain="BSC",
                available=False,
                reason="ConnectError",
            ),
            row("CHAIN_LIQUIDITY", END - 5 * HOUR, available=False, reason="NOT_SUPPORTED"),
            chain(END + HOUR, [(END, 4)], "Optimism"),
        ],
    )
    result = await ask(repo)
    assert [c.chain for c in result.chains] == [None, "Arbitrum", "BSC", "Base"]
    by_chain = {c.chain: c for c in result.chains}
    assert by_chain["Base"].supply_usd == Decimal(2) and by_chain["Arbitrum"].supply_usd == Decimal(3)
    assert by_chain["BSC"].available is False and by_chain["BSC"].reason == "ConnectError"
    assert by_chain["BSC"].supply_usd is None
    assert by_chain[None].reason == "NOT_SUPPORTED"


@pytest.mark.integration
@pytest.mark.postgres
async def test_the_assessment_reads_the_supply_of_every_window_and_the_rules_apply_to_what_it_got(repo):
    rows = week_of_inventory()
    rows[-1]["balances"][0]["qty_native"] = "1000"
    rows += [
        lending(END, {"P": 0.5}),
        lending(END - DAY, {"P": 0.4}),
        chain(END, [(END - 7 * DAY, 100), (END - DAY, 110), (END, 115)]),
    ]
    await store(repo, rows)
    event = {"type": "T0", "ts": iso(END - timedelta(seconds=79500))}
    shown = await truth.with_supply(repo, base() | {"anchor": event, "instruments": []}, CONFIG)
    assert [r.window for r in shown["supply_sql"].inventory] == list(WINDOWS)
    latest = [r for r in rows if r["ts"] == iso(END)]
    start = END - HOUR
    result = truth.supply_truth(
        dict(shown, supply=[dict(r, observed_at=r["ts"]) for r in latest], _primary="W1H"), start, END
    )
    inventory_ = result["exchange_inventory"]
    assert inventory_["chg_native_24h"] == Decimal(620)
    assert inventory_["chg_pct"] > inventory_["normal_range"][1]
    assert result["supply_in_evidence"] == "TRUE"
    assert result["lending"]["pools"][0]["chg_24h"] == pytest.approx(0.1)
    assert result["stablecoin_chain"]["supply_chg_7d"] == Decimal(15)


async def test_a_statement_takes_exactly_the_parameters_it_names_and_only_a_postgresql_repository_runs_it():
    with pytest.raises(TypeError, match="supply_lending takes"):
        await truth_sql.fetch_postgres(SimpleNamespace(_conn=None), truth_sql.SupplyPool, token_id="T")
    with pytest.raises(RuntimeError, match="PostgreSQL only"):
        await truth_sql.fetch_postgres(
            MemoryRepository(), truth_sql.SupplyPool, token_id="T", cutoff=END, assembled=ASSEMBLED
        )


class Recorder:
    """A repository that records what is sent to PostgreSQL and answers each statement with its rows."""

    def __init__(self, answers):
        self.answers, self.sent = answers, []

    @asynccontextmanager
    async def transaction(self):
        yield self

    def _conn(self):
        return self

    async def execute(self, query, params):
        name = query.split("-- ")[1].split(" ")[0]
        self.sent.append((name, params))

        class Cursor:
            async def fetchall(cursor):
                return self.answers.get(name, [])

        return Cursor()


def inventory_row(window, **changes):
    values = dict(
        window=window,
        current_at=None,
        previous_at=None,
        first_at=None,
        observed_balances=None,
        complete_balances=0,
        comparable_balances=0,
        base=None,
        total=None,
        change=None,
        pct=None,
        samples=0,
        normal_low=None,
        normal_high=None,
        venues=[],
        chainids=[],
        contracts=[],
        qty_native=[],
        chg_native=[],
        chg_pct=[],
    )
    return values | changes


@pytest.mark.asyncio
async def test_the_assessment_sends_every_window_and_a_condition_the_primary_one():
    recorder = Recorder({"supply_inventory": [inventory_row(name) for name in WINDOWS]})
    event = {"type": "T0", "ts": iso(END - timedelta(seconds=79500))}
    shown = await truth.with_supply(recorder, base() | {"anchor": event}, CONFIG)
    assert [name for name, _ in recorder.sent] == ["supply_inventory", "supply_lending", "supply_chains"]
    inventory_params = recorder.sent[0][1]
    assert inventory_params["names"] == ["W15M", "W1H", "W4H", "W24H", "EVENT"]
    assert inventory_params["starts"] == [END - timedelta(seconds=s) for s in WINDOWS.values()]
    assert inventory_params["token_id"] == "T" and inventory_params["lookback_s"] == 12 * 86400
    assert inventory_params["cutoff"] == END and inventory_params["assembled"] == ASSEMBLED
    assert all(p["token_id"] == "T" for _, p in recorder.sent)
    assert len(shown["supply_sql"].inventory) == 5
    recorder.sent.clear()
    await truth.with_supply(recorder, base() | {"anchor": event}, CONFIG, windows={"W1H": 3600})
    assert recorder.sent[0][1]["names"] == ["W1H"] and recorder.sent[0][1]["starts"] == [END - HOUR]


@pytest.mark.asyncio
async def test_a_snapshot_without_an_assembly_time_reads_every_row_and_one_without_a_window_is_returned_as_it_is():
    recorder = Recorder({})
    await truth.with_supply(recorder, {"token_id": "T", "cutoff": iso(END)}, CONFIG)
    assert recorder.sent[0][1]["assembled"] == truth.AS_OF_ALL
    recorder.sent.clear()
    snapshot = base() | {"anchor": {"type": "T0", "ts": iso(END)}}
    assert await truth.with_supply(recorder, snapshot, CONFIG) is snapshot and not recorder.sent


@pytest.mark.asyncio
async def test_a_condition_reads_the_supply_only_for_a_state():
    snapshot = base() | {"anchor": {"type": "T0", "ts": iso(END - 3 * HOUR)}}
    assert (
        truth.condition_supply_windows(snapshot, [{"metric": "deriv.native_oi_chg_pct", "window": "W1H"}])
        == {}
    )
    state = truth.condition_supply_windows(snapshot, [{"metric": "mt.move_class", "window": "W4H"}])
    assert list(state.values()) == [3 * 3600.0]


def record(window="EVENT", **changes):
    return truth_sql.SupplyInventory.decode(inventory_row(window, **changes))


def evaluate(records, rows, window="EVENT", **parameters):
    snapshot = {"supply": rows, "supply_sql": records, "_primary": window}
    with_parameters = truth._PARAMETERS.set(parameters)
    try:
        return truth.supply_truth(snapshot, END - HOUR, END)
    finally:
        truth._PARAMETERS.reset(with_parameters)


def current(**fields):
    return {"kind": "EXCHANGE_INVENTORY", "observed_at": iso(END), "available": True} | fields


SPANNING = dict(current_at=iso(END), first_at=iso(END - 8 * DAY))


def test_the_inventory_rises_beyond_its_range_only_when_the_history_spans_the_minimum_days():
    stats = dict(change="10", pct=0.2, samples=5, normal_low=-0.1, normal_high=0.1, complete_balances=1)
    result = evaluate(
        truth_sql.SupplyEvidence(inventory=(record(**stats, **SPANNING),)),
        [current(coverage_errors=[{"reason": "x"}], unsupported_venues=["kraken"], balances=[])],
    )
    shown = result["exchange_inventory"]
    assert (
        shown["normal_range"] == [-0.1, 0.1]
        and shown["chg_pct"] == 0.2
        and result["supply_in_evidence"] == "TRUE"
    )
    assert shown["coverage"] == dict(
        complete_balances=1,
        observed_balances=None,
        comparable_balances=0,
        coverage_errors=[{"reason": "x"}],
        unsupported_venues=["kraken"],
    )
    short = record(**stats, current_at=iso(END), first_at=iso(END - 6 * DAY))
    result = evaluate(truth_sql.SupplyEvidence(inventory=(short,)), [current()])
    assert result["exchange_inventory"]["normal_range"] is None and result["supply_in_evidence"] == "FALSE"
    assert evaluate(truth_sql.SupplyEvidence(inventory=(short,)), [current()], history_min_days=5)[
        "exchange_inventory"
    ]["normal_range"] == [-0.1, 0.1]
    empty = record(**stats | {"samples": 0}, **SPANNING)
    assert (
        evaluate(truth_sql.SupplyEvidence(inventory=(empty,)), [current()])["exchange_inventory"][
            "normal_range"
        ]
        is None
    )


def test_an_inventory_that_fell_or_has_no_change_is_not_evidence_and_says_why():
    stats = dict(change="-10", pct=-0.2, samples=5, normal_low=-0.1, normal_high=0.1)
    fell = evaluate(truth_sql.SupplyEvidence(inventory=(record(**stats, **SPANNING),)), [current()])
    assert fell["supply_in_evidence"] == "FALSE" and fell["exchange_inventory"]["reason"] is None
    unknown = evaluate(truth_sql.SupplyEvidence(inventory=(record(**SPANNING),)), [current()])
    assert (
        unknown["exchange_inventory"]["reason"] == "INCOMPLETE" and unknown["supply_in_evidence"] == "FALSE"
    )
    gone = evaluate(truth_sql.NO_SUPPLY, [current(available=False, reason="NOT_SUPPORTED")])
    assert (
        gone["unavailable"]["exchange_inventory"] == "NOT_SUPPORTED"
        and gone["supply_in_evidence"] == "UNKNOWN"
    )


def test_the_cohort_by_key_is_zipped_into_per_venue_entries():
    found = record(
        venues=["binance"],
        chainids=[1],
        contracts=["0xT"],
        qty_native=["12"],
        chg_native=["2"],
        chg_pct=[0.2],
        **SPANNING,
    )
    result = evaluate(truth_sql.SupplyEvidence(inventory=(found,)), [current()])
    assert result["exchange_inventory"]["per_venue"] == [
        dict(
            venue="binance",
            chainid=1,
            contract="0xT",
            qty_native=Decimal(12),
            chg_native_24h=Decimal(2),
            chg_pct=0.2,
        )
    ]


def test_lending_pools_carry_the_change_of_their_id_and_a_pool_without_one_has_none():
    pools = (truth_sql.SupplyPool(pool_id="A", chg_24h=0.1), truth_sql.SupplyPool(pool_id="B", chg_24h=None))
    rows = [
        {
            "kind": "LENDING",
            "observed_at": iso(END),
            "available": True,
            "pools": [{"pool_id": "A", "utilization": 0.5}, {"pool_id": "B"}, {"pool_id": "C"}],
        }
    ]
    result = evaluate(truth_sql.SupplyEvidence(pools=pools), rows)
    assert [p["chg_24h"] for p in result["lending"]["pools"]] == [0.1, None, None]
    assert result["lending"]["borrow_apr"] is None and result["lending"]["reason"] == "APY_IS_NOT_APR"


def test_a_chain_is_the_first_available_context_and_an_unavailable_one_is_listed_as_unavailable():
    chains = (
        truth_sql.SupplyChain("BSC", END, False, "ConnectError", None, None, None),
        truth_sql.SupplyChain("Base", END, True, None, Decimal(5), Decimal(1), None),
        truth_sql.SupplyChain("Arbitrum", END, True, None, None, None, None),
    )
    result = evaluate(truth_sql.SupplyEvidence(chains=chains), [])
    assert result["unavailable"]["stablecoin_chain"] == "ConnectError"
    assert result["stablecoin_chain"] == dict(
        chain="Base", supply_usd=Decimal(5), supply_chg_24h=Decimal(1), supply_chg_7d=None, reason=None
    )
    assert result["chain_contexts"][1]["reason"] == "INCOMPLETE"
    assert evaluate(truth_sql.NO_SUPPLY, [])["unavailable"]["stablecoin_chain"] == "INCOMPLETE"


def test_the_helper_that_stands_in_for_the_statements_agrees_with_the_documented_rules():
    rows = week_of_inventory()
    found = supply_evidence({"supply": rows, "cutoff": iso(END)}, windows={"EVENT": 79500})
    (inventory_,) = found.inventory
    assert inventory_.change == Decimal(40) and inventory_.base == Decimal(580)
    expected = [x for x, at in day_changes() if at < END - timedelta(seconds=79500)]
    assert inventory_.samples == len(expected)
    assert inventory_.normal_low == pytest.approx(percentile(expected, 0.025), rel=1e-12)
    result = truth.supply_truth(
        window_supply({"supply": rows, "cutoff": iso(END)}, END - HOUR, END), END - HOUR, END
    )
    assert result["exchange_inventory"]["chg_native_24h"] == Decimal(40)


@pytest.mark.asyncio
async def test_a_snapshot_carries_the_latest_row_of_each_supply_kind_and_chain_not_the_history():
    repo = MemoryRepository()
    await repo.open()
    rows = week_of_inventory()
    rows += [chain(END - HOUR, [(END, 1)], "Base"), chain(END - 5 * HOUR, [(END, 1)], "Base")]
    rows += [row("LENDING", END - 3 * HOUR, available=False, reason="X"), lending(END - 4 * HOUR, {"P": 0.1})]
    await store(repo, rows)
    found = await MarketService._latest_supply(SimpleNamespace(repo=repo), "T", END)
    pairs = sorted((r["kind"], r["ts"]) for r in found)
    assert pairs == [
        ("CHAIN_LIQUIDITY", iso(END - HOUR)),
        ("EXCHANGE_INVENTORY", iso(END)),
        ("LENDING", iso(END - 4 * HOUR)),
        ("LENDING", iso(END - 3 * HOUR)),
    ]
    await repo.close()
