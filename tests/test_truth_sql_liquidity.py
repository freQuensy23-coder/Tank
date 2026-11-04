import builtins
import json
import os
import subprocess
import sys
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from types import ModuleType

import pytest
from truth_sql_failure_evidence import EvidenceClickHouse, retain_liquidity_phase

from tank.analysis.truth_sql_liquidity import ctes, query, window_query, windows_query
from tank.util import canonical_json, iso, parse_time

pytestmark = pytest.mark.integration

ORIGINAL_COMMIT = "ef3b43e8457622d2f3c2c86926de41aecee101d6"


@pytest.fixture(scope="module")
def original_truth():
    root = Path(__file__).resolve().parents[1]
    modules = {}
    original_import = builtins.__import__

    def load(name):
        if name in modules:
            return modules[name]
        module = ModuleType("_liquidity_original_" + name)
        modules[name] = module
        sys.modules[module.__name__] = module
        path = "src/" + name.replace(".", "/")
        result = subprocess.run(
            ["git", "show", f"{ORIGINAL_COMMIT}:{path}.py"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode:
            assert name in {"tank", "tank.analysis", "tank.market"}, result.stderr
            module.__path__ = []
            return module
        module.__file__ = f"{ORIGINAL_COMMIT}/{path}.py"
        module.__dict__["__builtins__"] = {**vars(builtins), "__import__": frozen_import}
        exec(compile(result.stdout, module.__file__, "exec"), module.__dict__)
        return module

    def frozen_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name != "tank" and not name.startswith("tank."):
            return original_import(name, globals, locals, fromlist, level)
        module = load(name)
        for child in fromlist:
            if not hasattr(module, child):
                setattr(module, child, load(name + "." + child))
        return module

    yield load("tank.analysis.truth")
    for module in modules.values():
        sys.modules.pop(module.__name__, None)


@pytest.fixture
async def native_client(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        yield client
    finally:
        await client.close()


def _book(venue, stamp, *, bid="200", ask="300", native=True):
    return {
        "instrument_id": f"{venue}:spot:T",
        "token_id": "T",
        "exchange_ts": iso(stamp),
        "exchange_ts_prec": "EXACT",
        "integrity_ok": True,
        "mid": "100",
        "quote_asset": "USD",
        "spread_bps": 2,
        "range_covered_pct": {"bid": 2, "ask": 2},
        "depth": {
            "bid": {"p1": {"usd": bid, "native": "2" if native else None}},
            "ask": {"p1": {"usd": ask, "native": "3" if native else None}},
        },
        "bids": [["99", "5"], {"price": "98", "qty": "1000"}],
        "asks": [["101", "5"], {"price": "102", "qty": "1000"}],
    }


def _packet():
    end = parse_time("2026-10-16T12:00:00Z")
    start = end - timedelta(minutes=15)
    snapshot = {
        "token_id": "T",
        "cutoff": iso(end),
        "_start": iso(start),
        "_primary": "W15M",
        "_event_anchor": start,
        "books": [],
        "flow_bars": [],
    }
    for venue in ("B", "A"):
        snapshot["books"].extend(
            [
                _book(venue, start - timedelta(minutes=30), bid="400", ask="600"),
                _book(venue, end),
            ]
        )
        for minute in range(15):
            snapshot["flow_bars"].append(
                {
                    "instrument_id": f"{venue}:spot:T",
                    "minute_start": iso(start + timedelta(minutes=minute)),
                    "complete": True,
                    "buy_notional": "100",
                    "sell_notional": "20",
                    "buy_qty": "1",
                    "sell_qty": "0.2",
                    "low": "99",
                }
            )
    spot: dict = {"venues": [{"venue": "A"}, {"venue": "B"}]}
    relative = {"windows": {"W15M": {"r_raw": 0.005, "beyond_normal": False}}}
    return snapshot, spot, relative


def _assert_complete(actual, expected, path="result"):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), path
        assert set(actual) == set(expected), path
        for key, value in expected.items():
            _assert_complete(actual[key], value, f"{path}.{key}")
    elif isinstance(expected, (list, tuple)):
        assert isinstance(actual, list), path
        assert len(actual) == len(expected), path
        for index, value in enumerate(expected):
            _assert_complete(actual[index], value, f"{path}[{index}]")
    elif isinstance(expected, Decimal):
        assert isinstance(actual, str), path
        with localcontext() as context:
            context.prec = 100
            error = abs(Decimal(actual) - expected)
            assert error <= max(Decimal("1e-18"), abs(expected) * Decimal("1e-18")), path
    elif isinstance(expected, bool):
        assert actual is expected, path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-10, abs=1e-12), path
    else:
        assert actual == expected, path


async def _compare(native_client, original_truth, snapshot, spot, relative, config=None):
    config = config or {}
    token = original_truth._PARAMETERS.set(config)
    try:
        expected = original_truth.liquidity_truth(
            deepcopy(snapshot),
            parse_time(snapshot["_start"]),
            parse_time(snapshot["cutoff"]),
            deepcopy(spot),
            deepcopy(relative),
        )
    finally:
        original_truth._PARAMETERS.reset(token)
    statement, parameters = query(snapshot, config, spot=spot, relative=relative)
    rows = await native_client.rows(statement, parameters)
    assert len(rows) == 1
    actual = json.loads(rows[0]["result"])
    _assert_complete(actual, expected)
    return actual


@pytest.mark.parametrize(
    "case",
    [
        "complete",
        "scientific_book_and_flow",
        "no_books",
        "no_flows",
        "bad_integrity",
        "partial_ask",
        "missing_usd_depth",
        "missing_native_depth",
        "zero_depth",
        "missing_quantity",
        "missing_notional",
        "partial_volume_bucket",
        "incomplete_volume_bucket",
        "duplicate_volume_timestamp",
        "foreign_token_perp_future",
        "missing_fx",
        "non_usd_fx",
        "out_of_order",
        "no_event_anchor",
        "no_pre_event_books",
        "scalar_coverage",
        "nested_levels",
        "inadequate_walk",
        "unknown_precision",
    ],
)
async def test_native_liquidity_complete_original_output(
    native_client, original_truth, case, record_property
):
    snapshot, spot, relative = _packet()
    if case == "scientific_book_and_flow":
        for book in snapshot["books"]:
            book["mid"] = "1e2"
            book["bids"] = [["9.9e1", "5e0"], {"price": "9.8e1", "qty": "1e3"}]
            book["asks"] = [["1.01e2", "5e0"], {"price": "1.02e2", "qty": "1e3"}]
            for side in ("bid", "ask"):
                for unit, value in book["depth"][side]["p1"].items():
                    book["depth"][side]["p1"][unit] = format(Decimal(value), "E")
        for row in snapshot["flow_bars"]:
            row.update(buy_notional="1e2", sell_notional="2e1", buy_qty="1e0", sell_qty="2e-1")
    elif case == "no_books":
        snapshot["books"] = []
    elif case == "no_flows":
        snapshot["flow_bars"] = []
    elif case == "bad_integrity":
        for book in snapshot["books"]:
            book["integrity_ok"] = False
    elif case == "partial_ask":
        snapshot["books"][1]["range_covered_pct"]["ask"] = 0.5
    elif case == "missing_usd_depth":
        snapshot["books"][1]["depth"]["ask"]["p1"]["usd"] = None
    elif case == "missing_native_depth":
        snapshot["books"][1]["depth"]["ask"]["p1"]["native"] = None
    elif case == "zero_depth":
        for book in snapshot["books"]:
            for side in ("bid", "ask"):
                book["depth"][side]["p1"] = {"usd": "0", "native": "0"}
    elif case == "missing_quantity":
        snapshot["flow_bars"][0]["buy_qty"] = None
    elif case == "missing_notional":
        snapshot["flow_bars"][0]["sell_notional"] = None
    elif case == "partial_volume_bucket":
        snapshot["flow_bars"].pop(0)
    elif case == "incomplete_volume_bucket":
        snapshot["flow_bars"][0]["complete"] = False
    elif case == "duplicate_volume_timestamp":
        snapshot["flow_bars"].append(deepcopy(snapshot["flow_bars"][0]))
    elif case == "foreign_token_perp_future":
        for changes in (
            {"token_id": "OTHER"},
            {"market_type": "PERP_LINEAR"},
            {"exchange_ts": "2026-10-16T12:00:00.000001Z"},
            {"integrity_ok": "true"},
        ):
            snapshot["books"].append({**deepcopy(snapshot["books"][1]), **changes})
    elif case in ("missing_fx", "non_usd_fx"):
        snapshot["books"][1]["quote_asset"] = "EUR"
        if case == "non_usd_fx":
            snapshot["books"][1]["quote_usd_rate"] = "1.25"
    elif case == "out_of_order":
        snapshot["books"] = list(reversed(snapshot["books"]))
    elif case == "no_event_anchor":
        snapshot["_event_anchor"] = None
    elif case == "no_pre_event_books":
        snapshot["books"] = [snapshot["books"][1], snapshot["books"][3]]
    elif case == "scalar_coverage":
        for book in snapshot["books"]:
            book["range_covered_pct"] = 2
    elif case == "nested_levels":
        for book in snapshot["books"]:
            book["levels"] = {"asks": book.pop("asks"), "bids": book.pop("bids")}
    elif case == "inadequate_walk":
        snapshot["books"][1]["asks"] = [["101", "0.00001"]]
    elif case == "unknown_precision":
        for book in snapshot["books"]:
            book.pop("exchange_ts_prec")
    record_property("original_commit", ORIGINAL_COMMIT)
    await _compare(native_client, original_truth, snapshot, spot, relative)


@pytest.mark.parametrize("window", ["W15M", "W1H", "W4H", "W24H", "EVENT"])
async def test_native_liquidity_window_and_pre_event_anchor_are_independent(
    native_client, original_truth, window
):
    snapshot, spot, relative = _packet()
    end = parse_time(snapshot["cutoff"])
    seconds = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400, "EVENT": 2700}
    snapshot["_primary"] = window
    snapshot["_start"] = iso(end - timedelta(seconds=seconds[window]))
    relative["windows"][window] = relative["windows"]["W15M"]
    await _compare(native_client, original_truth, snapshot, spot, relative)


@pytest.mark.parametrize("side", ["bid", "ask"])
@pytest.mark.parametrize("notional", ["0.000000001", "1", "495", "496", "1000000"])
async def test_native_walk_consumes_actual_levels_on_both_sides(
    native_client, original_truth, side, notional
):
    snapshot, spot, relative = _packet()
    book = snapshot["books"][1]
    levels = book["asks" if side == "ask" else "bids"]
    expected = original_truth.walk_book(levels, notional, book["mid"], side=side)
    _, parameters = query(snapshot, spot=spot, relative=relative)
    parameters.update(
        {
            "lq_test_book": (canonical_json(book), "String"),
            "lq_test_notional": (notional, "String"),
            "lq_test_side": (side, "String"),
        }
    )
    statement = (
        "WITH " + ctes() + " SELECT lq_walk_book({lq_test_book:String}, "
        "lq_dec({lq_test_notional:String}), {lq_test_side:String}) AS value"
    )
    rows = await native_client.rows(statement, parameters)
    assert len(rows) == 1
    if expected is None:
        assert rows[0]["value"] is None
    else:
        assert rows[0]["value"] == pytest.approx(expected, rel=1e-10, abs=1e-12)


@pytest.mark.parametrize("net_sign", [-1, 1])
@pytest.mark.parametrize("offset", ["-0.000000000000000000000001", "0", "0.000000000000000000000001"])
async def test_native_absorption_exact_financial_boundary(native_client, original_truth, net_sign, offset):
    snapshot, spot, relative = _packet()
    snapshot["_start"] = "2026-10-16T11:59:00Z"
    spot["venues"] = [{"venue": "B"}]
    snapshot["books"] = snapshot["books"][:2]
    for row in snapshot["flow_bars"]:
        row["buy_qty"] = row["sell_qty"] = None
    with localcontext() as context:
        context.prec = 100
        depth = Decimal("300" if net_sign > 0 else "200")
        flow = depth + Decimal(offset)
    final = snapshot["flow_bars"][14]
    final["buy_notional"] = flow if net_sign > 0 else "0"
    final["sell_notional"] = flow if net_sign < 0 else "0"
    relative["windows"]["W15M"]["r_raw"] = 0.005 * net_sign
    actual = await _compare(native_client, original_truth, snapshot, spot, relative)
    assert actual["elasticity"]["depth_unit"] == "USD"
    assert actual["elasticity"]["weak_response"] is (Decimal(offset) >= 0)
    assert actual["elasticity"]["hit_side"] == ("ask" if net_sign > 0 else "bid")


@pytest.mark.parametrize("refill_seconds", [59, 60, 61])
@pytest.mark.parametrize("future_low", ["98", "99", None])
async def test_native_replenishment_and_resilience_cutoff_boundaries(
    native_client, original_truth, refill_seconds, future_low
):
    snapshot, spot, relative = _packet()
    start = parse_time(snapshot["_start"])
    spot["venues"] = [{"venue": "B"}]
    snapshot["books"] = [
        _book("B", start - timedelta(seconds=10), bid="100"),
        _book("B", start + timedelta(seconds=refill_seconds), bid="100"),
    ]
    snapshot["flow_bars"] = snapshot["flow_bars"][:15]
    for index, row in enumerate(snapshot["flow_bars"]):
        row["buy_notional"] = "0"
        row["sell_notional"] = "100" if index == 0 else "0"
        row["low"] = "99" if index == 0 else future_low
    actual = await _compare(
        native_client,
        original_truth,
        snapshot,
        spot,
        relative,
        {"resilience_window_min": 14},
    )
    observation = actual["replenishment"]["observations"][0]
    assert observation["replenished"] is (refill_seconds <= 60)
    assert observation["resilient"] is (None if future_low is None else Decimal(future_low) >= Decimal("99"))


async def test_native_fractional_boundary_aggregate_is_not_full_minute(native_client, original_truth):
    snapshot, spot, relative = _packet()
    snapshot["_start"] = "2026-10-16T11:59:30.123456Z"
    snapshot["cutoff"] = "2026-10-16T12:00:00.123456Z"
    spot["venues"] = [
        {
            "venue": "B",
            "boundary_segments": [
                {
                    "aggregates": [
                        {
                            "start": snapshot["_start"],
                            "end": snapshot["cutoff"],
                            "buy_qty": "2",
                            "sell_qty": "0",
                            "buy_notional": "200",
                            "sell_notional": "0",
                        }
                    ]
                }
            ],
        }
    ]
    actual = await _compare(native_client, original_truth, snapshot, spot, relative)
    assert Decimal(actual["elasticity"]["net_flow"]) == Decimal("2")


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "1" + "0" * 24, "0." + "0" * 24 + "1"])
async def test_native_liquidity_refuses_outside_financial_admission(native_client, bad_value):
    snapshot, spot, relative = _packet()
    snapshot["books"][1]["depth"]["ask"]["p1"]["usd"] = bad_value
    statement, parameters = query(snapshot, spot=spot, relative=relative)
    with pytest.raises(RuntimeError, match="ClickHouse"):
        await native_client.rows(statement, parameters)


def _enrichment_inputs(original_truth, snapshot):
    for row in snapshot["flow_bars"]:
        row.setdefault("sum_sq_qty", "1")
    snapshot.setdefault("instruments", [])
    snapshot.setdefault("klines", [])
    snapshot.setdefault("funding", [])
    snapshot.setdefault("_has_event", False)
    snapshot.setdefault("_lead_start", snapshot["_start"])
    start = parse_time(snapshot["_start"])
    end = parse_time(snapshot["cutoff"])
    spot = original_truth.spot_truth(
        snapshot["flow_bars"],
        [],
        snapshot["instruments"],
        start,
        end,
        flow_segments=original_truth.flow_segments(snapshot),
        assembled_at=snapshot.get("assembled_at"),
    )
    spot["composite_cohort"] = spot["composite_max"]
    relative = {
        "windows": {
            "W15M": {
                "r_raw": 0.005,
                "beyond_normal": False,
                "normal_range": [-0.01, 0.01],
            }
        },
        "ref_venue": "B:spot:T",
        "context_inputs": {"rv24": None, "rv_normal_range": None},
    }
    relative["windows"][snapshot["_primary"]] = relative["windows"]["W15M"]
    deriv = {"per_venue": [], "funding_dispersion_8h": None}
    return spot, relative, deriv


def _original_enriched(original_truth, snapshot, spot, relative, deriv):
    start = parse_time(snapshot["_start"])
    end = parse_time(snapshot["cutoff"])
    name = snapshot["_primary"]
    expected = original_truth.liquidity_truth(snapshot, start, end, spot, relative)
    timing = {"window": name, "start": iso(start), "cutoff": iso(end)}
    expected.update(timing)
    expected["elasticity"].update(timing)
    expected["replenishment"].update(timing)
    if expected["per_venue"]:
        expected["coverage"] = {"venues": [v["venue"] for v in expected["per_venue"]]}
        expected["observed_at"] = min(v["observed_at"] for v in expected["per_venue"])
    spot_windows = {name: deepcopy(spot)}
    if snapshot.get("_has_event") and name != "EVENT":
        spot_windows["EVENT"] = original_truth.spot_truth(
            snapshot["flow_bars"],
            [],
            snapshot.get("instruments", []),
            parse_time(snapshot["_event_anchor"]),
            end,
            flow_segments=original_truth.flow_segments(snapshot),
            assembled_at=snapshot.get("assembled_at"),
        )
    original_truth.enrich_evidence(
        snapshot, start, end, spot_windows, name, deepcopy(deriv), relative, expected
    )
    if name == "EVENT" and snapshot.get("_event_anchor") is None:
        return {"window": "EVENT", "start": None, "cutoff": iso(end), "reason": "INCOMPLETE"}
    return expected


@pytest.mark.parametrize(
    "case",
    [
        "ordinary",
        "empty",
        "persistent_gap",
        "rest_not_independent",
        "gap_closed",
        "cadence_hole",
        "cross_quote_missing_fx",
        "cross_quote_same_usd_price",
        "cross_quote_dislocated",
        "future_split_book",
        "volatility_history",
        "projected_volatility_history",
        "top_sign_flips",
        "top_sign_incomplete",
        "funding_validation",
        "funding_late_confirmation",
    ],
)
async def test_native_enriched_liquidity_complete_frozen_output(native_client, original_truth, case):
    snapshot, _, _ = _packet()
    start = parse_time(snapshot["_start"])
    end = parse_time(snapshot["cutoff"])
    config = {"history_min_days": 1}
    if case == "empty":
        snapshot["books"] = []
        snapshot["flow_bars"] = []
    elif case in {
        "persistent_gap",
        "rest_not_independent",
        "gap_closed",
        "cadence_hole",
        "cross_quote_missing_fx",
        "cross_quote_same_usd_price",
        "cross_quote_dislocated",
    }:
        snapshot["books"] = []
        for seconds in range(0, 901, 10):
            stamp = start + timedelta(seconds=seconds)
            for venue in ("A", "B"):
                book = _book(venue, stamp)
                book["mid"] = "101" if venue == "B" else "100"
                if case == "gap_closed" and seconds == 900:
                    book["mid"] = "100"
                if case.startswith("cross_quote") and venue == "B":
                    book["quote_asset"] = "EUR"
                    book["quote_usd_rate"] = None if case == "cross_quote_missing_fx" else "2"
                    if case == "cross_quote_same_usd_price":
                        book["mid"] = "50"
                book["second_path_confirmed"] = case != "rest_not_independent"
                book["independent_rest"] = {
                    "source": "PUBLIC_REST",
                    "ts": iso(stamp),
                    "mid": book["mid"],
                    "spread_bps": "2",
                }
                if not (case == "cadence_hole" and seconds == 450 and venue == "B"):
                    snapshot["books"].append(book)
    elif case == "future_split_book":
        future = _book("A", end + timedelta(seconds=1), ask="10000")
        future["asks"] = [["105", "1000000"]]
        snapshot["books"].append(future)
    elif case in {"volatility_history", "projected_volatility_history"}:
        points = []
        for hour in range(-72, 1):
            stamp = end + timedelta(hours=hour)
            value = 100 + (hour % 7) * 0.25 + (hour % 3) * 0.125
            points.append((stamp, value))
        if case == "volatility_history":
            snapshot["klines"] = [
                {
                    "instrument_id": "B:spot:T",
                    "open_time": iso(stamp - timedelta(hours=1)),
                    "interval_s": 3600,
                    "complete": True,
                    "c": value,
                }
                for stamp, value in points
            ]
        else:
            snapshot["stat_price_series"] = {
                "B:spot:T": {
                    "time_basis": "CLOSE_TIME",
                    "times_us": [int(stamp.timestamp()) * 1000000 for stamp, _ in points],
                    "values": [value for _, value in points],
                }
            }
    elif case in {"top_sign_flips", "top_sign_incomplete"}:
        start = end - timedelta(minutes=45)
        snapshot["_start"] = iso(start)
        snapshot["flow_bars"] = []
        for venue in ("A", "B"):
            for minute in range(45):
                positive = (minute // 15) % 2 == 0
                snapshot["flow_bars"].append(
                    {
                        "instrument_id": f"{venue}:spot:T",
                        "minute_start": iso(start + timedelta(minutes=minute)),
                        "complete": not (case == "top_sign_incomplete" and minute == 20 and venue == "A"),
                        "buy_qty": "10" if positive else "1",
                        "sell_qty": "1" if positive else "10",
                        "buy_notional": "1000" if positive else "100",
                        "sell_notional": "100" if positive else "1000",
                        "sum_sq_qty": "1",
                    }
                )
    spot, relative, deriv = _enrichment_inputs(original_truth, snapshot)
    if case in {"funding_validation", "funding_late_confirmation"}:
        deriv = {"per_venue": [], "funding_dispersion_8h": Decimal("0.1")}
        snapshot["assembled_at"] = snapshot["cutoff"]
        snapshot["funding_confirmations"] = []
        for venue, rate in (("P", "0"), ("Q", "0.1")):
            iid = f"{venue}:perp:T"
            deriv["per_venue"].append({"instrument_id": iid, "funding_8h_eq": rate})
            for hours in (48, 24, 1):
                snapshot["funding"].append(
                    {
                        "instrument_id": iid,
                        "kind": "SETTLED",
                        "ts": iso(start - timedelta(hours=hours)),
                        "rate": "0",
                        "interval_h": "8",
                    }
                )
            snapshot["funding_confirmations"].append(
                {
                    "instrument_id": iid,
                    "ts": snapshot["cutoff"],
                    "received_ts": iso(
                        end + timedelta(seconds=1 if case == "funding_late_confirmation" else 0)
                    ),
                    "rate_8h_eq": rate,
                    "independent_path_verified": True,
                    "source_path": "REST",
                    "primary_source_path": "WS",
                }
            )
            for minute in range(15):
                snapshot["flow_bars"].append(
                    {
                        "instrument_id": iid,
                        "minute_start": iso(end - timedelta(minutes=minute + 1)),
                        "complete": True,
                    }
                )
    token = original_truth._PARAMETERS.set(config)
    try:
        expected = _original_enriched(original_truth, snapshot, spot, relative, deriv)
    finally:
        original_truth._PARAMETERS.reset(token)
    statement, parameters = window_query(snapshot, config, spot=spot, relative=relative, deriv=deriv)
    rows = await native_client.rows(statement, parameters)
    assert len(rows) == 1
    actual = json.loads(rows[0]["result"])
    _assert_complete(actual, expected)
    if case == "persistent_gap":
        assert actual["fragmentation"]["price_path_validated"] is True
    elif case == "funding_validation":
        assert actual["fragmentation"]["funding_path_validated"] is True
    elif case == "funding_late_confirmation":
        assert actual["fragmentation"]["funding_path_validated"] is False
    elif case == "top_sign_flips":
        assert actual["fragmentation"]["top_sign_flips"] is True


@pytest.mark.parametrize("factor", [1, 1000, 1000000])
async def test_native_book_numeric_timestamp_units_preserve_observation(
    native_client, original_truth, factor
):
    snapshot, spot, relative = _packet()
    for book in snapshot["books"]:
        book["exchange_ts"] = int(parse_time(book["exchange_ts"]).timestamp()) * factor
    await _compare(native_client, original_truth, snapshot, spot, relative)


@pytest.mark.parametrize("band", [0.5, 1, 2])
async def test_native_depth_band_uses_only_covered_native_and_usd_fields(native_client, original_truth, band):
    snapshot, spot, relative = _packet()
    key = {0.5: "p0_5", 1: "p1", 2: "p2"}[band]
    for book in snapshot["books"]:
        for side in ("bid", "ask"):
            book["depth"][side][key] = deepcopy(book["depth"][side]["p1"])
    snapshot["books"][1]["range_covered_pct"]["ask"] = band / 2
    await _compare(native_client, original_truth, snapshot, spot, relative, {"depth_band_pct": band})


@pytest.mark.parametrize("offset", ["-0.000000000000000000000001", "0", "0.000000000000000000000001"])
async def test_native_vacuum_keeps_strict_depth_majority_before_float_display(
    native_client, original_truth, offset
):
    snapshot, spot, relative = _packet()
    for row in snapshot["flow_bars"]:
        row.update(buy_qty=None, sell_qty=None, buy_notional="1", sell_notional="0")
    with localcontext() as context:
        context.prec = 100
        snapshot["books"][1]["depth"]["ask"]["p1"]["usd"] = Decimal("300") + Decimal(offset)
    relative["windows"]["W15M"] = {"r_raw": 0.05, "beyond_normal": True}
    actual = await _compare(native_client, original_truth, snapshot, spot, relative)
    assert actual["depth_majority_lost"] is (Decimal(offset) < 0)
    assert (actual["liquidity_state"] == "VACUUM") is (Decimal(offset) < 0)


@pytest.mark.parametrize(
    ("canonical", "alias", "size"),
    [("EXACT", "EXACT", None), ("BUCKET_1M", "BUCKET(60s)", None), ("BUCKET", None, 60)],
)
async def test_native_book_precision_is_propagated_not_inferred(
    native_client, original_truth, canonical, alias, size
):
    snapshot, spot, relative = _packet()
    for book in snapshot["books"]:
        book["exchange_ts_prec"] = canonical
        if alias is not None:
            book["exchange_ts_precision"] = alias
        if size is not None:
            book["exchange_ts_bucket_size_s"] = size
    await _compare(native_client, original_truth, snapshot, spot, relative)


async def test_native_conflicting_precision_is_refused(native_client):
    snapshot, spot, relative = _packet()
    snapshot["books"][1]["exchange_ts_precision"] = "BUCKET(1s)"
    statement, parameters = query(snapshot, spot=spot, relative=relative)
    with pytest.raises(RuntimeError, match="ClickHouse"):
        await native_client.rows(statement, parameters)


@pytest.mark.parametrize("has_event", [False, True])
async def test_native_all_liquidity_windows_are_one_select(native_client, original_truth, has_event):
    snapshot, _, _ = _packet()
    snapshot["_primary"] = "W4H"
    snapshot["anchor"] = {"ts": snapshot["_start"]}
    snapshot["_has_event"] = has_event
    if not has_event:
        snapshot["_event_anchor"] = None
    _, relative, _ = _enrichment_inputs(original_truth, snapshot)
    end = parse_time(snapshot["cutoff"])
    spots = {}
    derivatives = {}
    expected_windows = {}
    for name, seconds in {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400, "EVENT": 900}.items():
        scoped = deepcopy(snapshot)
        scoped["_primary"] = name
        scoped["_start"] = iso(end - timedelta(seconds=seconds))
        spot, scoped_relative, deriv = _enrichment_inputs(original_truth, scoped)
        relative["windows"][name] = scoped_relative["windows"][name]
        relative["windows"]["EVENT"] = relative["windows"]["W15M"]
        spots[name] = spot
        derivatives[name] = deriv
        expected_windows[name] = _original_enriched(original_truth, scoped, spot, relative, deriv)
    statement, parameters = windows_query(snapshot, spots=spots, relative=relative, deriv=derivatives)
    rows = await native_client.rows(statement, parameters)
    assert len(rows) == 1
    actual = json.loads(rows[0]["result"])
    expected = dict(expected_windows["W4H"], windows=expected_windows)
    _assert_complete(actual, expected)


def test_liquidity_reducers_bind_consumed_array_graph_once():
    statement = ctes()
    for name in ("lq_sum", "lq_mean", "lq_float_mean"):
        declaration = statement.split(f" AS {name},", 1)[0].rsplit("\n(values ->", 1)[1]
        assert declaration.startswith(" arrayMap(items ->")
        assert declaration.endswith(", [values])[1])")
        assert declaration.count("values") == 1


@pytest.mark.parametrize("reducer", ["lq_sum", "lq_mean", "lq_float_mean"])
@pytest.mark.parametrize("case", ["empty", "complete", "missing", "zero", "duplicates"])
async def test_native_typed_liquidity_reducer_preserves_population(native_client, reducer, case):
    populations = {
        "empty": [],
        "complete": ["1", "2", "6"],
        "missing": ["1", None, "6"],
        "zero": ["0"],
        "duplicates": ["1", "1", "6"],
    }
    values = populations[case]
    _, parameters = query({})
    parameters["reducer_values"] = (canonical_json(values), "String")
    if reducer == "lq_float_mean":
        expression = "arrayMap(x -> lq_number(x), JSONExtractArrayRaw({reducer_values:String}))"
        expected = (
            sum(float(value) if value is not None else 0.0 for value in values) / len(values)
            if values
            else None
        )
        selection = f"{reducer}({expression})"
    else:
        expression = "arrayMap(x -> lq_dec(x), JSONExtractArrayRaw({reducer_values:String}))"
        with localcontext() as context:
            context.prec = 100
            expected = (
                sum((Decimal(value) for value in values), Decimal(0))
                if values and None not in values
                else None
            )
            if expected is not None and reducer == "lq_mean":
                expected /= len(values)
        selection = f"toString({reducer}({expression}))"
    rows = await native_client.rows("WITH " + ctes() + f" SELECT {selection} AS value", parameters)
    assert len(rows) == 1
    _assert_complete(rows[0]["value"], expected)


async def test_native_liquidity_absence_relational_phase_contracts(native_client, original_truth, request):
    snapshot, spot, relative = _packet()
    snapshot["_event_anchor"] = None
    token = original_truth._PARAMETERS.set({})
    try:
        expected = original_truth.liquidity_truth(
            deepcopy(snapshot),
            parse_time(snapshot["_start"]),
            parse_time(snapshot["cutoff"]),
            deepcopy(spot),
            deepcopy(relative),
        )
    finally:
        original_truth._PARAMETERS.reset(token)
    _, parameters = query(snapshot, spot=spot, relative=relative)
    statements = (
        ("volume_population", "SELECT length(lq_volume_flows) AS value FROM lq_volume_population", 30),
        ("volume_coverage", "SELECT length(lq_covered_buckets) AS value FROM lq_volume_coverage", 1),
        (
            "volume_totals",
            "SELECT arrayMap(x -> toString(x), lq_volumes) AS value FROM lq_volume_totals",
            [Decimal("3600")],
        ),
        (
            "tier_values",
            "SELECT arrayMap(x -> toString(x), lq_tiers) AS value FROM lq_tier_values",
            [Decimal("72"), Decimal("720"), Decimal("3600")],
        ),
        (
            "venue_stats",
            "SELECT arraySort(groupArray(tuple(venue, pre_spread))) AS value FROM lq_venue_stats",
            [["A", None], ["B", None]],
        ),
        (
            "venue_depth",
            "SELECT arraySort(groupArray(tuple(venue, toString(bid), toString(ask), "
            "toString(current_total), spread))) AS value FROM lq_venue_stats",
            [
                [
                    v["venue"],
                    v["bid_depth_p1_usd"],
                    v["ask_depth_p1_usd"],
                    v["depth_p1_usd"],
                    v["spread_bps"],
                ]
                for v in sorted(expected["per_venue"], key=lambda v: v["venue"])
            ],
        ),
        (
            "venue_precision",
            "SELECT arraySort(groupArray(tuple(venue, lq_precision(current.3)))) AS value FROM lq_venue_stats",
            [[v["venue"], v["precision"]] for v in sorted(expected["per_venue"], key=lambda v: v["venue"])],
        ),
        *(
            (
                phase,
                f"SELECT arraySort(groupArray(tuple(venue, lq_walk_book(current.3, lq_tiers[{index}], 'ask')))) "
                "AS value FROM lq_venue_stats",
                [
                    [v["venue"], v["slippage_bps"][tier]]
                    for v in sorted(expected["per_venue"], key=lambda v: v["venue"])
                ],
            )
            for index, tier, phase in (
                (1, "T1", "venue_walk_t1"),
                (2, "T2", "venue_walk_t2"),
                (3, "T3", "venue_walk_t3"),
            )
        ),
        (
            "venue_output",
            "SELECT arraySort(x -> JSONExtractString(x, 'venue'), groupArray(venue_json)) AS value FROM lq_venues",
            sorted(expected["per_venue"], key=lambda v: v["venue"]),
        ),
        (
            "burst_output",
            "SELECT observations AS value FROM lq_observed",
            expected["replenishment"]["observations"],
        ),
        ("state", "SELECT toString(lq_net) AS value FROM lq_state", expected["elasticity"]["net_flow"]),
    )
    for phase, selection, reference in statements:
        native_client.nodeid = request.node.nodeid + "::" + phase
        retain_liquidity_phase(
            Path(__file__).resolve().parents[1] / ".test-results",
            request.node.nodeid,
            phase,
            status="STARTED",
        )
        rows = await native_client.rows("WITH " + ctes() + " " + selection, parameters)
        assert len(rows) == 1
        actual = rows[0]["value"]
        if phase in {"venue_output", "burst_output"}:
            actual = [json.loads(value) for value in actual]
        _assert_complete(actual, reference)
        retain_liquidity_phase(
            Path(__file__).resolve().parents[1] / ".test-results", request.node.nodeid, phase
        )


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "all_filtered",
        "other_token_malformed",
        "nonspot_malformed",
        "unused_timestamp_alternates",
        "null_timestamp_alternates",
        "interleaved_equal_time_duplicates",
        "uncovered_bad_money",
        "old_bad_precision",
        "late_base_receipt",
        "null_anchor",
        "previous_half_open",
        "unused_malformed_walk_tail",
    ],
)
async def test_native_relational_root_complete_frozen_controls(native_client, original_truth, case):
    snapshot, spot, relative = _packet()
    end = parse_time(snapshot["cutoff"])
    start = parse_time(snapshot["_start"])
    if case == "empty":
        snapshot["books"] = []
    elif case == "all_filtered":
        for book in snapshot["books"]:
            book["integrity_ok"] = False
    elif case in {"other_token_malformed", "nonspot_malformed"}:
        book = _book("X", end)
        book.update(exchange_ts="not-a-time", mid="NaN", exchange_ts_prec="INVALID")
        if case == "other_token_malformed":
            book["token_id"] = "OTHER"
        else:
            book["market_type"] = "PERP_LINEAR"
        snapshot["books"].insert(0, book)
    elif case == "unused_timestamp_alternates":
        for book in snapshot["books"]:
            book.update(ts="not-a-time", as_of="1e999", observed_at="invalid")
    elif case == "null_timestamp_alternates":
        for book in snapshot["books"]:
            book.update(minute_start=None, open_time=None)
    elif case == "interleaved_equal_time_duplicates":
        b0, b1, a0, a1 = snapshot["books"]
        last = deepcopy(b1)
        last["depth"]["bid"]["p1"]["usd"] = "777"
        snapshot["books"] = [b0, a0, b1, a1, deepcopy(b1), last]
    elif case == "uncovered_bad_money":
        snapshot["books"][1]["range_covered_pct"]["ask"] = 0
        snapshot["books"][1]["depth"]["ask"]["p1"] = {"usd": "NaN", "native": "NaN"}
    elif case == "old_bad_precision":
        snapshot["books"][0]["exchange_ts_prec"] = "INVALID"
    elif case == "late_base_receipt":
        snapshot["assembled_at"] = snapshot["cutoff"]
        for book in snapshot["books"]:
            book["received_ts"] = iso(end + timedelta(days=1))
    elif case == "null_anchor":
        snapshot["_event_anchor"] = None
    elif case == "previous_half_open":
        snapshot["books"] = [
            _book("B", start - timedelta(hours=1), bid="100", ask="200"),
            _book("B", start - timedelta(hours=1, microseconds=1), bid="900", ask="900"),
            _book("B", start, bid="800", ask="800"),
            _book("B", end),
        ]
    elif case == "unused_malformed_walk_tail":
        for book in snapshot["books"]:
            book["asks"] = [["101", "1000000"], ["NaN", "invalid"]]
    actual = await _compare(native_client, original_truth, snapshot, spot, relative)
    if case in {"empty", "all_filtered"}:
        assert actual["per_venue"] == []
        assert actual["aggregate_depth_p1_usd"] is None
        assert actual["replenishment"]["available"] is False
    elif case == "interleaved_equal_time_duplicates":
        assert [row["venue"] for row in actual["per_venue"]] == ["B", "A"]
        assert Decimal(actual["per_venue"][0]["bid_depth_p1_usd"]) == Decimal("777")
    elif case == "null_anchor":
        assert actual["pre_event_depth_window"] == {
            "start": None,
            "cutoff": None,
            "reason": "INCOMPLETE",
        }
    elif case == "previous_half_open":
        assert Decimal(actual["pre_event_depth_p1_usd"]) == Decimal("300")
    elif case == "late_base_receipt":
        assert [row["venue"] for row in actual["per_venue"]] == ["B", "A"]


@pytest.mark.parametrize(
    "case", ["first_malformed", "before_integrity", "missing_time", "consumed_walk", "current_precision"]
)
async def test_native_relational_root_demanded_refusals(native_client, original_truth, case):
    from decimal import InvalidOperation

    snapshot, spot, relative = _packet()
    book = snapshot["books"][1]
    if case == "consumed_walk":
        book["asks"] = [["invalid", "5"], ["101", "1000000"]]
    elif case == "current_precision":
        book["exchange_ts_prec"] = "INVALID"
    elif case == "missing_time":
        del book["exchange_ts"]
    else:
        book["minute_start"] = "not-a-time"
        if case == "before_integrity":
            book["integrity_ok"] = False
    token = original_truth._PARAMETERS.set({})
    expected_error = (
        InvalidOperation if case == "consumed_walk" else TypeError if case == "missing_time" else ValueError
    )
    try:
        with pytest.raises(expected_error):
            original_truth.liquidity_truth(
                deepcopy(snapshot),
                parse_time(snapshot["_start"]),
                parse_time(snapshot["cutoff"]),
                deepcopy(spot),
                deepcopy(relative),
            )
    finally:
        original_truth._PARAMETERS.reset(token)
    statement, parameters = query(snapshot, spot=spot, relative=relative)
    refusal = (
        "Market Truth decimal input outside native financial range"
        if case == "consumed_walk"
        else "Unsupported timestamp precision"
        if case == "current_precision"
        else "Invalid demanded observation timestamp"
    )
    with pytest.raises(RuntimeError, match=refusal):
        await native_client.rows(statement, parameters)


@pytest.mark.parametrize("case", ["complete", "no_books", "all_filtered"])
async def test_native_root_spine_forwards_flow_tiers_and_empty_context(native_client, original_truth, case):
    snapshot, spot, relative = _packet()
    snapshot["_event_anchor"] = None
    if case == "no_books":
        snapshot["books"] = []
    elif case == "all_filtered":
        for book in snapshot["books"]:
            book["integrity_ok"] = False
    await _compare(native_client, original_truth, snapshot, spot, relative)
    _, parameters = query(snapshot, spot=spot, relative=relative)
    rows = await native_client.rows(
        "WITH " + ctes() + " SELECT first_ordinal, length(series) AS books, "
        "length(ctx_flows) AS flows, length(venue_flows) AS venue_flows, "
        "arrayMap(x -> toString(x), lq_tiers) AS tiers, lq_iso(ctx_anchor) AS anchor, "
        "lq_iso(ctx_start) AS start, lq_iso(ctx_end) AS cutoff "
        "FROM lq_root_inputs ORDER BY first_ordinal",
        parameters,
    )
    _assert_complete(
        rows,
        [
            {
                "first_ordinal": ordinal,
                "books": 2 if case == "complete" else 0,
                "flows": 30,
                "venue_flows": 15 if case == "complete" else 0,
                "tiers": [Decimal("72"), Decimal("720"), Decimal("3600")],
                "anchor": None,
                "start": snapshot["_start"],
                "cutoff": snapshot["cutoff"],
            }
            for ordinal in ([1, 3] if case == "complete" else [0])
        ],
    )


async def test_native_relational_root_filtered_ordinals_and_duplicate_series(native_client, original_truth):
    snapshot, spot, relative = _packet()
    b0, b1, a0, a1 = snapshot["books"]
    foreign = dict(b0, token_id="OTHER", exchange_ts="invalid")
    snapshot["books"] = [foreign, b0, a0, b1, deepcopy(b1), a1]
    assert original_truth._observation_time(b0) == parse_time("2026-10-16T11:15:00Z")
    _, parameters = query(snapshot, spot=spot, relative=relative)
    rows = await native_client.rows(
        "WITH " + ctes() + " SELECT arraySort(x -> x.2, groupArray(tuple(venue, first_ordinal, "
        "arrayMap(book -> tuple(lq_iso(book.1), book.2), series)))) AS value FROM lq_venue_books",
        parameters,
    )
    assert rows == [
        {
            "value": [
                [
                    "B",
                    1,
                    [
                        ["2026-10-16T11:15:00Z", 1],
                        ["2026-10-16T12:00:00Z", 3],
                        ["2026-10-16T12:00:00Z", 4],
                    ],
                ],
                ["A", 2, [["2026-10-16T11:15:00Z", 2], ["2026-10-16T12:00:00Z", 5]]],
            ]
        }
    ]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1792152000", "2026-10-16T12:00:00Z"),
        ("1792152000000", "2026-10-16T12:00:00Z"),
        ("1792152000000000", "2026-10-16T12:00:00Z"),
        ("1.792152e9", "2026-10-16T12:00:00Z"),
        ("1792152000.0000025", "2026-10-16T12:00:00.000002Z"),
        ("1792152000.0000035", "2026-10-16T12:00:00.000004Z"),
        ('"2026-10-16T14:00:00.123456+02:00"', "2026-10-16T12:00:00.123456Z"),
        ("100000000001", "1973-03-03T09:46:40.001000Z"),
        ("100000000000001", "1973-03-03T09:46:40.000001Z"),
    ],
)
async def test_native_relational_root_exact_timestamp_primitives(native_client, raw, expected):
    snapshot, spot, relative = _packet()
    snapshot["books"] = []
    _, parameters = query(snapshot, spot=spot, relative=relative)
    parameters["root_timestamp"] = (raw, "String")
    rows = await native_client.rows(
        "WITH " + ctes() + " SELECT lq_iso(lq_parse({root_timestamp:String})) AS value",
        parameters,
    )
    assert rows == [{"value": expected}]


@pytest.mark.parametrize(
    "field",
    [
        "minute_start",
        "open_time",
        "exchange_ts",
        "ts",
        "as_of",
        "settlement_time",
        "funding_time",
        "observed_at",
    ],
)
async def test_native_relational_root_first_present_timestamp(native_client, original_truth, field):
    snapshot, spot, relative = _packet()
    book = _book("B", parse_time(snapshot["cutoff"]))
    fields = [
        "minute_start",
        "open_time",
        "exchange_ts",
        "ts",
        "as_of",
        "settlement_time",
        "funding_time",
        "observed_at",
    ]
    for name in fields:
        book[name] = None if fields.index(name) < fields.index(field) else "not-a-time"
    book[field] = snapshot["cutoff"]
    snapshot["books"] = [book]
    assert original_truth._observation_time(book) == parse_time("2026-10-16T12:00:00Z")
    _, parameters = query(snapshot, spot=spot, relative=relative)
    rows = await native_client.rows(
        "WITH "
        + ctes()
        + " SELECT arrayMap(book -> tuple(lq_iso(book.1), book.2, book.4), ordered_books) AS value FROM lq_book_ordinals",
        parameters,
    )
    assert rows == [{"value": [["2026-10-16T12:00:00Z", 1, "B"]]}]


@pytest.mark.parametrize("offset_us", [-1, 0, 1])
async def test_native_relational_root_rest_receipt_boundary(native_client, original_truth, offset_us):
    snapshot, _, _ = _packet()
    snapshot["assembled_at"] = snapshot["cutoff"]
    start = parse_time(snapshot["_start"])
    end = parse_time(snapshot["cutoff"])
    snapshot["books"] = []
    for seconds in (0, 450, 900):
        stamp = start + timedelta(seconds=seconds)
        for venue in ("A", "B"):
            book = _book(venue, stamp)
            book["mid"] = "100" if venue == "A" else "101"
            book["received_ts"] = iso(end + timedelta(days=1))
            book["second_path_confirmed"] = True
            book["independent_rest"] = {
                "source": "PUBLIC_REST",
                "ts": iso(stamp),
                "received_ts": iso(end + timedelta(microseconds=offset_us)),
                "mid": book["mid"],
                "spread_bps": "2",
            }
            snapshot["books"].append(book)
    spot, relative, deriv = _enrichment_inputs(original_truth, snapshot)
    config = {"book_interval_s": 450}
    token = original_truth._PARAMETERS.set(config)
    try:
        expected = _original_enriched(original_truth, snapshot, spot, relative, deriv)
    finally:
        original_truth._PARAMETERS.reset(token)
    statement, parameters = window_query(snapshot, config, spot=spot, relative=relative, deriv=deriv)
    rows = await native_client.rows(statement, parameters)
    assert len(rows) == 1
    actual = json.loads(rows[0]["result"])
    _assert_complete(actual, expected)
    assert actual["fragmentation"]["price_path_validated"] is (offset_us <= 0)


@pytest.mark.parametrize(
    ("raw", "micros"),
    [
        ("100000000000", 100000000000000000),
        ("100000000001", 100000000001000),
        ("100000000000000", 100000000000000000),
        ("100000000000001", 100000000000001),
        ("-100000000001", -100000000001000),
    ],
)
async def test_native_relational_root_strict_numeric_time_thresholds(native_client, raw, micros):
    snapshot, spot, relative = _packet()
    _, parameters = query(snapshot, spot=spot, relative=relative)
    parameters["root_timestamp"] = (raw, "String")
    rows = await native_client.rows(
        "WITH " + ctes() + " SELECT toUnixTimestamp64Micro(lq_parse({root_timestamp:String})) AS value",
        parameters,
    )
    assert rows == [{"value": micros}]


@pytest.mark.parametrize("anchor", ["absent", "null"])
@pytest.mark.parametrize("selection", ["books", "spread", "depth"])
async def test_native_private_book_branch_isolates_filtered_flow_timestamp(
    native_client, original_truth, anchor, selection
):
    snapshot, spot, relative = _packet()
    if anchor == "absent":
        del snapshot["_event_anchor"]
    else:
        snapshot["_event_anchor"] = None
    token = original_truth._PARAMETERS.set({})
    try:
        expected = original_truth.liquidity_truth(
            deepcopy(snapshot),
            parse_time(snapshot["_start"]),
            parse_time(snapshot["cutoff"]),
            deepcopy(spot),
            deepcopy(relative),
        )
        snapshot["flow_bars"][0]["minute_start"] = "not-a-time"
        with pytest.raises(ValueError, match="Invalid isoformat string"):
            original_truth.liquidity_truth(
                deepcopy(snapshot),
                parse_time(snapshot["_start"]),
                parse_time(snapshot["cutoff"]),
                deepcopy(spot),
                deepcopy(relative),
            )
    finally:
        original_truth._PARAMETERS.reset(token)
    if selection == "books":
        selected = (
            "SELECT venue, first_ordinal, length(series) AS samples "
            "FROM lq_venue_books ORDER BY first_ordinal"
        )
        reference = [
            {"venue": "B", "first_ordinal": 1, "samples": 2},
            {"venue": "A", "first_ordinal": 3, "samples": 2},
        ]
    elif selection == "spread":
        selected = "SELECT venue, pre_spread FROM lq_venue_stats ORDER BY first_ordinal"
        reference = [
            {"venue": venue["venue"], "pre_spread": None if anchor == "null" else 2.0}
            for venue in expected["per_venue"]
        ]
    else:
        selected = (
            "SELECT venue, toString(bid) AS bid, toString(ask) AS ask, "
            "toString(current_total) AS total FROM lq_venue_stats ORDER BY first_ordinal"
        )
        reference = [
            {
                "venue": venue["venue"],
                "bid": venue["bid_depth_p1_usd"],
                "ask": venue["ask_depth_p1_usd"],
                "total": venue["depth_p1_usd"],
            }
            for venue in expected["per_venue"]
        ]
    _, parameters = query(snapshot, spot=spot, relative=relative)
    rows = await native_client.rows("WITH " + ctes() + " " + selected, parameters)
    assert len(rows) == 2
    _assert_complete(rows, reference)


@pytest.mark.parametrize("selection", ["full", "state", "burst", "full_no_books", "state_no_books"])
async def test_native_filtered_flow_timestamp_remains_demanded(native_client, original_truth, selection):
    snapshot, spot, relative = _packet()
    snapshot["_event_anchor"] = None
    snapshot["flow_bars"][0]["minute_start"] = "not-a-time"
    if selection.endswith("_no_books"):
        snapshot["books"] = []
    token = original_truth._PARAMETERS.set({})
    try:
        with pytest.raises(ValueError, match="Invalid isoformat string"):
            original_truth.liquidity_truth(
                deepcopy(snapshot),
                parse_time(snapshot["_start"]),
                parse_time(snapshot["cutoff"]),
                deepcopy(spot),
                deepcopy(relative),
            )
    finally:
        original_truth._PARAMETERS.reset(token)
    statement, parameters = query(snapshot, spot=spot, relative=relative)
    if selection in {"state", "state_no_books"}:
        statement = "WITH " + ctes() + " SELECT toString(lq_net) AS value FROM lq_state"
    elif selection == "burst":
        statement = "WITH " + ctes() + " SELECT observations AS value FROM lq_observed"
    with pytest.raises(RuntimeError, match="Invalid demanded observation timestamp"):
        await native_client.rows(statement, parameters)


@pytest.mark.parametrize("selection", ["population", "tiers"])
async def test_native_volume_demands_ineligible_spot_timestamp(native_client, original_truth, selection):
    snapshot, spot, relative = _packet()
    snapshot["_event_anchor"] = None
    snapshot["flow_bars"].append(
        dict(snapshot["flow_bars"][0], instrument_id="X:spot:T", minute_start="not-a-time")
    )
    token = original_truth._PARAMETERS.set({})
    try:
        with pytest.raises(ValueError, match="Invalid isoformat string"):
            original_truth.liquidity_truth(
                deepcopy(snapshot),
                parse_time(snapshot["_start"]),
                parse_time(snapshot["cutoff"]),
                deepcopy(spot),
                deepcopy(relative),
            )
    finally:
        original_truth._PARAMETERS.reset(token)
    _, parameters = query(snapshot, spot=spot, relative=relative)
    rows = await native_client.rows(
        "WITH " + ctes() + " SELECT length(ctx_flows) AS value FROM lq_flow_context",
        parameters,
    )
    assert rows == [{"value": 30}]
    selected = (
        "SELECT length(lq_volume_flows) AS value FROM lq_volume_population"
        if selection == "population"
        else "SELECT arrayMap(x -> toString(x), lq_tiers) AS value FROM lq_tier_values"
    )
    with pytest.raises(RuntimeError, match="Invalid demanded observation timestamp"):
        await native_client.rows("WITH " + ctes() + " " + selected, parameters)


@pytest.mark.parametrize("anchor", ["absent", "null"])
@pytest.mark.parametrize(
    "case", ["no_books", "all_filtered", "empty_eligible", "empty_flows", "boundary_only", "empty_all"]
)
async def test_native_separated_context_preserves_singleton_state(
    native_client, original_truth, anchor, case
):
    snapshot, spot, relative = _packet()
    if anchor == "absent":
        del snapshot["_event_anchor"]
    else:
        snapshot["_event_anchor"] = None
    if case in {"no_books", "boundary_only", "empty_all"}:
        snapshot["books"] = []
    elif case == "all_filtered":
        for book in snapshot["books"]:
            book["integrity_ok"] = False
    if case == "empty_eligible":
        spot["venues"] = []
    if case in {"empty_flows", "boundary_only", "empty_all"}:
        snapshot["flow_bars"] = []
    if case == "boundary_only":
        spot["venues"] = [
            {
                "venue": "B",
                "boundary_segments": [
                    {
                        "aggregates": [
                            {
                                "start": snapshot["_start"],
                                "end": snapshot["cutoff"],
                                "buy_qty": "2",
                                "sell_qty": "0",
                                "buy_notional": "200",
                                "sell_notional": "0",
                            }
                        ]
                    }
                ],
            }
        ]
    actual = await _compare(native_client, original_truth, snapshot, spot, relative)
    no_venues = case in {"no_books", "all_filtered", "boundary_only", "empty_all"}
    no_flow = case in {"empty_eligible", "empty_flows", "empty_all"}
    net = None if no_flow else Decimal("2") if case == "boundary_only" else Decimal("24")
    _assert_complete(actual["elasticity"]["net_flow"], net)
    assert actual["replenishment"]["available"] is False
    assert actual["replenishment"]["observations"] == []
    if no_venues:
        assert actual["per_venue"] == []
        assert actual["aggregate_depth_p1_usd"] is None
        assert actual["elasticity"]["hit_side_coverage_complete"] is False
        assert actual["elasticity"]["hit_side_depth_lower_bound"] is None
    else:
        assert [venue["venue"] for venue in actual["per_venue"]] == ["B", "A"]
    _, parameters = query(snapshot, spot=spot, relative=relative)
    rows = await native_client.rows(
        "WITH " + ctes() + " SELECT length(lq_venues_array) AS venues, "
        "length(lq_observations) AS observations, toString(lq_net) AS net, "
        "CAST(lq_replenishment_available, 'Bool') AS available, "
        "lq_iso(ctx_anchor) AS anchor FROM lq_state",
        parameters,
    )
    _assert_complete(
        rows,
        [
            {
                "venues": 0 if no_venues else 2,
                "observations": 0,
                "net": net,
                "available": False,
                "anchor": None if anchor == "null" else snapshot["_start"],
            }
        ],
    )


async def test_native_filtered_context_preserves_duplicates_and_boundary_append_order(
    native_client, original_truth
):
    snapshot, spot, relative = _packet()
    snapshot["_event_anchor"] = None
    snapshot["flow_bars"].append(deepcopy(snapshot["flow_bars"][0]))
    for venue, quantity in (("B", "2"), ("A", "3")):
        spot["venues"] = [v for v in spot["venues"] if v["venue"] != venue]
        spot["venues"].append(
            {
                "venue": venue,
                "boundary_segments": [
                    {
                        "aggregates": [
                            {
                                "start": snapshot["_start"],
                                "end": snapshot["cutoff"],
                                "buy_qty": quantity,
                                "sell_qty": "0",
                                "buy_notional": "200",
                                "sell_notional": "0",
                            }
                        ]
                    }
                ],
            }
        )
    await _compare(native_client, original_truth, snapshot, spot, relative)
    reference = [
        [original_truth._venue(row), iso(original_truth._observation_time(row)), row["buy_qty"]]
        for row in snapshot["flow_bars"]
    ] + [["B", snapshot["_start"], "2"], ["A", snapshot["_start"], "3"]]
    _, parameters = query(snapshot, spot=spot, relative=relative)
    rows = await native_client.rows(
        "WITH " + ctes() + " SELECT arrayMap(flow -> tuple(lq_venue(flow), "
        "lq_iso(lq_time(flow)), JSONExtractString(flow, 'buy_qty')), ctx_flows) "
        "AS value FROM lq_flow_context",
        parameters,
    )
    assert rows == [{"value": reference}]


@pytest.mark.parametrize("case", ["duplicate_bursts", "no_bursts", "no_books"])
async def test_native_canonical_root_keeps_one_venue_and_duplicate_burst_observations(
    native_client, original_truth, case
):
    snapshot, spot, relative = _packet()
    start = parse_time(snapshot["_start"])
    snapshot["books"] = [
        _book("B", start - timedelta(seconds=10), bid="100"),
        _book("A", start - timedelta(seconds=10), bid="100"),
        _book("B", start + timedelta(seconds=60), bid="100"),
        _book("A", start + timedelta(seconds=60), bid="100"),
    ]
    for index, row in enumerate(snapshot["flow_bars"]):
        row.update(
            buy_notional="0",
            sell_notional="100" if index in (0, 15) and case != "no_bursts" else "0",
            buy_qty="0",
            sell_qty="0",
        )
    snapshot["flow_bars"].extend([deepcopy(snapshot["flow_bars"][0]), deepcopy(snapshot["flow_bars"][15])])
    if case == "no_books":
        snapshot["books"] = []
    actual = await _compare(native_client, original_truth, snapshot, spot, relative)
    assert [venue["venue"] for venue in actual["per_venue"]] == ([] if case == "no_books" else ["B", "A"])
    assert actual["replenishment"]["available"] is (case != "no_books")
    assert actual["replenishment"]["observations"] == (
        [
            {
                "venue": venue,
                "ts": iso(start),
                "replenished": True,
                "recovery_s": 60.0,
                "resilient": True,
            }
            for venue in ("B", "B", "A", "A")
        ]
        if case == "duplicate_bursts"
        else []
    )
    assert actual["replenishment"]["bursts"] == (
        None if case == "no_books" else 4 if case == "duplicate_bursts" else 0
    )
