import ast
import builtins
import json
import os
import subprocess
import sys
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.analysis import truth_sql_result
from tank.util import canonical_json, iso, jsonable, parse_time

pytestmark = pytest.mark.integration
BASELINE = "ef3b43e8457622d2f3c2c86926de41aecee101d6"
END = datetime(2026, 10, 21, 12, tzinfo=UTC)
WINDOWS = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400, "EVENT": 3600}


@pytest.fixture(scope="module")
def original_result():
    root = Path(__file__).resolve().parents[1]
    modules = {}
    original_import = builtins.__import__

    def load(name):
        if name in modules:
            return modules[name]
        module = ModuleType("_result_original_" + name)
        modules[name] = module
        sys.modules[module.__name__] = module
        path = "src/" + name.replace(".", "/")
        result = subprocess.run(
            ["git", "show", f"{BASELINE}:{path}.py"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode:
            assert name in {"tank", "tank.analysis", "tank.market"}, result.stderr
            module.__path__ = []
            return module
        module.__file__ = f"{BASELINE}/{path}.py"
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
async def result_client(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        yield client
    finally:
        await client.close()


def packet(*, anchor_seconds=3600):
    return {
        "token_id": "T",
        "cutoff": iso(END),
        "anchor": {"ts": iso(END - timedelta(seconds=anchor_seconds))},
        "assembled_at": iso(END + timedelta(seconds=1)),
        "flow_bars": [],
        "tickers": [],
        "instruments": [
            {"instrument_id": "A:spot:T", "token_id": "T"},
            {"instrument_id": "B:spot:T", "token_id": "T"},
        ],
        "capture_status": {"status": "COMPLETE"},
    }


def venue(name, *, sign=1, volume="100", cvd="20"):
    return {
        "venue": name,
        "volume_native": volume,
        "cvd": cvd,
        "sum_sq_qty": "1",
        "sign": sign,
    }


def domains():
    spot = {
        "venues": [venue("A"), venue("B")],
        "composite_max": {"cvd": "40", "volume": "200", "venues": ["A", "B"]},
        "composite_cohort": {"cvd": "40", "volume": "200", "sign": 1},
        "classification": "BROAD",
        "direction": "BUY",
        "cohort_consistent": {},
        "coverage": {"n_complete": 2, "covered_share_est": 1.0},
        "exact_majority": {"buy": True, "sell": False},
        "cw_breadth": {"buy": 1.0, "sell": 0.0},
        "topn": {"venues": ["A", "B"]},
        "size_profile": None,
    }
    derivatives = {
        "synchronized": {
            "venues": ["P"],
            "oi_native_chg_pct": 0.01,
            "native_normal_range": [-0.02, 0.02],
            "native_oi_vs_normal": "WITHIN",
            "oi_class": "REAL_NATIVE",
            "oi_usd_chg_pct": 0.02,
            "usd_beyond": False,
            "classification_ready": True,
        },
        "per_venue": [
            {
                "venue": "P",
                "instrument_id": "P:perp:T",
                "funding_vs_normal": "WITHIN",
                "funding_start": "-0.0001",
                "funding_start_interval_h": "8",
                "funding_start_observed_at": iso(END - timedelta(hours=1)),
                "funding_8h_eq": "0.0001",
                "oi_class": "REAL_NATIVE",
                "oi_usd_chg_pct": 0.02,
            }
        ],
        "incremental_share": {"increment_usd": {"spot": "70", "perp": "30"}},
        "liquidations": {"complete_venues_total": {"short": "1", "long": "2"}},
        "lead_lag": {"first_mover": "UNKNOWN"},
        "support_features": {},
    }
    liquidity = {
        "per_venue": [],
        "depth_drop": 0.0,
        "depth_majority_lost": False,
        "spread_ratio": 1.0,
        "liquidity_state": "NORMAL",
        "elasticity": {
            "weak_response": False,
            "excess_response": False,
            "hit_side": "ask",
            "hit_side_depth_p1_usd": "100000",
        },
        "replenishment": {"bursts": 2, "replenished": 2, "resilient_bursts": 2},
        "fragmentation": {
            "validated": False,
            "persistent_gap": False,
            "funding_beyond_normal": None,
            "funding_normal_range_8h": None,
            "gap_closed_quickly": False,
            "top_sign_flips": None,
            "venues": [],
        },
    }
    supply = {"supply_in_evidence": "UNKNOWN", "exchange_inventory": {}, "flows": []}
    relative_window = {
        "relative_return": 0.01,
        "r_btc_adj": 0.01,
        "r_raw": 0.01,
        "normal_range": [-0.02, 0.02],
        "relative_measure": "R_BTC_ADJ",
        "start": iso(END - timedelta(hours=1)),
        "cutoff": iso(END),
    }
    return {
        "spots": {name: deepcopy(spot) for name in WINDOWS},
        "derivatives": {name: deepcopy(derivatives) for name in WINDOWS},
        "liquidity": {name: deepcopy(liquidity) for name in WINDOWS},
        "supply": {name: deepcopy(supply) for name in WINDOWS},
        "relative": {
            "windows": {name: deepcopy(relative_window) for name in WINDOWS},
            "anchor": {"ref_price": "10"},
            "ref_venue": "A:spot:T",
            "reference_coverage": {},
            "pre_event_runup": {"flag": False},
            "context_inputs": {"rv24": None, "rv_normal_range": None},
        },
        "confidence": {"class": "HIGH", "limiting": []},
    }


def assert_complete(actual, expected, path=()):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), path
        assert actual.keys() == expected.keys(), path
        for key in expected:
            assert_complete(actual[key], expected[key], (*path, key))
    elif isinstance(expected, list):
        assert isinstance(actual, list), path
        assert len(actual) == len(expected), path
        for index, (left, right) in enumerate(zip(actual, expected, strict=True)):
            assert_complete(left, right, (*path, index))
    elif isinstance(expected, bool) or expected is None:
        assert actual is expected, path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, abs=1e-13, rel=1e-12), path
    elif isinstance(expected, (Decimal, int)):
        assert abs(Decimal(str(actual)) - Decimal(str(expected))) <= Decimal("1e-22"), path
    elif isinstance(expected, str):
        try:
            expected_decimal = Decimal(expected)
        except Exception:
            assert actual == expected, path
        else:
            assert isinstance(actual, str), path
            assert abs(Decimal(actual) - expected_decimal) <= Decimal("1e-22"), path
    else:
        assert actual == expected, path


def typed_domain(value: Any, parent: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            key: typed_domain(
                item,
                key if parent not in {"increment_usd", "complete_venues_total", "tier_notional"} else parent,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [typed_domain(item, parent) for item in value]
    if isinstance(value, str) and parent in {
        "cvd",
        "volume",
        "volume_native",
        "sum_sq_qty",
        "buy_qty",
        "sell_qty",
        "funding_start",
        "funding_8h_eq",
        "funding_start_interval_h",
        "funding_dispersion_8h",
        "funding_dispersion_normal_range_8h",
        "increment_usd",
        "complete_venues_total",
        "tier_notional",
        "large_ticket_buy_notional",
        "large_ticket_sell_notional",
    }:
        return Decimal(value)
    return value


def reference_enriched(original, snapshot, inputs, config=None):
    spots = deepcopy(inputs["spots"])
    derivatives = typed_domain(deepcopy(inputs["derivatives"]))
    end = parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    av = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = parse_time(av) if av else end - timedelta(hours=4)
    primary = "EVENT" if av and 900 <= (end - start).total_seconds() <= 86400 else "W4H"
    windows = dict(WINDOWS, EVENT=(end - start).total_seconds())
    scoped = dict(snapshot, _has_event=bool(av), _lead_start=start, _event_anchor=start if av else None)
    context = original._PARAMETERS.set((config or {}).get("parameters", config or {}))
    try:
        for name, seconds in windows.items():
            original.enrich_evidence(
                dict(scoped, _primary=name),
                end - timedelta(seconds=seconds),
                end,
                spots,
                name,
                derivatives[name],
                deepcopy(inputs["relative"]),
                typed_domain(deepcopy(inputs["liquidity"][name])),
            )
            derivatives[name]["window"] = name
            derivatives[name]["start"] = iso(end - timedelta(seconds=seconds))
            derivatives[name]["cutoff"] = iso(end)
            if derivatives[name].get("synchronized"):
                derivatives[name]["synchronized"]["coverage"] = {
                    "venues": derivatives[name]["synchronized"]["venues"]
                }
    finally:
        original._PARAMETERS.reset(context)
    return spots, derivatives, windows, primary


async def rows_for(client, snapshot, inputs, *, table, fields, config=None):
    sql, params = truth_sql_result.query(snapshot, config, **inputs)
    statement = sql[: sql.rfind("\nSELECT ")] + f"\nSELECT {fields} FROM {table}"
    return await client.rows(statement, params)


@pytest.mark.parametrize(
    "scenario",
    [
        "accumulation",
        "organic",
        "chase",
        "squeeze",
        "sell_absorption",
        "distribution",
        "vacuum",
        "fragmentation",
        "fake_oi",
        "unknown",
        "low",
        "negative_return",
        "unknown_funding",
        "unsupported_native",
    ],
)
async def test_native_selection_matches_frozen_priority(result_client, original_result, scenario):
    inputs = domains()
    for name in WINDOWS:
        s, d, liquidity, su, p = (
            inputs["spots"][name],
            inputs["derivatives"][name],
            inputs["liquidity"][name],
            inputs["supply"][name],
            inputs["relative"]["windows"][name],
        )
        if scenario == "organic":
            d["support_features"] = {
                "spot_volume_up_beyond_normal": True,
                "perp_volume_up_beyond_normal": True,
                "basis_within_normal": True,
            }
            liquidity["elasticity"]["weak_response"] = None
        elif scenario == "chase":
            p["relative_return"] = 0.03
            d["synchronized"]["oi_native_chg_pct"] = 0.04
            d["incremental_share"]["increment_usd"] = {"spot": "20", "perp": "80"}
            s["direction"] = "NEUTRAL"
            d["support_features"] = {"basis_widening": True, "perp_taker_flow_buy": True}
        elif scenario == "squeeze":
            p["relative_return"] = 0.03
            d["synchronized"]["oi_native_chg_pct"] = -0.04
            d["support_features"] = {"liquidations_first_half_majority": True, "perp_taker_flow_buy": True}
        elif scenario == "sell_absorption":
            liquidity["elasticity"]["weak_response"] = True
            d["support_features"] = {"ask_replenishment": True}
        elif scenario == "distribution":
            su["supply_in_evidence"] = "TRUE"
            s["direction"] = "SELL"
            s["exact_majority"]["sell"] = True
            d["support_features"] = {"ask_depth_building": True, "exchange_inflows": True}
        elif scenario == "vacuum":
            p["relative_return"] = -0.03
            liquidity["depth_drop"] = 0.6
            liquidity["depth_majority_lost"] = True
            liquidity["elasticity"]["excess_response"] = True
            d["support_features"] = {"large_tickets_minority": True, "spot_volume_within_normal": True}
        elif scenario == "fragmentation":
            liquidity["fragmentation"]["validated"] = True
            liquidity["fragmentation"]["gap_closed_quickly"] = True
            liquidity["elasticity"]["weak_response"] = None
            d["synchronized"]["oi_native_chg_pct"] = None
        elif scenario == "fake_oi":
            s["direction"] = "NEUTRAL"
            d["synchronized"].update(oi_class="PRICE_MARKUP", oi_usd_chg_pct=0.5, usd_beyond=True)
        elif scenario in {"unknown", "negative_return"}:
            s["direction"] = "NEUTRAL"
            if scenario == "negative_return":
                p["relative_return"] = -0.03
        elif scenario == "low":
            inputs["confidence"]["class"] = "LOW"
        elif scenario == "unknown_funding":
            d["per_venue"][0]["funding_vs_normal"] = None
        elif scenario == "unsupported_native":
            d["synchronized"] = None
    snapshot = packet()
    spots, derivs, _, _ = reference_enriched(original_result, snapshot, inputs)
    rows = await rows_for(
        result_client, snapshot, inputs, table="res_selected", fields="res_name, res_selected_json AS result"
    )
    assert {row["res_name"] for row in rows} == set(WINDOWS)
    for row in rows:
        name = row["res_name"]
        expected = original_result.select_move(
            spots[name],
            derivs[name],
            inputs["liquidity"][name],
            inputs["supply"][name],
            inputs["relative"],
            inputs["confidence"]["class"],
            name,
        )
        assert_complete(json.loads(row["result"]), expected)


@pytest.mark.parametrize(
    ("bursts", "replenished", "resilient", "depth_lost", "perp_increment", "validated", "confidence"),
    [
        (2, 2, 2, False, "30", False, "HIGH"),
        (2, 1, 2, False, "30", False, "HIGH"),
        (2, 0, 0, False, "30", False, "HIGH"),
        (None, None, None, True, "70", False, "HIGH"),
        (3, 3, 3, False, "70", True, "HIGH"),
        (2, 2, 2, False, "30", False, "LOW"),
    ],
)
async def test_native_divergence_candidates_refutations_and_ties(
    result_client,
    original_result,
    bursts,
    replenished,
    resilient,
    depth_lost,
    perp_increment,
    validated,
    confidence,
):
    inputs = domains()
    inputs["confidence"]["class"] = confidence
    for name in WINDOWS:
        spot = inputs["spots"][name]
        spot.update(
            direction="SELL", classification="DIVERGENT", size_profile={"large_ticket_sell_share": 0.7}
        )
        d = inputs["derivatives"][name]
        d["incremental_share"]["increment_usd"]["perp"] = perp_increment
        d["support_features"] = {"basis_widening": True, "localized_inventory_transfer": True}
        liquidity = inputs["liquidity"][name]
        liquidity["depth_majority_lost"] = depth_lost
        liquidity["replenishment"] = {
            "bursts": bursts,
            "replenished": replenished,
            "resilient_bursts": resilient,
        }
        liquidity["fragmentation"].update(persistent_gap=True, validated=validated)
    snapshot = packet()
    spots, derivs, _, _ = reference_enriched(original_result, snapshot, inputs)
    rows = await rows_for(
        result_client, snapshot, inputs, table="res_selected", fields="res_name, res_selected_json AS result"
    )
    for row in rows:
        name = row["res_name"]
        expected = original_result.select_move(
            spots[name],
            derivs[name],
            inputs["liquidity"][name],
            inputs["supply"][name],
            inputs["relative"],
            confidence,
            name,
        )
        assert_complete(json.loads(row["result"]), expected)


@pytest.mark.parametrize(
    ("spot", "perp", "expected"),
    [
        ("1", "1", False),
        ("1.000000000000000000000001", "1", True),
        ("0.999999999999999999999999", "1", False),
    ],
)
async def test_increment_majority_is_not_rounded_display(result_client, spot, perp, expected):
    inputs = domains()
    for value in inputs["derivatives"].values():
        value["incremental_share"]["increment_usd"] = {"spot": spot, "perp": perp}
    rows = await rows_for(
        result_client, packet(), inputs, table="res_gate_inputs", fields="res_name, res_spotmost"
    )
    assert len(rows) == 5
    assert all(row["res_spotmost"] == expected for row in rows)


@pytest.mark.parametrize("missing", ["spots", "derivatives", "liquidity", "supply", "relative", "confidence"])
def test_missing_domain_is_explicit_not_an_empty_business_result(missing):
    inputs = domains()
    inputs.pop(missing)
    with pytest.raises(ValueError, match=missing):
        truth_sql_result.query(packet(), **inputs)


async def test_incomplete_window_map_fails_native_admission(result_client):
    inputs = domains()
    del inputs["derivatives"]["W24H"]
    sql, params = truth_sql_result.query(packet(), **inputs)
    with pytest.raises(Exception, match="complete native domain inputs"):
        await result_client.rows(sql, params)


@pytest.mark.parametrize("removed", [False, True])
async def test_pairwise_common_cohorts_differ_from_all_window_cohort(result_client, original_result, removed):
    inputs = domains()
    inputs["spots"]["W24H"]["venues"] = [venue("A"), venue("C")]
    if removed:
        inputs["spots"]["W1H"]["venues"] = []
    spots, _, _, _ = reference_enriched(original_result, packet(), inputs)
    rows = await rows_for(
        result_client, packet(), inputs, table="res_enriched", fields="res_name, res_spot AS result"
    )
    for row in rows:
        assert_complete(json.loads(row["result"]), spots[row["res_name"]])
    result = {row["res_name"]: json.loads(row["result"]) for row in rows}
    assert result["W15M"]["cohort_consistent"]["EVENT"] is True
    assert result["W15M"]["cohort_consistent"]["W24H"] is False


@pytest.mark.parametrize(("offset", "incomplete"), [(120, False), (-120, False), (0, False), (120, True)])
async def test_lead_lag_sequence_and_spot_confirmation(result_client, original_result, offset, incomplete):
    inputs = domains()
    snapshot = packet()
    start = END - timedelta(hours=1)
    snapshot["klines"] = [
        {
            "instrument_id": iid,
            "token_id": "T",
            "open_time": iso(start + timedelta(minutes=index)),
            "o": "10",
            "c": "10.5" if index * 60 >= threshold else "10",
            "complete": not (incomplete and index == 1),
        }
        for iid, threshold in [("A:spot:T", 180 + max(-offset, 0)), ("P:perp:T", 180 + max(offset, 0))]
        for index in range(10)
    ]
    _, derivs, _, _ = reference_enriched(original_result, snapshot, inputs)
    rows = await rows_for(
        result_client,
        snapshot,
        inputs,
        table="res_enriched",
        fields="res_name, JSONExtractRaw(res_deriv, 'lead_lag') AS result",
    )
    for row in rows:
        assert_complete(json.loads(row["result"]), derivs[row["res_name"]]["lead_lag"])


@pytest.mark.parametrize(
    ("cvd", "move"),
    [
        ("40.125", "SPOT_LED_ACCUMULATION"),
        ("-0.0012345", "LIQUIDITY_VACUUM"),
        ("0.000012345", "PERP_LED_CHASE"),
        ("4000000", "SHORT_SQUEEZE"),
        ("40", "DISTRIBUTION"),
        ("40", "REAL_MARKET_FRAGMENTATION"),
    ],
)
async def test_decisive_fact_numbers_units_scope_and_order(result_client, original_result, cvd, move):
    inputs = domains()
    for name in WINDOWS:
        inputs["spots"][name]["composite_max"]["cvd"] = cvd
        inputs["liquidity"][name]["depth_drop"] = 0.6
        inputs["liquidity"][name]["fragmentation"].update(validated=True, venues=[["A", "B"]])
        inputs["supply"][name].update(
            supply_in_evidence="TRUE",
            exchange_inventory={
                "chg_pct": 0.1,
                "coverage": {"complete_balances": 3, "comparable_balances": 2},
            },
            flows=[{"class": "MM_INVENTORY", "tx_hash": "transaction"}],
        )
    snapshot = packet()
    sql, params = truth_sql_result.query(snapshot, **inputs)
    sql = sql[: sql.rfind("\nSELECT ")]
    sql = sql.replace("AS res_move\n", "AS res_unused_move\n")
    sql = sql.replace(
        "FROM res_eligible_rows\n",
        "FROM (SELECT *, {res_test_move:String} AS res_move FROM res_eligible_rows)\n",
    )
    params["res_test_move"] = (move, "String")
    rows = await result_client.rows(
        sql
        + "\nSELECT res_name, concat('[', arrayStringConcat(res_facts, ','), ']') AS result FROM res_fact_rows",
        params,
    )
    spots, derivs, windows, _ = reference_enriched(original_result, snapshot, inputs)
    for row in rows:
        name = row["res_name"]
        expected = original_result.decisive_facts(
            spots[name],
            derivs[name],
            inputs["liquidity"][name],
            inputs["relative"],
            inputs["supply"][name],
            move,
            name,
            END - timedelta(seconds=windows[name]),
            END,
            token_id="T",
        )
        for fact in expected:
            fact["text"] = original_result.canonical_fact_text(fact)
        actual = json.loads(row["result"])
        assert_complete(actual, jsonable(expected))
        assert len(actual) <= 3
        assert all(fact["evidence"] for fact in actual)


def test_query_builder_has_no_python_business_fallback():
    tree = ast.parse(Path(truth_sql_result.__file__).read_text())
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert not calls & {
        "compute_market_truth",
        "select_move",
        "decisive_facts",
        "enrich_evidence",
        "Decimal",
        "float",
        "sum",
        "eval",
        "exec",
    }
    sql, params = truth_sql_result.query(packet(), **domains())
    assert sql.count(" AS result FROM res_assembly") == 1
    assert all(name.startswith("res_") for name in params)
    assert params["res_snapshot"] == (canonical_json(packet()), "String")


def assembled_reference(original, snapshot, inputs, monkeypatch, config=None):
    anchor = snapshot.get("anchor")
    av = anchor.get("ts") if isinstance(anchor, dict) else anchor
    end = parse_time(snapshot["cutoff"])
    start = parse_time(av) if av else end - timedelta(hours=4)
    windows = dict(WINDOWS, EVENT=(end - start).total_seconds())
    primary = "EVENT" if av and 900 <= windows["EVENT"] <= 86400 else "W4H"
    enrich = original.enrich_evidence
    derivative = original.derivatives_truth

    def enrich_residual(scoped, left, right, spots, name, deriv, relative, liquidity):
        preserved_liquidity = deepcopy(liquidity)
        enrich(scoped, left, right, spots, name, deriv, relative, liquidity)
        liquidity.clear()
        liquidity.update(preserved_liquidity)

    def derivatives(scoped, left, right):
        if not av and scoped.get("oi") == [] and scoped.get("liquidations") == []:
            return derivative(scoped, left, right)
        return typed_domain(deepcopy(inputs["derivatives"][scoped["_primary"]]))

    confidence = deepcopy(inputs["confidence"])
    confidence["class_"] = confidence.pop("class")
    with monkeypatch.context() as patcher:
        patcher.setattr(
            original,
            "spot_window_graph",
            lambda *args: (deepcopy(inputs["spots"]), windows, primary, deepcopy(snapshot["instruments"])),
        )
        patcher.setattr(original, "relative_truth", lambda *args: deepcopy(inputs["relative"]))
        patcher.setattr(original, "derivatives_truth", derivatives)
        patcher.setattr(
            original,
            "liquidity_truth",
            lambda scoped, *args: typed_domain(deepcopy(inputs["liquidity"][scoped["_primary"]])),
        )
        patcher.setattr(
            original, "supply_truth", lambda scoped, *args: deepcopy(inputs["supply"][scoped["_primary"]])
        )
        patcher.setattr(original, "enrich_evidence", enrich_residual)
        patcher.setattr(
            original,
            "support_features",
            lambda scoped, *args: deepcopy(
                inputs["derivatives"][scoped["_primary"]].get("support_features", {})
            ),
        )
        patcher.setattr(original, "data_confidence", lambda *args: deepcopy(confidence))
        patcher.setattr(original, "timestamp_companions", lambda result, snapshot: result)
        patcher.setattr(original, "measurement_metadata", lambda result: {})
        context = original._PARAMETERS.set((config or {}).get("parameters", config or {}))
        try:
            expected = original._compute_market_truth(snapshot, config=config)
        finally:
            original._PARAMETERS.reset(context)
    expected.pop("measurement_metadata")
    return expected


@pytest.mark.parametrize("anchor_seconds", [None, 60, 900, 3600, 86400, 90000])
async def test_native_full_business_body_matches_frozen_assembly(
    result_client, original_result, monkeypatch, anchor_seconds
):
    snapshot = packet(anchor_seconds=anchor_seconds or 3600)
    if anchor_seconds is None:
        snapshot.pop("anchor")
    snapshot["requested_cutoff"] = iso(END - timedelta(microseconds=200))
    snapshot["backfill_runs"] = [
        {
            "backfill_id": "owned-run",
            "status": "COMPLETE",
            "started_at": iso(END - timedelta(days=1)),
            "finished_at": iso(END),
            "anchor": snapshot.get("anchor"),
        }
    ]
    inputs = domains()
    inputs["derivatives"]["W15M"]["synchronized"]["oi_native_chg_pct"] = None
    inputs["liquidity"]["W24H"]["depth_majority_lost"] = True
    inputs["liquidity"]["W24H"]["elasticity"]["excess_response"] = True
    inputs["relative"]["windows"]["W24H"]["relative_return"] = -0.04
    inputs["confidence"]["class"] = "MEDIUM"
    config = {"config_version": "result-contract", "code_version": "native-assembly"}
    sql, params = truth_sql_result.query(snapshot, config, **inputs)
    rows = await result_client.rows(sql, params)
    assert len(rows) == 1
    assert rows[0].keys() == {"result"}
    actual = json.loads(rows[0]["result"])
    expected = assembled_reference(original_result, snapshot, inputs, monkeypatch, config)
    actual_policy = actual["measurement_context"].pop("numeric_policy")
    expected["measurement_context"].pop("numeric_policy")
    assert "Decimal256(24)" in actual_policy
    assert_complete(actual, expected)
    assert actual["primary_window"] == (
        "EVENT" if anchor_seconds is not None and 900 <= anchor_seconds <= 86400 else "W4H"
    )
    assert actual["derivatives"]["windows"]["W15M"]["synchronized"]["oi_native_chg_pct"] is None
    assert actual["detail"]["windows"]["W24H"]["book_depth_ready"] is False


@pytest.mark.parametrize("gap", [False, True])
@pytest.mark.parametrize("scale", ["1", "10000000000"])
async def test_primary_size_profile_uses_complete_compatible_buckets(
    result_client, original_result, gap, scale
):
    snapshot = packet()
    snapshot["semantics"] = [
        {"venue": "A", "trade_aggregation": "RAW"},
        {"venue": "B", "trade_aggregation": "RAW"},
    ]
    snapshot["flow_bars"] = [
        {
            "instrument_id": f"{venue_name}:spot:T",
            "minute_start": iso(END - timedelta(minutes=index)),
            "complete": True,
            "buy_qty": "10",
            "sell_qty": "2",
            "sum_sq_qty": "1",
            "buy_notional": "100",
            "sell_notional": "20",
            "size_buckets": {
                "B4": {"buy_notional": "10", "sell_notional": "2" if index <= 60 else "1"},
                "B5": {"buy_notional": "5", "sell_notional": "1"},
            },
        }
        for venue_name in ("A", "B")
        for index in range(1, 2881)
        if not (gap and venue_name == "B" and index == 10)
    ]
    for row in snapshot["flow_bars"]:
        for key in ("buy_notional", "sell_notional"):
            row[key] = str(Decimal(row[key]) * Decimal(scale))
            for bucket in row["size_buckets"].values():
                bucket[key] = str(Decimal(bucket[key]) * Decimal(scale))
    inputs = domains()
    spots, _, _, _ = reference_enriched(original_result, snapshot, inputs)
    rows = await rows_for(
        result_client,
        snapshot,
        inputs,
        table="res_enriched",
        fields="res_name, JSONExtractRaw(res_spot, 'size_profile') AS result",
    )
    for row in rows:
        assert_complete(json.loads(row["result"]), jsonable(spots[row["res_name"]]["size_profile"]))


async def test_different_windows_keep_their_own_business_selection(result_client, original_result):
    inputs = domains()
    inputs["spots"]["W15M"]["direction"] = "NEUTRAL"
    inputs["derivatives"]["W15M"]["synchronized"].update(
        oi_class="PRICE_MARKUP", oi_usd_chg_pct=0.5, usd_beyond=True
    )
    inputs["liquidity"]["W24H"]["depth_majority_lost"] = True
    inputs["liquidity"]["W24H"]["elasticity"]["excess_response"] = True
    inputs["relative"]["windows"]["W24H"]["relative_return"] = -0.05
    spots, derivs, _, _ = reference_enriched(original_result, packet(), inputs)
    rows = await rows_for(
        result_client, packet(), inputs, table="res_selected", fields="res_name, res_selected_json AS result"
    )
    results = {row["res_name"]: json.loads(row["result"]) for row in rows}
    for name in WINDOWS:
        expected = original_result.select_move(
            spots[name],
            derivs[name],
            inputs["liquidity"][name],
            inputs["supply"][name],
            inputs["relative"],
            "HIGH",
            name,
        )
        assert_complete(results[name], expected)
    assert results["W15M"]["move_class"] == "PRICE_MARKUP_FAKE_OI"
    assert results["W24H"]["move_class"] == "LIQUIDITY_VACUUM"
    assert results["W1H"]["move_class"] == "SPOT_LED_ACCUMULATION"


async def test_real_native_domain_outputs_feed_one_result_select(result_client, original_result, monkeypatch):
    from tank.analysis import (
        truth_sql_derivatives,
        truth_sql_liquidity,
        truth_sql_relative,
        truth_sql_spot,
    )

    snapshot = packet()
    snapshot.update(
        _primary="EVENT",
        _event_anchor=snapshot["anchor"]["ts"],
        _lead_start=snapshot["anchor"]["ts"],
        _has_event=True,
    )

    async def native(builder, **kwargs):
        sql, parameters = builder(snapshot, **kwargs)
        rows = await result_client.rows(sql, parameters)
        assert len(rows) == 1
        return json.loads(rows[0]["result"])

    spot_graph = await native(truth_sql_spot.query)
    derivative_windows = await native(truth_sql_derivatives.query)
    relative = await native(truth_sql_relative.query)
    liquidity = await native(
        truth_sql_liquidity.windows_query,
        spots=spot_graph["spots"],
        relative=relative,
        deriv=derivative_windows,
    )
    inputs = domains()
    inputs.update(
        spots=spot_graph["spots"], derivatives=derivative_windows, relative=relative, liquidity=liquidity
    )
    for derivative in derivative_windows.values():
        derivative["support_features"] = {}
    inputs["confidence"]["class"] = "LOW"
    sql, parameters = truth_sql_result.query(snapshot, **inputs)
    rows = await result_client.rows(sql, parameters)
    assert len(rows) == 1
    actual = json.loads(rows[0]["result"])
    reference_inputs = deepcopy(inputs)
    reference_inputs["liquidity"] = reference_inputs["liquidity"]["windows"]
    expected = assembled_reference(original_result, snapshot, reference_inputs, monkeypatch)
    actual["measurement_context"].pop("numeric_policy")
    expected["measurement_context"].pop("numeric_policy")
    assert_complete(actual, expected)
    assert actual["move_class"] == "UNKNOWN"
    assert actual["detail"]["primary_window_ready"] is False
    assert all(value["classification"] == "INSUFFICIENT_DATA" for value in actual["spot"].values())


@pytest.mark.parametrize(
    ("shared", "majority"),
    [("100", False), ("100.000000000000000000000001", True), ("99.999999999999999999999999", False)],
)
async def test_pairwise_cohort_share_uses_native_strict_majority(
    result_client, original_result, shared, majority
):
    inputs = domains()
    inputs["spots"]["W15M"]["venues"] = [
        venue("A", volume=shared),
        venue("B", volume="0"),
        venue("C", volume="100"),
    ]
    inputs["spots"]["W15M"]["composite_max"]["volume"] = str(Decimal(shared) + Decimal("100"))
    spots, _, _, _ = reference_enriched(original_result, packet(), inputs)
    rows = await rows_for(
        result_client, packet(), inputs, table="res_enriched", fields="res_name, res_spot AS result"
    )
    for row in rows:
        assert_complete(json.loads(row["result"]), spots[row["res_name"]])
    selected = next(json.loads(row["result"]) for row in rows if row["res_name"] == "W15M")
    assert selected["cohort_comparisons"]["W1H"]["share_majority"] is majority


async def test_funding_normal_range_is_taken_from_native_liquidity_enrichment(result_client, original_result):
    from tank.analysis import truth_sql_liquidity

    snapshot = packet()
    snapshot.update(_primary="EVENT", _event_anchor=snapshot["anchor"]["ts"])
    inputs = domains()
    for derivative in inputs["derivatives"].values():
        second = deepcopy(derivative["per_venue"][0])
        second.update(venue="Q", instrument_id="Q:perp:T", funding_8h_eq="0.004")
        derivative["per_venue"].append(second)
        derivative["funding_dispersion_8h"] = "0.0039"
    snapshot["funding"] = [
        {
            "instrument_id": f"{venue_name}:perp:T",
            "ts": iso(END - timedelta(hours=hour)),
            "kind": "SETTLED",
            "rate": rate,
            "interval_h": "8",
        }
        for hour in range(264, 0, -8)
        for venue_name, rate in [("P", "0.0001"), ("Q", "0.0002")]
    ]
    sql, parameters = truth_sql_liquidity.windows_query(
        snapshot, spots=inputs["spots"], relative=inputs["relative"], deriv=inputs["derivatives"]
    )
    rows = await result_client.rows(sql, parameters)
    inputs["liquidity"] = json.loads(rows[0]["result"])["windows"]
    _, expected_derivatives, _, _ = reference_enriched(original_result, snapshot, inputs)
    rows = await rows_for(
        result_client, snapshot, inputs, table="res_enriched", fields="res_name, res_deriv AS result"
    )
    for row in rows:
        actual = json.loads(row["result"])
        expected = expected_derivatives[row["res_name"]]
        assert_complete(
            actual["funding_dispersion_normal_range_8h"],
            jsonable(expected["funding_dispersion_normal_range_8h"]),
        )
        assert actual["funding_dispersion_beyond_normal"] is expected["funding_dispersion_beyond_normal"]


@pytest.mark.parametrize("status", [None, "PENDING", "RUNNING", "PARTIAL", "FAILED", "COMPLETE"])
async def test_capture_readiness_is_not_a_fabricated_success(
    result_client, original_result, monkeypatch, status
):
    snapshot = packet()
    if status is None:
        snapshot.pop("capture_status")
    else:
        snapshot["capture_status"] = {"status": status}
    inputs = domains()
    sql, parameters = truth_sql_result.query(snapshot, **inputs)
    rows = await result_client.rows(sql, parameters)
    actual = json.loads(rows[0]["result"])
    expected = assembled_reference(original_result, snapshot, inputs, monkeypatch)
    assert_complete(actual["detail"], expected["detail"])
    assert actual["data_confidence"]["capture_readiness"] == actual["detail"]
    assert actual["detail"]["prerequisite_pending"] is (status in {"PENDING", "RUNNING", "PARTIAL", "FAILED"})


@pytest.mark.parametrize("amount", ["1000000000000000000000000", "0.0000000000000000000000001"])
async def test_unsupported_financial_increment_is_refused(result_client, amount):
    inputs = domains()
    inputs["derivatives"]["EVENT"]["incremental_share"]["increment_usd"]["perp"] = amount
    sql, parameters = truth_sql_result.query(packet(), **inputs)
    with pytest.raises(Exception, match="financial input outside native range"):
        await result_client.rows(sql, parameters)


async def test_configured_cohort_admission_changes_native_flags(result_client, original_result):
    inputs = domains()
    config = {"parameters": {"min_spot_venues": 3}}
    spots, derivs, _, _ = reference_enriched(original_result, packet(), inputs, config)
    rows = await rows_for(
        result_client,
        packet(),
        inputs,
        config=config,
        table="res_selected",
        fields="res_name, res_selected_json AS result",
    )
    for row in rows:
        name = row["res_name"]
        expected = original_result.select_move(
            spots[name],
            derivs[name],
            inputs["liquidity"][name],
            inputs["supply"][name],
            inputs["relative"],
            "HIGH",
            name,
        )
        actual = json.loads(row["result"])
        assert_complete(actual, expected)
        assert "NOT_COHORT_CONSISTENT" in actual["flags"]
