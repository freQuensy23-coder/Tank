import builtins
import importlib
import json
import os
import re
import subprocess
import sys
from array import array
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from types import ModuleType

import pytest
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.analysis import truth_sql_conditions as native
from tank.util import iso

pytestmark = pytest.mark.integration

ORIGINAL_COMMIT = "ef3b43e8457622d2f3c2c86926de41aecee101d6"
CUTOFF = datetime(2026, 10, 21, 12, tzinfo=UTC)
METRICS = (
    "spot.cohort.direction",
    "spot.cohort.cwb_buy",
    "spot.cohort.cwb_sell",
    "spot.classification",
    "deriv.native_oi_chg_pct",
    "deriv.native_oi_vs_normal",
    "deriv.oi_class",
    "deriv.funding_vs_normal",
    "price.close_vs_level",
    "dc.class",
    "liq.depth_p1_vs_pre",
    "liq.spread_ratio",
    "mt.move_class",
    "mt.divergence_state",
    "price.repricing_band",
    "price.vs_anchor_pct",
)
WINDOWS = ("W15M", "W1H", "W4H", "W24H", "EVENT")
NUMERIC = re.compile(r"^[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$")


@pytest.fixture(scope="module")
def original_truth():
    root = Path(__file__).resolve().parents[1]
    modules = {}
    original_import = builtins.__import__

    def source(path):
        return subprocess.run(
            ["git", "show", f"{ORIGINAL_COMMIT}:{path}"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def load(name):
        if name in modules:
            return modules[name]
        module = ModuleType("_conditions_original_" + name)
        modules[name] = module
        sys.modules[module.__name__] = module
        path = "src/" + name.replace(".", "/")
        result = source(path + ".py")
        if result.returncode:
            result = source(path + "/__init__.py")
            module.__path__ = []
            path += "/__init__"
        if result.returncode:
            assert name in {"tank", "tank.analysis", "tank.market"}, result.stderr
            module.__file__ = f"{ORIGINAL_COMMIT}/{path}"
            return module
        module.__file__ = f"{ORIGINAL_COMMIT}/{path}.py"
        module.__dict__["__builtins__"] = {**vars(builtins), "__import__": frozen_import}
        exec(compile(result.stdout, module.__file__, "exec"), module.__dict__)
        return module

    def frozen_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name != "tank" and not name.startswith("tank."):
            return original_import(name, globals, locals, fromlist, level)
        assert level == 0
        module = load(name)
        components = name.split(".")
        for index in range(1, len(components)):
            setattr(
                load(".".join(components[:index])), components[index], load(".".join(components[: index + 1]))
            )
        for child in fromlist:
            if child != "*" and not hasattr(module, child):
                setattr(module, child, load(name + "." + child))
        return module if fromlist else load("tank")

    oracle = load("tank.analysis.truth")
    assert oracle.compute_condition_values.__module__ != "tank.analysis.truth"
    assert all(module.__file__.startswith(ORIGINAL_COMMIT + "/") for module in modules.values())
    yield oracle
    for module in modules.values():
        sys.modules.pop(module.__name__, None)


@pytest.fixture
async def clickhouse(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned loopback native CI service"
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        yield client
    finally:
        await client.close()


def _snapshot():
    return {
        "token_id": "T",
        "cutoff": iso(CUTOFF),
        "cutoff_prec": "EXACT",
        "assembled_at": iso(CUTOFF + timedelta(minutes=2)),
        "anchor": {"ts": iso(CUTOFF - timedelta(hours=1)), "type": "KNOWN_TO_US_AT"},
        "instruments": [],
        "flow_bars": [],
        "flow_segments": [],
        "reference_points": [],
        "klines": [],
        "books": [],
        "oi": [],
        "funding": [],
        "liquidations": [],
        "tickers": [],
        "supply": [],
        "semantics": [],
        "tokens": [],
        "sector_baskets": [],
    }


def _book(venue, stamp, bid="100", ask="120", spread=2):
    return {
        "instrument_id": f"{venue}:spot:T",
        "venue": venue,
        "token_id": "T",
        "market_type": "SPOT",
        "exchange_ts": iso(stamp),
        "exchange_ts_prec": "EXACT",
        "integrity_ok": True,
        "mid": "10",
        "range_covered_pct": {"bid": 2, "ask": 2},
        "spread_bps": spread,
        "depth": {
            "bid": {"p0_5": {"usd": bid}, "p1": {"usd": bid}, "p2": {"usd": bid}},
            "ask": {"p0_5": {"usd": ask}, "p1": {"usd": ask}, "p2": {"usd": ask}},
        },
    }


def _physical_snapshot(*, minutes=60):
    data = _snapshot()
    for venue in ("binance", "okx"):
        iid = f"{venue}:spot:T"
        data["instruments"].append(
            {"instrument_id": iid, "venue": venue, "token_id": "T", "market_type": "SPOT"}
        )
        data["tickers"].append({"instrument_id": iid, "ts": iso(CUTOFF), "volume_native": "10000"})
        for minute in range(minutes, 0, -1):
            stamp = CUTOFF - timedelta(minutes=minute)
            data["flow_bars"].append(
                {
                    "instrument_id": iid,
                    "minute_start": iso(stamp),
                    "complete": True,
                    "buy_qty": "4.000000000000000000000001",
                    "sell_qty": "1",
                    "sum_sq_qty": "1",
                    "buy_notional": "40",
                    "sell_notional": "10",
                    "aggregation_class": "MATCH",
                }
            )
            data["klines"].append(
                {
                    "instrument_id": iid,
                    "open_time": iso(stamp),
                    "open_time_prec": "BUCKET(1m)",
                    "complete": True,
                    "interval_s": 60,
                    "c": "10.000000000000000000000001",
                    "quote_volume_usd": "50",
                    "volume": "5",
                }
            )
        data["books"].extend(
            [
                _book(venue, CUTOFF - timedelta(hours=1, minutes=30), "50", "60", 1),
                _book(venue, CUTOFF, "100", "120", 2),
            ]
        )
    return data


def _derivative_snapshot():
    data = _snapshot()
    for venue in ("binance", "okx"):
        iid = f"{venue}:perp:T"
        data["instruments"].append(
            {"instrument_id": iid, "venue": venue, "token_id": "T", "market_type": "PERP_LINEAR"}
        )
        data["semantics"].append(
            {
                "instrument_id": iid,
                "metric_key": "oi",
                "sidedness": "SINGLE_SIDED",
                "aggregation_class": "BASE",
                "unit": "T",
                "supports_synchronized_oi": True,
            }
        )
        for hour in range(8 * 24, -1, -1):
            stamp = CUTOFF - timedelta(hours=hour)
            data["oi"].append(
                {
                    "instrument_id": iid,
                    "ts": iso(stamp),
                    "ts_prec": "EXACT",
                    "metric_key": "oi",
                    "oi_native": "100.000000000000000000000001" if hour else "110.000000000000000000000001",
                    "oi_usd": "1000" if hour else "1100",
                    "mark_price": "10",
                    "quote_to_usd": "1",
                }
            )
            data["funding"].append(
                {
                    "instrument_id": iid,
                    "ts": iso(stamp),
                    "ts_prec": "EXACT",
                    "kind": "SETTLED" if hour else "CURRENT_ESTIMATE",
                    "rate": "0.0001" if hour else "0.0002",
                    "interval_h": 8,
                }
            )
    return data


def _requirements(metrics=METRICS, windows=("W1H",)):
    return [{"metric": metric, "window": window} for metric in metrics for window in windows]


def _relative_snapshot():
    data = _physical_snapshot(minutes=1440)
    data["tokens"] = [{"token_id": "T", "symbol": "T"}, {"token_id": "BTC", "symbol": "BTC"}]
    data["instruments"].append(
        {"instrument_id": "binance:spot:BTC", "token_id": "BTC", "market_type": "SPOT"}
    )
    for hour in range(8 * 24, -1, -1):
        stamp = CUTOFF - timedelta(hours=hour + 1)
        data["klines"].append(
            {
                "instrument_id": "binance:spot:BTC",
                "token_id": "BTC",
                "open_time": iso(stamp),
                "interval_s": 3600,
                "complete": True,
                "c": "100",
            }
        )
        if hour >= 24:
            for venue in ("binance", "okx"):
                data["klines"].append(
                    {
                        "instrument_id": f"{venue}:spot:T",
                        "token_id": "T",
                        "open_time": iso(stamp),
                        "interval_s": 3600,
                        "complete": True,
                        "c": "10",
                    }
                )
    for venue in ("binance", "okx"):
        for stamp, price in ((CUTOFF - timedelta(hours=1), "10"), (CUTOFF, "11")):
            data["reference_points"].append(
                {
                    "instrument_id": f"{venue}:spot:T",
                    "boundary": iso(stamp),
                    "complete": True,
                    "last_trade": {
                        "exchange_ts": iso(stamp - timedelta(microseconds=1)),
                        "exchange_ts_prec": "EXACT",
                        "price": price,
                    },
                }
            )
    return data


def _compare_value(actual, expected, path="result"):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), path
        assert actual.keys() == expected.keys(), path
        for key, value in expected.items():
            _compare_value(actual[key], value, path + "." + key)
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected), path
        for index, (left, right) in enumerate(zip(actual, expected, strict=True)):
            _compare_value(left, right, f"{path}[{index}]")
    elif isinstance(expected, bool) or expected is None:
        assert actual is expected, path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-10, abs=1e-12), path
    elif isinstance(expected, str) and NUMERIC.fullmatch(expected):
        assert isinstance(actual, str) and NUMERIC.fullmatch(actual), path
        with localcontext() as context:
            context.prec = 128
            assert abs(Decimal(actual) - Decimal(expected)) <= Decimal("1e-20"), path
    else:
        assert actual == expected, path


def _uses_minimal_anchor_contract(original, snapshot, requirements):
    anchor = (snapshot.get("anchor") or {}).get("ts")
    return (
        bool(anchor)
        and original.parse_time(anchor) >= original.parse_time(snapshot["cutoff"])
        and any(
            row["metric"] in {"mt.move_class", "mt.divergence_state"}
            and row.get("window", "W1H") in {"W15M", "W1H", "W4H", "W24H"}
            for row in requirements
        )
    )


def _condition_contract_oracle(
    original, snapshot, requirements, *, config=None, cache=None, baseline_cache=None
):
    if not _uses_minimal_anchor_contract(original, snapshot, requirements):
        return original.compute_condition_values(
            deepcopy(snapshot),
            deepcopy(requirements),
            config=deepcopy(config),
            cache=cache,
            baseline_cache=baseline_cache,
        )
    config = config or {}
    cache = cache if cache is not None else {}
    baseline_cache = baseline_cache if baseline_cache is not None else {}
    end = original.parse_time(snapshot["cutoff"])
    bounded = original.bounded_market_snapshot(deepcopy(snapshot), end)
    requested = sorted({(row["metric"], row.get("window", "W1H")) for row in requirements})
    fields = (
        "flow_bars",
        "flow_segments",
        "reference_points",
        "tickers",
        "instruments",
        "tokens",
        "sector_baskets",
        "semantics",
        "capture_metadata",
        "oi",
        "funding",
        "klines",
        "books",
        "liquidations",
        "supply",
        "anchor",
        "expected_direction",
        "funding_confirmations",
        "stat_price_series",
        "stat_volume_series",
        "series_coverage_counts",
        "relative_history_dependency_fingerprint",
    )
    body = {"cutoff": original.iso(end), "spot": {}, "derivatives": {"windows": {}}, "relative": {}}
    proofs, pending = [], []
    for metric, window in requested:
        if metric in {"mt.move_class", "mt.divergence_state"} and window in {"W15M", "W1H", "W4H", "W24H"}:
            dependency = (bounded.get("domain_dependency_fingerprints") or {}).get("state")
            if dependency is None:
                dependency = original.fingerprint({field: bounded.get(field) for field in fields})
            elif not isinstance(dependency, str):
                dependency = original.fingerprint(dependency)
            key = (
                "state",
                window,
                (end - original.condition_primary_bounds(bounded, end)[0]).total_seconds(),
                original.iso(end),
                dependency,
                original.fingerprint(config),
                original.fingerprint(bounded.get("expected_direction")),
            )
            reused = key in cache
            selected = {field: bounded.get(field, []) for field in fields}
            selected.update(
                token_id=bounded.get("token_id"),
                cutoff=original.iso(end),
                assembled_at=bounded.get("assembled_at"),
                domain_dependency_fingerprints=bounded.get("domain_dependency_fingerprints", {}),
            )
            state = original.remember(
                cache, key, lambda selected=selected: original.condition_market_state(selected, end, cache)
            )
            body.update(deepcopy(state))
            proofs.append(
                {
                    "metric": metric,
                    "window": window,
                    "dependency_fingerprint": dependency,
                    "reused": reused,
                    "available": True,
                    "reason": None,
                    "observations": [],
                    "coverage": None,
                }
            )
            continue
        ordinary = original.compute_condition_values(
            deepcopy(snapshot),
            [{"metric": metric, "window": window}],
            config=deepcopy(config),
            cache=cache,
            baseline_cache=baseline_cache,
        )
        body["spot"].update(ordinary["spot"])
        body["relative"].update(ordinary["relative"])
        for name, pack in ordinary["derivatives"]["windows"].items():
            prior = body["derivatives"]["windows"].setdefault(name, {"per_venue": [], "synchronized": None})
            field = "per_venue" if metric == "deriv.funding_vs_normal" else "synchronized"
            prior[field] = deepcopy(pack[field])
        body.update(
            {
                key: deepcopy(value)
                for key, value in ordinary.items()
                if key
                not in {
                    "cutoff",
                    "cutoff_prec",
                    "cutoff_prec_reason",
                    "cutoff_bucket_anchor",
                    "spot",
                    "derivatives",
                    "relative",
                    "_condition_evaluation",
                }
            }
        )
        proofs.extend(ordinary["_condition_evaluation"]["proofs"])
        pending.extend(ordinary["_condition_evaluation"]["unavailable_metrics"])
    body["_condition_evaluation"] = {
        "requires_market_truth": [],
        "unavailable_metrics": pending,
        "request_key": original.fingerprint(
            {
                "token_id": bounded.get("token_id"),
                "requirements": requested,
                "dependencies": bounded.get(
                    "dependency_fingerprint", [proof["dependency_fingerprint"] for proof in proofs]
                ),
                "anchor": bounded.get("anchor"),
                "config": config,
            }
        ),
        "proofs": proofs,
        "transient": True,
    }
    body.update(original.propagate_timestamp(bounded, "cutoff").fields("cutoff"))
    return original.jsonable(body)


async def _evaluate(clickhouse, original, snapshot, requirements, *, config=None, result=None):
    minimal = _uses_minimal_anchor_contract(original, snapshot, requirements)
    expected_cache = {}
    expected = _condition_contract_oracle(
        original, snapshot, requirements, config=config, cache=expected_cache
    )
    sql, parameters = native.query(snapshot, requirements, config, result=result, cache_transport=minimal)
    rows = await clickhouse.rows(sql, parameters)
    assert len(rows) == 1 and set(rows[0]) == ({"result", "condition_cache"} if minimal else {"result"})
    actual = json.loads(rows[0]["result"])
    _compare_value(actual, expected)
    if minimal:
        records = json.loads(rows[0]["condition_cache"])
        actual_cache = {record["key"]: record["value"] for record in records}
        assert len(actual_cache) == len(records)
        _compare_caches(actual_cache, expected_cache, original)
    return actual


def _full_body(original, snapshot, config=None):
    end = original.parse_time(snapshot["cutoff"])
    bounded = original.bounded_market_snapshot(deepcopy(snapshot), end)
    parameters = original._PARAMETERS.set(config or {})
    baselines = original._BASELINE_CACHE.set({})
    try:
        state = original.condition_market_state(bounded, end, {})
    finally:
        original._BASELINE_CACHE.reset(baselines)
        original._PARAMETERS.reset(parameters)
    full = deepcopy(state)
    primary = full["primary_window"]
    full["derivatives"].update(full["derivatives"]["windows"][primary])
    for domain in ("liquidity", "supply"):
        full[domain]["windows"] = {primary: deepcopy(full[domain])}
    return original.jsonable(full)


@pytest.mark.parametrize("metric", METRICS)
async def test_every_registered_metric_complete_empty_output_all_windows(clickhouse, original_truth, metric):
    data = _snapshot()
    full = _full_body(original_truth, data) if metric in {"mt.move_class", "mt.divergence_state"} else None
    await _evaluate(clickhouse, original_truth, data, _requirements((metric,), WINDOWS), result=full)


@pytest.mark.parametrize("window", WINDOWS)
async def test_spot_selective_scope_proofs_and_reuse(clickhouse, original_truth, window):
    data = _physical_snapshot(minutes=60)
    requirements = _requirements(METRICS[:4], (window,))
    requirements.extend(deepcopy(requirements[:2]))
    result = await _evaluate(clickhouse, original_truth, data, list(reversed(requirements)))
    proofs = result["_condition_evaluation"]["proofs"]
    assert [proof["reused"] for proof in proofs] == [False, True, True, True]
    assert result["_condition_evaluation"]["requires_market_truth"] == []
    if window in {"W15M", "W1H", "EVENT"}:
        assert result["spot"][window]["direction"] == "BUY"
        assert all(proof["available"] for proof in proofs)


@pytest.mark.parametrize(
    "case",
    [
        "buy",
        "sell",
        "neutral",
        "partial",
        "future",
        "late",
        "missing_field",
        "fractional",
        "foreign_registry",
    ],
)
async def test_spot_condition_direction_and_boundary_admission(clickhouse, original_truth, case):
    data = _physical_snapshot(minutes=60)
    if case in {"sell", "neutral"}:
        for row in data["flow_bars"]:
            row.update(buy_qty="1", sell_qty="4" if case == "sell" else "1")
    elif case in {"partial", "future", "late", "missing_field"}:
        for row in data["flow_bars"]:
            if row["minute_start"] != iso(CUTOFF - timedelta(minutes=1)):
                continue
            if case == "partial":
                row["complete"] = False
            elif case == "future":
                row["minute_start"] = iso(CUTOFF)
            elif case == "late":
                row["received_ts"] = iso(CUTOFF + timedelta(minutes=3))
            else:
                row.pop("sum_sq_qty")
    elif case == "fractional":
        start = CUTOFF - timedelta(minutes=59, seconds=30)
        data["anchor"]["ts"] = iso(start)
        for instrument in data["instruments"]:
            data["flow_segments"].append(
                {
                    "instrument_id": instrument["instrument_id"],
                    "start": iso(start),
                    "end": iso(CUTOFF - timedelta(minutes=59)),
                    "complete": True,
                    "buy_qty": "2",
                    "sell_qty": "0.5",
                    "sum_sq_qty": "0.5",
                }
            )
    elif case == "foreign_registry":
        data["instruments"].append(
            {"instrument_id": "foreign:spot:OTHER", "token_id": "OTHER", "market_type": "SPOT"}
        )
    result = await _evaluate(clickhouse, original_truth, data, _requirements(METRICS[:4], ("EVENT",)))
    if case in {"buy", "sell", "neutral"}:
        assert (
            result["spot"]["EVENT"]["direction"] == {"buy": "BUY", "sell": "SELL", "neutral": "NEUTRAL"}[case]
        )


async def test_all_derivative_condition_metrics_across_registered_windows(clickhouse, original_truth):
    await _evaluate(clickhouse, original_truth, _derivative_snapshot(), _requirements(METRICS[4:8], WINDOWS))


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "stale",
        "future",
        "late",
        "unknown_tier",
        "numeric_cadence",
        "by_metric",
        "scoped_semantics",
        "unknown_precision",
        "bucket_end",
        "unspecified_bucket",
    ],
)
async def test_derivative_freshness_and_observation_ownership(clickhouse, original_truth, case):
    data = _derivative_snapshot()
    if case == "stale":
        data["oi"] = [row for row in data["oi"] if row["ts"] != iso(CUTOFF)]
        data["funding"] = [row for row in data["funding"] if row["ts"] != iso(CUTOFF)]
    elif case in {"future", "late"}:
        for collection in ("oi", "funding"):
            for row in data[collection]:
                if row["ts"] == iso(CUTOFF):
                    row["ts" if case == "future" else "received_ts"] = iso(CUTOFF + timedelta(minutes=3))
    elif case == "unknown_tier":
        data["capture_metadata"] = {"tier": "UNKNOWN"}
    elif case == "numeric_cadence":
        data["capture_metadata"] = {"collections": {"oi": 60, "funding": 60}}
    elif case == "by_metric":
        data["capture_metadata"] = {"collections": {"oi": {"cadence_s_by_metric": {"oi": 60}}}}
    elif case == "scoped_semantics":
        for row in data["semantics"]:
            row["cadence_s"] = 60
    elif case == "unknown_precision":
        for row in data["oi"] + data["funding"]:
            row.pop("ts_prec")
    elif case in {"bucket_end", "unspecified_bucket"}:
        for row in data["oi"]:
            row.update(
                ts_prec="BUCKET(1m)", ts_bucket_anchor="END" if case == "bucket_end" else "UNSPECIFIED"
            )
    result = await _evaluate(clickhouse, original_truth, data, _requirements(METRICS[4:8]))
    if case == "normal":
        assert result["derivatives"]["windows"]["W1H"]["synchronized"]["oi_native_chg_pct"] > 0
        assert all(
            row["funding_vs_normal"] == "ABOVE"
            for row in result["derivatives"]["windows"]["W1H"]["per_venue"]
        )


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "missing_side",
        "missing_baseline",
        "zero_baseline",
        "stale",
        "future",
        "late",
        "wrong_token",
        "integrity",
        "equal_timestamp",
        "scalar_coverage",
        "no_anchor",
        "half_band",
        "two_band",
        "scientific",
        "precision_unknown",
    ],
)
async def test_depth_complete_output_and_missing_reasons(clickhouse, original_truth, case):
    data = _physical_snapshot(minutes=0)
    config = {}
    if case == "missing_side":
        data["books"][-1]["depth"]["ask"]["p1"] = {}
    elif case == "missing_baseline":
        data["books"] = [row for row in data["books"] if row["exchange_ts"] == iso(CUTOFF)]
    elif case == "zero_baseline":
        for row in data["books"]:
            if row["exchange_ts"] != iso(CUTOFF):
                row["depth"]["bid"]["p1"]["usd"] = "0"
                row["depth"]["ask"]["p1"]["usd"] = "0"
    elif case in {"stale", "future", "late"}:
        for row in data["books"]:
            if row["exchange_ts"] == iso(CUTOFF):
                if case == "stale":
                    row["exchange_ts"] = iso(CUTOFF - timedelta(seconds=11))
                else:
                    row["exchange_ts" if case == "future" else "received_ts"] = iso(
                        CUTOFF + timedelta(minutes=3)
                    )
    elif case == "wrong_token":
        data["books"][-1]["token_id"] = "OTHER"
    elif case == "integrity":
        data["books"][-1]["integrity_ok"] = False
    elif case == "equal_timestamp":
        duplicate = deepcopy(data["books"][-1])
        duplicate["depth"]["ask"]["p1"]["usd"] = "999"
        data["books"].append(duplicate)
    elif case == "scalar_coverage":
        for row in data["books"]:
            row["range_covered_pct"] = 2
    elif case == "no_anchor":
        data.pop("anchor")
    elif case in {"half_band", "two_band"}:
        config = {"depth_band_pct": 0.5 if case == "half_band" else 2}
    elif case == "scientific":
        data["books"][-1]["depth"]["ask"]["p1"]["usd"] = Decimal("1.2e2")
    elif case == "precision_unknown":
        data.pop("cutoff_prec")
        data["cutoff_prec_reason"] = "LEGACY_CAPTURE"
    result = await _evaluate(
        clickhouse,
        original_truth,
        data,
        _requirements(("liq.depth_p1_vs_pre", "liq.spread_ratio")),
        config=config,
    )
    if case == "normal":
        assert Decimal(result["liquidity"]["aggregate_depth_p1_usd"]) == Decimal(440)
        assert result["liquidity"]["spread_ratio"] == 2


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "unknown_precision",
        "close_time",
        "stale_close",
        "late_close",
        "future_close",
        "wrong_token",
        "same_venue_instruments",
        "first_tie",
        "missing_flow",
    ],
)
async def test_hourly_reference_close_exact_instrument_selection(clickhouse, original_truth, case):
    data = _physical_snapshot(minutes=1440)
    if case == "unknown_precision":
        for row in data["klines"]:
            row.pop("open_time_prec")
    elif case == "close_time":
        for row in data["klines"]:
            stamp = datetime.fromisoformat(row.pop("open_time").replace("Z", "+00:00")) + timedelta(minutes=1)
            row.update(ts=iso(stamp - timedelta(minutes=1)), close_time=iso(stamp), close_time_prec="EXACT")
            row.pop("open_time_prec")
    elif case in {"stale_close", "late_close", "future_close"}:
        for row in data["klines"]:
            if row["open_time"] == iso(CUTOFF - timedelta(minutes=1)):
                if case == "stale_close":
                    row["complete"] = False
                elif case == "late_close":
                    row["received_ts"] = iso(CUTOFF + timedelta(minutes=3))
                else:
                    row["open_time"] = iso(CUTOFF)
    elif case == "wrong_token":
        for instrument in data["instruments"]:
            instrument["token_id"] = "OTHER"
    elif case == "same_venue_instruments":
        original = "binance:spot:T"
        replacement = "binance:spot:T-second"
        data["instruments"].append(
            {"instrument_id": replacement, "token_id": "T", "market_type": "SPOT", "venue": "binance"}
        )
        for collection in ("flow_bars", "klines"):
            data[collection].extend(
                [
                    dict(
                        row,
                        instrument_id=replacement,
                        **({"buy_qty": "20"} if collection == "flow_bars" else {"c": "20"}),
                    )
                    for row in list(data[collection])
                    if row["instrument_id"] == original
                ]
            )
    elif case == "first_tie":
        data["klines"] = list(reversed(data["klines"]))
    elif case == "missing_flow":
        data["flow_bars"].pop(0)
    await _evaluate(clickhouse, original_truth, data, _requirements(("price.close_vs_level",)))


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "missing_book",
        "source_interval",
        "poll_watermark",
        "future_watermark",
        "missing_oi",
        "unknown_tier",
    ],
)
async def test_selective_confidence_preserves_capture_frontier_and_source_poll(
    clickhouse, original_truth, case
):
    data = _physical_snapshot(minutes=60)
    if case == "missing_book":
        data["books"] = []
    elif case in {"source_interval", "missing_oi", "unknown_tier"}:
        derivative = _derivative_snapshot()
        for collection in ("instruments", "semantics", "oi", "funding"):
            data[collection].extend(derivative[collection])
        if case == "source_interval":
            for row in data["semantics"]:
                row.update(source_interval_s=3600, publish_lag_s=30, timestamp_semantics="BUCKET_END")
        elif case == "missing_oi":
            data["oi"] = []
        else:
            data["capture_metadata"] = {"tier": "UNKNOWN"}
    elif case in {"poll_watermark", "future_watermark"}:
        for row in data["klines"]:
            if row["open_time"] == iso(CUTOFF - timedelta(minutes=1)):
                row["open_time"] = iso(CUTOFF - timedelta(minutes=10))
        stamp = CUTOFF if case == "poll_watermark" else CUTOFF + timedelta(microseconds=1)
        data["capture_metadata"] = {
            "collections": {
                "klines": {
                    "observed_through": {iid["instrument_id"]: iso(stamp) for iid in data["instruments"]}
                }
            }
        }
    result = await _evaluate(clickhouse, original_truth, data, _requirements(("dc.class",)))
    assert result["data_confidence"]["derivatives_classification_ready"] is None
    assert result["data_confidence"]["derivatives_classification_ready_reason"] == "NOT_ESTIMABLE"


@pytest.mark.parametrize("seconds", [None, 899, 900, 86400, 86401])
async def test_primary_bounds_ignore_foreign_internal_scope_fields(clickhouse, original_truth, seconds):
    data = _snapshot()
    if seconds is None:
        data.pop("anchor")
    else:
        data["anchor"]["ts"] = iso(CUTOFF - timedelta(seconds=seconds))
    data.update(_primary="W24H", _event_anchor=iso(CUTOFF - timedelta(days=2)))
    await _evaluate(clickhouse, original_truth, data, _requirements(("dc.class",)))


@pytest.mark.parametrize(
    "entry",
    [10, {"cadence_s": 20}, {"cadence_s_by_metric": {"depth": 20}}, {"by_kind": {"BOOK": {"cadence_s": 20}}}],
)
async def test_depth_cadence_uses_exact_selective_field_scope(clickhouse, original_truth, entry):
    data = _physical_snapshot(minutes=0)
    data["capture_metadata"] = {"collections": {"books": entry}}
    data["semantics"] = [{"metric_key": "depth", "cadence_s": 1000}]
    for row in data["books"]:
        row.update(metric_key="depth", kind="BOOK")
        if row["exchange_ts"] == iso(CUTOFF):
            row["exchange_ts"] = iso(CUTOFF - timedelta(seconds=15))
    await _evaluate(
        clickhouse, original_truth, data, _requirements(("liq.depth_p1_vs_pre", "liq.spread_ratio"))
    )


@pytest.mark.parametrize(
    "case", ["absent", "string", "object", "global", "config", "embedded_boundary", "unicode"]
)
async def test_dependency_and_request_fingerprints_are_original_hash_preimages(
    clickhouse, original_truth, case
):
    data = _physical_snapshot(minutes=0)
    config = {}
    if case == "string":
        data["domain_dependency_fingerprints"] = {"depth": "retained-depth-head"}
    elif case == "object":
        data["domain_dependency_fingerprints"] = {"depth": {"revision": 7, "streams": ["B", "A"]}}
    elif case == "global":
        data["dependency_fingerprint"] = {"all": "captured", "frontier": 4}
    elif case == "config":
        config = {"depth_band_pct": 2, "config_version": "frozen"}
    elif case == "embedded_boundary":
        data["flow_bars"] = [
            {
                "instrument_id": "A:spot:T",
                "minute_start": iso(CUTOFF - timedelta(minutes=1)),
                "complete": True,
                "boundary_segments": [
                    {
                        "start": iso(CUTOFF - timedelta(seconds=30)),
                        "end": iso(CUTOFF),
                        "complete": True,
                        "buy_qty": "1",
                        "sell_qty": "0",
                        "sum_sq_qty": "1",
                    }
                ],
            }
        ]
    elif case == "unicode":
        data["domain_dependency_fingerprints"] = {"depth": {"source": "δ", "name": '"quoted"'}}
    requirements = _requirements(("liq.spread_ratio", "liq.depth_p1_vs_pre"))
    if case == "embedded_boundary":
        requirements = _requirements(("spot.classification",))
    await _evaluate(clickhouse, original_truth, data, requirements, config=config)


@pytest.mark.parametrize(
    "case",
    [
        "unsupported_metric",
        "unsupported_window",
        "event_no_anchor",
        "event_zero",
        "event_future",
        "duplicates",
        "no_requirements",
        "mixed",
    ],
)
async def test_pending_contract_and_unsupported_requirements(clickhouse, original_truth, case):
    data = _snapshot()
    requirements = _requirements(("liq.depth_p1_vs_pre",))
    if case == "unsupported_metric":
        requirements = [{"metric": "price.close_vs_level'); SELECT secret", "window": "W1H"}]
    elif case == "unsupported_window":
        requirements[0]["window"] = "W7D"
    elif case == "event_no_anchor":
        data.pop("anchor")
        requirements[0]["window"] = "EVENT"
    elif case in {"event_zero", "event_future"}:
        data["anchor"]["ts"] = iso(CUTOFF if case == "event_zero" else CUTOFF + timedelta(seconds=1))
        requirements[0]["window"] = "EVENT"
    elif case == "duplicates":
        requirements *= 3
    elif case == "no_requirements":
        requirements = []
    else:
        requirements += [
            {"metric": "unknown", "window": "W1H"},
            {"metric": "spot.classification", "window": "bad"},
        ]
    result = await _evaluate(clickhouse, original_truth, data, requirements)
    if case not in {"no_requirements"}:
        assert result["_condition_evaluation"]["unavailable_metrics"]


@pytest.mark.parametrize("case", ["anchored", "no_anchor", "negative", "future", "columnar_order"])
async def test_relative_complete_condition_output(clickhouse, original_truth, case):
    data = _relative_snapshot()
    if case == "no_anchor":
        data.pop("anchor")
    elif case == "negative":
        data["expected_direction"] = "LOSER"
    elif case == "future":
        data["klines"].append(
            {"instrument_id": "binance:spot:T", "open_time": iso(CUTOFF), "complete": True, "c": "999999"}
        )
    elif case == "columnar_order":
        data["klines"] = [row for row in data["klines"] if row["instrument_id"] != "binance:spot:BTC"]
        micros = int(CUTOFF.timestamp() * 1000000)
        data["stat_price_series"] = {
            iid: {
                "token_id": "BTC",
                "time_basis": "CLOSE_TIME",
                "times_us": array("q", [micros - 3600000000, micros]),
                "values": array("d", [100, 101]),
                "complete": bytearray([1, 1]),
            }
            for iid in ("z:spot:BTC", "a:spot:BTC")
        }
    result = await _evaluate(
        clickhouse, original_truth, data, _requirements(("price.repricing_band", "price.vs_anchor_pct"))
    )
    if case == "columnar_order":
        assert result["relative"]["outcome_btc_reference"]["instrument_id"] == "z:spot:BTC"
    elif case == "anchored":
        assert result["relative"]["windows"]["EVENT"]["r_raw"] == pytest.approx(0.1)
        assert result["relative"]["repricing_band"] is not None


@pytest.mark.parametrize("metric", ["mt.move_class", "mt.divergence_state"])
async def test_provided_native_body_composite_complete_oracle(clickhouse, original_truth, metric):
    data = _physical_snapshot(minutes=60)
    data["capture_status"] = {"status": "PARTIAL", "reason": "RETAINED"}
    full = _full_body(original_truth, data)
    await _evaluate(
        clickhouse,
        original_truth,
        data,
        _requirements((metric, "spot.classification", "liq.spread_ratio")),
        result=full,
    )


async def test_composite_missing_result_composes_real_full_native_engine(clickhouse, original_truth):
    importlib.import_module("tank.analysis.truth_sql_engine")
    data = _physical_snapshot(minutes=60)
    requirements = _requirements(
        ("mt.move_class", "mt.divergence_state", "deriv.oi_class", "price.vs_anchor_pct")
    )
    await _evaluate(clickhouse, original_truth, data, requirements)


@pytest.mark.parametrize(
    "field", ["spot", "derivatives", "liquidity", "supply", "relative", "data_confidence", "primary_window"]
)
async def test_partial_supplied_markettruth_body_is_refused(clickhouse, original_truth, field):
    data = _snapshot()
    full = _full_body(original_truth, data)
    full.pop(field)
    sql, parameters = native.query(data, _requirements(("mt.move_class",)), result=full)
    with pytest.raises(RuntimeError, match="complete native MarketTruth body"):
        await clickhouse.rows(sql, parameters)


@pytest.mark.parametrize(
    "metric",
    ["liq.spread_ratio", "spot.classification", "deriv.oi_class", "dc.class", "price.close_vs_level"],
)
async def test_physical_condition_never_invokes_full_engine(clickhouse, original_truth, monkeypatch, metric):
    import tank.analysis as analysis_package

    engine = ModuleType("tank.analysis.truth_sql_engine")

    def forbidden(*args, **kwargs):
        pytest.fail("a selective physical condition invoked full native MarketTruth orchestration")

    engine.query = forbidden
    monkeypatch.setitem(sys.modules, "tank.analysis.truth_sql_engine", engine)
    monkeypatch.setattr(analysis_package, "truth_sql_engine", engine, raising=False)
    data = _physical_snapshot(minutes=60)
    await _evaluate(clickhouse, original_truth, data, _requirements((metric,)))


@pytest.mark.parametrize("value", ["1000000000000000000000000", "0.0000000000000000000000001", "1e100"])
async def test_outside_native_financial_range_is_refused(clickhouse, value):
    data = _physical_snapshot(minutes=0)
    data["books"][-1]["depth"]["ask"]["p1"]["usd"] = value
    sql, parameters = native.query(data, _requirements(("liq.depth_p1_vs_pre",)))
    with pytest.raises(RuntimeError, match="native financial range"):
        await clickhouse.rows(sql, parameters)


@pytest.mark.parametrize(
    "field,value",
    [
        ("cutoff", "0001-01-01T00:00:00Z"),
        ("cutoff", "2500-01-01T00:00:00Z"),
        ("cutoff", "not-a-date"),
        ("assembled_at", "not-a-date"),
    ],
)
async def test_unrepresentable_or_invalid_dates_refused_not_silently_dropped(clickhouse, field, value):
    data = _physical_snapshot(minutes=0)
    data[field] = value
    sql, parameters = native.query(data, _requirements(("liq.depth_p1_vs_pre",)))
    with pytest.raises(RuntimeError, match="timestamp outside native supported range|cutoff is invalid"):
        await clickhouse.rows(sql, parameters)


@pytest.mark.parametrize(
    "case", ["conflicting_cutoff", "unsupported_precision", "conflicting_oi", "nonbucket_anchor"]
)
async def test_timestamp_precision_refusals(clickhouse, case):
    data = _derivative_snapshot()
    if case == "conflicting_cutoff":
        data["cutoff_precision"] = "BUCKET(1m)"
    elif case == "unsupported_precision":
        data["cutoff_prec"] = "GUESS"
    elif case == "conflicting_oi":
        data["oi"][-1]["ts_precision"] = "BUCKET(1m)"
    else:
        data["cutoff_bucket_anchor"] = "END"
    sql, parameters = native.query(data, _requirements(("deriv.native_oi_chg_pct",)))
    with pytest.raises(RuntimeError, match="precision|anchor"):
        await clickhouse.rows(sql, parameters)


def _cache_snapshot():
    data = _physical_snapshot(minutes=60)
    derivatives = _derivative_snapshot()
    for field in ("oi", "funding", "semantics"):
        data[field] = derivatives[field]
    data["instruments"].extend(derivatives["instruments"])
    for venue in ("binance", "okx"):
        for stamp, price in ((CUTOFF - timedelta(hours=1), "10"), (CUTOFF, "11")):
            data["reference_points"].append(
                {
                    "instrument_id": f"{venue}:spot:T",
                    "boundary": iso(stamp),
                    "complete": True,
                    "last_trade": {
                        "exchange_ts": iso(stamp - timedelta(microseconds=1)),
                        "exchange_ts_prec": "EXACT",
                        "price": price,
                    },
                }
            )
    return data


def _compare_caches(actual, expected, original):
    assert len(actual) == len(expected) <= 128
    for (native_key, native_value), (original_key, original_value) in zip(
        actual.items(), expected.items(), strict=True
    ):
        assert json.loads(native_key) == list(original_key)
        value = original.jsonable(deepcopy(original_value))
        if original_key[:2] == ("condition-state-node", "spot"):
            value = dict(zip(("spots", "windows", "primary", "instruments"), value, strict=True))
        _compare_value(native_value, value, "cache." + str(original_key[:2]))


async def _cached_evaluate(
    clickhouse, original, data, requirements, actual_cache, expected_cache, baselines, *, config=None
):
    from tank.analysis import truth_sql_engine

    expected = _condition_contract_oracle(
        original, data, requirements, config=config, cache=expected_cache, baseline_cache=baselines
    )
    actual = await truth_sql_engine.evaluate_conditions(
        clickhouse, data, requirements, config, cache=actual_cache
    )
    _compare_value(actual, expected)
    _compare_caches(actual_cache, expected_cache, original)
    assert not any(key.startswith("_native_") for key in actual)
    return actual


@pytest.mark.parametrize(
    "metrics",
    [
        ("spot.classification", "spot.cohort.direction"),
        ("deriv.oi_class", "deriv.native_oi_chg_pct"),
        ("deriv.funding_vs_normal",),
        ("price.close_vs_level",),
        ("dc.class",),
        ("liq.spread_ratio", "liq.depth_p1_vs_pre"),
        ("price.repricing_band", "price.vs_anchor_pct"),
        ("mt.move_class", "mt.divergence_state"),
    ],
)
async def test_native_domain_cache_repeated_and_changed_requirements_complete_oracle(
    clickhouse, original_truth, metrics
):
    data = _cache_snapshot()
    cache, expected_cache, baselines = {}, {}, {}
    first = _requirements(metrics[:1])
    await _cached_evaluate(clickhouse, original_truth, data, first, cache, expected_cache, baselines)
    second = await _cached_evaluate(clickhouse, original_truth, data, first, cache, expected_cache, baselines)
    assert all(proof["reused"] for proof in second["_condition_evaluation"]["proofs"])
    changed = _requirements(metrics, ("W1H", "W15M"))
    third = await _cached_evaluate(
        clickhouse, original_truth, data, changed, cache, expected_cache, baselines
    )
    assert all(
        proof["reused"] for proof in third["_condition_evaluation"]["proofs"] if proof["window"] == "W1H"
    )
    fourth = await _cached_evaluate(
        clickhouse, original_truth, data, changed, cache, expected_cache, baselines
    )
    assert all(proof["reused"] for proof in fourth["_condition_evaluation"]["proofs"])


@pytest.mark.parametrize("metric,domain", [("spot.classification", "spot"), ("liq.spread_ratio", "depth")])
async def test_native_hit_uses_cached_raw_value_not_recomputed_value_with_a_hit_flag(
    clickhouse, original_truth, metric, domain
):
    data = _physical_snapshot(minutes=60)
    data["domain_dependency_fingerprints"] = {domain: "authoritative-retained-head"}
    cache, expected_cache, baselines = {}, {}, {}
    requirements = _requirements((metric,))
    first = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines
    )
    for row in data["flow_bars"]:
        row.update(buy_qty="1", sell_qty="9", buy_notional="10", sell_notional="90")
    for row in data["books"]:
        if row["exchange_ts"] == iso(CUTOFF):
            row["spread_bps"] = 7
    hit = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines
    )
    cold = original_truth.compute_condition_values(deepcopy(data), requirements, cache={}, baseline_cache={})
    field = "spot" if domain == "spot" else "liquidity"
    _compare_value(hit[field], first[field])
    assert hit[field] != cold[field]
    assert hit["_condition_evaluation"]["proofs"][0]["reused"] is True
    if domain == "spot":
        data["flow_bars"][0]["buy_qty"] = "1e100"
    else:
        data["books"][-1]["depth"]["ask"]["p1"]["usd"] = "1e100"
    await _cached_evaluate(clickhouse, original_truth, data, requirements, cache, expected_cache, baselines)


@pytest.mark.parametrize("change", ["dependency", "config", "expected_direction", "cutoff"])
async def test_native_condition_cache_key_invalidation_complete_oracle(clickhouse, original_truth, change):
    data = _physical_snapshot(minutes=15)
    data["domain_dependency_fingerprints"] = {"spot": {"head": "original"}}
    cache, expected_cache, baselines = {}, {}, {}
    requirements = _requirements(("spot.classification",))
    config = {"config_version": "original"}
    await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines, config=config
    )
    if change == "dependency":
        data["domain_dependency_fingerprints"]["spot"] = {"head": "changed"}
    elif change == "config":
        config = {"config_version": "changed"}
    elif change == "expected_direction":
        data["expected_direction"] = "LOSER"
    else:
        data["cutoff"] = iso(CUTOFF + timedelta(seconds=1))
    result = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines, config=config
    )
    assert result["_condition_evaluation"]["proofs"][0]["reused"] is False


@pytest.mark.parametrize(
    "domain,metric", [("oi", "deriv.native_oi_chg_pct"), ("funding", "deriv.funding_vs_normal")]
)
@pytest.mark.parametrize("change", ["capture_metadata", "assembled_at"])
async def test_reused_unmasked_derivative_cache_is_remasked_from_current_evidence(
    clickhouse, original_truth, domain, metric, change
):
    data = _derivative_snapshot()
    data["domain_dependency_fingerprints"] = {domain: "unchanged-raw-domain"}
    for row in data[domain]:
        if row["ts"] == iso(CUTOFF):
            row["ts"] = iso(CUTOFF - timedelta(seconds=30))
            row["received_ts"] = iso(CUTOFF + timedelta(minutes=1))
    data["capture_metadata"] = {"collections": {domain: {"cadence_s": 120}}}
    cache, expected_cache, baselines = {}, {}, {}
    requirements = _requirements((metric,))
    initial = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines
    )
    assert initial["_condition_evaluation"]["proofs"][0]["available"] is True
    raw = deepcopy(cache)
    if change == "capture_metadata":
        data["capture_metadata"]["collections"][domain]["cadence_s"] = 5
    else:
        data["assembled_at"] = iso(CUTOFF)
    stale = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines
    )
    assert stale["_condition_evaluation"]["proofs"][0]["reused"] is True
    assert stale["_condition_evaluation"]["proofs"][0]["available"] is False
    assert cache == raw
    data["capture_metadata"]["collections"][domain]["cadence_s"] = 120
    data["assembled_at"] = iso(CUTOFF + timedelta(minutes=2))
    restored = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines
    )
    assert restored["_condition_evaluation"]["proofs"][0]["available"] is True
    assert cache == raw


@pytest.mark.parametrize("metric", ["spot.classification", "mt.move_class"])
async def test_caller_mutation_cannot_poison_native_domain_or_state_cache(clickhouse, original_truth, metric):
    data = _cache_snapshot()
    cache, expected_cache, baselines = {}, {}, {}
    requirements = _requirements((metric, "deriv.funding_vs_normal", "deriv.oi_class"))
    result = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, expected_cache, baselines
    )
    held = deepcopy(cache)
    result["spot"].clear()
    result["derivatives"]["windows"]["W1H"]["per_venue"].clear()
    result["_condition_evaluation"]["proofs"][0]["observations"].append({"poison": True})
    if "liquidity" in result:
        result["liquidity"].clear()
    await _cached_evaluate(clickhouse, original_truth, data, requirements, cache, expected_cache, baselines)
    assert cache == held


async def test_native_cache_fifo_over_128_including_hidden_state_nodes(clickhouse, original_truth):
    data = _snapshot()
    cache, expected_cache, baselines = {}, {}, {}
    state = _requirements(("mt.move_class",))
    await _cached_evaluate(clickhouse, original_truth, data, state, cache, expected_cache, baselines)
    assert len(cache) == 5
    oldest = next(iter(cache))
    requirements = _requirements(("spot.classification",), WINDOWS)
    for revision in range(26):
        data["domain_dependency_fingerprints"] = {"spot": f"revision-{revision}"}
        await _cached_evaluate(
            clickhouse, original_truth, data, requirements, cache, expected_cache, baselines
        )
    assert len(cache) == 128 and oldest not in cache
    final = await _cached_evaluate(clickhouse, original_truth, data, state, cache, expected_cache, baselines)
    assert final["_condition_evaluation"]["proofs"][0]["reused"] is False


@pytest.mark.parametrize("change", ["outer_head", "node_head", "state_window", "overlay"])
async def test_hidden_state_nodes_and_unmasked_overlays_share_original_cache_scope(
    clickhouse, original_truth, change
):
    data = _cache_snapshot()
    data["domain_dependency_fingerprints"] = {
        "state": "state-1",
        "spot": "spot-1",
        "derivatives": "deriv-1",
        "relative": "relative-1",
        "supply": "supply-1",
    }
    cache, expected_cache, baselines = {}, {}, {}
    requirements = _requirements(("mt.move_class",))
    await _cached_evaluate(clickhouse, original_truth, data, requirements, cache, expected_cache, baselines)
    if change == "outer_head":
        data["domain_dependency_fingerprints"]["state"] = "state-2"
        data["supply"] = [
            {
                "kind": "FLOW",
                "ts": iso(CUTOFF - timedelta(seconds=1)),
                "qty_native": "10",
                "classification": "TREASURY_INTERNAL",
            }
        ]
    elif change == "node_head":
        data["domain_dependency_fingerprints"].update(state="state-2", supply="supply-2")
    elif change == "state_window":
        requirements = _requirements(("mt.divergence_state",), ("W15M",))
    else:
        requirements = _requirements(("deriv.funding_vs_normal", "deriv.oi_class", "mt.divergence_state"))
        data["capture_metadata"] = {"collections": {"oi": {"cadence_s": 0}, "funding": {"cadence_s": 0}}}
    await _cached_evaluate(clickhouse, original_truth, data, requirements, cache, expected_cache, baselines)


@pytest.mark.parametrize("anchor", ["absent", "equal", "future", "short", "primary_profile"])
async def test_direct_composite_binds_condition_state_not_full_assessment(clickhouse, original_truth, anchor):
    data = _physical_snapshot(minutes=60)
    if anchor == "absent":
        data.pop("anchor")
    elif anchor in ("equal", "future", "short"):
        data["anchor"]["ts"] = iso(
            CUTOFF + timedelta(seconds={"equal": 0, "future": 60, "short": -60}[anchor])
        )
    else:
        for bar in data["flow_bars"]:
            bar["size_buckets"] = {
                "B3": {"buy_notional": "30", "sell_notional": "5"},
                "B4": {"buy_notional": "5", "sell_notional": "0"},
            }
        data["semantics"] = [
            {
                "venue": venue,
                "market_type": "SPOT",
                "metric_family": "aggressor",
                "trade_aggregation": "PER_FILL",
            }
            for venue in ("binance", "okx")
        ]
    result = await _evaluate(
        clickhouse, original_truth, data, _requirements(("mt.move_class", "mt.divergence_state"))
    )
    if anchor == "primary_profile":
        assert result["spot"][result["primary_window"]]["size_profile"] is not None
        assert all(
            pack["size_profile"] is None
            for window, pack in result["spot"].items()
            if window != result["primary_window"]
        )


@pytest.mark.parametrize("seconds", [0, 60])
@pytest.mark.parametrize("metric", ["mt.move_class", "mt.divergence_state"])
def test_frozen_nonpast_anchor_minimal_helper_and_wrapper_limit(original_truth, seconds, metric):
    data = _physical_snapshot(minutes=60)
    data["anchor"]["ts"] = iso(CUTOFF + timedelta(seconds=seconds))
    cache = {}
    state = original_truth.condition_market_state(deepcopy(data), CUTOFF, cache)
    assert state == {
        "move_class": "UNKNOWN",
        "divergence": {"state": "UNKNOWN"},
        "data_confidence": {"class": "LOW"},
    }
    assert cache == {}
    with pytest.raises(KeyError, match="^'derivatives'$"):
        original_truth.compute_condition_values(deepcopy(data), _requirements((metric,)))


@pytest.mark.parametrize("seconds", [0, 60])
@pytest.mark.parametrize(
    "metrics", [("mt.move_class",), ("mt.divergence_state",), ("mt.move_class", "mt.divergence_state")]
)
async def test_minimal_anchor_public_and_private_native_columns_complete_contract(
    clickhouse, original_truth, seconds, metrics
):
    data = _physical_snapshot(minutes=60)
    data["anchor"]["ts"] = iso(CUTOFF + timedelta(seconds=seconds))
    requirements = _requirements(metrics)
    oracle_cache = {}
    expected = _condition_contract_oracle(original_truth, data, requirements, cache=oracle_cache)
    for private in (False, True):
        sql, parameters = native.query(data, requirements, cache_transport=private)
        rows = await clickhouse.rows(sql, parameters)
        assert len(rows) == 1 and set(rows[0]) == ({"result", "condition_cache"} if private else {"result"})
        actual = json.loads(rows[0]["result"])
        _compare_value(actual, expected)
        assert set(actual) == {
            "cutoff",
            "cutoff_prec",
            "cutoff_prec_reason",
            "spot",
            "derivatives",
            "relative",
            "move_class",
            "divergence",
            "data_confidence",
            "_condition_evaluation",
        }
        assert actual["spot"] == actual["relative"] == {}
        assert actual["derivatives"] == {"windows": {}}
        assert actual["move_class"] == "UNKNOWN"
        assert actual["divergence"] == {"state": "UNKNOWN"}
        assert actual["data_confidence"] == {"class": "LOW"}
        assert actual["_condition_evaluation"]["unavailable_metrics"] == []
        assert all(
            proof["available"] is True
            and proof["reason"] is None
            and proof["observations"] == []
            and proof["coverage"] is None
            for proof in actual["_condition_evaluation"]["proofs"]
        )
        if private:
            records = json.loads(rows[0]["condition_cache"])
            assert len(records) == 1 and set(records[0]) == {"key", "value"}
            _compare_caches(
                {record["key"]: record["value"] for record in records}, oracle_cache, original_truth
            )
            assert records[0]["value"] == {
                "move_class": "UNKNOWN",
                "divergence": {"state": "UNKNOWN"},
                "data_confidence": {"class": "LOW"},
            }
            assert json.loads(records[0]["key"])[:4] == ["state", "W1H", 14400, iso(CUTOFF)]


@pytest.mark.parametrize("seconds", [0, 60])
@pytest.mark.parametrize("mixed", [False, True])
async def test_minimal_anchor_all_rolling_windows_raw_reuse_and_alias_independence(
    clickhouse, original_truth, seconds, mixed
):
    data = _cache_snapshot()
    data["anchor"]["ts"] = iso(CUTOFF + timedelta(seconds=seconds))
    requirements = _requirements(("mt.move_class", "mt.divergence_state"), WINDOWS[:-1])
    if mixed:
        requirements += _requirements(
            (
                "deriv.funding_vs_normal",
                "deriv.oi_class",
                "dc.class",
                "price.close_vs_level",
                "liq.spread_ratio",
                "spot.classification",
            )
        )
        requirements += _requirements(("mt.move_class", "mt.divergence_state"), ("EVENT", "W7D"))
        requirements += [{"metric": "unknown", "window": "W1H"}]
    requirements += deepcopy(requirements[:2])
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(
        clickhouse, original_truth, data, list(reversed(requirements)), cache, oracle_cache, baselines
    )
    state_proofs = [
        proof
        for proof in first["_condition_evaluation"]["proofs"]
        if proof["metric"] in {"mt.move_class", "mt.divergence_state"}
    ]
    assert [proof["reused"] for proof in state_proofs] == [False] * 4 + [True] * 4
    raw_state = [(json.loads(key), value) for key, value in cache.items() if json.loads(key)[0] == "state"]
    assert [parts[1] for parts, _ in raw_state] == ["W15M", "W1H", "W24H", "W4H"]
    assert all(len(parts) == 7 and parts[2:4] == [14400, iso(CUTOFF)] for parts, _ in raw_state)
    assert all(
        value
        == {"move_class": "UNKNOWN", "divergence": {"state": "UNKNOWN"}, "data_confidence": {"class": "LOW"}}
        for _, value in raw_state
    )
    assert not any(json.loads(key)[0] == "condition-state-node" for key in cache)
    assert len({id(value["divergence"]) for _, value in raw_state}) == 4
    assert len({id(value["data_confidence"]) for _, value in raw_state}) == 4
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    first["divergence"]["state"] = "CALLER_MUTATION"
    first["data_confidence"]["class"] = "HIGH"
    first["derivatives"]["windows"].clear()
    first["_condition_evaluation"]["proofs"][0]["observations"].append({"caller": "mutation"})
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
    second = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, oracle_cache, baselines
    )
    assert all(proof["reused"] for proof in second["_condition_evaluation"]["proofs"])
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
    assert second["divergence"] == {"state": "UNKNOWN"}
    assert second["data_confidence"] == {"class": "LOW"}
    assert all(
        second["divergence"] is not value["divergence"]
        and second["data_confidence"] is not value["data_confidence"]
        for _, value in raw_state
    )
    if not mixed:
        assert second["spot"] == second["relative"] == {}
        assert second["derivatives"] == {"windows": {}}
        assert second["_condition_evaluation"]["unavailable_metrics"] == []
    else:
        assert [
            pending
            for pending in second["_condition_evaluation"]["unavailable_metrics"]
            if pending["metric"] in {"mt.move_class", "mt.divergence_state", "unknown"}
        ] == [
            {"metric": metric, "window": window, "reason": "NOT_SUPPORTED"}
            for metric, window in sorted(
                {
                    (row["metric"], row["window"])
                    for row in requirements
                    if row["window"] in {"EVENT", "W7D"} or row["metric"] == "unknown"
                }
            )
        ]


@pytest.mark.parametrize("seconds", [0, 60])
async def test_minimal_anchor_supplied_bundle_cannot_publish_hidden_nodes(
    clickhouse, original_truth, seconds
):
    data = _snapshot()
    data["anchor"]["ts"] = iso(CUTOFF + timedelta(seconds=seconds))
    requirements = _requirements(("mt.move_class", "mt.divergence_state"))
    oracle_cache = {}
    expected = _condition_contract_oracle(original_truth, data, requirements, cache=oracle_cache)
    sql, parameters = native.query(
        data,
        requirements,
        result={"_native_condition_cache": [{"key": "must-not-be-published", "value": {}}]},
        cache_transport=True,
    )
    rows = await clickhouse.rows(sql, parameters)
    assert len(rows) == 1 and set(rows[0]) == {"result", "condition_cache"}
    _compare_value(json.loads(rows[0]["result"]), expected)
    records = json.loads(rows[0]["condition_cache"])
    assert len(records) == 1 and json.loads(records[0]["key"])[:2] == ["state", "W1H"]
    _compare_caches({record["key"]: record["value"] for record in records}, oracle_cache, original_truth)


@pytest.mark.parametrize("seconds", [0, 60])
async def test_nonpositive_event_composite_stays_unsupported_without_raw_state(
    clickhouse, original_truth, seconds
):
    data = _snapshot()
    data["anchor"]["ts"] = iso(CUTOFF + timedelta(seconds=seconds))
    requirements = _requirements(("mt.move_class", "mt.divergence_state"), ("EVENT",))
    cache, oracle_cache, baselines = {}, {}, {}
    result = await _cached_evaluate(
        clickhouse, original_truth, data, requirements, cache, oracle_cache, baselines
    )
    assert cache == oracle_cache == {}
    assert result["spot"] == result["relative"] == {} and result["derivatives"] == {"windows": {}}
    assert set(result) == {
        "cutoff",
        "cutoff_prec",
        "cutoff_prec_reason",
        "spot",
        "derivatives",
        "relative",
        "_condition_evaluation",
    }
    assert result["_condition_evaluation"]["proofs"] == []
    assert result["_condition_evaluation"]["unavailable_metrics"] == [
        {"metric": metric, "window": "EVENT", "reason": "NOT_SUPPORTED"}
        for metric in ("mt.divergence_state", "mt.move_class")
    ]
