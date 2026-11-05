import builtins
import json
import os
import subprocess
import sys
from array import array
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from types import ModuleType

import pytest
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.analysis import truth_sql_support as native
from tank.util import iso, parse_time

pytestmark = pytest.mark.integration

ORIGINAL_COMMIT = "ef3b43e8457622d2f3c2c86926de41aecee101d6"
CUTOFF = datetime(2026, 10, 21, 12, tzinfo=UTC)
WINDOWS = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400, "EVENT": 7200}


@pytest.fixture(scope="module")
def original_truth():
    root = Path(__file__).resolve().parents[1]
    modules = {}
    original_import = builtins.__import__

    def load(name):
        if name in modules:
            return modules[name]
        module = ModuleType("_support_original_" + name)
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
        "assembled_at": iso(CUTOFF + timedelta(minutes=2)),
        "anchor": {"ts": iso(CUTOFF - timedelta(hours=2)), "type": "KNOWN_TO_US_AT"},
        "_primary": "EVENT",
        "instruments": [],
        "flow_bars": [],
        "klines": [],
        "books": [],
        "oi": [],
        "funding": [],
        "liquidations": [],
        "tickers": [],
        "supply": [],
        "semantics": [],
    }


def _inputs():
    spots = {
        name: {
            "venues": [],
            "coverage": {"n_complete": 2, "exact_majority": True, "covered_share_est": 1.0},
            "size_profile": {
                "large_ticket_buy_majority": True,
                "large_tickets_minority": False,
                "sell_share_rising": None,
            },
        }
        for name in WINDOWS
    }
    derivatives = {name: {"per_venue": [], "synchronized": None} for name in WINDOWS}
    return spots, derivatives


def _balance(venue, quantity, *, complete=True, commit="frozen-wallet-list"):
    return {
        "venue": venue,
        "chainid": 1,
        "contract": "T",
        "wallet_list_commit": commit,
        "qty_native": quantity,
        "complete": complete,
    }


def _supply_snapshot():
    data = _snapshot()
    data["supply"] = [
        {
            "kind": "EXCHANGE_INVENTORY",
            "ts": iso(CUTOFF - timedelta(days=day)),
            "balances": [_balance("B", str(100 - day)), _balance("A", str(200 - day))],
            "coverage_errors": [],
            "unsupported_venues": ["unsupported"],
        }
        for day in range(9, -1, -1)
    ]
    data["supply"].extend(
        [
            {
                "kind": "WALLET_FLOWS",
                "ts": iso(CUTOFF),
                "flows": [
                    {
                        "ts": iso(CUTOFF - timedelta(minutes=10)),
                        "classification": "TREASURY_INTERNAL",
                        "reaches_exchange": True,
                        "qty_native": "12.25",
                        "wallet_list_commit": "captured",
                    },
                    {
                        "ts": iso(CUTOFF - timedelta(hours=3)),
                        "classification": "CEX_CUSTODY_MIGRATION",
                        "qty_native": "4",
                    },
                    {"ts": iso(CUTOFF), "qty_native": "999"},
                ],
            },
            {
                "kind": "FLOW",
                "ts": iso(CUTOFF - timedelta(minutes=30)),
                "class": "MM_INVENTORY",
                "to_exchange": True,
                "qty_native": "2.5",
            },
            {
                "kind": "UNLOCKS",
                "ts": iso(CUTOFF),
                "unlocks": [
                    {
                        "date": iso(CUTOFF),
                        "qty_native": "1000000",
                        "pct_circulating": 0.025,
                        "recipient_class": "TEAM",
                        "transferable": True,
                        "source_url": "https://example.test/unlock",
                    },
                    {"date": iso(CUTOFF + timedelta(days=14)), "pct_circulating": 0.01},
                    {"date": iso(CUTOFF + timedelta(days=14, microseconds=1))},
                ],
            },
            {
                "kind": "LENDING",
                "ts": iso(CUTOFF - timedelta(days=1)),
                "pools": [{"pool_id": "a", "utilization": 0.4, "apy": 0.08}],
            },
            {
                "kind": "LENDING",
                "ts": iso(CUTOFF),
                "pools": [
                    {"pool_id": "a", "utilization": 0.6, "apy": 0.12, "borrow_apr": 99},
                    {"pool_id": "b", "utilization": 0.2},
                ],
            },
            {
                "kind": "CHAIN_LIQUIDITY",
                "chain": "Ethereum",
                "ts": iso(CUTOFF),
                "series": [
                    {"ts": iso(CUTOFF - timedelta(days=7)), "supply_usd": "10000000000000000.01"},
                    {"ts": iso(CUTOFF - timedelta(days=1)), "supply_usd": "10000000000000000.02"},
                    {"ts": iso(CUTOFF), "supply_usd": "10000000000000000.03"},
                    {"ts": iso(CUTOFF + timedelta(microseconds=1)), "supply_usd": "999"},
                ],
            },
            {
                "kind": "STABLECOIN_CHAIN",
                "chain": "Arbitrum",
                "ts": iso(CUTOFF),
                "supply_usd": "200",
                "source": "captured",
            },
            {
                "kind": "FUNDAMENTALS",
                "ts": iso(CUTOFF - timedelta(minutes=1)),
                "received_ts": iso(CUTOFF),
                "identity_evidence": {"protocol": "parent", "scope": "captured", "unicode": "δ"},
                "protocols": {
                    "z-child": {
                        "dailyHoldersRevenue": {
                            "total24h": "12.3400",
                            "total30d": "1234.56",
                            "source_url": "https://example.test/revenue/z",
                            "totalDataChart": [
                                [iso(CUTOFF - timedelta(days=1)), "10"],
                                [iso(CUTOFF), "-1.25"],
                                [iso(CUTOFF + timedelta(seconds=1)), "999999"],
                                [True, "2"],
                                [iso(CUTOFF), True],
                                ["not-a-time", "3"],
                                [iso(CUTOFF), "NaN"],
                            ],
                        }
                    },
                    "a-parent": {
                        "dailyHoldersRevenue": {
                            "total24h": "0",
                            "total30d": None,
                            "totalDataChart": [[iso(CUTOFF), "1e-8"]],
                            "reason": "PARTIAL_CAPTURE",
                        }
                    },
                },
            },
        ]
    )
    return data


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
    elif isinstance(expected, bool):
        assert actual is expected, path
    elif isinstance(expected, Decimal):
        assert isinstance(actual, str), path
        with localcontext() as context:
            context.prec = 100
            assert abs(Decimal(actual) - expected) <= Decimal("1e-18"), path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-10, abs=1e-12), path
    elif isinstance(expected, str) and path.endswith(".value") and "holders_revenue" in path:
        try:
            assert Decimal(actual) == Decimal(expected), path
        except InvalidOperation:
            assert actual == expected, path
    else:
        assert actual == expected, path


def _expected(original, data, spots, derivatives, config):
    end = parse_time(data["cutoff"])
    anchor = data.get("anchor")
    anchor = anchor.get("ts") if isinstance(anchor, dict) else anchor
    anchor = data.get("_event_anchor", anchor)
    start = parse_time(anchor) if anchor else end - timedelta(hours=4)
    primary = data.get("_primary") or (
        "EVENT" if anchor and 900 <= (end - start).total_seconds() <= 86400 else "W4H"
    )
    derivative_map = derivatives.get("windows", derivatives) if derivatives is not None else {}
    supply_windows = {}
    derivative_windows = {}
    support_windows = {}
    context = original._PARAMETERS.set(config)
    try:
        for window, seconds in WINDOWS.items():
            at = start if window == "EVENT" else end - timedelta(seconds=seconds)
            scoped = dict(data, _primary=window)
            supply = original.supply_truth(scoped, at, end)
            derivative = deepcopy(derivative_map.get(window, {"per_venue": []}))
            for venue in derivative.get("per_venue", []):
                for key in ("basis_bps", "funding_8h_eq"):
                    if isinstance(venue.get(key), str):
                        venue[key] = Decimal(venue[key])
            spot = deepcopy((spots or {}).get(window, {}))
            feature = original.support_features(scoped, at, end, spot, derivative, {}, supply)
            derivative["support_features"] = feature
            if "per_venue" not in derivative:
                derivative["per_venue"] = []
            supply.update(window=window, start=iso(at), cutoff=iso(end))
            if window == "EVENT" and not anchor:
                supply = {"window": window, "start": None, "cutoff": iso(end), "reason": "INCOMPLETE"}
                derivative = deepcopy(derivative_map.get(window, {}))
                feature = None
            supply_windows[window] = supply
            derivative_windows[window] = derivative
            support_windows[window] = feature
        spot = deepcopy((spots or {}).get(primary, {}))
        spot.setdefault("coverage", {"n_complete": 0, "covered_share_est": None})
        confidence = original.data_confidence(data, spot, derivative_windows[primary], end)
        return {
            "supply": dict(supply_windows[primary], windows=supply_windows),
            "derivatives": dict(derivative_windows[primary], windows=derivative_windows),
            "support_features": support_windows,
            "data_confidence": confidence,
        }
    finally:
        original._PARAMETERS.reset(context)


async def _compare(clickhouse, original_truth, data, spots=None, derivatives=None, config=None):
    config = config or {}
    expected = _expected(original_truth, deepcopy(data), spots, derivatives, config)
    statement, parameters = native.query(data, config, spots=spots, derivatives=derivatives)
    rows = await clickhouse.rows(statement, parameters)
    assert len(rows) == 1
    actual = json.loads(rows[0]["result"])
    _assert_complete(actual, expected)
    return actual


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "scientific_balances_and_revenue",
        "missing",
        "future_fundamental",
        "future_receipt",
        "invalid_receipt",
        "stale_fundamental",
        "zero_revenue",
        "negative_revenue",
        "unavailable_fundamental",
        "unavailable_protocol",
        "no_protocols",
        "no_chart",
        "only_chart",
        "full_coverage",
        "provider_shutdown",
        "changed_wallet_cohort",
        "partial_balance",
        "unavailable_prior_inventory",
        "inventory_asof_expired",
        "legacy_inventory",
        "no_lending_prior",
        "chain_asof_expired",
        "duplicate_chain_time",
        "out_of_order",
        "missing_anchor",
    ],
)
async def test_native_supply_complete_frozen_original(clickhouse, original_truth, case, record_property):
    data = _supply_snapshot()
    spots, derivatives = _inputs()
    fundamental = data["supply"][-1]
    if case == "scientific_balances_and_revenue":
        for row in data["supply"]:
            for balance in row.get("balances", []):
                balance["qty_native"] = format(Decimal(balance["qty_native"]), "E")
        fundamental["protocols"]["z-child"]["dailyHoldersRevenue"]["total24h"] = "-9.99125e2"
        fundamental["protocols"]["z-child"]["dailyHoldersRevenue"]["total30d"] = "0E-8"
    elif case == "missing":
        data["supply"] = []
    elif case == "future_fundamental":
        fundamental["ts"] = iso(CUTOFF + timedelta(microseconds=1))
    elif case == "future_receipt":
        fundamental["received_ts"] = iso(CUTOFF + timedelta(microseconds=1))
    elif case == "invalid_receipt":
        fundamental["received_ts"] = True
    elif case == "stale_fundamental":
        fundamental["ts"] = iso(CUTOFF - timedelta(seconds=21601))
    elif case in {"zero_revenue", "negative_revenue"}:
        fundamental["protocols"]["z-child"]["dailyHoldersRevenue"]["total24h"] = (
            "0" if case == "zero_revenue" else "-999.125"
        )
    elif case == "unavailable_fundamental":
        fundamental["available"] = False
    elif case == "unavailable_protocol":
        fundamental["protocols"]["z-child"]["dailyHoldersRevenue"]["available"] = False
    elif case == "no_protocols":
        fundamental["protocols"] = {}
    elif case == "no_chart":
        for protocol in fundamental["protocols"].values():
            protocol["dailyHoldersRevenue"].pop("totalDataChart")
    elif case == "only_chart":
        for protocol in fundamental["protocols"].values():
            protocol["dailyHoldersRevenue"].pop("total24h")
            protocol["dailyHoldersRevenue"].pop("total30d")
    elif case == "full_coverage":
        fundamental["protocols"]["a-parent"]["dailyHoldersRevenue"]["total30d"] = "10"
    elif case == "provider_shutdown":
        for row in data["supply"]:
            row.update(available=False, reason="SOURCE_SHUTDOWN")
    elif case == "changed_wallet_cohort":
        data["supply"][9]["balances"][0]["wallet_list_commit"] = "new-list"
    elif case == "partial_balance":
        data["supply"][9]["balances"][0]["complete"] = False
    elif case == "unavailable_prior_inventory":
        data["supply"][8]["available"] = False
    elif case == "inventory_asof_expired":
        data["supply"] = [row for row in data["supply"] if row.get("ts") != iso(CUTOFF - timedelta(days=1))]
    elif case == "legacy_inventory":
        data["supply"][9] = {
            "kind": "INVENTORY",
            "ts": iso(CUTOFF),
            "chg_pct": 0.1,
            "normal_range": [-0.02, 0.03],
            "source": "captured-wrapper",
        }
    elif case == "no_lending_prior":
        data["supply"] = [
            row for row in data["supply"] if not (row.get("kind") == "LENDING" and row["ts"] != iso(CUTOFF))
        ]
    elif case == "chain_asof_expired":
        chain = next(row for row in data["supply"] if row.get("series"))
        chain["series"] = chain["series"][:1]
    elif case == "duplicate_chain_time":
        chain = next(row for row in data["supply"] if row.get("series"))
        chain["series"].append({"ts": iso(CUTOFF), "supply_usd": "10000000000000000.04"})
    elif case == "out_of_order":
        data["supply"].reverse()
    elif case == "missing_anchor":
        data.pop("anchor")
        data["_primary"] = "W4H"
    record_property("original_commit", ORIGINAL_COMMIT)
    await _compare(clickhouse, original_truth, data, spots, derivatives)


@pytest.mark.parametrize("window", list(WINDOWS))
@pytest.mark.parametrize("case", ["normal", "gap", "negative", "zero", "projected"])
async def test_native_support_volume_every_window(clickhouse, original_truth, window, case):
    data = _snapshot()
    spots, derivatives = _inputs()
    data["_primary"] = window
    seconds = WINDOWS[window]
    start = CUTOFF - timedelta(seconds=seconds)
    for market, venue in (("SPOT", "S"), ("PERP", "P")):
        iid = f"{venue}:{'spot' if market == 'SPOT' else 'perp'}:T"
        points = [
            (start - timedelta(days=10, seconds=seconds), "10"),
            (start - timedelta(days=2, seconds=seconds), "20"),
            (start - timedelta(seconds=seconds), "30"),
            (start, "60"),
        ]
        for stamp, value in points:
            data["klines"].append(
                {
                    "instrument_id": iid,
                    "market_type": market,
                    "token_id": "T",
                    "open_time": iso(stamp),
                    "interval_s": seconds,
                    "complete": True,
                    "c": "10",
                    "volume_native": "-5" if case == "negative" else "0" if case == "zero" else value,
                    "quote_volume_usd": value,
                }
            )
        if case == "gap":
            data["klines"][-1]["complete"] = False
        elif case == "projected":
            data.setdefault("stat_volume_series", {})[iid] = {
                "token_id": "T",
                "market_type": market,
                "time_basis": "OPEN_TIME",
                "interval_s": seconds,
                "times_us": array("q", [int(t.timestamp() * 1000000) for t, _ in points]),
                "values": array("d", [float(v) for _, v in points]),
                "complete": bytearray([1, 1, 1, 1]),
            }
    result = await _compare(clickhouse, original_truth, data, spots, derivatives)
    features = result["support_features"][window]
    if case == "normal" or case == "projected":
        assert features["spot_volume_up_beyond_normal"] is True
        assert Decimal(features["perp_volume"]["value"]) == 60
    elif case == "gap":
        assert features["spot_volume"]["value"] is None


def _fresh_snapshot():
    data = _snapshot()
    spots, derivatives = _inputs()
    for venue in ("A", "B"):
        iid = f"{venue}:spot:T"
        data["instruments"].append({"instrument_id": iid, "token_id": "T"})
        for spot in spots.values():
            spot["venues"].append({"venue": venue})
        data["flow_bars"].append(
            {"instrument_id": iid, "minute_start": iso(CUTOFF - timedelta(minutes=1)), "complete": True}
        )
        data["klines"].append(
            {
                "instrument_id": iid,
                "open_time": iso(CUTOFF - timedelta(minutes=1)),
                "complete": True,
                "c": "10",
            }
        )
        data["books"].append(
            {
                "instrument_id": iid,
                "exchange_ts": iso(CUTOFF),
                "exchange_ts_prec": "EXACT",
                "integrity_ok": True,
                "mid": "10",
                "range_covered_pct": {"bid": 1, "ask": 1},
            }
        )
    return data, spots, derivatives


@pytest.mark.parametrize(
    "case",
    [
        "high",
        "low",
        "medium",
        "exact_half",
        "above_half",
        "stale_book",
        "cadence_boundary",
        "cadence_exceeded",
        "foreign_history",
        "future",
        "late_assembled",
        "unknown_tier",
        "numeric_metadata",
        "metric_metadata",
        "kind_metadata",
        "scoped_semantics",
        "settled_funding",
        "source_interval_start",
        "source_interval_end",
        "watermark",
        "future_watermark",
        "shutdown",
        "precision_alias",
        "bucket_anchor",
        "unknown_precision",
        "derivative_majority_equal",
        "derivative_majority_above",
        "classification_unready",
    ],
)
async def test_native_primary_confidence_and_field_freshness(clickhouse, original_truth, case):
    data, spots, derivatives = _fresh_snapshot()
    config = {}
    if case in {"low", "medium", "exact_half", "above_half"}:
        for spot in spots.values():
            if case == "low":
                spot["coverage"]["n_complete"] = 1
            elif case == "medium":
                spot["coverage"]["exact_majority"] = False
            else:
                spot["coverage"].pop("exact_majority")
                spot["coverage"]["covered_share_est"] = (
                    "0.5" if case == "exact_half" else "0.500000000000000000000001"
                )
    elif case in {"stale_book", "cadence_boundary", "cadence_exceeded"}:
        data["books"][0]["exchange_ts"] = iso(
            CUTOFF - timedelta(seconds=10 if case == "cadence_boundary" else 11)
        )
    elif case == "foreign_history":
        data["klines"].append(
            {
                "instrument_id": "X:spot:BTC",
                "token_id": "BTC",
                "open_time": iso(CUTOFF - timedelta(days=7)),
                "complete": True,
                "c": "1",
            }
        )
    elif case == "future":
        data["books"].append({**data["books"][0], "exchange_ts": iso(CUTOFF + timedelta(seconds=1))})
    elif case == "late_assembled":
        data["books"][0]["received_ts"] = iso(CUTOFF + timedelta(minutes=3))
    elif case == "unknown_tier":
        data["capture_metadata"] = {"tier": "UNKNOWN"}
        data["oi"] = [{"instrument_id": "P:perp:T", "ts": iso(CUTOFF), "oi_native": "10"}]
    elif case in {"numeric_metadata", "metric_metadata", "kind_metadata", "scoped_semantics"}:
        data["books"][0]["metric_key"] = "depth.p1"
        if case == "numeric_metadata":
            entry = 20
        elif case == "metric_metadata":
            entry = {"cadence_s_by_metric": {"depth.p1": 20}}
        elif case == "kind_metadata":
            data["books"][0]["kind"] = "CAPTURED"
            entry = {"by_kind": {"CAPTURED": {"cadence_s": 20}}}
        else:
            entry = {}
            data["semantics"].append({"metric_key": "depth.p1", "instrument_id": "A:spot:T", "cadence_s": 20})
        data["capture_metadata"] = {"collections": {"books": entry}}
    elif case == "settled_funding":
        data["funding"] = [
            {
                "instrument_id": "P:perp:T",
                "ts": iso(CUTOFF - timedelta(hours=7)),
                "kind": "SETTLED",
                "interval_h": 8,
                "rate": "0.001",
            }
        ]
    elif case in {"source_interval_start", "source_interval_end"}:
        data["oi"] = [
            {
                "instrument_id": "P:perp:T",
                "ts": iso(CUTOFF - timedelta(seconds=600)),
                "metric_key": "oi",
                "oi_native": "10",
            }
        ]
        data["semantics"] = [
            {
                "metric_key": "oi",
                "source_interval_s": 300,
                "publish_lag_s": 10,
                "timestamp_semantics": "BUCKET_END" if case.endswith("end") else "BUCKET_START",
            }
        ]
    elif case in {"watermark", "future_watermark"}:
        for row in data["klines"]:
            row["open_time"] = iso(CUTOFF - timedelta(minutes=10))
        stamp = CUTOFF if case == "watermark" else CUTOFF + timedelta(microseconds=1)
        data["capture_metadata"] = {
            "collections": {"klines": {"observed_through": {"A:spot:T": iso(stamp), "B:spot:T": iso(stamp)}}}
        }
    elif case == "shutdown":
        data["liquidations"] = [
            {
                "instrument_id": "P:perp:T",
                "ts": iso(CUTOFF - timedelta(hours=1)),
                "side": "LONG_LIQ",
                "notional_usd": "1",
            }
        ]
        data["capture_metadata"] = {
            "collections": {
                "liquidations": {
                    "cadence_s": 60,
                    "coverage_status_by_instrument": {"P:perp:T": "DISCONNECTED"},
                }
            }
        }
    elif case == "precision_alias":
        data["books"][0]["exchange_ts_prec"] = "BUCKET(1m)"
        data["books"][0]["timestamp_precision"] = "BUCKET(60s)"
    elif case == "bucket_anchor":
        data["books"][0].update(exchange_ts_prec="BUCKET(1m)", exchange_ts_bucket_anchor="END")
    elif case == "unknown_precision":
        data["books"][0].pop("exchange_ts_prec")
    elif case in {"derivative_majority_equal", "derivative_majority_above", "classification_unready"}:
        for venue in ("P", "Q"):
            iid = f"{venue}:perp:T"
            data["instruments"].append({"instrument_id": iid, "token_id": "T"})
            data["oi"].append({"instrument_id": iid, "ts": iso(CUTOFF), "oi_native": "10"})
            data["funding"].append({"instrument_id": iid, "ts": iso(CUTOFF), "rate": "0.01", "interval_h": 8})
            data["klines"].append(
                {
                    "instrument_id": iid,
                    "open_time": iso(CUTOFF - timedelta(minutes=1)),
                    "complete": True,
                    "c": "10",
                    "quote_volume_usd": "1.000000000000000000000001"
                    if venue == "P" and case == "derivative_majority_above"
                    else "1",
                }
            )
        for derivative in derivatives.values():
            derivative["synchronized"] = {
                "venues": ["P"],
                "classification_ready": case != "classification_unready",
            }
    result = await _compare(clickhouse, original_truth, data, spots, derivatives, config)
    if case == "high":
        assert result["data_confidence"]["class_"] == "HIGH"
    elif case == "low":
        assert result["data_confidence"]["class_"] == "LOW"
    elif case == "medium":
        assert result["data_confidence"]["class_"] == "MEDIUM"


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "missing_metric",
        "unconfigured",
        "missing_flow",
        "late_piece",
        "ask_not_recovered",
        "half_equal",
        "half_above",
        "funding_equal",
        "basis_equal",
    ],
)
async def test_native_support_flow_basis_funding_and_ask_evidence(clickhouse, original_truth, case):
    data = _snapshot()
    spots, derivatives = _inputs()
    iid = "P:perp:T"
    start = CUTOFF - timedelta(minutes=15)
    data["semantics"] = [{"venue": "P", "market_type": "PERP_LINEAR", "metric_family": "aggressor"}]
    for minute in range(15):
        data["flow_bars"].append(
            {
                "instrument_id": iid,
                "minute_start": iso(start + timedelta(minutes=minute)),
                "complete": True,
                "buy_qty": "4",
                "sell_qty": "1",
                "sum_sq_qty": "1",
                "buy_notional": "40",
                "sell_notional": "10",
            }
        )
    data["oi"] = [
        {"instrument_id": iid, "ts": iso(stamp), "mark_price": mark, "index_price": "100", "oi_native": "10"}
        for stamp, mark in (
            (start - timedelta(days=8), "101"),
            (start - timedelta(days=1), "102"),
            (start, "101"),
            (CUTOFF, "103"),
        )
    ]
    for derivative in derivatives.values():
        derivative["per_venue"] = [
            {
                "instrument_id": iid,
                "venue": "P",
                "basis_bps": "100" if case == "basis_equal" else "300",
                "funding_start": "0.001",
                "funding_start_interval_h": 4,
                "funding_8h_eq": "0.002" if case == "funding_equal" else "0.003",
                "liq_coverage": "COMPLETE",
            }
        ]
    data["liquidations"] = [
        {
            "instrument_id": iid,
            "ts": iso(start + timedelta(minutes=1)),
            "notional_usd": "1.000000000000000000000001" if case == "half_above" else "1",
        },
        {"instrument_id": iid, "ts": iso(start + timedelta(minutes=10)), "notional_usd": "1"},
    ]
    data["books"] = [
        {
            "instrument_id": "S:spot:T",
            "exchange_ts": iso(stamp),
            "integrity_ok": True,
            "depth": {"ask": {"p1": {"usd": ask}}},
        }
        for stamp, ask in (
            (start - timedelta(seconds=5), "10"),
            (start + timedelta(seconds=30), "5" if case == "ask_not_recovered" else "10"),
            (CUTOFF, "20"),
        )
    ]
    data["flow_bars"].append(
        {
            "instrument_id": "S:spot:T",
            "minute_start": iso(start),
            "complete": True,
            "buy_notional": "15",
            "sell_notional": "5",
        }
    )
    if case == "missing_metric":
        data["flow_bars"][0].pop("sum_sq_qty")
    elif case == "unconfigured":
        data["semantics"] = []
    elif case == "missing_flow":
        data["flow_bars"].pop(0)
    elif case == "late_piece":
        data["anchor"]["ts"] = iso(start + timedelta(seconds=30))
        piece = {
            "instrument_id": iid,
            "start": iso(start + timedelta(seconds=30)),
            "end": iso(start + timedelta(minutes=1)),
            "complete": True,
            "buy_qty": "2",
            "sell_qty": "1",
            "sum_sq_qty": "1",
            "buy_notional": "20",
            "sell_notional": "10",
            "received_ts": iso(CUTOFF + timedelta(hours=1)),
        }
        data["flow_segments"] = [piece]
    result = await _compare(clickhouse, original_truth, data, spots, derivatives)
    feature = result["support_features"]["W15M"]
    if case == "normal":
        assert feature["perp_taker_flow_buy"] is True
        assert feature["ask_replenishment"] is True
    elif case == "half_equal":
        assert feature["liquidations_first_half_majority"] is False
    elif case == "half_above":
        assert feature["liquidations_first_half_majority"] is True
    elif case == "funding_equal":
        assert feature["funding_rising"] is False


@pytest.mark.parametrize("window", list(WINDOWS))
async def test_native_supply_flow_admission_at_exact_window_bounds(clickhouse, original_truth, window):
    data = _snapshot()
    spots, derivatives = _inputs()
    start = CUTOFF - timedelta(seconds=WINDOWS[window])
    data["supply"] = [
        {
            "kind": "WALLET_FLOWS",
            "ts": iso(CUTOFF),
            "flows": [
                {"ts": iso(stamp), "qty_native": quantity}
                for stamp, quantity in (
                    (start - timedelta(microseconds=1), "1"),
                    (start, "2"),
                    (CUTOFF - timedelta(microseconds=1), "3"),
                    (CUTOFF, "4"),
                )
            ],
        }
    ]
    result = await _compare(clickhouse, original_truth, data, spots, derivatives)
    assert [Decimal(row["qty"]) for row in result["supply"]["windows"][window]["flows"]] == [
        Decimal(2),
        Decimal(3),
    ]


@pytest.mark.parametrize("value", [".5", "1.", "+2.5e-4", "1e-24"])
async def test_native_fundamentals_preserve_meaningful_numeric_spellings(clickhouse, original_truth, value):
    data = _supply_snapshot()
    data["supply"][-1]["protocols"]["z-child"]["dailyHoldersRevenue"]["total24h"] = value
    spots, derivatives = _inputs()
    await _compare(clickhouse, original_truth, data, spots, derivatives)


@pytest.mark.parametrize("value", ["1000000000000000000000000", "0.0000000000000000000000001", "1e100"])
async def test_native_meaningful_fundamentals_outside_policy_refused(clickhouse, value):
    data = _supply_snapshot()
    data["supply"][-1]["protocols"]["z-child"]["dailyHoldersRevenue"]["total24h"] = value
    statement, parameters = native.query(data)
    with pytest.raises(RuntimeError, match="native financial range"):
        await clickhouse.rows(statement, parameters)


async def test_native_inventory_aggregate_is_not_rejected_as_an_input_quantity(clickhouse, original_truth):
    data = _snapshot()
    spots, derivatives = _inputs()
    data["supply"] = [
        {
            "kind": "EXCHANGE_INVENTORY",
            "ts": iso(stamp),
            "balances": [_balance("A", quantity), _balance("B", quantity)],
        }
        for stamp, quantity in (
            (CUTOFF - timedelta(days=1), "0"),
            (CUTOFF, "999999999999999999999999"),
        )
    ]
    result = await _compare(clickhouse, original_truth, data, spots, derivatives)
    assert Decimal(result["supply"]["exchange_inventory"]["chg_native_24h"]) == Decimal(
        "1999999999999999999999998"
    )
    assert result["support_features"]["EVENT"]["exchange_inflows"] is True


async def test_native_ctes_compose_without_parameter_collisions(clickhouse, original_truth):
    data = _snapshot()
    spots, derivatives = _inputs()
    statement, parameters = native.query(data, spots=spots, derivatives=derivatives)
    assert all(name.startswith("sup_") for name in parameters)
    rows = await clickhouse.rows(
        "WITH 7 AS another_domain_sentinel, " + native.ctes() + " " + native.select_sql(),
        parameters,
    )
    assert len(rows) == 1
    _assert_complete(json.loads(rows[0]["result"]), _expected(original_truth, data, spots, derivatives, {}))
    assert statement.startswith("WITH ")


async def test_native_missing_dependency_inputs_remain_unavailable(clickhouse, original_truth):
    result = await _compare(clickhouse, original_truth, _snapshot())
    assert result["data_confidence"]["class_"] == "LOW"
    for window in WINDOWS:
        assert result["support_features"][window]["large_ticket_buy_majority"] is None
        assert result["support_features"][window]["perp_taker_flow_buy"] is None


async def test_native_support_accepts_actual_four_domain_sql_outputs(clickhouse, original_truth):
    from tank.analysis import truth_sql_derivatives, truth_sql_liquidity, truth_sql_relative, truth_sql_spot

    data = _snapshot()
    sql, parameters = truth_sql_spot.query(data)
    spots = json.loads((await clickhouse.rows(sql, parameters))[0]["result"])["spots"]
    sql, parameters = truth_sql_derivatives.query(data)
    derivatives = json.loads((await clickhouse.rows(sql, parameters))[0]["result"])
    sql, parameters = truth_sql_relative.query(data)
    relative = json.loads((await clickhouse.rows(sql, parameters))[0]["result"])
    sql, parameters = truth_sql_liquidity.windows_query(
        data, spots=spots, relative=relative, deriv=derivatives
    )
    liquidity = json.loads((await clickhouse.rows(sql, parameters))[0]["result"])
    sql, parameters = native.query(
        data, spots=spots, derivatives=derivatives, liquidity=liquidity, relative=relative
    )
    rows = await clickhouse.rows(sql, parameters)
    assert len(rows) == 1
    _assert_complete(json.loads(rows[0]["result"]), _expected(original_truth, data, spots, derivatives, {}))
