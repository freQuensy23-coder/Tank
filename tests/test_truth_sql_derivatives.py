import ast
import hashlib
import json
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

import pytest
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.analysis import truth as reference
from tank.analysis import truth_sql_derivatives as native
from tank.util import iso

pytestmark = pytest.mark.integration

CUTOFF = datetime(2026, 10, 21, 12, tzinfo=UTC)
WINDOWS = {"W15M": 900, "W1H": 3600, "W4H": 14400, "W24H": 86400}
REFERENCE_FUNCTIONS = {
    "percentile",
    "normal",
    "beyond",
    "decomposition",
    "oi_class",
    "oi_temporally_admissible",
    "_oi_rows",
    "_stored_usd",
    "_oi_split",
    "_oi_stats",
    "window_usd_volume",
    "liquidation_side_evidence",
    "_history_slice",
    "_composite_history",
    "synchronized_oi",
    "derivatives_truth",
}
REFERENCE_SHA256 = "b875e8ac3a0c77bd2b105fa705d975d1ace29b81e7b5de7fc95d34b1cb963994"


@pytest.fixture(scope="module", autouse=True)
def frozen_reference_source():
    source = Path(reference.__file__).read_text()
    functions = [
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name in REFERENCE_FUNCTIONS
    ]
    assert {node.name for node in functions} == REFERENCE_FUNCTIONS
    segments = [ast.get_source_segment(source, node) for node in functions]
    assert all(isinstance(segment, str) for segment in segments)
    frozen = "\n".join(segment for segment in segments if segment is not None)
    assert hashlib.sha256(frozen.encode()).hexdigest() == REFERENCE_SHA256


@pytest.fixture
async def clickhouse(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        yield client
    finally:
        await client.close()


def snapshot():
    return {
        "token_id": "T",
        "cutoff": iso(CUTOFF),
        "assembled_at": iso(CUTOFF + timedelta(minutes=2)),
        "anchor": {"ts": iso(CUTOFF - timedelta(hours=2)), "ts_prec": "EXACT"},
        "instruments": [],
        "oi": [],
        "funding": [],
        "liquidations": [],
        "semantics": [],
        "tickers": [],
        "klines": [],
        "flow_bars": [],
        "flow_segments": [],
    }


def add_instrument(data, venue, *, market="PERP_LINEAR", category="BASE_SINGLE_SIDED"):
    iid = f"{venue}:{'spot' if market == 'SPOT' else 'perp'}:T"
    data["instruments"].append(
        {"instrument_id": iid, "token_id": "T", "venue": venue, "market_type": market}
    )
    data["semantics"].append(
        {
            "instrument_id": iid,
            "metric_key": iid + ".oi",
            "sidedness": "SINGLE_SIDED",
            "aggregation_class": category,
        }
    )
    return iid


def oi_row(iid, at, native_value, usd_value, **fields):
    return {
        "instrument_id": iid,
        "ts": iso(at),
        "ts_prec": "EXACT",
        "oi_native": native_value,
        "oi_usd": usd_value,
        "metric_key": iid + ".oi",
        **fields,
    }


def constant_history(data, iid, *, stored=True):
    for index in range(12):
        data["oi"].append(
            oi_row(
                iid,
                CUTOFF - timedelta(hours=80 - index * 6),
                "10",
                "30" if stored else None,
                mark_price="3",
            )
        )


def original(data, config):
    end = reference.parse_time(data["cutoff"])
    start = reference.parse_time(data["anchor"]["ts"])
    token = reference._PARAMETERS.set(config)
    try:
        return {
            name: reference.derivatives_truth(deepcopy(data), end - timedelta(seconds=length), end)
            for name, length in {**WINDOWS, "EVENT": (end - start).total_seconds()}.items()
        }
    finally:
        reference._PARAMETERS.reset(token)


def assert_parity(actual, expected, path="result"):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), path
        assert actual.keys() == expected.keys(), path
        for key, value in expected.items():
            assert_parity(actual[key], value, path + "." + key)
    elif isinstance(expected, list):
        assert isinstance(actual, list), path
        assert len(actual) == len(expected), path
        for index, (left, right) in enumerate(zip(actual, expected, strict=True)):
            assert_parity(left, right, f"{path}[{index}]")
    elif isinstance(expected, Decimal):
        assert isinstance(actual, str), path
        with localcontext() as context:
            context.prec = 100
            tolerance = max(Decimal("1e-20"), abs(expected) * Decimal("1e-20"))
            assert abs(Decimal(actual) - expected) <= tolerance, path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-12, abs=1e-12), path
    else:
        assert actual == expected, path


async def execute(clickhouse, data, config=None):
    sql, parameters = native.query(data, config)
    rows = await clickhouse.rows(sql, parameters)
    assert len(rows) == 1
    assert isinstance(rows[0]["result"], str)
    result = json.loads(rows[0]["result"])
    assert result.keys() == {*WINDOWS, "EVENT"}
    assert_parity(result, original(data, config or {}))
    return result


async def test_empty_domain_preserves_all_window_structures(clickhouse):
    result = await execute(clickhouse, snapshot())
    for window in result.values():
        assert window["per_venue"] == []
        assert window["synchronized"] is None
        assert window["liquidations"]["complete_venues_total"] == {"long": None, "short": None}
        assert window["incremental_share"] is None


async def test_native_ctes_are_independently_composable(clickhouse, record_property):
    data = snapshot()
    iid = add_instrument(data, "binance")
    data["oi"] = [
        oi_row(iid, CUTOFF - timedelta(hours=2), Decimal("10"), Decimal("30")),
        oi_row(iid, CUTOFF, Decimal("20"), Decimal("90")),
    ]
    sql, parameters = native.query(data)
    assert all(name.startswith("deriv_") for name in parameters)
    assert json.loads(parameters["deriv_snapshot"][0])["oi"][0]["oi_native"] == "10"
    composed = "WITH 17 AS other_domain_sentinel, " + native.ctes() + "\n"
    composed += "SELECT deriv_result AS result, other_domain_sentinel FROM deriv_output"
    rows = await clickhouse.rows(composed, parameters)
    assert len(rows) == 1
    assert rows[0]["other_domain_sentinel"] == 17
    assert_parity(json.loads(rows[0]["result"]), original(data, {}))
    record_property("sql_sha256", hashlib.sha256(sql.encode()).hexdigest())
    record_property("oracle_sha256", REFERENCE_SHA256)


async def test_full_native_history_funding_volume_and_synchronized_cohort(clickhouse):
    data = snapshot()
    ids = [add_instrument(data, venue) for venue in ("binance", "bybit", "hyperliquid")]
    spot = add_instrument(data, "binance", market="SPOT")
    for index, iid in enumerate(ids):
        for hour in range(80, 4, -1):
            value = 1000 + (80 - hour) * (index + 1)
            data["oi"].append(
                oi_row(iid, CUTOFF - timedelta(hours=hour), str(value), str(value * (100 + hour % 5)))
            )
        for minutes in (240, 120, 60, 45, 30, 15, 0):
            value = 1200 + (240 - minutes) * (index + 1)
            data["oi"].append(
                oi_row(iid, CUTOFF - timedelta(minutes=minutes), str(value), str(value * 105))
            )
        for hour in range(80, -1, -8):
            data["funding"].append(
                {
                    "instrument_id": iid,
                    "as_of": iso(CUTOFF - timedelta(hours=hour)),
                    "kind": "SETTLED",
                    "rate": str(Decimal(hour % 7 - 3) / Decimal(10000)),
                    "interval_h": "4" if index == 1 else "8",
                }
            )
        data["funding"].append(
            {
                "instrument_id": iid,
                "as_of": iso(CUTOFF - timedelta(seconds=10)),
                "kind": "CURRENT_ESTIMATE",
                "rate": "0.001",
                "interval_h": "1" if index == 1 else "8",
            }
        )
        data["liquidations"].append(
            {
                "instrument_id": iid,
                "ts": iso(CUTOFF - timedelta(minutes=10)),
                "side": "LONG_LIQ" if index == 1 else "SHORT_LIQ",
                "notional_usd": "120.25",
                "qty_native": "1.5",
            }
        )
    data["capture_metadata"] = {
        "collections": {
            "liquidations": {
                "coverage_start": {iid: iso(CUTOFF - timedelta(days=1)) for iid in ids},
                "observed_through": {iid: iso(CUTOFF) for iid in ids},
            }
        }
    }
    for iid in [spot, *ids]:
        for step in range(192, 0, -1):
            data["klines"].append(
                {
                    "instrument_id": iid,
                    "open_time": iso(CUTOFF - timedelta(minutes=15 * step)),
                    "interval_s": 900,
                    "complete": True,
                    "quote_volume_usd": str(1000 - step),
                }
            )
    result = await execute(clickhouse, data, {"history_min_days": 0.125})
    for window in result.values():
        assert window["synchronized"]["venues"] == ["binance", "bybit"]
        assert len(window["per_venue"]) == 3
        assert window["incremental_share"] is not None
        assert window["incremental_volume_coverage"]["spot"]["complete"] is True
        assert Decimal(window["funding_dispersion_8h"]) == Decimal("0.007")
    assert result["W24H"]["synchronized"]["classification_ready"] is True


@pytest.mark.parametrize(
    ("native_end", "usd_end", "mark", "classification"),
    [
        ("20", "89.9999999999999999999998", None, "REAL_NATIVE"),
        ("20", "90", None, "MIXED"),
        ("20", "90.0000000000000000000002", None, "MIXED"),
        ("20", "80", None, "REAL_NATIVE"),
        ("10", "60", None, "PRICE_MARKUP"),
        ("5", "5", None, "MIXED"),
        ("0", "0", "3", "REAL_NATIVE"),
        ("20", None, None, None),
    ],
)
async def test_decomposition_exact_majority_and_price_markup_boundaries(
    clickhouse, native_end, usd_end, mark, classification
):
    data = snapshot()
    iid = add_instrument(data, "binance")
    constant_history(data, iid)
    data["oi"].extend(
        [
            oi_row(iid, CUTOFF - timedelta(hours=2), "10", "30"),
            oi_row(iid, CUTOFF, native_end, usd_end, **({"mark_price": mark} if mark else {})),
        ]
    )
    result = await execute(clickhouse, data, {"history_min_days": 0.125})
    venue = result["EVENT"]["per_venue"][0]
    assert venue["oi_class"] == classification
    assert venue["classification_ready"] is (classification is not None)
    assert venue["native_normal_range"] == [0.0, 0.0]
    if usd_end is None:
        assert venue["decomposition"]["detail"] == "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"


@pytest.mark.parametrize("source", ["ROW_MARK", "TICKER_MARK"])
async def test_derived_usd_is_not_stored_history_and_mark_tie_uses_earlier_time(clickhouse, source):
    data = snapshot()
    iid = add_instrument(data, "binance")
    constant_history(data, iid, stored=False)
    start = CUTOFF - timedelta(hours=2)
    for at, amount in ((start, "10"), (CUTOFF, "20")):
        data["oi"].append(
            oi_row(iid, at, amount, None, **({"mark_price": "3"} if source == "ROW_MARK" else {}))
        )
        if source == "TICKER_MARK":
            for seconds, price in ((30, "9"), (-30, "3")):
                data["tickers"].append(
                    {"instrument_id": iid, "ts": iso(at + timedelta(seconds=seconds)), "mark_price": price}
                )
    result = await execute(clickhouse, data, {"history_min_days": 0.125})
    venue = result["EVENT"]["per_venue"][0]
    assert Decimal(venue["oi_usd_start"]) == 30
    assert Decimal(venue["oi_usd_end"]) == 60
    assert venue["usd_normal_range"] is None
    assert venue["native_normal_range"] == [0.0, 0.0]
    assert venue["oi_class"] == "REAL_NATIVE"


async def test_oi_endpoint_tie_uses_input_order_and_never_looks_past_cutoff(clickhouse):
    data = snapshot()
    iid = add_instrument(data, "binance")
    start = CUTOFF - timedelta(hours=2)
    data["oi"] = [
        oi_row(iid, start + timedelta(seconds=30), "12", "36"),
        oi_row(iid, start - timedelta(seconds=30), "10", "30"),
        oi_row(iid, CUTOFF + timedelta(seconds=1), "999", "2997"),
        oi_row(iid, CUTOFF - timedelta(seconds=30), "20", "60"),
    ]
    result = await execute(clickhouse, data)
    venue = result["EVENT"]["per_venue"][0]
    assert Decimal(venue["oi_native_start"]) == 12
    assert Decimal(venue["oi_native_end"]) == 20
    assert venue["observed_window_duration_s"] == 7140


@pytest.mark.parametrize("resolved", [False, True])
async def test_unspecified_bucket_edges_remain_supplemental_not_endpoints(clickhouse, resolved):
    data = snapshot()
    iid = add_instrument(data, "bybit")
    for at, amount in ((CUTOFF - timedelta(hours=2), "10"), (CUTOFF, "20")):
        if resolved:
            data["oi"].append(
                oi_row(iid, at - timedelta(seconds=30), amount, str(Decimal(amount) * 3))
            )
        data["oi"].append(
            oi_row(iid, at, "999", "2997", ts_prec="BUCKET(5m)", ts_bucket_anchor=None)
        )
    result = await execute(clickhouse, data)
    venue = result["EVENT"]["per_venue"][0]
    exclusion = venue["oi_temporal_exclusions"]
    assert exclusion["count"] == 2
    assert exclusion["latest_observation"]["ts_bucket_anchor"] == "UNSPECIFIED"
    assert (
        exclusion["latest_observation"]["scope"]
        == "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"
    )
    if resolved:
        assert Decimal(venue["oi_native_end"]) == 20
    else:
        assert venue["oi_native_end"] is None
        assert venue["detail"] == "OI_BUCKET_EDGE_UNSPECIFIED"
        assert result["EVENT"]["synchronized"] is None


@pytest.mark.parametrize(
    ("market", "category", "sidedness", "expected_venues"),
    [
        ("PERP_INVERSE", "USD_CONTRACT_SINGLE_SIDED", "SINGLE_SIDED", None),
        ("PERP_LINEAR", "BASE_SINGLE_SIDED", "DOUBLE_SIDED", ["binance"]),
        ("PERP_LINEAR", "BASE_SINGLE_SIDED", "SINGLE_SIDED", ["binance", "okx"]),
    ],
)
async def test_native_metric_units_and_sidedness_define_strict_cohort(
    clickhouse, market, category, sidedness, expected_venues
):
    data = snapshot()
    first = add_instrument(data, "binance")
    second = add_instrument(data, "okx", market=market, category=category)
    data["semantics"][-1]["sidedness"] = sidedness
    data["semantics"][-1]["native_unit"] = "CONTRACTS" if market == "PERP_INVERSE" else "BASE"
    for iid in (first, second):
        for at, amount in ((CUTOFF - timedelta(hours=2), "10"), (CUTOFF, "20")):
            data["oi"].append(oi_row(iid, at, amount, str(Decimal(amount) * 3)))
    result = await execute(clickhouse, data)
    synchronized = result["EVENT"]["synchronized"]
    if expected_venues is None:
        assert synchronized is None
    else:
        assert synchronized["venues"] == expected_venues
        assert Decimal(synchronized["oi_native_end"]) == 20 * len(expected_venues)
    assert len(result["EVENT"]["per_venue"]) == 2


async def test_changing_history_population_does_not_count_venue_arrival_as_oi_change(clickhouse):
    data = snapshot()
    first = add_instrument(data, "binance")
    second = add_instrument(data, "bybit")
    for hour in range(80, 4, -1):
        data["oi"].append(oi_row(first, CUTOFF - timedelta(hours=hour), "10", "30"))
    for hour in (3, 2, 1, 0):
        data["oi"].append(oi_row(second, CUTOFF - timedelta(hours=hour), "900", "2700"))
    data["oi"].extend(
        [
            oi_row(first, CUTOFF - timedelta(hours=2), "10", "30"),
            oi_row(first, CUTOFF, "10", "30"),
        ]
    )
    result = await execute(clickhouse, data, {"history_min_days": 0.125})
    synchronized = result["EVENT"]["synchronized"]
    assert synchronized["native_normal_range"] == [0.0, 0.0]
    assert synchronized["usd_normal_range"] == [0.0, 0.0]
    assert synchronized["oi_class"] == "MIXED"


async def test_default_seven_day_composite_history_keeps_per_measure_paired_populations(clickhouse):
    data = snapshot()
    first = add_instrument(data, "binance")
    second = add_instrument(data, "bybit")
    for half_day in range(22, 0, -1):
        data["oi"].append(oi_row(first, CUTOFF - timedelta(hours=12 * half_day), "10", "30"))
    for hour in range(24, 2, -1):
        data["oi"].append(oi_row(second, CUTOFF - timedelta(hours=hour), "900", None))
    for iid, amount, usd in ((first, "10", "30"), (second, "900", "2700")):
        for hour in (24, 2, 0):
            data["oi"].append(oi_row(iid, CUTOFF - timedelta(hours=hour), amount, usd))
    result = await execute(clickhouse, data)
    synchronized = result["W24H"]["synchronized"]
    assert synchronized["venues"] == ["binance", "bybit"]
    assert synchronized["native_normal_range"] == [0.0, 0.0]
    assert synchronized["usd_normal_range"] == [0.0, 0.0]
    assert synchronized["oi_class"] == "MIXED"
    assert Decimal(synchronized["oi_native_end"]) == 910
    assert Decimal(synchronized["oi_usd_end"]) == 2730


@pytest.mark.parametrize("estimate_interval", ["0", "1", "3"])
async def test_funding_settlements_and_current_estimates_have_independent_normalization(
    clickhouse, estimate_interval
):
    data = snapshot()
    iid = add_instrument(data, "bybit")
    for day in (20, 19, 18, 17):
        data["funding"].append(
            {
                "instrument_id": iid,
                "settlement_time": iso(CUTOFF - timedelta(days=day)),
                "kind": "SETTLED",
                "rate": "-0.0003",
                "rate_8h_eq": "-0.0006",
                "interval_h": "4",
            }
        )
    data["funding"].extend(
        [
            {
                "instrument_id": iid,
                "as_of": iso(CUTOFF),
                "kind": "SETTLED",
                "rate": "-0.0009",
                "interval_h": "3",
            },
            {
                "instrument_id": iid,
                "as_of": iso(CUTOFF),
                "kind": "CURRENT_ESTIMATE",
                "rate": "0.001",
                "interval_h": estimate_interval,
            },
            {
                "instrument_id": iid,
                "as_of": iso(CUTOFF + timedelta(seconds=1)),
                "kind": "CURRENT_ESTIMATE",
                "rate": "9",
                "interval_h": "1",
            },
        ]
    )
    result = await execute(clickhouse, data, {"history_min_days": 2, "historical_lookback_days": 1})
    venue = result["EVENT"]["per_venue"][0]
    assert Decimal(venue["funding_settled_8h_eq"]) == Decimal("-0.0024")
    assert venue["funding_8h_kind"] == ("SETTLED" if estimate_interval == "0" else "CURRENT_ESTIMATE")
    assert venue["funding_vs_normal"] == ("BELOW" if estimate_interval == "0" else "ABOVE")
    assert Decimal(venue["funding_start"]) == Decimal("-0.0003")


@pytest.mark.parametrize("capture_complete", [False, True])
async def test_liquidation_observed_zero_missing_conversion_and_coverage_remain_distinct(
    clickhouse, capture_complete
):
    data = snapshot()
    complete_iid = add_instrument(data, "bybit")
    sampled_iid = add_instrument(data, "binance")
    missing_iid = add_instrument(data, "okx")
    data["liquidations"].append(
        {
            "instrument_id": sampled_iid,
            "ts": iso(CUTOFF - timedelta(minutes=1)),
            "side": "LONG_LIQ",
            "qty_native": "2.5",
            "notional_usd": None,
        }
    )
    if capture_complete:
        data["capture_metadata"] = {
            "collections": {
                "liquidations": {
                    "coverage_start": {complete_iid: iso(CUTOFF - timedelta(days=2))},
                    "observed_through": {complete_iid: iso(CUTOFF)},
                }
            }
        }
    result = await execute(clickhouse, data)
    venues = {row["instrument_id"]: row for row in result["EVENT"]["per_venue"]}
    assert venues[complete_iid]["liq_source_coverage"] == "COMPLETE"
    assert venues[complete_iid]["liq_coverage"] == ("COMPLETE" if capture_complete else "UNKNOWN")
    if capture_complete:
        assert Decimal(venues[complete_iid]["liq_long_usd"]) == 0
        assert Decimal(result["EVENT"]["liquidations"]["complete_venues_total"]["long"]) == 0
    else:
        assert venues[complete_iid]["liq_long_usd"] is None
    assert venues[sampled_iid]["liq_long_usd"] is None
    assert Decimal(venues[sampled_iid]["liq_long_native"]) == Decimal("2.5")
    assert Decimal(venues[sampled_iid]["liq_short_usd"]) == 0
    assert venues[missing_iid]["liq_short_usd"] is None
    assert result["EVENT"]["liquidations"]["sampled_venues_min"]["long"] is None


async def test_fractional_native_volume_uses_complete_boundary_segments(clickhouse):
    data = snapshot()
    end = CUTOFF + timedelta(microseconds=500000)
    start = CUTOFF - timedelta(seconds=89, microseconds=750000)
    data["cutoff"] = iso(end)
    data["anchor"]["ts"] = iso(start)
    spot = add_instrument(data, "binance", market="SPOT")
    perp = add_instrument(data, "bybit")
    for iid in (spot, perp):
        for minute, buy in ((3, "10"), (1, "20")):
            data["flow_bars"].append(
                {
                    "instrument_id": iid,
                    "minute_start": iso(CUTOFF - timedelta(minutes=minute)),
                    "complete": True,
                    "buy_notional": buy,
                    "sell_notional": "5",
                }
            )
        for left, right, buy in (
            (start, CUTOFF - timedelta(minutes=1), "7"),
            (CUTOFF, end, "3"),
            (CUTOFF - timedelta(minutes=2), start, "2"),
        ):
            data["flow_segments"].append(
                {
                    "instrument_id": iid,
                    "start": iso(left),
                    "end": iso(right),
                    "complete": True,
                    "buy_notional": buy,
                    "sell_notional": "1",
                    "observed_at": iso(CUTOFF + timedelta(seconds=1)),
                }
            )
    result = await execute(clickhouse, data)
    share = result["EVENT"]["incremental_share"]
    assert share is not None
    assert share["spot"] == 0.5
    assert share["perp"] == 0.5
    assert Decimal(share["increment_usd"]["spot"]) == 19


@pytest.mark.parametrize("invalid", ["1e25", "0.0000000000000000000000001", "NaN", "--1e2"])
async def test_domain_refuses_unrepresentable_native_financial_values(clickhouse, invalid):
    data = snapshot()
    iid = add_instrument(data, "binance")
    data["oi"] = [
        oi_row(iid, CUTOFF - timedelta(hours=2), invalid, "30"),
        oi_row(iid, CUTOFF, "20", "60"),
    ]
    sql, parameters = native.query(data)
    with pytest.raises(RuntimeError, match="ClickHouse"):
        await clickhouse.rows(sql, parameters)


async def test_decimal_scientific_transport_retains_the_scale_boundary(clickhouse):
    data = snapshot()
    iid = add_instrument(data, "binance")
    data["oi"] = [
        oi_row(iid, CUTOFF - timedelta(hours=2), Decimal("1e-24"), Decimal("1e-24")),
        oi_row(iid, CUTOFF, Decimal("2e-24"), Decimal("2e-24")),
    ]
    result = await execute(clickhouse, data)
    venue = result["EVENT"]["per_venue"][0]
    assert Decimal(venue["oi_native_start"]) == Decimal("1e-24")
    assert Decimal(venue["decomposition"]["position"]) == Decimal("1e-24")
    assert venue["oi_native_chg_pct"] == 1.0


async def test_precision_alias_equivalence_and_global_semantic_fallback(clickhouse):
    data = snapshot()
    iid = add_instrument(data, "binance")
    data["semantics"][0].pop("instrument_id")
    data["oi"] = [
        oi_row(
            iid,
            at,
            "10",
            "30",
            ts_prec="BUCKET(5m)",
            timestamp_precision="BUCKET(300s)",
            ts_bucket_anchor="END",
        )
        for at in (CUTOFF - timedelta(hours=2), CUTOFF)
    ]
    result = await execute(clickhouse, data)
    assert result["EVENT"]["per_venue"][0]["precision"] == "BUCKET(5m)"
    assert result["EVENT"]["synchronized"]["venues"] == ["binance"]


async def test_conflicting_precision_is_refused_instead_of_voting_in_the_cohort(clickhouse):
    data = snapshot()
    iid = add_instrument(data, "binance")
    data["oi"] = [
        oi_row(
            iid,
            at,
            "10",
            "30",
            ts_prec="BUCKET(5m)",
            timestamp_precision="EXACT",
            ts_bucket_anchor="END",
        )
        for at in (CUTOFF - timedelta(hours=2), CUTOFF)
    ]
    sql, parameters = native.query(data)
    with pytest.raises(RuntimeError, match="ClickHouse"):
        await clickhouse.rows(sql, parameters)
