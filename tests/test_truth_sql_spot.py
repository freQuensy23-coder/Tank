import json
import os
import subprocess
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from urllib.parse import urlparse

import pytest

from tank.analysis import truth_sql_spot
from tank.clickhouse_market import ClickHouse
from tank.util import iso, jsonable, parse_time

pytestmark = pytest.mark.integration
BASELINE = "ef3b43e8457622d2f3c2c86926de41aecee101d6"
END = datetime(2026, 10, 21, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def original_spot():
    root = Path(__file__).resolve().parents[1]
    source = subprocess.run(
        ["git", "show", f"{BASELINE}:src/tank/analysis/truth.py"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout
    module = ModuleType("original_spot_truth")
    exec(compile(source, f"{BASELINE}/src/tank/analysis/truth.py", "exec"), module.__dict__)
    return module


@pytest.fixture
async def spot_client():
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    assert urlparse(url).hostname in {"localhost", "127.0.0.1", "::1"}
    client = ClickHouse(url)
    try:
        yield client
    finally:
        await client.close()


def instrument(venue, suffix="T", token: str | None = "T"):
    return {"instrument_id": f"{venue}:spot:{suffix}", "token_id": token}


def flow(venue, at, buy="6", sell="2", q="1", **fields):
    return {
        "instrument_id": f"{venue}:spot:T",
        "minute_start": iso(at),
        "complete": True,
        "buy_qty": buy,
        "sell_qty": sell,
        "sum_sq_qty": q,
        **fields,
    }


def packet(rates, minutes=30, event_seconds=900):
    return {
        "token_id": "T",
        "cutoff": iso(END),
        "anchor": {"ts": iso(END - timedelta(seconds=event_seconds))},
        "assembled_at": iso(END + timedelta(seconds=1)),
        "instruments": [instrument(venue) for venue in rates],
        "flow_bars": [
            flow(venue, END - timedelta(minutes=minute), *rate)
            for venue, rate in rates.items()
            for minute in range(minutes, 0, -1)
        ],
        "tickers": [],
    }


def original_result(module, snapshot, config=None):
    snapshot = deepcopy(snapshot)
    end = parse_time(snapshot["cutoff"])
    anchor = snapshot.get("anchor")
    av = anchor.get("ts") if isinstance(anchor, dict) else anchor
    start = parse_time(av) if av else end - timedelta(hours=4)
    snapshot = module.bounded_market_snapshot(snapshot, end)
    context = module._PARAMETERS.set(config or {})
    try:
        spots, windows, primary, instruments = module.spot_window_graph(snapshot, start, end, av)
    finally:
        module._PARAMETERS.reset(context)
    return jsonable(
        {"spots": spots, "windows": windows, "primary": primary, "instruments": instruments}
    )


def assert_complete_result(actual, expected, path=()):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), path
        assert actual.keys() == expected.keys(), path
        for key, value in expected.items():
            assert_complete_result(actual[key], value, (*path, key))
    elif isinstance(expected, list):
        assert isinstance(actual, list), path
        assert len(actual) == len(expected), path
        for index, (left, right) in enumerate(zip(actual, expected, strict=True)):
            assert_complete_result(left, right, (*path, index))
    elif expected is None or isinstance(expected, bool):
        assert actual is expected, path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-12, abs=1e-15), path
    elif path and path[-1] in {
        "cvd",
        "volume",
        "volume_native",
        "buy_qty",
        "sell_qty",
        "sum_sq_qty",
    }:
        assert abs(Decimal(str(actual)) - Decimal(str(expected))) <= Decimal("1e-24"), path
    else:
        assert actual == expected, path


async def native_result(client, original, snapshot, record_property, config=None):
    sql, params = truth_sql_spot.query(snapshot, config=config)
    record_property("spot_original_sha", BASELINE)
    record_property("spot_native_sql", sql)
    rows = await client.rows(sql, params)
    assert len(rows) == 1
    assert set(rows[0]) == {"result"}
    actual = json.loads(rows[0]["result"])
    expected = original_result(original, snapshot, config)
    assert_complete_result(actual, expected)
    assert set(actual["spots"]) == {"W15M", "W1H", "W4H", "W24H", "EVENT"}
    return actual


@pytest.mark.parametrize(
    ("rates", "classification"),
    [
        ({"A": ("6", "2", "1"), "B": ("6", "2", "1")}, "BROAD"),
        ({"A": ("2", "6", "1"), "B": ("2", "6", "1")}, "BROAD"),
        (
            {"A": ("10", "0", "0"), "B": ("1", "2", "0"), "C": ("0", "1", "0")},
            "CONCENTRATED",
        ),
        (
            {
                "A": ("5", "5", "0"),
                "B": ("3", "1", "0"),
                "C": ("3", "1", "0"),
                "D": ("0", "2", "0"),
            },
            "DIVERGENT",
        ),
        ({"A": ("0", "0", "0"), "B": ("0", "0", "0")}, "INSUFFICIENT_DATA"),
    ],
)
async def test_native_spot_full_classification_structure(
    rates, classification, spot_client, original_spot, record_property
):
    result = await native_result(spot_client, original_spot, packet(rates), record_property)
    event = result["spots"]["EVENT"]
    assert event["classification"] == classification
    if classification == "DIVERGENT":
        assert event["topn"]["venues"] == ["A", "B"]
        assert event["concentration"]["top_contrib_venue"] == "B"
        assert event["topn"]["agreement"] == 0.5


@pytest.mark.parametrize(
    ("buy", "q", "sign"),
    [
        ("3.999999999999999999999999", "4", 0),
        ("4", "4", 1),
        ("4.000000000000000000000001", "4", 1),
        ("0.000000000002", "0.000000000000000000000001", 1),
        ("0.000000000001999999999999", "0.000000000000000000000001", 0),
        ("0.000000000000000000000001", "0", 1),
    ],
)
async def test_native_spot_significance_uses_exact_native_decimal_products(
    buy, q, sign, spot_client, original_spot, record_property
):
    snapshot = packet({"A": (buy, "0", q), "B": (buy, "0", q)}, minutes=1, event_seconds=60)
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    assert [venue["sign"] for venue in result["spots"]["EVENT"]["venues"]] == [sign, sign]


@pytest.mark.parametrize("extra", ["0", "0.000000000000000000000001"])
async def test_native_spot_strict_majority_and_topn_do_not_use_display_float(
    extra, spot_client, original_spot, record_property
):
    buy = "1" if extra == "0" else "1.000000000000000000000001"
    snapshot = packet({"A": (buy, "0", "0"), "B": ("0", "1", "0")}, minutes=1, event_seconds=60)
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    event = result["spots"]["EVENT"]
    assert event["cw_breadth"]["buy"] == 0.5
    assert event["exact_majority"]["buy"] is (extra != "0")
    assert event["topn"]["n"] == (2 if extra == "0" else 1)


async def test_native_spot_decimal_transport_wrappers_and_nondefault_parameters(
    spot_client, original_spot, record_property
):
    snapshot = packet(
        {
            "A": ({"value": Decimal("2e-12")}, Decimal("0"), Decimal("1e-24")),
            "B": (Decimal("2e-12"), Decimal("0"), Decimal("1e-24")),
        },
        minutes=1,
        event_seconds=60,
    )
    result = await native_result(
        spot_client,
        original_spot,
        snapshot,
        record_property,
        config={"significant_flow_sigma": Decimal("1.5"), "min_spot_venues": 3},
    )
    assert [venue["sign"] for venue in result["spots"]["EVENT"]["venues"]] == [1, 1]
    assert result["spots"]["EVENT"]["classification"] == "INSUFFICIENT_DATA"


async def test_native_spot_full_minute_coverage_is_venue_union_without_dropping_instruments(
    spot_client, original_spot, record_property
):
    snapshot = packet({"A": ("1", "0", "0"), "B": ("1", "0", "0")}, minutes=15)
    snapshot["instruments"].append(instrument("A", suffix="T2"))
    snapshot["flow_bars"].append(
        flow("A", END - timedelta(minutes=1), "2", "0", "0", instrument_id="A:spot:T2")
    )
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    event = result["spots"]["EVENT"]
    assert event["coverage"]["n_complete"] == 2
    assert Decimal(event["venues"][0]["volume_native"]) == 17


@pytest.mark.parametrize(
    "missing", ["empty", "minute", "complete", "metric", "wrapped_null", "receipt", "ticker"]
)
async def test_native_spot_missing_data_admission_and_exclusion_reasons(
    missing, spot_client, original_spot, record_property
):
    snapshot = packet({"A": ("6", "2", "1"), "B": ("6", "2", "1")})
    if missing == "empty":
        snapshot.update(instruments=[], flow_bars=[])
    elif missing == "minute":
        snapshot["flow_bars"].pop()
    elif missing == "complete":
        snapshot["flow_bars"][-1].update(complete=False, gap_reason="SEQUENCE_GAP")
    elif missing == "metric":
        snapshot["flow_bars"][-1].pop("sum_sq_qty")
    elif missing == "wrapped_null":
        snapshot["flow_bars"][-1]["sum_sq_qty"] = {"value": None}
    elif missing == "receipt":
        snapshot["flow_bars"][-1]["received_ts"] = iso(END + timedelta(seconds=2))
    else:
        snapshot["instruments"].append(instrument("C"))
        snapshot["tickers"].append(
            {"instrument_id": "C:spot:T", "ts": iso(END), "volume_native": None, "volume": "1"}
        )
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    event = result["spots"]["EVENT"]
    assert event["classification"] == "INSUFFICIENT_DATA"
    if missing == "complete":
        assert event["coverage"]["excluded"][0]["source_reasons"] == ["SEQUENCE_GAP"]
    elif missing == "metric":
        assert event["coverage"]["excluded"][0]["missing_metrics"] == ["sum_sq_qty"]
    elif missing == "empty":
        assert event["composite_max"]["reason"] == "INCOMPLETE"
        assert event["direction"] is None


@pytest.mark.parametrize(
    ("volume", "majority"),
    [("2880", False), ("2879.999999999999999999999", True), (None, None)],
)
async def test_native_spot_estimated_coverage_exact_half_and_null_ticker(
    volume, majority, spot_client, original_spot, record_property
):
    snapshot = packet({"A": ("1", "0", "0"), "B": ("1", "0", "0")}, minutes=1, event_seconds=60)
    snapshot["instruments"].append(instrument("C"))
    snapshot["tickers"] = [
        {"venue": "C", "ts": iso(END), "volume_native": volume},
        {"venue": "C", "ts": iso(END + timedelta(microseconds=1)), "volume_native": "0"},
    ]
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    event = result["spots"]["EVENT"]
    assert event["coverage"]["exact_majority"] is majority
    assert event["classification"] == ("BROAD" if majority else "INSUFFICIENT_DATA")


async def test_native_spot_coverage_majority_compares_unrounded_estimated_sum(
    spot_client, original_spot, record_property
):
    snapshot = packet({"A": ("1", "0", "0"), "B": ("1", "0", "0")}, minutes=1, event_seconds=60)
    snapshot["instruments"].extend([instrument("C"), instrument("D")])
    snapshot["tickers"] = [
        {"venue": "C", "ts": iso(END), "volume_native": "1439.999999999999999999999999"},
        {"venue": "D", "ts": iso(END), "volume_native": "1440.000000000000000000000002"},
    ]
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    event = result["spots"]["EVENT"]
    assert event["coverage"]["covered_share_est"] == 0.5
    assert event["coverage"]["exact_majority"] is False
    assert event["classification"] == "INSUFFICIENT_DATA"


def fractional_packet(mode):
    start = END + timedelta(seconds=5, microseconds=1)
    midpoint = END + timedelta(seconds=12)
    cutoff = END + timedelta(seconds=17, microseconds=123456)
    snapshot = {
        "token_id": "T",
        "anchor": {"ts": iso(start)},
        "cutoff": iso(cutoff),
        "assembled_at": iso(cutoff + timedelta(seconds=1)),
        "instruments": [instrument("A"), instrument("B")],
        "tickers": [],
        "flow_bars": [],
        "flow_segments": [],
    }
    for venue in ("A", "B"):
        long_piece = {
            "start": iso(start),
            "end": iso(midpoint),
            "complete": True,
            "buy_qty": "2",
            "sell_qty": "1",
            "sum_sq_qty": "1",
            "n_buy": 2,
            "n_sell": 1,
        }
        ending = {
            **long_piece,
            "start": iso(midpoint),
            "end": iso(cutoff),
            "buy_qty": "3",
        }
        snapshot["flow_bars"].append(
            flow(
                venue,
                END,
                complete=False,
                boundary_segments=[
                    {**long_piece, "end": iso(END + timedelta(seconds=10)), "buy_qty": "999"},
                    {**long_piece, "buy_qty": "888"},
                    ending,
                ],
            )
        )
        explicit = {**long_piece, "instrument_id": f"{venue}:spot:T"}
        snapshot["flow_segments"].extend([{**explicit, "buy_qty": "777"}, explicit])
        if venue == "B":
            if mode == "observed":
                explicit["observed_at"] = iso(cutoff + timedelta(seconds=2))
            elif mode == "received":
                explicit["received_ts"] = iso(cutoff + timedelta(seconds=2))
            elif mode == "gap":
                explicit.update(complete=False, gap_reason="BOUNDARY_GAP")
            elif mode == "instrument":
                snapshot["instruments"].append(instrument("B", suffix="T2"))
    return snapshot


@pytest.mark.parametrize("mode", ["complete", "observed", "received", "gap", "instrument"])
async def test_native_spot_fractional_microsecond_greedy_cover_and_receipts(
    mode, spot_client, original_spot, record_property
):
    result = await native_result(spot_client, original_spot, fractional_packet(mode), record_property)
    event = result["spots"]["EVENT"]
    assert event["start"].endswith("05.000001Z")
    assert event["cutoff"].endswith("17.123456Z")
    assert result["primary"] == "W4H"
    assert event["coverage"]["n_complete"] == (2 if mode == "complete" else 1)
    first = event["venues"][0]
    assert Decimal(first["buy_qty"]) == 5
    assert len(first["boundary_segments"]) == 1
    assert first["boundary_segments"][0]["trade_count"] == 6
    assert len(first["boundary_segments"][0]["aggregates"]) == 2
    if mode != "complete":
        assert event["coverage"]["excluded"][0]["detail"] == "BOUNDARY_AGGREGATE_UNAVAILABLE"


async def test_native_spot_two_fractional_boundaries_and_full_minute(
    spot_client, original_spot, record_property
):
    snapshot = packet({"A": ("6", "2", "1"), "B": ("6", "2", "1")}, minutes=3)
    start = END - timedelta(minutes=2) + timedelta(seconds=17, microseconds=123456)
    cutoff = END + timedelta(seconds=17, microseconds=654321)
    snapshot.update(
        anchor={"ts": iso(start)},
        cutoff=iso(cutoff),
        assembled_at=iso(cutoff + timedelta(seconds=1)),
        flow_segments=[],
    )
    for venue in ("A", "B"):
        left = {
            "start": iso(start),
            "end": iso(END - timedelta(minutes=1)),
            "buy_qty": "1",
            "sell_qty": "0",
            "sum_sq_qty": "0",
            "complete": True,
        }
        snapshot["flow_segments"].append({**left, "instrument_id": f"{venue}:spot:T"})
        snapshot["flow_bars"].append(
            flow(
                venue,
                END,
                complete=False,
                boundary_segments=[{**left, "start": iso(END), "end": iso(cutoff), "buy_qty": "2"}],
            )
        )
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    event = result["spots"]["EVENT"]
    assert event["coverage"]["n_complete"] == 2
    assert len(event["venues"][0]["boundary_segments"]) == 2
    assert Decimal(event["venues"][0]["buy_qty"]) == 9


@pytest.mark.parametrize("variant", ["complete", "partial_cohort_and_previous_gap"])
async def test_native_spot_all_five_windows_intersection_and_previous_cohort_velocity(
    variant, spot_client, original_spot, record_property
):
    snapshot = packet(
        {"A": ("4", "0", "1"), "B": ("2", "0", "1")},
        minutes=2880,
        event_seconds=14400,
    )
    for row in snapshot["flow_bars"]:
        if parse_time(row["minute_start"]) < END - timedelta(days=1):
            row["buy_qty"] = "2" if row["instrument_id"].startswith("A:") else "1"
    snapshot["instruments"].extend(
        [instrument("foreign", token="OTHER"), instrument("unscoped", token=None)]
    )
    snapshot["instruments"][-1]["market_type"] = "PERP_LINEAR"
    if variant != "complete":
        snapshot["flow_bars"] = [
            row
            for row in snapshot["flow_bars"]
            if not (
                row["instrument_id"] == "A:spot:T"
                and parse_time(row["minute_start"]) == END - timedelta(hours=36)
            )
        ]
        snapshot["instruments"].append(instrument("C"))
        snapshot["flow_bars"].extend(
            flow("C", END - timedelta(minutes=minute), "20", "0", "1")
            for minute in range(60, 0, -1)
        )
        snapshot["tickers"].append({"venue": "C", "ts": iso(END), "volume": "0"})
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    assert result["primary"] == "EVENT"
    assert result["spots"]["W24H"]["composite_cohort"]["cohort"] == ["A", "B"]
    assert result["spots"]["W24H"]["composite_cohort"]["velocity"] == (
        2.0 if variant == "complete" else None
    )
    assert result["spots"]["W4H"]["composite_cohort"]["velocity"] == 1.0
    if variant == "complete":
        assert all(result["spots"][name]["composite_cohort"] for name in result["spots"])
    else:
        assert result["spots"]["W15M"]["composite_cohort"] is None
        assert result["spots"]["W1H"]["composite_cohort"] is None
        assert not any(result["spots"]["W15M"]["cohort_consistent"].values())


@pytest.mark.parametrize("anchor", [None, "2026-10-19T12:00:00Z", "2026-10-21T11:45:00Z"])
async def test_native_spot_primary_selection_and_empty_registered_instruments(
    anchor, spot_client, original_spot, record_property
):
    snapshot = {"token_id": "T", "cutoff": iso(END), "anchor": anchor, "flow_bars": [], "tickers": []}
    result = await native_result(spot_client, original_spot, snapshot, record_property)
    assert result["primary"] == ("EVENT" if anchor == "2026-10-21T11:45:00Z" else "W4H")
    assert all(spot["venues"] == [] for spot in result["spots"].values())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("buy_qty", "1000000000000"),
        ("buy_qty", "0.0000000000000000000000001"),
        ("sum_sq_qty", "1000000000000000000000000"),
        ("sell_qty", "NaN"),
    ],
)
async def test_native_spot_explicit_financial_range_refusal(field, value, spot_client):
    snapshot = packet({"A": ("1", "0", "0"), "B": ("1", "0", "0")}, minutes=1, event_seconds=60)
    snapshot["flow_bars"][0][field] = value
    sql, params = truth_sql_spot.query(snapshot)
    with pytest.raises(RuntimeError, match="ClickHouse"):
        await spot_client.rows(sql, params)
