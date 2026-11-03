import copy
import json
import os
from array import array
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from truth_sql_failure_evidence import EvidenceClickHouse

import tank.analysis.truth as reference
from tank.analysis.truth_sql_relative import benchmark_query, ctes, query
from tank.util import iso, parse_time

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def original_truth():
    namespace = dict(vars(reference))
    exec(compile(_FROZEN_REFERENCE, "original_relative_truth.py", "exec"), namespace)
    return SimpleNamespace(**namespace)


@pytest.fixture
async def native_clickhouse(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        yield client
    finally:
        await client.close()


def assert_complete(actual, expected, path="relative"):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), path
        assert actual.keys() == expected.keys(), path
        for key in expected:
            assert_complete(actual[key], expected[key], path + "." + key)
    elif isinstance(expected, (list, tuple)):
        assert isinstance(actual, list), path
        assert len(actual) == len(expected), path
        for index, (left, right) in enumerate(zip(actual, expected, strict=True)):
            assert_complete(left, right, f"{path}[{index}]")
    elif isinstance(expected, Decimal):
        assert Decimal(actual) == expected, path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=2e-10, abs=2e-12), path
    else:
        assert actual == expected, path
        if isinstance(expected, bool):
            assert isinstance(actual, bool), path


def packet(*, secondary=False, fractional=False, history_days=10, benchmark=False):
    start = datetime(2026, 1, 12, tzinfo=UTC)
    end = start + timedelta(hours=4, minutes=15, seconds=15 if fractional else 0)
    token = "chain:BTC" if benchmark else "chain:AAA"
    identities = [
        (token, "BTC" if benchmark else "AAA"),
        ("chain:ETH", "ETH"),
        ("chain:P1", "P1"),
        ("chain:P2", "P2"),
    ]
    if not benchmark:
        identities.insert(1, ("chain:BTC", "BTC"))
    instruments = [
        dict(instrument_id="A:spot:" + symbol, token_id=identity,
             market_type="SPOT", venue="A", base_asset=symbol)
        for identity, symbol in identities
    ]
    if secondary:
        instruments.insert(1, dict(instruments[0], instrument_id="B:spot:" + identities[0][1], venue="B"))
    result: dict[str, Any] = dict(
        token_id=token, cutoff=iso(end), assembled_at=iso(end + timedelta(minutes=1)),
        anchor=dict(type="KNOWN_TO_US_AT", ts=iso(start)), expected_direction="BULLISH",
        tokens=[dict(token_id=identity, symbol=symbol, sector="DEX_EXECUTION") for identity, symbol in identities],
        instruments=instruments, klines=[], stat_price_series={}, flow_bars=[], flow_segments=[], reference_points=[],
    )
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    own = [item for item in instruments if item["token_id"] == token]
    for ordinal, instrument in enumerate(instruments):
        times = {
            start - timedelta(days=history_days) + timedelta(hours=index)
            for index in range(history_days * 24 + 5)
        }
        times.update(end - timedelta(seconds=seconds) for seconds in (0, 900, 3600, 14400, 86400))
        times.add(start)
        times = sorted(times)
        values = []
        for index, _stamp in enumerate(times):
            value = Decimal(100 + ordinal * 20) + Decimal(index) / 100 + Decimal((index * (ordinal + 3)) % 19) / 50
            values.append(float(value))
        iid = instrument["instrument_id"]
        result["stat_price_series"][iid] = dict(
            token_id=instrument["token_id"], time_basis="CLOSE_TIME", coverage_count=len(times),
            times_us=array("q", (int((stamp - epoch).total_seconds() * 1000000) for stamp in times)),
            values=array("d", values), complete=bytearray([1]) * len(times),
        )
        hourly = max(stamp for stamp in times if stamp.minute == 0 and stamp.second == 0)
        for stamp, value in zip(times, values, strict=True):
            if stamp in {times[-1], hourly, start}:
                result["klines"].append(dict(
                    instrument, open_time=iso(stamp - timedelta(minutes=1)), interval_s=60,
                    c=format(Decimal(str(value)), "f"), complete=True, quote_volume_usd="1",
                ))
    for instrument in own:
        iid = instrument["instrument_id"]
        left = end - timedelta(days=1)
        first = left.replace(second=0, microsecond=0)
        if first < left:
            first += timedelta(minutes=1)
        last = end.replace(second=0, microsecond=0)
        multiplier = 2 if instrument["venue"] == "B" else 1
        for minute in range(int((last - first).total_seconds() / 60)):
            result["flow_bars"].append(dict(
                instrument_id=iid, minute_start=iso(first + timedelta(minutes=minute)), complete=True,
                buy_qty=str(3 * multiplier), sell_qty=str(2 * multiplier), sum_sq_qty="5",
            ))
        if fractional:
            for lower, upper in ((left, first), (last, end)):
                result["flow_segments"].append(dict(
                    instrument, start=iso(lower), end=iso(upper), complete=True,
                    buy_qty=str(3 * multiplier), sell_qty=str(2 * multiplier), sum_sq_qty="5", n_buy=1, n_sell=1,
                    observed_at=iso(end), received_ts=iso(end),
                ))
        for boundary, price in ((start, "100.000000000000000000000001"), (end, "103.000000000000000000000001")):
            result["reference_points"].append(dict(
                instrument_id=iid, boundary=iso(boundary), complete=True, observed_at=iso(end), received_ts=iso(end),
                last_trade=dict(ts=iso(boundary - timedelta(microseconds=1)), price=price,
                                received_ts=iso(end), trade_id="captured-" + iso(boundary)),
            ))
    return result


def oracle(original, snapshot, config=None, *, reaction=False):
    config = config or {}
    context = original._PARAMETERS.set(config)
    try:
        method = original.benchmark_reaction_values if reaction else original.relative_truth
        kwargs = {} if reaction else {"include_outcome": config.get("include_outcome", True)}
        return method(snapshot, parse_time(config.get("start", snapshot["anchor"]["ts"])),
                      parse_time(config.get("end", snapshot["cutoff"])), **kwargs)
    finally:
        original._PARAMETERS.reset(context)


async def execute(native_clickhouse, snapshot, config=None, *, reaction=False, record_property=None):
    sql, params = (benchmark_query if reaction else query)(snapshot, config)
    if record_property is not None:
        record_property("executed_sql", sql)
        record_property("domain", "benchmark" if reaction else "relative")
    rows = await native_clickhouse.rows(sql, params)
    assert len(rows) == 1
    assert rows[0].keys() == {"result"}
    return json.loads(rows[0]["result"])


@pytest.mark.parametrize("mode", ["regular", "secondary", "fractional", "benchmark", "short", "empty"])
async def test_native_relative_complete_original_output(native_clickhouse, original_truth, mode, record_property):
    snapshot = packet(secondary=mode == "secondary", fractional=mode == "fractional",
                      benchmark=mode == "benchmark", history_days=2 if mode == "short" else 10)
    if mode == "empty":
        snapshot.update(klines=[], stat_price_series={}, flow_bars=[], reference_points=[])
    actual = await execute(native_clickhouse, snapshot, record_property=record_property)
    assert_complete(actual, oracle(original_truth, snapshot))
    if mode == "secondary":
        assert actual["ref_venue"] == "B:spot:AAA"
    if mode == "short":
        assert actual["beta_flags"] == {"btc": "INSUFFICIENT_HISTORY", "eth": "INSUFFICIENT_HISTORY"}
        assert actual["windows"]["EVENT"]["normal_range_reason"] == "INCOMPLETE"
    if mode == "benchmark":
        assert actual["windows"]["EVENT"]["relative_measure"] == "R_RAW"
        assert actual["windows"]["EVENT"]["r_btc_adj_reason"] == "NOT_APPLICABLE_BENCHMARK"
    if mode == "fractional":
        assert len(actual["reference_coverage"][actual["ref_venue"]]["boundary_segments"]) == 2
    if mode == "regular":
        assert actual["beta_flags"] == {"btc": None, "eth": None}
        assert actual["normal_ranges"]["W7D"]["coverage"]["sample_count"] > 0
        assert actual["windows"]["W15M"]["historical_measurement"]["sample_count"] > 200


@pytest.mark.parametrize("control", ["no_anchor", "late_anchor", "late_trade", "no_btc", "missing_flow", "missing_metric", "late_segment"])
async def test_native_relative_missing_and_availability_controls(native_clickhouse, original_truth, control):
    snapshot = packet(fractional=control == "late_segment")
    if control == "no_anchor":
        snapshot["reference_points"] = [row for row in snapshot["reference_points"] if row["boundary"] != snapshot["anchor"]["ts"]]
    elif control in {"late_anchor", "late_trade"}:
        row = next(row for row in snapshot["reference_points"] if row["boundary"] == snapshot["anchor"]["ts"])
        target = row["last_trade"] if control == "late_trade" else row
        target["received_ts"] = iso(parse_time(snapshot["assembled_at"]) + timedelta(microseconds=1))
    elif control == "no_btc":
        snapshot["klines"] = [row for row in snapshot["klines"] if row["token_id"] != "chain:BTC"]
        snapshot["stat_price_series"].pop("A:spot:BTC")
    elif control == "missing_flow":
        snapshot["flow_bars"].pop()
    elif control == "missing_metric":
        snapshot["flow_bars"][0].pop("sum_sq_qty")
    else:
        snapshot["flow_segments"][0]["observed_at"] = iso(parse_time(snapshot["assembled_at"]) + timedelta(microseconds=1))
    actual = await execute(native_clickhouse, snapshot)
    assert_complete(actual, oracle(original_truth, snapshot))
    if control in {"no_anchor", "late_anchor", "late_trade"}:
        assert actual["anchor"]["price_reason"] == "INCOMPLETE"
        assert actual["windows"]["EVENT"]["r_raw"] is None
    if control in {"missing_flow", "missing_metric", "late_segment"}:
        assert actual["ref_venue"] is None
        assert actual["reference_coverage"]["A:spot:AAA"]["excluded"]


@pytest.mark.parametrize("control", ["basket", "fallback", "identity_payments", "no_sector", "coverage_choice", "stale_btc", "degenerate"])
async def test_native_relative_selection_and_history_controls(native_clickhouse, original_truth, control):
    snapshot = packet()
    if control == "basket":
        snapshot["sector_baskets"] = [dict(sector="CAPTURED", weighting="EQUAL", constituents=["chain:AAA", "chain:P1", "chain:P2"])]
        for row in snapshot["tokens"]:
            row.pop("sector")
    elif control == "fallback":
        snapshot["tokens"][0]["sector"] = "LONE"
    elif control == "identity_payments":
        snapshot["tokens"][0]["sector"] = "IDENTITY_PAYMENTS"
    elif control == "no_sector":
        for row in snapshot["tokens"]:
            row.pop("sector")
    elif control == "coverage_choice":
        source = copy.deepcopy(snapshot["stat_price_series"]["A:spot:BTC"])
        source["values"] = array("d", (value * 1.001 + index / 100 for index, value in enumerate(source["values"])))
        source["coverage_count"] = 999999
        snapshot["stat_price_series"]["preferred:spot:BTC"] = source
        snapshot["series_coverage_counts"] = {"A:spot:BTC": 1000000, "preferred:spot:BTC": 1}
    elif control == "stale_btc":
        end = parse_time(snapshot["cutoff"])
        cutoff_us = int((end - timedelta(hours=3)).timestamp() * 1000000)
        source = snapshot["stat_price_series"]["A:spot:BTC"]
        pairs = [(stamp, value) for stamp, value in zip(source["times_us"], source["values"], strict=True) if stamp < cutoff_us]
        source["times_us"] = array("q", (stamp for stamp, _ in pairs))
        source["values"] = array("d", (value for _, value in pairs))
        snapshot["klines"] = [row for row in snapshot["klines"] if row["token_id"] != "chain:BTC" or parse_time(row["open_time"]) < end - timedelta(hours=3)]
    else:
        for source in snapshot["stat_price_series"].values():
            source["values"] = array("d", [100.] * len(source["values"]))
        for row in snapshot["klines"]:
            row["c"] = "100"
    actual = await execute(native_clickhouse, snapshot)
    assert_complete(actual, oracle(original_truth, snapshot))
    event = actual["windows"]["EVENT"]
    if control in {"basket", "fallback"}:
        assert event["r_sector_adj"] is not None
    elif control == "identity_payments":
        assert event["sector_reason"] == "NOT_APPLICABLE"
    elif control == "no_sector":
        assert event["sector_reason"] == "NOT_ESTIMABLE"
    elif control == "stale_btc":
        assert event["r_btc_adj"] is None
    elif control == "degenerate":
        assert event["normal_range_reason"] == "DEGENERATE_HISTORY"
        assert event["beyond_normal"] is None
        assert actual["beta_flags"]["btc"] == "NOT_ESTIMABLE"


async def test_native_relative_preserves_columnar_population_and_pre_event_cutoff(native_clickhouse, original_truth):
    snapshot = packet()
    first = await execute(native_clickhouse, snapshot)
    changed = copy.deepcopy(snapshot)
    cutoff_us = int(parse_time(snapshot["cutoff"]).timestamp() * 1000000)
    start_us = int(parse_time(snapshot["anchor"]["ts"]).timestamp() * 1000000)
    for source in changed["stat_price_series"].values():
        source["times_us"].append(cutoff_us + 1)
        source["values"].append(999999.)
        for index, stamp in enumerate(source["times_us"]):
            if start_us <= stamp <= cutoff_us:
                source["values"][index] *= 1.03
    second = await execute(native_clickhouse, changed)
    assert_complete(second, oracle(original_truth, changed))
    assert second["beta_btc"] == first["beta_btc"]
    assert second["beta_eth"] == first["beta_eth"]
    assert second["normal_ranges"] == first["normal_ranges"]
    for name in first["windows"]:
        assert second["windows"][name]["historical_measurement"] == first["windows"][name]["historical_measurement"]
        assert second["windows"][name]["normal_range"] == first["windows"][name]["normal_range"]


@pytest.mark.parametrize("mode", ["embedded_references", "segment_chain", "projected_tie", "parallel_unequal_arrays"])
async def test_native_relative_native_input_shapes(native_clickhouse, original_truth, mode):
    snapshot = packet(fractional=mode == "segment_chain")
    if mode == "embedded_references":
        for point in snapshot["reference_points"]:
            minute = iso(parse_time(point["boundary"]) - timedelta(minutes=1))
            bar = next(row for row in snapshot["flow_bars"] if row["minute_start"] == minute)
            bar["reference_points"] = [point]
        snapshot["reference_points"] = []
    elif mode == "segment_chain":
        first = snapshot["flow_segments"].pop(0)
        middle = iso(parse_time(first["start"]) + timedelta(seconds=10))
        snapshot["flow_segments"].extend([dict(first, end=middle), dict(first, start=middle)])
    elif mode == "projected_tie":
        btc = snapshot["stat_price_series"].pop("A:spot:BTC")
        other = copy.deepcopy(btc)
        other["values"] = array("d", (value + index / 100 for index, value in enumerate(other["values"])))
        snapshot["stat_price_series"]["z-first:spot:BTC"] = btc
        snapshot["stat_price_series"]["a-second:spot:BTC"] = other
        snapshot["klines"] = [row for row in snapshot["klines"] if row["token_id"] != "chain:BTC"]
    else:
        for source in snapshot["stat_price_series"].values():
            source["times_us"].append(source["times_us"][-1] + 1)
    actual = await execute(native_clickhouse, snapshot)
    assert_complete(actual, oracle(original_truth, snapshot))
    if mode == "embedded_references":
        assert actual["anchor"]["evidence"]["trade_id"].startswith("captured-")
    if mode == "projected_tie":
        assert actual["outcome_btc_reference"]["instrument_id"] == "z-first:spot:BTC"
    if mode == "segment_chain":
        segments = actual["reference_coverage"]["A:spot:AAA"]["boundary_segments"]
        assert len(segments[0]["aggregates"]) == 2


async def test_native_relative_exact_large_reference_return(native_clickhouse, original_truth):
    snapshot = packet()
    for point in snapshot["reference_points"]:
        point["last_trade"]["price"] = (
            "10000000000000000.01" if point["boundary"] == snapshot["anchor"]["ts"]
            else "10000000000000000.02"
        )
    actual = await execute(native_clickhouse, snapshot)
    assert_complete(actual, oracle(original_truth, snapshot))
    value = actual["windows"]["EVENT"]["r_raw"]
    expected = float(Decimal("0.01") / Decimal("10000000000000000.01"))
    assert value > 0
    assert value == pytest.approx(expected, abs=1e-24, rel=0)


async def test_native_relative_custom_parameters_and_no_outcomes(native_clickhouse, original_truth):
    snapshot = packet()
    config = dict(history_min_days=3, historical_lookback_days=5, beta_blume_weight=0.25,
                  normal_range_quantiles=[0.1, 0.9], include_outcome=False)
    actual = await execute(native_clickhouse, snapshot, config)
    assert_complete(actual, oracle(original_truth, snapshot, config))
    assert actual["normal_ranges"] == {}
    assert actual["windows"]["EVENT"]["historical_measurement"]["sample_count"] > 0


@pytest.mark.parametrize("control", ["selected", "stale_start", "stale_end", "zero_volume", "incomplete", "nonspot"])
async def test_native_benchmark_reaction_complete_original_output(native_clickhouse, original_truth, control, record_property):
    snapshot = packet(secondary=True, benchmark=True)
    for row in snapshot["klines"]:
        if row["token_id"] == snapshot["token_id"]:
            row["quote_volume_usd"] = "10" if row["venue"] == "B" else "1"
    if control in {"stale_start", "stale_end"}:
        boundary = parse_time(snapshot["anchor"]["ts"] if control == "stale_start" else snapshot["cutoff"])
        snapshot["klines"] = [row for row in snapshot["klines"] if not
            (row["venue"] == "B" and row["token_id"] == snapshot["token_id"] and
             boundary - timedelta(minutes=2) <= parse_time(row["open_time"]) + timedelta(minutes=1) <= boundary)]
    elif control == "zero_volume":
        for row in snapshot["klines"]:
            row["quote_volume_usd"] = "0"
    elif control == "incomplete":
        for row in snapshot["klines"]:
            if row["venue"] == "B":
                row["complete"] = False
    elif control == "nonspot":
        for row in snapshot["klines"]:
            if row["venue"] == "B":
                row["market_type"] = "PERP_LINEAR"
    actual = await execute(native_clickhouse, snapshot, reaction=True, record_property=record_property)
    assert_complete(actual, oracle(original_truth, snapshot, reaction=True))
    if control == "selected":
        assert actual["r_raw_venue"] == "B:spot:BTC"
        assert actual["r_raw"] is not None
    if control in {"stale_start", "stale_end", "zero_volume"}:
        assert actual["r_raw_reason"] == "INCOMPLETE"


@pytest.mark.parametrize(("values", "value", "reason", "beyond"), [
    ([], 0., "INCOMPLETE", None),
    ([1., 1. + 5e-13], 1.01, "DEGENERATE_HISTORY", None),
    ([-1., 1.], -0.95, None, False),
    ([-1., 1.], 0.95, None, False),
    ([-1., 1.], -0.950000001, None, True),
    ([-1., 1.], 0.950000001, None, True),
])
async def test_native_relative_statistical_classification_boundaries(native_clickhouse, values, value, reason, beyond):
    _, params = query(packet(history_days=0))
    sql = "WITH " + ctes() + (
        " SELECT concat('{\"reason\":', toJSONString(rel_range_reason({rel_test_values:Array(Float64)})), "
        "',\"beyond\":', rel_beyond_json({rel_test_value:Float64}, {rel_test_values:Array(Float64)}), '}') AS result"
    )
    params.update(rel_test_values=(json.dumps(values), "Array(Float64)"), rel_test_value=(value, "Float64"))
    rows = await native_clickhouse.rows(sql, params)
    assert json.loads(rows[0]["result"]) == dict(reason=reason, beyond=beyond)


@pytest.mark.parametrize(
    ("value", "event_history", "day_history", "expected"),
    [
        (10., [-1., 0., 1.], [-10., 0., 10.], "HEAVY"),
        (-2., [-1., 0., 1.], [-10., 0., 10.], "NEGATIVE"),
        (2., [-1., 0., 1.], [-10., 0., 10.], "SUBSTANTIAL"),
        (0.75, [-1., 0., 1.], [-10., 0., 10.], "EARLY"),
        (0., [-1., 0., 1.], [-10., 0., 10.], "UNPRICED"),
        (0., [], [-10., 0., 10.], None),
        (0.1, [0., 0.], [0., 0.], None),
        (2., [], [-1., 0., 1.], "HEAVY"),
        (0.5, [-1., 0., 1.], [-10., 0., 10.], "UNPRICED"),
    ],
)
async def test_native_relative_repricing_classification_priority(
    native_clickhouse, original_truth, value, event_history, day_history, expected
):
    _, params = query(packet(history_days=0))
    sql = "WITH " + ctes() + (
        " SELECT toJSONString(rel_repricing_for({rel_test_value:Float64}, "
        "{rel_test_event:Array(Float64)}, {rel_test_day:Array(Float64)})) AS result"
    )
    params.update(
        rel_test_value=(value, "Float64"),
        rel_test_event=(json.dumps(event_history), "Array(Float64)"),
        rel_test_day=(json.dumps(day_history), "Array(Float64)"),
    )
    rows = await native_clickhouse.rows(sql, params)
    actual = json.loads(rows[0]["result"])
    assert actual == expected
    assert actual == original_truth.repricing(value, event_history, day_history)


@pytest.mark.parametrize(
    ("value", "volatility", "expected"),
    [
        (-0.1, 0.02, "STRESS"),
        (0., 0.03, "STRESS"),
        (-0.050001, 0.02, "HEADWIND"),
        (-0.05, 0.02, "NEUTRAL"),
        (0., 0.02, "NEUTRAL"),
    ],
)
async def test_native_relative_market_context_classification(
    native_clickhouse, original_truth, value, volatility, expected
):
    _, params = query(packet(history_days=0))
    sql = "WITH " + ctes() + (
        " SELECT toJSONString(rel_market_context_for({rel_test_value:Float64}, "
        "{rel_test_volatility:Float64}, [-0.1, 0., 0.1], [0.01, 0.02, 0.03])) AS result"
    )
    params.update(rel_test_value=(value, "Float64"), rel_test_volatility=(volatility, "Float64"))
    rows = await native_clickhouse.rows(sql, params)
    actual = json.loads(rows[0]["result"])
    assert actual == expected
    assert actual == original_truth.market_context(value, volatility, [-0.1, 0., 0.1], [0.01, 0.02, 0.03])


@pytest.mark.parametrize("direction", ["BEARISH", "LOSER", -1])
async def test_native_relative_negative_repricing_direction(native_clickhouse, original_truth, direction):
    snapshot = packet()
    snapshot["expected_direction"] = direction
    actual = await execute(native_clickhouse, snapshot)
    assert_complete(actual, oracle(original_truth, snapshot))
    assert actual["repricing_sign"] == -1


@pytest.mark.parametrize("invalid", ["1000000000000000000000000", "0.0000000000000000000000001"])
async def test_native_relative_refuses_unrepresentable_reference_price(native_clickhouse, invalid):
    snapshot = packet(history_days=0)
    snapshot["reference_points"][0]["last_trade"]["price"] = invalid
    sql, params = query(snapshot)
    with pytest.raises(RuntimeError, match="native financial range"):
        await native_clickhouse.rows(sql, params)


_FROZEN_REFERENCE = r'''def parameter(name, default):
    return (_PARAMETERS.get() or {}).get(name, default)

def lookback_days():
    return parameter("historical_lookback_days", parameter("base_retention_days", 90))

def decimal(v):
    if isinstance(v, dict):
        v = v.get("value")
    if v is None:
        return None
    value = v if isinstance(v, Decimal) else Decimal(str(v))
    exact_context(value)
    return value

def exact_context(*values):
    values = [v for v in values if isinstance(v, Decimal) and v.is_finite()]
    if values:
        span = max(v.adjusted() for v in values) - min(v.as_tuple().exponent for v in values) + 1
        getcontext().prec = max(getcontext().prec, 2 * span + 32)

def precise(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        with localcontext() as context:
            context.prec = 128
            return function(*args, **kwargs)

    return wrapped

def number(v):
    if isinstance(v, dict):
        v = v.get("value")
    return None if v is None else float(v)

def ratio(a, b):
    if a is None or not b:
        return None
    return float(decimal(a) / decimal(b))

@precise
def strict_majority(part, total):
    """The §0 predicate, independent of rounded dimensionless display statistics."""
    if part is None or total is None:
        return None
    part, total = decimal(part), decimal(total)
    if total <= 0:
        return None
    exact_context(part, total)
    return 2 * part > total

def remember(cache, key, build):
    if cache is None:
        return build()
    if key not in cache:
        cache[key] = build()
        if len(cache) > 128:
            cache.pop(next(iter(cache)))
    return cache[key]

def percentile(values, p):
    s = (
        values.ordered
        if isinstance(values, StatisticalPopulation)
        else sorted(v for v in values if v is not None)
    )
    if not s:
        return None
    k = (len(s) - 1) * p
    i = int(k)
    return s[i] + (s[min(i + 1, len(s) - 1)] - s[i]) * (
        decimal(k - i) if isinstance(s[i], Decimal) else k - i
    )

def normal(values):
    qs = parameter("normal_range_quantiles", [0.025, 0.975])
    return [percentile(values, float(qs[0])), percentile(values, float(qs[1]))] if values else None

def float_normal(values):
    """normal() for measures built with float arithmetic (beta-adjusted returns): a zero-width
    range there ([x, x] up to 1e-12) comes from identical history and float noise alone could
    exceed it (BTC-adjusted returns of BTC are ~1e-16 around 0), so it is unavailable
    (DEGENERATE_HISTORY). Exact Decimal measures (OI, funding, volume) keep normal()."""
    r = normal(values)
    if r is None or None in r:
        return r
    lo, hi = float(r[0]), float(r[1])
    return None if abs(hi - lo) <= 1e-12 * max(1.0, abs(lo), abs(hi)) else r

def range_reason(values):
    """Why float_normal(values) is unavailable: INCOMPLETE (no history) or DEGENERATE_HISTORY."""
    if not values:
        return "INCOMPLETE"
    return "DEGENERATE_HISTORY" if float_normal(values) is None else None

def beyond(v, r):
    return None if v is None or not r or None in r else v < r[0] or v > r[1]

def beta(token, benchmark):
    pairs = [(benchmark[t], token[t]) for t in token.keys() & benchmark.keys()]
    if len(pairs) < int(parameter("history_min_days", 7)) * 24:
        return 1.0, "INSUFFICIENT_HISTORY"
    x, y = zip(*pairs, strict=False)
    xm, ym = mean(x), mean(y)
    den = sum((v - xm) ** 2 for v in x)
    weight = float(parameter("beta_blume_weight", 2 / 3))
    return (
        (weight * sum((a - xm) * (b - ym) for a, b in pairs) / den + (1 - weight), None)
        if den
        else (1.0, "NOT_ESTIMABLE")
    )

def market_context(r, rv, returns, vols):
    rr, vr = normal(returns), normal(vols)
    if not rr or not vr or r is None or rv is None:
        return None
    return "STRESS" if r < rr[0] or rv > vr[1] else "HEADWIND" if r < percentile(returns, 0.25) else "NEUTRAL"

def repricing(value, event_history, day_history):
    er, dr = float_normal(event_history), float_normal(day_history)
    if value is None:
        return None
    if dr and value > dr[1]:
        return "HEAVY"
    if not er:
        return None
    if value < er[0]:
        return "NEGATIVE"
    if value > er[1]:
        return "SUBSTANTIAL"
    return "EARLY" if value > percentile(event_history, 0.75) else "UNPRICED"

def _time(row):
    times = _TIMES.get()
    if times is not None:
        hit = times.get(id(row))
        if hit is not None:
            return hit[1]
    value = _observation_time(row)
    if times is not None:
        times[id(row)] = (row, value)
    return value

def _observation_time(row):
    for k in (
        "minute_start",
        "open_time",
        "exchange_ts",
        "ts",
        "as_of",
        "settlement_time",
        "funding_time",
        "observed_at",
    ):
        if row.get(k) is not None:
            return parse_time(row[k])
    return None

def kline_close_time(row):
    """The bar's close boundary, open_time + interval_s.

    Binance closeTime (column [6], 03 §5.2.7) is the bar's last millisecond (hh:59:59.999);
    keying prices by it splits hourly archive bars from minute bars closing at hh:00 and
    drops them from every hourly series (F-16: 331 instead of ~2100 BTC 24h samples).
    """
    if row.get("open_time") is not None:
        return parse_time(row["open_time"]) + timedelta(seconds=int(row.get("interval_s", 60)))
    return parse_time(row["close_time"]) if row.get("close_time") else None

def _venue(row):
    return row.get("venue") or str(row.get("instrument_id", "")).split(":")[0]

def _market(row):
    return row.get("market_type") or ("SPOT" if ":spot:" in row.get("instrument_id", "") else "PERP_LINEAR")

@precise
def _sum(rows, key):
    vals = [decimal(r.get(key)) for r in rows]
    if not vals or None in vals:
        return None
    exact_context(*vals)
    return sum(vals, Decimal(0))

@precise
def _composite(venues):
    if not venues:
        return dict(
            cvd=None, volume=None, buy_share=None, bs_ratio=None, venues=[], sign=0, reason="INCOMPLETE"
        )
    exact_context(*(decimal(x[k]) for x in venues for k in ("volume_native", "cvd", "sum_sq_qty")))
    v = sum((decimal(x["volume_native"]) for x in venues), Decimal(0))
    cvd = sum((decimal(x["cvd"]) for x in venues), Decimal(0))
    q = sum((decimal(x["sum_sq_qty"]) for x in venues), Decimal(0))
    sign = (
        1
        if cvd > 0 and cvd * cvd >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
        else -1
        if cvd < 0 and cvd * cvd >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
        else 0
    )
    return dict(
        cvd=cvd,
        volume=v,
        buy_share=ratio(v + cvd, 2 * v),
        bs_ratio=ratio(v + cvd, v - cvd),
        venues=[x["venue"] for x in venues],
        sign=sign,
    )

def _available(row, snapshot):
    assembled = snapshot.get("assembled_at")
    received = row.get("received_ts")
    if not assembled or not received:
        return True
    inputs = _ASSESSMENT_INPUTS.get()
    if inputs is None:
        return parse_time(received) <= parse_time(assembled)
    memo = inputs.setdefault("available", {})
    hit = memo.get((id(row), assembled))
    if hit is None or hit[0] is not row:
        hit = (row, parse_time(received) <= parse_time(assembled))
        memo[(id(row), assembled)] = hit
    return hit[1]

def flow_segments(snapshot):
    """Bounded aggregate pieces extracted from typed minute bars, never individual prints.
    Built once per assessment per snapshot (callers only read it)."""
    inputs = _ASSESSMENT_INPUTS.get()
    sources = (snapshot.get("flow_segments", []), snapshot.get("flow_bars", []))
    if inputs is not None:
        memo = inputs.setdefault("flow_segments", {})
        hit = memo.get(tuple(map(id, sources)))
        if hit is not None and hit[0] is sources[0] and hit[1] is sources[1]:
            return hit[2]
    pieces = _flow_segments(snapshot)
    if inputs is not None:
        memo[tuple(map(id, sources))] = (*sources, pieces)
    return pieces

def _flow_segments(snapshot):
    result = {}
    for row in snapshot.get("flow_segments", []):
        result[(row["instrument_id"], row["start"], row["end"])] = row
    for bar in snapshot.get("flow_bars", []):
        for piece in bar.get("boundary_segments") or []:
            row = dict(
                piece,
                instrument_id=bar["instrument_id"],
                token_id=bar.get("token_id"),
                venue=_venue(bar),
                market_type=_market(bar),
            )
            result.setdefault((row["instrument_id"], row["start"], row["end"]), row)
    return list(result.values())

def segment_cover(pieces, iid, start, end, assembled_at=None):
    selected = [
        r
        for r in pieces
        if r.get("instrument_id") == iid
        and r.get("complete") is True
        and start <= parse_time(r["start"]) < parse_time(r["end"]) <= end
        and _available(r, {"assembled_at": assembled_at})
        and (
            assembled_at is None
            or r.get("observed_at") is None
            or parse_time(r["observed_at"]) <= parse_time(assembled_at)
        )
    ]
    cursor = start
    result = []
    while cursor < end:
        choices = [r for r in selected if parse_time(r["start"]) == cursor]
        if not choices:
            return None
        row = max(choices, key=lambda r: parse_time(r["end"]))
        result.append(row)
        cursor = parse_time(row["end"])
    return result

def flow_reference(snapshot, iid, boundary):
    points = list(snapshot.get("reference_points", []))
    for bar in snapshot.get("flow_bars", []):
        if bar.get("instrument_id") != iid:
            continue
        points.extend(dict(point, instrument_id=iid) for point in (bar.get("reference_points") or []))
        if bar.get("complete") is True and _time(bar) + timedelta(minutes=1) == boundary:
            points.append(
                dict(
                    instrument_id=iid, boundary=iso(boundary), complete=True, last_trade=bar.get("last_trade")
                )
            )
    eligible = [
        p
        for p in points
        if p.get("instrument_id") == iid
        and p.get("boundary")
        and parse_time(p["boundary"]) == boundary
        and p.get("complete") is True
        and _available(p, snapshot)
        and (
            snapshot.get("assembled_at") is None
            or p.get("observed_at") is None
            or parse_time(p["observed_at"]) <= parse_time(snapshot["assembled_at"])
        )
    ]
    if not eligible:
        return None
    trades = [
        p["last_trade"]
        for p in eligible
        if p.get("last_trade")
        and _time(p["last_trade"]) is not None
        and _time(p["last_trade"]) < boundary
        and _available(p["last_trade"], snapshot)
        and decimal(p["last_trade"].get("price")) is not None
    ]
    return dict(max(trades, key=_time), instrument_id=iid) if trades else None

@precise
def spot_truth(
    rows,
    tickers,
    instruments,
    start,
    end,
    *,
    flow_segments=None,
    assembled_at=None,
):
    pieces = flow_segments or []
    grouped = defaultdict(list)
    ids = defaultdict(set)
    for i in instruments:
        if _market(i) == "SPOT":
            ids[_venue(i)].add(i["instrument_id"])
    for r in rows:
        if _market(r) == "SPOT" and _time(r) < end and _time(r) + timedelta(minutes=1) > start:
            ids[_venue(r)].add(r["instrument_id"])
            if start <= _time(r) and _time(r) + timedelta(minutes=1) <= end:
                grouped[_venue(r)].append(r)
    registered = set(ids)
    venues = []
    excluded = []
    floor_start = start.replace(second=0, microsecond=0)
    floor_end = end.replace(second=0, microsecond=0)
    ceil_start = floor_start + timedelta(minutes=1) if start != floor_start else start
    expected = max(0, int((floor_end - ceil_start).total_seconds() / 60))
    partial = []
    if start != floor_start:
        partial.append((start, min(ceil_start, end)))
    if end != floor_end and not (start != floor_start and floor_end == floor_start):
        partial.append((max(start, floor_end), end))
    for venue in sorted(registered):
        rs = list(grouped[venue])
        minutes = {_time(r) for r in rs}
        boundary_failed = False
        segments = []
        for segment_start, segment_end in partial:
            selected = []
            for iid in ids[venue]:
                covered = segment_cover(pieces, iid, segment_start, segment_end, assembled_at)
                if covered is None:
                    boundary_failed = True
                    break
                selected.extend(covered)
            if boundary_failed:
                break
            rs.extend(selected)
            segments.append(
                dict(
                    start=iso(segment_start),
                    cutoff=iso(segment_end),
                    trade_count=sum((r.get("n_buy", 0) + r.get("n_sell", 0)) for r in selected),
                    aggregates=selected,
                )
            )
        if len(minutes) < expected or not all(r.get("complete") is True for r in rs) or boundary_failed:
            excluded.append(
                dict(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="BOUNDARY_AGGREGATE_UNAVAILABLE"
                    if boundary_failed
                    else "FULL_MINUTE_COVERAGE_INCOMPLETE",
                    source_reasons=sorted(
                        {
                            str(r.get("gap_reason") or r.get("coverage_reason"))
                            for r in list(grouped[venue]) + pieces
                            if r.get("instrument_id") in ids[venue]
                            and (r.get("gap_reason") or r.get("coverage_reason"))
                        }
                    ),
                )
            )
            continue
        b, s, q = (_sum(rs, k) for k in ("buy_qty", "sell_qty", "sum_sq_qty"))
        if None in (b, s, q):
            excluded.append(
                dict(
                    venue=venue,
                    reason="INCOMPLETE",
                    detail="FLOW_AGGREGATE_FIELDS_MISSING",
                    missing_metrics=[
                        k for k in ("buy_qty", "sell_qty", "sum_sq_qty") if any(r.get(k) is None for r in rs)
                    ],
                )
            )
            continue
        exact_context(b, s, q)
        v = b + s
        c = b - s
        sign = (
            1
            if c > 0 and c * c >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
            else -1
            if c < 0 and c * c >= decimal(parameter("significant_flow_sigma", 2)) ** 2 * q
            else 0
        )
        venues.append(
            dict(
                venue=venue,
                complete=True,
                volume_native=v,
                buy_qty=b,
                sell_qty=s,
                cvd=c,
                buy_share=ratio(b, v),
                bs_ratio=ratio(b, s),
                sign=sign,
                sum_sq_qty=q,
                boundary_segments=segments,
            )
        )
    c = _composite(venues)
    v = c["volume"] or 0
    sign = c["sign"]
    estimated = []
    for ex in excluded:
        ts = [r for r in tickers if _venue(r) == ex["venue"] and _time(r) and _time(r) <= end]
        t = max(ts, key=_time) if ts else {}
        n = decimal(t.get("volume_native", t.get("volume")))
        estimated.append(n * decimal((end - start).total_seconds()) / 86400 if n is not None else None)
    covered = ratio(v, v + sum(estimated)) if None not in estimated else None
    cwb = {
        name: ratio(sum(x["volume_native"] for x in venues if x["sign"] == s), v)
        for name, s in [("buy", 1), ("sell", -1), ("neutral", 0)]
    }
    ranked = sorted(venues, key=lambda x: x["volume_native"], reverse=True)
    top = []
    vol = 0
    for x in ranked:
        top.append(x)
        vol += x["volume_native"]
        if vol > v / 2:
            break
    contributions = [(max(0, sign * x["cvd"]), x) for x in venues]
    total = sum(z[0] for z in contributions)
    tc = max(contributions, key=lambda x: x[0])[1] if contributions else None
    others = [x for x in venues if x is not tc]
    other_v = sum(x["volume_native"] for x in others)
    other_dir = ratio(sum(x["volume_native"] for x in others if x["sign"] == sign), other_v)
    top_share = ratio(max((z[0] for z in contributions), default=0), total)
    exact_majority = {
        name: strict_majority(sum((x["volume_native"] for x in venues if x["sign"] == s), Decimal(0)), v)
        for name, s in [("buy", 1), ("sell", -1), ("neutral", 0)]
    }
    exact_majority["covered"] = strict_majority(v, v + sum(estimated)) if None not in estimated else None
    direction_volume = sum(x["volume_native"] for x in venues if x["sign"] == sign)
    other_direction_volume = sum(x["volume_native"] for x in others if x["sign"] == sign)
    cls = (
        "INSUFFICIENT_DATA"
        if len(venues) < int(parameter("min_spot_venues", 2)) or covered is None or v <= sum(estimated)
        else "CONCENTRATED"
        if total
        and 2 * max(z[0] for z in contributions) > total
        and other_v
        and 2 * other_direction_volume < other_v
        else "BROAD"
        if 2 * direction_volume > v and ranked[0]["sign"] == sign
        else "DIVERGENT"
    )
    for x in venues:
        x["volume_share"] = ratio(x["volume_native"], v)
    return dict(
        venues=venues,
        composite_max=c,
        composite_cohort=None,
        breadth=dict(
            n_pos=sum(x["sign"] == 1 for x in venues),
            n_neg=sum(x["sign"] == -1 for x in venues),
            n_neutral=sum(x["sign"] == 0 for x in venues),
            pct_pos=ratio(sum(x["sign"] == 1 for x in venues), len(venues)),
            pct_neg=ratio(sum(x["sign"] == -1 for x in venues), len(venues)),
        ),
        cw_breadth=cwb,
        exact_majority=exact_majority,
        topn=dict(
            n=len(top),
            venues=[x["venue"] for x in top],
            agreement=ratio(sum(x["sign"] == sign for x in top), len(top)),
        ),
        concentration=dict(
            hhi=sum((ratio(x["volume_native"], v) or 0) ** 2 for x in venues) if v else None,
            top_volume_venue=ranked[0]["venue"] if ranked else None,
            top_volume_share=ratio(ranked[0]["volume_native"], v) if ranked else None,
            top_contrib_venue=tc["venue"] if tc else None,
            top_contrib_share=top_share,
            others_cwb_dir=other_dir,
        ),
        direction={1: "BUY", -1: "SELL", 0: "NEUTRAL"}[sign] if venues else None,
        classification=cls,
        coverage=dict(
            covered_share_est=covered,
            exact_majority=exact_majority["covered"],
            n_complete=len(venues),
            excluded=excluded,
        ),
        cohort_consistent={},
        size_profile=None,
    )

def _prices(snapshot, token=None, *, exact=False, reference_only=False):
    inputs = _ASSESSMENT_INPUTS.get()
    if exact or inputs is None:
        return _price_groups(snapshot, token, exact=exact, reference_only=reference_only)
    candles = snapshot.get("klines")
    projected = snapshot.get("stat_price_series")
    key = (id(candles), id(projected), snapshot["cutoff"], token)
    prices = inputs.setdefault("prices", {})
    if key not in prices:
        prices[key] = (candles, projected, _price_groups(snapshot, token))
    return prices[key][2]

def _price_groups(snapshot, token=None, *, exact=False, reference_only=False):
    groups = defaultdict(dict)
    references = {}
    cutoff = parse_time(snapshot["cutoff"])
    for r in snapshot.get("klines", []):
        if r.get("complete") is not True or _market(r) != "SPOT":
            continue
        if token and r.get("token_id") != token and token not in r.get("instrument_id", "").upper():
            continue
        t = kline_close_time(r) or _time(r) + timedelta(seconds=int(r.get("interval_s", 60)))
        raw = r.get("c", r.get("close"))
        if exact and reference_only:
            raw = raw.get("value") if isinstance(raw, dict) else raw
            price = None if raw is None else raw if isinstance(raw, Decimal) else Decimal(str(raw))
        else:
            price = (decimal if exact else number)(raw)
        if price and t <= cutoff:
            if exact and reference_only:
                latest, hourly = references.setdefault(r["instrument_id"], [None, None])
                if latest is None or t >= latest[0]:
                    references[r["instrument_id"]][0] = (t, price)
                if t.minute == 0 and t.second == 0 and (hourly is None or t >= hourly[0]):
                    references[r["instrument_id"]][1] = (t, price)
            else:
                groups[r["instrument_id"]][t] = price
    if exact and reference_only:
        return {
            iid: {point[0]: decimal(point[1]) for point in points if point is not None}
            for iid, points in references.items()
        }
    if not exact:
        epoch = parse_time("1970-01-01T00:00:00Z")
        for iid, projected in (snapshot.get("stat_price_series") or {}).items():
            if projected.get("time_basis") != "CLOSE_TIME":
                continue
            if token and projected.get("token_id") != token:
                continue
            for micros, value in zip(
                projected.get("times_us", ()), projected.get("values", ()), strict=False
            ):
                t = epoch + timedelta(microseconds=int(micros))
                price = number(value)
                if price and price > 0 and t <= cutoff:
                    groups[iid][t] = price
    return groups

def relative_truth(snapshot, start, end, *, include_outcome=True):
    groups = _prices(snapshot)
    exact_groups = _prices(snapshot, exact=True, reference_only=True)
    token_id = snapshot.get("token_id", "")
    token_symbols = {
        r["token_id"]: r["symbol"].upper()
        for r in snapshot.get("tokens", [])
        if r.get("token_id") and r.get("symbol")
    }
    identities = {
        r["instrument_id"]: r.get("token_id")
        for r in snapshot.get("instruments", [])
        if r.get("instrument_id")
    }
    identities.update(
        {
            r["instrument_id"]: r["token_id"]
            for r in snapshot.get("klines", [])
            if r.get("instrument_id") and r.get("token_id")
        }
    )
    identities.update(
        {iid: row.get("token_id") for iid, row in (snapshot.get("stat_price_series") or {}).items()}
    )
    coverage_counts = dict(snapshot.get("series_coverage_counts") or {})
    for iid, projected in (snapshot.get("stat_price_series") or {}).items():
        coverage_counts.setdefault(iid, projected.get("coverage_count", len(groups.get(iid, {}))))
    for r in snapshot.get("instruments", []):
        if r.get("token_id") and (r.get("base_asset") or r.get("base_symbol")):
            token_symbols.setdefault(r["token_id"], str(r.get("base_asset", r.get("base_symbol"))).upper())
    token_symbols.setdefault(token_id, str(token_id).split(":")[-1].upper())
    for identity in identities.values():
        if identity:
            token_symbols.setdefault(identity, str(identity).split(":")[-1].upper())
    candidates = {iid: p for iid, p in groups.items() if identities.get(iid) == token_id}
    ref_coverage = {}
    volumes = {}
    for iid in candidates:
        instrument = next(
            (i for i in snapshot.get("instruments", []) if i.get("instrument_id") == iid),
            dict(instrument_id=iid, market_type="SPOT"),
        )
        bars = [r for r in snapshot.get("flow_bars", []) if r.get("instrument_id") == iid]
        coverage = spot_truth(
            bars,
            [],
            [instrument],
            end - timedelta(days=1),
            end,
            flow_segments=flow_segments(snapshot),
            assembled_at=snapshot.get("assembled_at"),
        )
        observed = coverage["venues"][0] if coverage["venues"] else None
        ref_coverage[iid] = dict(
            complete=observed is not None,
            expected_duration_s=86400,
            boundary_segments=observed.get("boundary_segments", []) if observed else [],
            excluded=coverage["coverage"]["excluded"],
            volume_unit="NATIVE_TOKEN",
            covered_volume_native=observed["volume_native"] if observed else None,
        )
        if observed is not None:
            volumes[iid] = observed["volume_native"]
    ref = max(volumes, key=volumes.get) if volumes else None
    prices = candidates.get(ref, {})

    def series(symbol):
        matches = {iid: p for iid, p in groups.items() if token_symbols.get(identities.get(iid)) == symbol}
        return (
            matches[max(matches, key=lambda iid: coverage_counts.get(iid, len(matches[iid])))]
            if matches
            else {}
        )

    cutoff_row = flow_reference(snapshot, ref, end)
    anchor_row = flow_reference(snapshot, ref, start)
    anchor_price = decimal(anchor_row.get("price")) if anchor_row else None
    btc = series("BTC")
    eth = series("ETH")
    history_end = start
    cache = _BASELINE_CACHE.get()
    history_head = snapshot.get("relative_history_dependency_fingerprint")
    if history_head is None and cache is not None:
        history_head = history_fingerprint(
            [(iid, [(t, v) for t, v in rows.items() if t < start]) for iid, rows in groups.items()]
        )
    elif history_head is not None and not isinstance(history_head, str):
        history_head = fingerprint(history_head)
    scope = (
        history_head,
        iso(start),
        lookback_days(),
        parameter("history_min_days", 7),
        parameter("beta_blume_weight", 2 / 3),
    )

    def series_id(p):
        return tuple(iid for iid, rows in groups.items() if rows is p)

    def hourly_values(p):
        return {
            t: log(p[t] / p[t - timedelta(hours=1)])
            for t in p
            if t.minute == 0
            and t.second == 0
            and t < history_end
            and t - timedelta(hours=1) in p
            and t >= history_end - timedelta(days=lookback_days())
        }

    def hourly(p):
        return remember(cache, ("relative_hourly", scope, series_id(p)), lambda: hourly_values(p))

    bb, bflag = remember(
        cache,
        ("relative_beta", scope, series_id(prices), series_id(btc)),
        lambda: beta(hourly(prices), hourly(btc)),
    )
    be, eflag = remember(
        cache,
        ("relative_beta", scope, series_id(prices), series_id(eth)),
        lambda: beta(hourly(prices), hourly(eth)),
    )
    price_times = {}

    def price(p, t):
        times = price_times.setdefault(id(p), sorted(p)) if id(p) not in price_times else price_times[id(p)]
        index = bisect_right(times, t) - 1
        return p[times[index]] if index >= 0 else None

    def ret(p, a, z):
        pa, pz = price(p, a), price(p, z)
        return pz / pa - 1 if pa and pz else None

    def fresh_price(p, t):
        """A benchmark/peer price at t only if its last point is within that series' own bar
        spacing: a base-tier outage must not turn two copies of one old close into a 0 return."""
        times = price_times.setdefault(id(p), sorted(p)) if id(p) not in price_times else price_times[id(p)]
        index = bisect_right(times, t) - 1
        if index < 0:
            return None
        step = (times[index] - times[index - 1]).total_seconds() if index > 0 else 60
        return p[times[index]] if (t - times[index]).total_seconds() <= max(step, 60) else None

    def bench_ret(p, a, z):
        pa, pz = fresh_price(p, a), fresh_price(p, z)
        return pz / pa - 1 if pa and pz else None

    benchmark = token_symbols.get(token_id) == "BTC"
    historical_times = sorted(t for t in prices if t < start)
    deltas = [
        (b - a).total_seconds() for a, b in zip(historical_times, historical_times[1:], strict=False) if b > a
    ]
    history_resolution_s = int(median(deltas)) if deltas else None
    history_diagnostics = {}

    def hist_values(length):
        if (
            not historical_times
            or (historical_times[-1] - historical_times[0]).total_seconds()
            < float(parameter("history_min_days", 7)) * 86400
        ):
            return []
        out = []
        durations = []
        stalenesses = []
        ends = []

        for t in historical_times:
            target = t - timedelta(seconds=length)
            i = bisect_left(historical_times, target)
            if i == len(historical_times) or historical_times[i] != target:
                i -= 1
            if i < 0:
                continue
            a = historical_times[i]
            lag = (target - a).total_seconds()
            if (
                a < start - timedelta(days=lookback_days())
                or lag >= history_resolution_s
                or not benchmark
                and (t not in btc or a not in btc)
            ):
                continue
            out.append(
                prices[t] / prices[a] - 1
                if benchmark
                else prices[t] / prices[a] - 1 - bb * (btc[t] / btc[a] - 1)
            )
            durations.append((t - a).total_seconds())
            stalenesses.append(lag)
            ends.append(t)
        history_diagnostics[length] = dict(
            target_window_duration_s=length,
            observed_duration_bounds_s=[min(durations), max(durations)] if durations else None,
            endpoint_staleness_bounds_s=[min(stalenesses), max(stalenesses)] if stalenesses else None,
            exact=bool(stalenesses) and max(stalenesses) == 0,
            resolution_s=history_resolution_s,
            alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
            sample_count=len(out),
            latest_observed_at=iso(max(ends)) if ends else None,
        )
        return out

    def hist(length):
        key = ("relative_hist", scope, ref, series_id(btc), bb, benchmark, length)

        def build():
            values = hist_values(length)
            return StatisticalPopulation(values) if cache is not None else values, deepcopy(
                history_diagnostics.get(length, {})
            )

        values, diagnostic = remember(cache, key, build)
        if diagnostic:
            history_diagnostics[length] = diagnostic
        return values

    windows = {}
    for name, length in dict(WINDOWS, EVENT=(end - start).total_seconds()).items():
        a = end - timedelta(seconds=length)
        r = (
            (
                float(
                    (
                        decimal(cutoff_row.get("price", cutoff_row.get("p")))
                        if cutoff_row
                        else price(exact_groups.get(ref, {}), end)
                    )
                    / anchor_price
                    - 1
                )
                if anchor_price and (cutoff_row or price(prices, end))
                else None
            )
            if name == "EVENT"
            else ret(prices, a, end)
        )
        rb = bench_ret(btc, a, end)
        re = bench_ret(eth, a, end)
        adj = r - bb * rb if r is not None and rb is not None and not benchmark else None
        measure = r if benchmark else adj
        h = hist(length)
        windows[name] = dict(
            window=name,
            start=iso(a),
            cutoff=iso(end),
            observed_at=iso(max(prices)) if prices else None,
            precision=f"BUCKET({int(next((r.get('interval_s', 60) for r in reversed(snapshot.get('klines', [])) if r.get('instrument_id') == ref), 60))}s)"
            if prices
            else None,
            coverage={"reference_venue": ref, "complete_24h": bool(ref)},
            r_raw=r,
            r_btc_adj=adj,
            r_btc_adj_reason="NOT_APPLICABLE_BENCHMARK" if benchmark else None,
            relative_measure="R_RAW" if benchmark else "R_BTC_ADJ",
            relative_return=measure,
            r_eth_adj=r - be * re if r is not None and re is not None else None,
            r_sector_adj=None,
            sector_reason="NOT_ESTIMABLE",
            pctile=ratio(sum(x <= measure for x in h), len(h)) if measure is not None and h else None,
            normal_range=float_normal(h),
            normal_range_reason=range_reason(h),
            beyond_normal=beyond(measure, float_normal(h)),
            historical_window_resolution_s=history_resolution_s,
            historical_window_duration_s=length,
            requested_window_duration_s=length,
            historical_alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
            historical_measurement=history_diagnostics.get(
                length, {"exact": False, "reason": "INCOMPLETE", "target_window_duration_s": length}
            ),
        )

    sector_members = defaultdict(list)
    token_sectors = {}
    for row in snapshot.get("tokens", []):
        identity, sector = row.get("token_id"), row.get("sector")
        if identity and sector:
            token_sectors[identity] = sector
            sector_members[sector].append(identity)
    for basket in snapshot.get("sector_baskets", []):
        if basket.get("sector") and basket.get("weighting") == "EQUAL":
            sector_members[basket["sector"]] = list(basket.get("constituents", []))
            for identity in sector_members[basket["sector"]]:
                token_sectors[identity] = basket["sector"]
    sector = token_sectors.get(token_id)

    defi_sectors = (
        "DEX_EXECUTION",
        "LENDING_CREDIT",
        "PERP_DEX",
        "RWA",
        "INTEROPERABILITY",
        "SYNTHETIC_DOLLAR",
    )

    def peer_series(identity):
        matches = {iid: p for iid, p in groups.items() if identities.get(iid) == identity}
        return (
            matches[max(matches, key=lambda iid: coverage_counts.get(iid, len(matches[iid])))]
            if matches
            else {}
        )

    peers = [identity for identity in sector_members.get(sector, []) if identity != token_id]
    tradable = [identity for identity in peers if peer_series(identity)]
    if len(tradable) < 2:
        tradable = [
            identity
            for name in defi_sectors
            for identity in sector_members.get(name, [])
            if identity != token_id and peer_series(identity)
        ]
    not_applicable = sector == "IDENTITY_PAYMENTS"
    for name, length in dict(WINDOWS, EVENT=(end - start).total_seconds()).items():
        peer_returns = [
            bench_ret(peer_series(identity), end - timedelta(seconds=length), end) for identity in tradable
        ]
        peer_returns = [v for v in peer_returns if v is not None]
        raw = windows[name]["r_raw"]
        windows[name]["r_sector_adj"] = (
            raw - mean(peer_returns)
            if not not_applicable and sector and raw is not None and len(peer_returns) >= 2
            else None
        )
        windows[name]["sector_reason"] = (
            "NOT_APPLICABLE"
            if not_applicable
            else None
            if windows[name]["r_sector_adj"] is not None
            else "NOT_ESTIMABLE"
        )
    eh = hist((end - start).total_seconds())
    dh = hist(86400)
    sign = -1 if snapshot.get("expected_direction") in ("BEARISH", "LOSER", -1) else 1
    eventr = windows["EVENT"]["relative_return"]
    br = bench_ret(btc, end - timedelta(days=1), end)
    bh = []
    rvhist = []
    rv = None
    btc_hourly = {t: p for t, p in btc.items() if t.minute == 0 and t.second == 0 and t.microsecond == 0}

    def btc_baseline():
        bh = []
        rvhist = []
        for t in sorted(btc_hourly):
            if t >= start or t < start - timedelta(days=lookback_days()):
                continue
            a = t - timedelta(days=1)
            if a in btc:
                bh.append(btc[t] / btc[a] - 1)
            lr = [
                log(btc_hourly[x] / btc_hourly[x - timedelta(hours=1)])
                for x in btc_hourly
                if a < x <= t and x - timedelta(hours=1) in btc_hourly
            ]
            if len(lr) == 24:
                rvhist.append(stdev(lr))
        return StatisticalPopulation(bh) if cache is not None else bh, StatisticalPopulation(
            rvhist
        ) if cache is not None else rvhist

    bh, rvhist = remember(cache, ("btc_context_history", scope, series_id(btc)), btc_baseline)
    lr = [
        log(btc_hourly[t] / btc_hourly[t - timedelta(hours=1)])
        for t in btc_hourly
        if end.replace(minute=0, second=0, microsecond=0) - timedelta(days=1)
        < t
        <= end.replace(minute=0, second=0, microsecond=0)
        and t - timedelta(hours=1) in btc_hourly
    ]
    if len(lr) == 24:
        rv = stdev(lr)
    if not btc or (max(btc) - min(btc)).total_seconds() < float(parameter("history_min_days", 7)) * 86400:
        bh = []
        rvhist = []
    context = market_context(br, rv, bh, rvhist)
    pre = ret(prices, start - timedelta(days=1), start)
    preb = bench_ret(btc, start - timedelta(days=1), start)
    ar = pre if benchmark else pre - bb * preb if pre is not None and preb is not None else None
    horizon_ranges = {}
    for name, length in (
        [
            ("W1H", 3600),
            ("W4H", 14400),
            ("W12H", 43200),
            ("W24H", 86400),
            ("W3D", 259200),
            ("W7D", 604800),
        ]
        if include_outcome
        else []
    ):
        history = hist(length)
        diagnostic = history_diagnostics.get(length, {})
        horizon_ranges[name] = dict(
            value=float_normal(history),
            unit="DECIMAL_RETURN",
            window=name,
            start=iso(start - timedelta(days=lookback_days())),
            cutoff=iso(start),
            observed_at=diagnostic.get("latest_observed_at"),
            precision=f"BUCKET({history_resolution_s}s)" if history_resolution_s else None,
            coverage={"sample_count": len(history)},
            reason=None if history else "NOT_ESTIMABLE",
            historical_measurement=diagnostic,
        )
    btc_ids = {iid: p for iid, p in groups.items() if token_symbols.get(identities.get(iid)) == "BTC"}
    btc_iid = max(btc_ids, key=lambda iid: coverage_counts.get(iid, len(btc_ids[iid]))) if btc_ids else None
    btc_last = flow_reference(snapshot, btc_iid, end)
    btc_entry = decimal(btc_last.get("price")) if btc_last else price(exact_groups.get(btc_iid, {}), end)
    outcome_btc = dict(
        instrument_id=btc_iid,
        entry_ref_price=btc_entry,
        observed_at=iso(_time(btc_last)) if btc_last else iso(max(btc)) if btc else None,
        price_source="LAST_TRADE" if btc_last else "KLINE_CLOSE" if btc else None,
        reason=None if btc_entry is not None else "INCOMPLETE",
    )
    hourly_closes = [
        (t, p) for t, p in exact_groups.get(ref, {}).items() if t <= end and t.minute == 0 and t.second == 0
    ]
    return dict(
        expected_direction=snapshot.get("expected_direction"),
        repricing_sign=sign,
        outcome_btc_reference=outcome_btc,
        normal_ranges=horizon_ranges,
        entry_ref_price=decimal(cutoff_row.get("price", cutoff_row.get("p")))
        if cutoff_row
        else price(exact_groups.get(ref, {}), end),
        entry_price_source="LAST_TRADE" if cutoff_row else "KLINE_CLOSE",
        entry_price_observed_at=iso(_time(cutoff_row))
        if cutoff_row
        else iso(max(prices))
        if prices
        else None,
        close_1h_price=max(hourly_closes, key=lambda x: x[0])[1] if hourly_closes else None,
        ref_venue=ref,
        reference_coverage=ref_coverage,
        reference_reason=None if ref else "INCOMPLETE",
        anchor=dict(
            type=snapshot.get("anchor", {}).get("type", "KNOWN_TO_US_AT")
            if isinstance(snapshot.get("anchor"), dict)
            else "KNOWN_TO_US_AT",
            ts=iso(start),
            ref_price=anchor_price,
            price_observed_at=iso(_time(anchor_row)) if anchor_row else None,
            evidence=anchor_row,
            price_reason=None if anchor_row else "INCOMPLETE",
            price_source="LAST_TRADE" if anchor_row else None,
        ),
        windows=windows,
        beta_btc=bb,
        beta_eth=be,
        beta_flags=dict(btc=bflag, eth=eflag),
        pre_event_runup=dict(
            window="PRE_EVENT_W24H",
            start=iso(start - timedelta(days=1)),
            cutoff=iso(start),
            ar=ar,
            pctile=ratio(sum(x <= ar for x in dh), len(dh)) if ar is not None else None,
            flag=beyond(ar, float_normal(dh)),
        ),
        repricing_band=repricing(
            eventr * sign if eventr is not None else None,
            eh.scaled(sign) if isinstance(eh, StatisticalPopulation) else [x * sign for x in eh],
            dh.scaled(sign) if isinstance(dh, StatisticalPopulation) else [x * sign for x in dh],
        ),
        market_context=context,
        context_inputs=dict(
            btc_return_24h=br,
            rv24=rv,
            rv24_observed_cutoff=iso(end.replace(minute=0, second=0, microsecond=0)),
            rv24_resolution_s=3600,
            return_normal_range=normal(bh),
            return_lower_quartile=percentile(bh, 0.25),
            rv_normal_range=normal(rvhist),
            rule="normal_range",
        ),
    )

def benchmark_reaction_values(snapshot, start, end):
    """Reaction checkpoint values for a BTC/ETH benchmark outside the analysis set (03 §8.1).

    Lead ruling 2026-10-20: the raw return comes from base-tier 1m kline closes of the
    benchmark's largest-volume spot venue (trailing 24 h before the window end); spot CVD,
    OI and funding are n/a NOT_APPLICABLE. No escalation, so no trades are needed.
    """
    token = snapshot.get("token_id")
    rows = [
        r
        for r in snapshot.get("klines", [])
        if _market(r) == "SPOT"
        and r.get("complete") is True
        and r.get("token_id", token) == token
        and number(r.get("c", r.get("close")))
    ]

    def close_time(r):
        return kline_close_time(r) or _time(r) + timedelta(seconds=int(r.get("interval_s", 60)))

    volume = defaultdict(Decimal)
    for r in rows:
        if end - timedelta(hours=24) < close_time(r) <= end:
            v = decimal(r.get("quote_volume_usd"))
            volume[r["instrument_id"]] += v if v is not None else Decimal(0)
    venue = max(volume, key=volume.get) if volume and max(volume.values()) > 0 else None

    def last_close(bound):
        eligible = [r for r in rows if r["instrument_id"] == venue and close_time(r) <= bound]
        latest = max(eligible, key=close_time) if eligible else None

        if latest is not None and (bound - close_time(latest)).total_seconds() > int(
            latest.get("interval_s", 60)
        ):
            return None
        return latest

    first, last = (last_close(start), last_close(end)) if venue else (None, None)
    p0 = decimal((first or {}).get("c", (first or {}).get("close")))
    p1 = decimal((last or {}).get("c", (last or {}).get("close")))
    r_raw = float(p1 / p0 - 1) if p0 and p1 else None
    return {
        "r_raw": r_raw,
        "r_raw_reason": None if r_raw is not None else "INCOMPLETE",
        "r_raw_method": "BASE_KLINE_CLOSE_LARGEST_SPOT_VENUE",
        "r_raw_venue": venue,
        "price_start": p0,
        "price_start_observed_at": iso(close_time(first)) if first else None,
        "price_end": p1,
        "price_end_observed_at": iso(close_time(last)) if last else None,
        "not_applicable": ["r_btc_adj", "pctile", "spot_cvd", "oi", "funding", "liquidations"],
        "reason": "NOT_APPLICABLE",
    }'''
