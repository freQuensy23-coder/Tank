import asyncio
import cProfile
import gzip
import hashlib
import inspect
import json
import pstats
import re
from array import array
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from time import perf_counter, process_time

import pytest
import yaml

from tank import clickhouse_market
from tank.analysis import truth
from tank.market.service import MarketService
from tank.util import iso, parse_time

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).parent / "fixtures" / "market-truth-latency"
PRICE_FIELDS = {
    "open_time",
    "open_time_prec",
    "recorded_at",
    "complete",
    "interval_s",
    "c",
    "_cursor_time",
    "_cursor_key",
}
FLOW_FIELDS = {
    "instrument_id",
    "token_id",
    "venue",
    "market_type",
    "minute_start",
    "minute_start_prec",
    "minute_start_prec_reason",
    "buy_qty",
    "sell_qty",
    "buy_usd",
    "sell_usd",
    "sum_sq_qty",
    "complete",
    "first_trade",
    "last_trade",
    "boundary_segments",
    "reference_points",
    "recorded_at",
    "capture_method",
    "trade_aggregation",
    "notional_reason",
    "n_buy",
    "n_sell",
    "h",
    "l",
    "o",
    "c",
}
OI_FIELDS = {
    "storage_key",
    "gap_reason",
    "index_as_of",
    "index_as_of_prec",
    "index_as_of_prec_reason",
    "index_price",
    "instrument_id",
    "mark_as_of",
    "mark_as_of_prec",
    "mark_as_of_prec_reason",
    "mark_price",
    "metric_key",
    "oi_native",
    "oi_usd",
    "oi_usd_reason",
    "raw_unit",
    "raw_value",
    "received_ts",
    "received_ts_prec",
    "received_ts_prec_reason",
    "recorded_at",
    "recorded_at_prec",
    "recorded_at_prec_reason",
    "semantic_confidence",
    "sidedness",
    "synchronized_eligible",
    "timestamp_precision",
    "token_id",
    "ts",
    "ts_prec",
    "ts_prec_reason",
    "ts_bucket_size_s",
    "ts_bucket_anchor",
}
NESTED_FIELDS = FLOW_FIELDS | {
    "price",
    "trade_id",
    "ts",
    "ts_prec",
    "ts_prec_reason",
    "start",
    "start_prec",
    "start_prec_reason",
    "end",
    "end_prec",
    "end_prec_reason",
    "boundary",
    "boundary_prec",
    "boundary_prec_reason",
    "coverage_reason",
    "gap_reason",
    "buy_notional",
    "sell_notional",
    "open",
    "high",
    "low",
    "close",
    "first_trade_id",
    "last_trade_id",
    "observed_at",
    "observed_at_prec",
    "observed_at_prec_reason",
    "received_ts",
    "received_ts_prec",
    "received_ts_prec_reason",
    "quote_asset",
    "quote_volume",
    "size_buckets",
    "null_reasons",
    "volume_usd",
    "vwap",
    "B1",
    "B2",
    "B3",
    "B4",
    "B5",
    "n",
}
ARCHIVE_HASHES = {
    "ena-price.jsonl.gz": "9d1e1d1331b5e05109ca47bd24138dfae9cd49bd5cce144d4661aa7aa576590a",
    "btc-price.jsonl.gz": "c1d404386d5938fcaf2245f4e5c6195a7f91b52f6c57121ecce30ca30cb57df3",
    "eth-price.jsonl.gz": "7dbab455bc343d90f4f682343fe72bf0988c63adb9b54f7c6a56e3e694d47980",
    "ena-flow.jsonl.gz": "6643eff347129932f98620768dd26eb4a64871178d6df670bbcbf8d27c31e0c5",
    "ena-oi.jsonl.gz": "8dbf33df9b10dfb9d5cc6065946782c99e9b907f7e8fe9e65faff9e622be5915",
}
PRIVATE_CONTENT = re.compile(
    rb"://|[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|-----BEGIN|"
    rb"(?:password|passwd|api_key|access_token|authorization|telegram_id|chat_id|user_id|email)\s*[\"=:]",
    re.IGNORECASE,
)
IDENTITIES = {"ENA": "ethena:ENA", "BTC": "bitcoin:BTC", "ETH": "ethereum:ETH"}
HOT_FUNCTIONS = {
    "relative_truth",
    "_price_groups",
    "hist_values",
    "hourly_values",
    "btc_baseline",
    "beta",
    "history_fingerprint",
    "percentile",
    "float_normal",
    "range_reason",
    "derivatives_truth",
    "_oi_stats",
    "_oi_rows",
    "_composite_history",
    "synchronized_oi",
}


def assert_public_structure(value):
    if isinstance(value, dict):
        assert set(value) <= NESTED_FIELDS, "Unexpected nested public-market fields"
        for child in value.values():
            assert_public_structure(child)
    elif isinstance(value, list):
        for child in value:
            assert_public_structure(child)


def read_retained_rows(entry, record_property):
    name = entry["name"]
    archive = (FIXTURES / name).read_bytes()
    archive_hash = hashlib.sha256(archive).hexdigest()
    assert archive_hash == ARCHIVE_HASHES[name] == entry["archive_sha256"]
    assert len(archive) == entry["compressed_bytes"]
    payload = gzip.decompress(archive)
    payload_hash = hashlib.sha256(payload).hexdigest()
    assert payload_hash == entry["sha256"]
    assert len(payload) == entry["bytes"]
    has_private_content = bool(PRIVATE_CONTENT.search(payload))
    assert not has_private_content, "Fixture contains prohibited private content"
    rows = [json.loads(line) for line in payload.splitlines()]
    assert len(rows) == entry["rows"]
    allowed = PRICE_FIELDS if "price" in name else OI_FIELDS if "-oi." in name else FLOW_FIELDS
    assert set(entry["columns"]) == allowed
    for row in rows:
        assert set(row) == allowed, "Fixture public-market field allowlist changed"
        if allowed == FLOW_FIELDS:
            for field in ("first_trade", "last_trade", "boundary_segments", "reference_points"):
                assert_public_structure(json.loads(row[field]) if row[field] is not None else None)
    record_property(
        "incident_input_" + name,
        json.dumps({"rows": len(rows), "sha256": payload_hash, "archive_sha256": archive_hash}),
    )
    return rows


class RetainedProjectionRows:
    def __init__(self, rows):
        self.retained_rows = rows

    async def rows(self, sql, parameters):
        return self.retained_rows


def time_phase(record_property, phase, operation):
    wall_start, cpu_start = perf_counter(), process_time()
    try:
        return operation()
    finally:
        record_property(
            "incident_timing_" + phase,
            json.dumps(
                {
                    "wall_seconds": perf_counter() - wall_start,
                    "process_cpu_seconds": process_time() - cpu_start,
                    "profiled": False,
                }
            ),
        )


def profile_phase(record_property, phase, operation):
    worker_profiles = []

    def initialize_worker():
        profile = cProfile.Profile()
        worker_profiles.append(profile)
        profile.enable()

    async def invoke():
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(initializer=initialize_worker))
        value = operation()
        return await value if inspect.isawaitable(value) else value

    profile = cProfile.Profile()
    wall_start, cpu_start = perf_counter(), process_time()
    profile.enable()
    try:
        result = asyncio.run(invoke())
    finally:
        profile.disable()
        wall_seconds, cpu_seconds = perf_counter() - wall_start, process_time() - cpu_start
        stats = pstats.Stats(profile)
        for worker in worker_profiles:
            stats.add(worker)
        calls = {}
        functions = []
        for (filename, line, function), (primitive, total, own, cumulative, _) in stats.stats.items():
            normalized = filename.replace("\\", "/")
            if "/tank/" not in normalized:
                continue
            location = "tank/" + normalized.split("/tank/", 1)[1]
            functions.append(
                {
                    "function": function,
                    "file": location,
                    "line": line,
                    "primitive_calls": primitive,
                    "calls": total,
                    "self_seconds": round(own, 6),
                    "cumulative_seconds": round(cumulative, 6),
                }
            )
            if normalized.endswith("/analysis/truth.py"):
                calls[function] = calls.get(function, 0) + total
        functions.sort(key=lambda item: item["cumulative_seconds"], reverse=True)
        record_property(
            "incident_profile_" + phase,
            json.dumps(
                {
                    "wall_seconds": wall_seconds,
                    "process_cpu_seconds": cpu_seconds,
                    "total_calls": stats.total_calls,
                    "profiled_workers": len(worker_profiles),
                    "hot_calls": {name: calls.get(name, 0) for name in sorted(HOT_FUNCTIONS)},
                    "top_cumulative": functions[:15],
                },
                sort_keys=True,
            ),
        )
    return result, calls


def evaluate_with_cache(operation, cache):
    baseline = truth._BASELINE_CACHE.set(cache)
    inputs = truth._ASSESSMENT_INPUTS.set({})
    times = truth._TIMES.set({})
    parameters = truth._PARAMETERS.set({})
    try:
        return operation()
    finally:
        truth._PARAMETERS.reset(parameters)
        truth._TIMES.reset(times)
        truth._ASSESSMENT_INPUTS.reset(inputs)
        truth._BASELINE_CACHE.reset(baseline)


def compare_original_components(relative, derivatives, record_property):
    payload = (FIXTURES / "expected-components.json").read_bytes()
    expected = json.loads(payload)
    differences = []

    def compare(actual, original, location):
        if isinstance(original, dict):
            for key, value in original.items():
                if key.endswith(("_prec", "_prec_reason")) or key == "precision":
                    continue
                compare(actual.get(key) if isinstance(actual, dict) else None, value, location + "/" + key)
        elif isinstance(original, list):
            if not isinstance(actual, list) or len(actual) != len(original):
                differences.append(location)
            else:
                for index, value in enumerate(original):
                    compare(actual[index], value, location + "/" + str(index))
        elif isinstance(original, float):
            if actual is None or actual != pytest.approx(original, rel=1e-10, abs=1e-12):
                differences.append(location)
        elif isinstance(original, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", original):
            if actual is None or truth.decimal(actual) != truth.decimal(original):
                differences.append(location)
        elif actual != original:
            differences.append(location)

    expected_relative = expected["relative"]
    for field in ("ref_venue", "beta_btc", "beta_eth", "normal_ranges", "market_context", "context_inputs"):
        compare(relative.get(field), expected_relative[field], "relative/" + field)
    for name, original in expected["oi_windows"].items():
        actual = derivatives[name]
        synchronized = original["synchronized"]
        if synchronized is None:
            compare(actual["synchronized"], None, "oi/" + name + "/synchronized")
        else:
            selected = {
                key: value
                for key, value in synchronized.items()
                if key not in {"decomposition", "coverage", "venues"}
            }
            compare(actual["synchronized"], selected, "oi/" + name + "/synchronized")
            compare(
                sorted(actual["synchronized"]["venues"]) if actual["synchronized"] else None,
                sorted(synchronized["venues"]),
                "oi/" + name + "/synchronized/venues",
            )
        by_instrument = {row["instrument_id"]: row for row in actual["per_venue"]}
        for row in original["per_venue"]:
            identity = row["instrument_id"]
            compare(by_instrument.get(identity), row, "oi/" + name + "/" + identity)
    record_property("incident_original_components_sha256", hashlib.sha256(payload).hexdigest())
    record_property(
        "incident_original_component_comparison",
        json.dumps(
            {
                "source": "original persisted derived output, not re-generated from fixtures",
                "difference_classification": "RE_READ_INPUT_OR_COMPONENT_RECONSTRUCTION_DRIFT"
                if differences
                else "MATCH_SELECTED_ORIGINAL_INVARIANTS",
                "difference_count": len(differences),
                "difference_paths": differences[:40],
                "ignored": "presentation precision enrichment and synchronized decomposition/coverage wrappers",
            },
            sort_keys=True,
        ),
    )
    assert not differences, (
        "Re-read component mismatch: inspect incident_original_component_comparison; do not regenerate goldens"
    )


def test_retained_ena_dense_relative_populations_cold_and_warm(record_property):
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    entries = {entry["name"]: entry for entry in manifest["files"]}
    assert set(entries) == set(ARCHIVE_HASHES)
    start = parse_time(manifest["anchor"])
    end = parse_time(manifest["semantic_cutoff"])
    assembled = parse_time("2026-10-28T12:08:24.291857Z")
    projection_start = parse_time(entries["ena-price.jsonl.gz"]["first_open_time"])
    history_start = end - timedelta(days=90)
    assert history_start - projection_start == timedelta(hours=2)
    snapshot = {
        "token_id": "ethena:ENA",
        "cutoff": iso(end),
        "assembled_at": iso(assembled),
        "anchor": {"type": "KNOWN_TO_US_AT", "ts": iso(start)},
        "tokens": [],
        "instruments": [],
        "klines": [],
        "stat_price_series": {},
    }
    record_property(
        "incident_profile_scope",
        "Public SQL re-read at original cutoffs, not retained incident row versions; "
        "actual projection decoding, price arrays, relative_truth and five-window derivatives_truth; "
        "component reconstruction with funding, books, supply and liquidations omitted; "
        "not full compute_market_truth, database query timing or historical wall-time reproduction",
    )
    record_property("incident_profile_capture_started_at", manifest["capture_started_at"])
    record_property("incident_profile_capture_completed_at", manifest["capture_completed_at"])
    record_property("incident_profile_availability_cutoff", manifest["availability_cutoff"])
    record_property("incident_profile_assembled_at", iso(assembled))
    record_property("incident_profile_semantic_cutoff", iso(end))
    record_property("incident_profile_anchor", iso(start))
    service = MarketService.__new__(MarketService)
    for symbol, token_id in IDENTITIES.items():
        instrument_id = f"binance:spot:{symbol}USDT"
        instrument = {
            "instrument_id": instrument_id,
            "token_id": token_id,
            "venue": "binance",
            "market_type": "SPOT",
            "base_asset": symbol,
        }
        snapshot["instruments"].append(instrument)
        snapshot["tokens"].append({"token_id": token_id, "symbol": symbol})
        raw, _ = profile_phase(
            record_property,
            "fixture_" + symbol,
            lambda: read_retained_rows(entries[symbol.lower() + "-price.jsonl.gz"], record_property),
        )
        selected = {field: field for field in PRICE_FIELDS if not field.startswith("_cursor")}
        page, _ = profile_phase(
            record_property,
            "projection_decode_" + symbol,
            lambda: clickhouse_market.projection_page(
                RetainedProjectionRows(raw),
                "mkt.klines_1m",
                selected,
                "open_time",
                start=projection_start,
                end=end,
                filters=None,
                instrument_ids=[instrument_id],
                assembled_at=parse_time(manifest["availability_cutoff"]),
                after=None,
                limit=200000,
            ),
        )
        del raw
        decoded = [row for _, _, row in page]
        del page
        series, _ = profile_phase(
            record_property,
            "price_arrays_" + symbol,
            lambda: service._condition_kline_series(
                instrument,
                history_start,
                end,
                assembled,
                kind="price",
                coverage_count=len(decoded),
                fixed_window=True,
                captured_rows=decoded,
            ),
        )
        assert isinstance(series["times_us"], array) and series["times_us"].typecode == "q"
        assert isinstance(series["values"], array) and series["values"].typecode == "d"
        assert len(series["times_us"]) == len(series["values"]) == 129600
        assert series["time_basis"] == "CLOSE_TIME"
        assert series["precision"] == "BUCKET(60s)" and series["precision_reason"] is None
        assert all(series["complete"])
        epoch = parse_time("1970-01-01T00:00:00Z")
        assert epoch + timedelta(microseconds=series["times_us"][0]) == history_start + timedelta(minutes=1)
        assert epoch + timedelta(microseconds=series["times_us"][-1]) == end
        snapshot["stat_price_series"][instrument_id] = series
        wanted = {
            end,
            max(
                epoch + timedelta(microseconds=stamp)
                for stamp in series["times_us"]
                if stamp % 3600000000 == 0
            ),
        }
        snapshot["klines"].extend(
            dict(row, **instrument)
            for row in decoded
            if parse_time(row["open_time"]) + timedelta(minutes=1) in wanted
        )
        del decoded
    raw_flow, _ = profile_phase(
        record_property,
        "fixture_flow",
        lambda: read_retained_rows(entries["ena-flow.jsonl.gz"], record_property),
    )
    flow, _ = profile_phase(
        record_property,
        "flow_typed_decode",
        lambda: clickhouse_market._records("mkt.flow_bars_1m", raw_flow),
    )
    assert len(flow) == 1444
    for raw, decoded in zip(raw_flow, flow, strict=True):
        assert decoded["complete"] is raw["complete"]
        assert decoded["buy_notional"] == raw["buy_usd"]
        assert decoded["sell_notional"] == raw["sell_usd"]
        for field in ("first_trade", "last_trade", "boundary_segments", "reference_points"):
            assert decoded[field] == json.loads(raw[field])
    del raw_flow
    snapshot["flow_bars"] = flow
    points = [point for bar in flow for point in bar["reference_points"]]
    assert any(point["complete"] is False for point in points)
    assert any(point["complete"] is True for point in points)
    assert any(bar["boundary_segments"] for bar in flow)
    cache = {}
    cold, cold_calls = profile_phase(
        record_property,
        "relative_cold",
        lambda: evaluate_with_cache(lambda: truth.relative_truth(snapshot, start, end), cache),
    )
    cached_objects = dict(cache)
    warm = time_phase(
        record_property,
        "relative_warm",
        lambda: evaluate_with_cache(lambda: truth.relative_truth(snapshot, start, end), cache),
    )
    assert warm == cold
    assert len(cache) >= 14 and set(cache) == set(cached_objects)
    assert all(cache[key] is value for key, value in cached_objects.items())
    assert 8 <= cold_calls.get("hist_values", 0) <= 12
    assert cold_calls.get("beta", 0) == 2
    assert cold_calls.get("hourly_values", 0) == 3
    assert cold_calls.get("btc_baseline", 0) == 1
    reference = "binance:spot:ENAUSDT"
    assert cold["ref_venue"] == reference and cold["reference_reason"] is None
    coverage = cold["reference_coverage"][reference]
    assert coverage["complete"] is True and coverage["excluded"] == []
    assert coverage["covered_volume_native"] > 0
    assert cold["entry_price_source"] == "LAST_TRADE"
    assert cold["entry_ref_price"] == truth.decimal("0.20290000")
    assert cold["anchor"]["ref_price"] == truth.decimal("0.20330000")
    assert cold["anchor"]["price_source"] == "LAST_TRADE"
    assert cold["anchor"]["price_reason"] is None
    assert cold["beta_flags"] == {"btc": None, "eth": None}
    assert cold["outcome_btc_reference"]["instrument_id"] == "binance:spot:BTCUSDT"
    assert cold["outcome_btc_reference"]["entry_ref_price"] > 0
    assert cold["outcome_btc_reference"]["price_source"] == "KLINE_CLOSE"
    assert cold["market_context"] in {"STRESS", "HEADWIND", "NEUTRAL"}
    assert set(cold["normal_ranges"]) == {"W1H", "W4H", "W12H", "W24H", "W3D", "W7D"}
    diagnostics = {}
    for window in cold["windows"].values():
        assert window["coverage"] == {"reference_venue": reference, "complete_24h": True}
        assert window["relative_measure"] == "R_BTC_ADJ"
        assert window["relative_return"] is not None and window["r_eth_adj"] is not None
        assert 0 <= window["pctile"] <= 1
        assert window["normal_range"] is not None and window["normal_range_reason"] is None
        diagnostic = window["historical_measurement"]
        diagnostics[diagnostic["target_window_duration_s"]] = diagnostic
    for horizon in cold["normal_ranges"].values():
        assert horizon["value"] is not None and horizon["reason"] is None
        assert horizon["unit"] == "DECIMAL_RETURN" and parse_time(horizon["cutoff"]) == start
        diagnostic = horizon["historical_measurement"]
        assert horizon["coverage"]["sample_count"] == diagnostic["sample_count"]
        diagnostics[diagnostic["target_window_duration_s"]] = diagnostic
    assert len(diagnostics) == 8
    population_count = sum(diagnostic["sample_count"] for diagnostic in diagnostics.values())
    assert 1000000 <= population_count <= 1050000
    for length, diagnostic in diagnostics.items():
        assert 100000 <= diagnostic["sample_count"] <= 129720
        assert diagnostic["resolution_s"] == 60
        assert diagnostic["alignment"] == "AS_OF_AT_EXACT_TARGET_BOUNDS"
        assert parse_time(diagnostic["latest_observed_at"]) < start
        if length == (end - start).total_seconds():
            assert diagnostic["exact"] is False
            assert diagnostic["endpoint_staleness_bounds_s"] == [26, 26]
            assert diagnostic["observed_duration_bounds_s"] == [300, 300]
        else:
            assert diagnostic["exact"] is True
            assert diagnostic["endpoint_staleness_bounds_s"] == [0, 0]
            assert diagnostic["observed_duration_bounds_s"] == [length, length]
    raw_oi, _ = profile_phase(
        record_property,
        "fixture_oi",
        lambda: read_retained_rows(entries["ena-oi.jsonl.gz"], record_property),
    )
    oi, _ = profile_phase(
        record_property,
        "oi_typed_decode",
        lambda: clickhouse_market._records("mkt.oi_obs", raw_oi),
    )
    assert len(oi) == 57399
    for raw, decoded in zip(raw_oi, oi, strict=True):
        assert decoded["ts_bucket_anchor"] == raw["ts_bucket_anchor"]
        assert decoded["ts_bucket_size_s"] == raw["ts_bucket_size_s"]
        assert decoded["ts_prec"] == raw["ts_prec"]
        assert decoded["synchronized_eligible"] is raw["synchronized_eligible"]
        assert parse_time(decoded["recorded_at"]) <= parse_time(manifest["availability_cutoff"])
    del raw_oi
    snapshot["oi"] = oi
    config_dir = Path(__file__).parents[1] / "config"
    registry_bytes = (config_dir / "registry.yaml").read_bytes()
    semantics_bytes = (config_dir / "semantics.yaml").read_bytes()
    registry = yaml.safe_load(registry_bytes)
    service.semantics = yaml.safe_load(semantics_bytes)["metrics"]
    oi_ids = {row["instrument_id"] for row in oi}
    registered = [row for row in registry["instruments"] if row["instrument_id"] in oi_ids]
    assert {row["instrument_id"] for row in registered} == oi_ids
    assert len(registered) == 4
    snapshot["instruments"].extend(registered)
    snapshot["semantics"] = service._instrument_semantics(registered)
    assert {row["metric_key"] for row in oi} <= {row["metric_key"] for row in snapshot["semantics"]}
    record_property(
        "incident_registry_provenance",
        json.dumps(
            {
                "registry_sha256": hashlib.sha256(registry_bytes).hexdigest(),
                "semantics_sha256": hashlib.sha256(semantics_bytes).hexdigest(),
                "interpretation": "checked-in registry through actual _instrument_semantics; no promotion",
            }
        ),
    )
    venues = {row["instrument_id"]: row["venue"] for row in registered}
    counts = Counter(venues[row["instrument_id"]] for row in oi)
    assert counts == {
        "binance": 26753 + 1059,
        "bybit": 25212 + 577,
        "hyperliquid": 1425 + 481,
        "okx": 1410 + 482,
    }
    record_property("incident_oi_rows_by_venue", json.dumps(counts, sort_keys=True))
    windows = dict(truth.WINDOWS, EVENT=(end - start).total_seconds())

    def derivatives():
        return {
            name: truth.derivatives_truth(snapshot, end - timedelta(seconds=length), end)
            for name, length in windows.items()
        }

    derivative_cache = {}
    derivative_cold, derivative_calls = profile_phase(
        record_property,
        "derivatives_cold",
        lambda: evaluate_with_cache(derivatives, derivative_cache),
    )
    derivative_cached_objects = dict(derivative_cache)
    derivative_warm = time_phase(
        record_property,
        "derivatives_warm",
        lambda: evaluate_with_cache(derivatives, derivative_cache),
    )
    assert derivative_warm == derivative_cold
    assert set(derivative_cache) == set(derivative_cached_objects)
    assert all(derivative_cache[key] is value for key, value in derivative_cached_objects.items())
    assert 5 <= derivative_calls.get("derivatives_truth", 0) <= 10
    assert 20 <= derivative_calls.get("_oi_stats", 0) <= 30
    assert derivative_calls.get("_composite_history", 0) >= 3
    assert derivative_cold["W1H"]["synchronized"] is not None
    assert set(derivative_cold["W1H"]["synchronized"]["venues"]) == {"binance", "okx", "bybit"}
    assert derivative_cold["EVENT"]["synchronized"] is None
    for output in derivative_cold.values():
        per_venue = {row["venue"]: row for row in output["per_venue"]}
        assert set(per_venue) == {"binance", "bybit", "hyperliquid", "okx"}
        assert per_venue["hyperliquid"]["oi_native_start"] is None
        assert per_venue["hyperliquid"]["oi_class"] is None
    compare_original_components(cold, derivative_cold, record_property)
    record_property("incident_population_count", population_count)
    record_property("incident_historical_measurements", json.dumps(diagnostics, sort_keys=True))
    gap, _ = profile_phase(
        record_property,
        "missing_real_minute_coverage",
        lambda: truth.spot_truth(
            flow[:-1], [], [snapshot["instruments"][0]], end - timedelta(days=1), end
        ),
    )
    assert gap["venues"] == [] and gap["composite_max"]["volume"] is None
    assert gap["coverage"]["excluded"][0]["reason"] == "INCOMPLETE"
    assert gap["coverage"]["excluded"][0]["detail"] == "FULL_MINUTE_COVERAGE_INCOMPLETE"
