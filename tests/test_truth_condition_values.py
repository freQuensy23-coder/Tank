"""Selective condition inputs agree with full truth without unrelated work or MTA writes."""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_truth_start_fixture import compact_start_snapshot

import tank.analysis.truth as module
from tank.scheduler import metric_value


def snapshot():
    return compact_start_snapshot()


@pytest.fixture(scope="module")
def baseline_truth():

    result = module.compute_market_truth(snapshot())

    def fresh():
        return deepcopy(result)

    return fresh


REQUIREMENTS = [
    dict(metric=m, window="W1H")
    for m in (
        "spot.cohort.direction",
        "spot.cohort.cwb_buy",
        "deriv.native_oi_vs_normal",
        "deriv.funding_vs_normal",
        "price.close_vs_level",
    )
]


def test_selective_same_input_parity_and_no_unrequested_domains(
    monkeypatch: pytest.MonkeyPatch, baseline_truth
) -> None:
    packet = snapshot()
    full = baseline_truth()

    def forbidden(*args, **kwargs) -> None:
        raise AssertionError("Unrequested full/cross-domain assessment")

    for name in (
        "compute_market_truth",
        "relative_truth",
        "liquidity_truth",
        "supply_truth",
        "enrich_evidence",
        "select_move",
    ):
        monkeypatch.setattr(module, name, forbidden)
    selected = module.compute_condition_values(packet, REQUIREMENTS)
    for requirement in REQUIREMENTS:
        metric, window = requirement["metric"], requirement["window"]
        assert metric_value(selected, {}, metric, window) == metric_value(full, {}, metric, window)
    assert "_condition_evaluation" in selected and "mta_id" not in selected


def test_identical_closed_window_inputs_reused_but_corrected_row_invalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet = snapshot()
    cache = {}
    calls = []
    original = module.spot_truth

    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "spot_truth", counted)
    requirements = REQUIREMENTS[:2]
    first = module.compute_condition_values(packet, requirements, cache=cache)
    second = module.compute_condition_values(packet, requirements, cache=cache)
    assert len(calls) == 1
    assert all(p["reused"] for p in second["_condition_evaluation"]["proofs"])
    changed = deepcopy(packet)
    changed["flow_bars"][-1]["buy_qty"] = "101"
    third = module.compute_condition_values(changed, requirements, cache=cache)
    assert len(calls) == 2
    assert third["_condition_evaluation"]["request_key"] != first["_condition_evaluation"]["request_key"]


def test_native_baseline_reused_when_only_current_observation_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet = snapshot()
    baseline = {}
    original = module.normal
    calls = []

    def counted(values):
        calls.append(1)
        return original(values)

    monkeypatch.setattr(module, "normal", counted)
    req = [dict(metric="deriv.native_oi_vs_normal", window="W1H")]
    module.compute_condition_values(packet, req, baseline_cache=baseline)
    count = len(calls)
    packet["oi"][-1]["oi_native"] = "1001"
    value = module.compute_condition_values(packet, req, baseline_cache=baseline)
    assert len(calls) == count
    assert Decimal(value["derivatives"]["windows"]["W1H"]["synchronized"]["oi_native_end"]) == 1001


@pytest.mark.asyncio
async def test_service_uses_only_light_loader_and_never_writes_assessments() -> None:
    packet = snapshot()
    reads = []

    class Repo:
        async def get(self, collection, key):
            reads.append(collection)
            return {"public_at": packet["anchor"]["ts"]}

        async def put(self, *args, **kwargs) -> None:
            raise AssertionError("Transient conditions must never persist")

        async def list(self, *args, **kwargs) -> None:
            raise AssertionError("No full assessment history query")

    class Market:
        async def condition_snapshot(self, token, cutoff, *, anchor, requirements):
            assert requirements == REQUIREMENTS[:2]
            return packet

        async def snapshot(self, *args, **kwargs) -> None:
            raise AssertionError("No full market snapshot")

    service = module.MarketTruthService(
        Repo(), SimpleNamespace(config_version="t", code_version="t"), Market()
    )
    result = await service.condition_values("E", "T", packet["cutoff"], REQUIREMENTS[:2])
    assert reads == ["evt.events"] and "mta_id" not in result
    assert result["spot"]["W1H"]["direction"] == "BUY"


def test_composite_requests_do_not_execute_full_assessment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module, "compute_market_truth", lambda *args, **kwargs: pytest.fail("Full inline recomputation")
    )
    result = module.compute_condition_values(snapshot(), [dict(metric="mt.move_class", window="W1H")])
    evaluation = result["_condition_evaluation"]
    assert evaluation["requires_market_truth"] == []
    assert result["move_class"] == "SPOT_LED_ACCUMULATION"
    assert evaluation["request_key"]


def test_unchanged_cached_funding_never_overrides_its_actual_capture_cadence() -> None:
    from datetime import timedelta

    from tank.util import parse_time

    packet = snapshot()
    end = parse_time(packet["cutoff"])
    packet["capture_metadata"] = {"tier": "HOT", "collections": {"funding": {"cadence_s": 60}}}
    packet["funding"] = [row for row in packet["funding"] if module._time(row) <= end - timedelta(seconds=90)]
    req = [dict(metric="deriv.funding_vs_normal", window="W1H")]
    cache = {}
    for _ in range(2):
        result = module.compute_condition_values(packet, req, cache=cache)
        assert metric_value(result, {}, "deriv.funding_vs_normal", "W1H") is None
        proof = result["_condition_evaluation"]["proofs"][0]
        assert proof["reason"] == "INCOMPLETE"
        assert proof["observations"][0]["cadence_s"] == 60


def test_completed_close_uses_actual_largest_complete_reference_venue() -> None:
    packet = snapshot()
    for row in packet["flow_bars"]:
        if row["instrument_id"] == "B:spot:T":
            row["buy_qty"] = "200"
    for row in packet["klines"]:
        if row["instrument_id"] == "B:spot:T":
            row["c"] = "202.2"
    full = module.compute_market_truth(packet)
    selected = module.compute_condition_values(packet, [dict(metric="price.close_vs_level", window="W1H")])
    assert full["relative"]["ref_venue"] == selected["relative"]["ref_venue"] == "B:spot:T"
    assert Decimal(selected["relative"]["close_1h_price"]) == Decimal("202.2")
    assert selected["relative"]["close_1h_price"] == full["relative"]["close_1h_price"]


def test_funding_baseline_reused_when_only_current_estimate_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    packet = snapshot()
    baseline = {}
    original = module.normal
    calls = []

    def counted(values):
        calls.append(1)
        return original(values)

    monkeypatch.setattr(module, "normal", counted)
    req = [dict(metric="deriv.funding_vs_normal", window="W1H")]
    module.compute_condition_values(packet, req, baseline_cache=baseline)
    count = len(calls)
    packet["funding"].append(
        dict(
            instrument_id="binance:perp:T",
            kind="CURRENT_ESTIMATE",
            ts=packet["cutoff"],
            rate=".001",
            interval_h=8,
        )
    )
    value = module.compute_condition_values(packet, req, baseline_cache=baseline)
    assert len(calls) == count
    assert metric_value(value, {}, "deriv.funding_vs_normal", "W1H") == "ABOVE"


def test_missing_completed_hour_close_is_not_replaced_by_an_older_hour() -> None:
    from datetime import timedelta

    from tank.util import parse_time

    packet = snapshot()
    end = parse_time(packet["cutoff"])
    packet["klines"] = [
        row
        for row in packet["klines"]
        if module._time(row) + timedelta(seconds=row.get("interval_s", 60)) != end
    ]
    result = module.compute_condition_values(packet, [dict(metric="price.close_vs_level", window="W1H")])
    assert result["relative"]["close_1h_price"] is None
    assert result["_condition_evaluation"]["unavailable_metrics"][0]["reason"] == "INCOMPLETE"


def test_proven_source_heads_avoid_serializing_full_history_for_cache_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet = snapshot()
    packet["domain_dependency_fingerprints"] = {"oi": "ACTUAL-SOURCE-HEAD"}
    packet["history_dependency_fingerprints"] = {"oi": {"W1H": "ACTUAL-ELIGIBLE-HISTORY-HEAD"}}
    original = module.fingerprint

    def no_full_history(value):
        if isinstance(value, list) and value and isinstance(value[0], tuple) and len(value[0]) == 3:
            pytest.fail("Full historical native/USD arrays rehashed despite proven head")
        if isinstance(value, dict) and "oi" in value:
            pytest.fail("Whole OI domain rehashed despite proven head")
        return original(value)

    monkeypatch.setattr(module, "fingerprint", no_full_history)
    result = module.compute_condition_values(packet, [dict(metric="deriv.native_oi_vs_normal", window="W1H")])
    assert metric_value(result, {}, "deriv.native_oi_vs_normal", "W1H") == "WITHIN"


@pytest.mark.asyncio
async def test_actual_market_loader_selective_sql_footprint_and_no_assessment_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import httpx

    from tank.market.service import MarketService
    from tank.storage import MemoryRepository

    packet = snapshot()
    reads = []
    repo = MemoryRepository()
    await repo.put_market_batch("mkt.flow_bars_1m", packet["flow_bars"])
    await repo.put_market_batch(
        "mkt.klines_1m", [row for row in packet["klines"] if row.get("interval_s") == 60]
    )
    original_range = repo.iter_range

    async def ranges(collection, field, **kwargs):
        reads.append(collection)
        async for row in original_range(collection, field, **kwargs):
            yield row

    repo.iter_range = ranges
    original_get = repo.get

    async def get(collection, key):
        if collection == "evt.events":
            return {"public_at": packet["anchor"]["ts"]}
        return await original_get(collection, key)

    repo.get = get

    async def forbidden(*args, **kwargs) -> None:
        raise AssertionError("Selective evaluation cannot write/flush/persist full truth")

    repo.put = forbidden
    repo.put_market_batch = forbidden
    registry = {"instruments": packet["instruments"], "tokens": [{"token_id": "T", "symbol": "T"}]}
    import json

    from tank.settings import Settings

    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text(json.dumps(registry))
    (config / "semantics.yaml").write_text(json.dumps({"metrics": packet["semantics"]}))
    (config / "thresholds.yaml").write_bytes(Settings().read_config_bytes("thresholds.yaml"))
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path)
    async with httpx.AsyncClient() as http:
        market = MarketService(repo, settings, http=http)
        monkeypatch.setattr(market, "snapshot", forbidden)
        monkeypatch.setattr(market, "_put", forbidden)
        monkeypatch.setattr(market, "flush_market", forbidden)
        truth = module.MarketTruthService(repo, settings, market)
        req = [
            dict(metric="spot.cohort.direction", window="W1H"),
            dict(metric="price.close_vs_level", window="W1H"),
        ]
        result = await truth.condition_values("E", "T", packet["cutoff"], req)
    assert result["spot"]["W1H"]["direction"] == "BUY"
    assert Decimal(result["relative"]["close_1h_price"]) == Decimal("101.1")
    assert set(reads) == {"mkt.flow_bars_1m", "mkt.klines_1m"}
    assert "mta_id" not in result


@pytest.mark.parametrize(
    "case", ["HIGH", "UNVERIFIED", "UNKNOWN", "STALE_BOOK", "MISSING_USD", "STALE_SUPPLY"]
)
def test_selective_confidence_matches_full_rules_without_relative_liquidity_or_support(
    case, monkeypatch: pytest.MonkeyPatch, baseline_truth
) -> None:
    from datetime import timedelta

    from tank.util import iso, parse_time

    packet = snapshot()
    if case == "UNVERIFIED":
        packet["semantics"][0]["semantic_confidence"] = "DOCUMENTED_UNTESTED"
    if case == "UNKNOWN":
        packet["semantics"][0]["semantic_confidence"] = "UNKNOWN"
    if case == "STALE_BOOK":
        for row in packet["books"]:
            if row["exchange_ts"] == packet["cutoff"]:
                row["exchange_ts"] = iso(parse_time(packet["cutoff"]) - timedelta(seconds=30))
    if case == "MISSING_USD":
        for row in packet["oi"]:
            row["oi_usd"] = None
    if case == "STALE_SUPPLY":
        packet["supply"] = [
            dict(
                kind="FUNDAMENTALS", ts=iso(parse_time(packet["cutoff"]) - timedelta(days=1)), available=True
            )
        ]
    full = baseline_truth() if case == "HIGH" else module.compute_market_truth(packet)

    def forbidden(*args, **kwargs) -> None:
        raise AssertionError("DC must not compute statistical repricing/liquidity/support/class")

    for name in (
        "compute_market_truth",
        "relative_truth",
        "liquidity_truth",
        "derivatives_truth",
        "enrich_evidence",
        "support_features",
        "select_move",
    ):
        monkeypatch.setattr(module, name, forbidden)
    selected = module.compute_condition_values(packet, [dict(metric="dc.class", window="W1H")])
    assert selected["data_confidence"]["class"] == full["data_confidence"]["class"]
    assert selected["data_confidence"]["limiting"] == full["data_confidence"]["limiting"]
    assert not selected["_condition_evaluation"]["requires_market_truth"]


def test_depth_ratio_uses_only_actual_books_and_reuses_pre_event_baseline(
    monkeypatch: pytest.MonkeyPatch, baseline_truth
) -> None:
    packet = snapshot()
    full = baseline_truth()
    monkeypatch.setattr(
        module,
        "liquidity_truth",
        lambda *args, **kwargs: pytest.fail("No slippage/refill/flow/statistics for depth ratio"),
    )
    req = [dict(metric="liq.depth_p1_vs_pre", window="W1H")]
    selected = module.compute_condition_values(packet, req)
    assert (
        metric_value(selected, {}, "liq.depth_p1_vs_pre", "W1H")
        == metric_value(full, {}, "liq.depth_p1_vs_pre", "W1H")
        == 1
    )
    assert not selected["_condition_evaluation"]["requires_market_truth"]


def test_compact_statistical_projections_preserve_full_catalog_and_exact_prices() -> None:
    from array import array

    raw = snapshot()
    expected = module.compute_condition_values(
        raw,
        [
            {"metric": m, "window": "EVENT"}
            for m in ("mt.move_class", "mt.divergence_state", "price.repricing_band", "price.vs_anchor_pct")
        ],
    )
    compact = deepcopy(raw)
    compact["stat_price_series"] = {}
    compact["stat_volume_series"] = {}
    epoch = module.parse_time("1970-01-01T00:00:00Z")
    by_id = {}
    for row in raw["klines"]:
        by_id.setdefault(row["instrument_id"], []).append(row)
    for iid, rows in by_id.items():
        rows.sort(key=module._time)
        compact["stat_volume_series"][iid] = {
            "token_id": rows[0].get("token_id"),
            "market_type": module._market(rows[0]),
            "time_basis": "OPEN_TIME",
            "interval_s": 60,
            "times_us": array(
                "q", [int((module._time(r) - epoch).total_seconds() * 1_000_000) for r in rows]
            ),
            "values": [r.get("volume_native") for r in rows],
            "complete": array("b", [r.get("complete") is True for r in rows]),
            "dependency_fingerprint": iid,
        }
        if module._market(rows[0]) == "SPOT":
            compact["stat_price_series"][iid] = {
                "token_id": rows[0].get("token_id"),
                "coverage_count": len(rows),
                "time_basis": "CLOSE_TIME",
                "times_us": array(
                    "q",
                    [
                        int(
                            (
                                (module._time(r) + module.timedelta(seconds=int(r.get("interval_s", 60))))
                                - epoch
                            ).total_seconds()
                            * 1_000_000
                        )
                        for r in rows
                    ],
                ),
                "values": array("d", [float(r.get("c", r.get("close"))) for r in rows]),
            }

    end = module.parse_time(raw["cutoff"])
    compact["klines"] = [r for r in raw["klines"] if module._time(r) >= end - module.timedelta(hours=1)]
    actual = module.compute_condition_values(
        compact,
        [
            {"metric": m, "window": "EVENT"}
            for m in ("mt.move_class", "mt.divergence_state", "price.repricing_band", "price.vs_anchor_pct")
        ],
    )
    for metric in ("mt.move_class", "mt.divergence_state", "price.repricing_band", "price.vs_anchor_pct"):
        assert metric_value(actual, {}, metric, "EVENT") == metric_value(expected, {}, metric, "EVENT")
    assert actual["relative"]["entry_ref_price"] == expected["relative"]["entry_ref_price"]
    assert actual["_condition_evaluation"]["requires_market_truth"] == []


def test_compact_native_volume_keeps_exact_sums_and_continuity() -> None:
    from array import array

    end = module.parse_time("2026-10-19T12:00:00Z")
    start = end - module.timedelta(hours=1)
    epoch = module.parse_time("1970-01-01T00:00:00Z")
    times = [end - module.timedelta(minutes=m) for m in range(8 * 1440, 0, -1)]
    tiny = "100000000000000000000.00000000000000000001"
    rows = [
        dict(
            instrument_id="A:spot:T",
            token_id="T",
            market_type="SPOT",
            open_time=module.iso(t),
            interval_s=60,
            complete=True,
            volume_native=tiny,
        )
        for t in times
    ]
    base = dict(token_id="T", cutoff=module.iso(end), klines=rows)
    expected = module.support_features(base, start, end, {}, {}, {}, {})
    compact = dict(
        token_id="T",
        cutoff=module.iso(end),
        stat_volume_series={
            "A:spot:T": dict(
                token_id="T",
                market_type="SPOT",
                time_basis="OPEN_TIME",
                interval_s=60,
                times_us=array("q", [int((t - epoch).total_seconds() * 1_000_000) for t in times]),
                values=[tiny] * len(times),
                complete=array("b", [1] * len(times)),
                dependency_fingerprint="original",
            )
        },
    )
    actual = module.support_features(compact, start, end, {}, {}, {}, {})
    assert actual["spot_volume"] == expected["spot_volume"]
    assert actual["spot_volume"]["value"] == Decimal("6000000000000000000000.00000000000000000060")
    assert actual["spot_volume_within_normal"] is True
    compact["stat_volume_series"]["A:spot:T"]["complete"][-1] = 0
    missing = module.support_features(compact, start, end, {}, {}, {}, {})
    assert missing["spot_volume"]["value"] is None
    assert missing["spot_volume_up_beyond_normal"] is None


def test_composite_primary_graph_reused_across_requested_windows(
    monkeypatch: pytest.MonkeyPatch, baseline_truth
) -> None:
    packet = snapshot()
    full = baseline_truth()
    calls = []
    original = module.derivatives_truth

    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "derivatives_truth", counted)
    cache = {}
    baselines = {}
    for window in ("W1H", "W4H"):
        result = module.compute_condition_values(
            packet,
            [
                dict(metric="mt.move_class", window=window),
                dict(metric="mt.divergence_state", window=window),
            ],
            cache=cache,
            baseline_cache=baselines,
        )
        for metric in ("mt.move_class", "mt.divergence_state"):
            assert metric_value(result, {}, metric, window) == metric_value(full, {}, metric, window)
        assert result["_condition_evaluation"]["requires_market_truth"] == []
    assert len(calls) == 1


def test_composite_conditions_change_from_unknown_when_actual_evidence_arrives() -> None:
    complete = snapshot()
    missing = deepcopy(complete)
    missing["flow_bars"] = []
    missing["flow_segments"] = []
    requirements = [dict(metric="mt.move_class", window="W1H")]
    cache = {}
    before = module.compute_condition_values(missing, requirements, cache=cache)
    after = module.compute_condition_values(complete, requirements, cache=cache)
    assert metric_value(before, {}, "mt.move_class", "W1H") == "UNKNOWN"
    assert metric_value(after, {}, "mt.move_class", "W1H") == "SPOT_LED_ACCUMULATION"
    assert after["_condition_evaluation"]["requires_market_truth"] == []
    assert "mta_id" not in after


@pytest.mark.asyncio
async def test_selective_relative_uses_canonical_latest_beneficiary_direction() -> None:
    packet = snapshot()

    class Repo:
        async def get(self, collection, key):
            return {"public_at": packet["anchor"]["ts"]}

        async def list(self, collection, **kwargs):
            assert collection == "evt.beneficiary_edges"
            return [
                {"map_version": 1, "edge_id": "old", "role": "BENEFICIARY"},
                {"map_version": 2, "edge_id": "new", "role": "LOSER"},
            ]

        async def put(self, *args, **kwargs) -> None:
            raise AssertionError("Transient conditions must not persist assessments")

    class Market:
        async def condition_snapshot(self, *args, **kwargs):
            return deepcopy(packet)

    service = module.MarketTruthService(
        Repo(), SimpleNamespace(config_version="t", code_version="t"), Market()
    )
    actual = await service.condition_values(
        "E", "T", packet["cutoff"], [{"metric": "price.repricing_band", "window": "EVENT"}]
    )
    expected = module.compute_condition_values(
        dict(packet, expected_direction="LOSER"), [{"metric": "price.repricing_band", "window": "EVENT"}]
    )
    assert actual["relative"]["expected_direction"] == "LOSER"
    assert actual["relative"]["repricing_band"] == expected["relative"]["repricing_band"]


def test_beneficiary_direction_invalidates_market_head_cache() -> None:
    packet = snapshot()
    packet["domain_dependency_fingerprints"] = {"relative": "unchanged-price-source-head"}
    cache = {}
    requirements = [dict(metric="price.repricing_band", window="EVENT")]
    first = module.compute_condition_values(
        dict(packet, expected_direction="BULLISH"), requirements, cache=cache
    )
    second = module.compute_condition_values(
        dict(packet, expected_direction="LOSER"), requirements, cache=cache
    )
    assert first["relative"]["expected_direction"] == "BULLISH"
    assert second["relative"]["expected_direction"] == "LOSER"
    assert second["_condition_evaluation"]["proofs"][0]["reused"] is False


def test_complete_market_condition_catalog_matches_same_input_full_truth(baseline_truth) -> None:
    packet = snapshot()
    catalog = sorted(
        module.SPOT_CONDITION_METRICS
        | module.DERIVATIVE_CONDITION_METRICS
        | module.COMPOSITE_CONDITION_METRICS
        | module.RELATIVE_CONDITION_METRICS
        | {"dc.class", "price.close_vs_level", "liq.depth_p1_vs_pre", "liq.spread_ratio"}
    )
    requirements = [dict(metric=metric, window="W1H") for metric in catalog]
    full = baseline_truth()
    result = module.compute_condition_values(packet, requirements)
    for metric in catalog:
        assert metric_value(result, {}, metric, "W1H") == metric_value(full, {}, metric, "W1H"), metric
    assert result["_condition_evaluation"]["requires_market_truth"] == []


@pytest.mark.parametrize("days,ready", [(6, False), (7, True)])
@pytest.mark.parametrize("compact", [False, True])
def test_native_volume_weekly_cycle_uses_only_eligible_pre_window_samples(days, ready, compact) -> None:
    """TDD§0 full weekly history is historical; the current24h cannot complete it."""
    from array import array

    start = module.parse_time("2026-10-18T12:00:00Z")
    end = start + module.timedelta(days=1)
    first = start - module.timedelta(days=days, minutes=1)
    times = [first + module.timedelta(minutes=i) for i in range((days + 1) * 1440 + 1)]
    packet = dict(token_id="T", cutoff=module.iso(end))
    if compact:
        epoch = module.parse_time("1970-01-01T00:00:00Z")
        packet["stat_volume_series"] = {
            "A:spot:T": dict(
                token_id="T",
                market_type="SPOT",
                time_basis="OPEN_TIME",
                interval_s=60,
                times_us=array("q", [int((t - epoch).total_seconds() * 1_000_000) for t in times]),
                values=["1"] * len(times),
                complete=array("b", [1] * len(times)),
            )
        }
    else:
        packet["klines"] = [
            dict(
                instrument_id="A:spot:T",
                token_id="T",
                market_type="SPOT",
                open_time=module.iso(t),
                interval_s=60,
                complete=True,
                volume_native="1",
            )
            for t in times
        ]
    features = module.support_features(packet, start, end, {}, {}, {}, {})
    assert (features["spot_volume"]["normal_range"] is not None) is ready
    assert features["spot_volume"]["value"] == Decimal("1440")
    assert features["spot_volume_within_normal"] is (True if ready else None)


def test_compact_native_prefix_extends_only_with_proven_append_and_rebuilds_correction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from array import array

    end = module.parse_time("2026-10-19T12:00:00Z")
    start = end - module.timedelta(hours=1)
    epoch = module.parse_time("1970-01-01T00:00:00Z")
    first = end - module.timedelta(days=8)
    stamps = array(
        "q",
        [
            int(((first + module.timedelta(minutes=i)) - epoch).total_seconds() * 1_000_000)
            for i in range(8 * 1440)
        ],
    )
    value = "1.2345678901234567890123456789"
    series = dict(
        token_id="T",
        market_type="SPOT",
        time_basis="OPEN_TIME",
        interval_s=60,
        times_us=stamps,
        values=[value] * len(stamps),
        complete=bytearray([1]) * len(stamps),
        dependency_fingerprint="before",
    )
    packet = dict(token_id="T", cutoff=module.iso(end), stat_volume_series={"A:spot:T": series})
    converted = []
    original = module.decimal

    def counted(raw):
        if raw == value:
            converted.append(1)
        return original(raw)

    monkeypatch.setattr(module, "decimal", counted)
    token = module._BASELINE_CACHE.set({})
    try:
        module.support_features(packet, start, end, {}, {}, {}, {})
        assert len(converted) == len(stamps)
        converted.clear()
        updated = deepcopy(series)
        updated["times_us"].append(int((end - epoch).total_seconds() * 1_000_000))
        updated["values"].append(value)
        updated["complete"].append(1)
        updated.update(
            dependency_fingerprint="after",
            previous_dependency_fingerprint="before",
            append_only=True,
            changed_from_us=updated["times_us"][-1],
        )
        packet["stat_volume_series"]["A:spot:T"] = updated
        result = module.support_features(packet, start, end + module.timedelta(minutes=1), {}, {}, {}, {})
        assert len(converted) == 1
        assert result["spot_volume"]["value"] == Decimal("75.3086412975308641297530864129")
        converted.clear()
        corrected = deepcopy(updated)
        corrected.update(
            dependency_fingerprint="corrected",
            previous_dependency_fingerprint="after",
            append_only=False,
            changed_from_us=stamps[0],
        )
        corrected["complete"][0] = 0
        packet["stat_volume_series"]["A:spot:T"] = corrected
        module.support_features(packet, start, end + module.timedelta(minutes=1), {}, {}, {}, {})
        assert len(converted) == len(corrected["times_us"])
    finally:
        module._BASELINE_CACHE.reset(token)
