import asyncio
import json
import os
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, InvalidOperation, localcontext
from types import SimpleNamespace

import pytest
from test_truth_sql_conditions import original_truth as original_truth
from test_truth_sql_support import _fresh_snapshot, _snapshot, _supply_snapshot
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.analysis import truth
from tank.analysis import truth_sql_engine as native
from tank.util import iso, parse_time


@pytest.fixture
async def engine_client(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        yield client
    finally:
        await client.close()


def assert_complete(actual, expected, path="result"):
    if isinstance(expected, dict):
        assert isinstance(actual, dict), path
        assert set(actual) == set(expected), path
        for key, value in expected.items():
            assert_complete(actual[key], value, f"{path}.{key}")
    elif isinstance(expected, list):
        assert isinstance(actual, list), path
        assert len(actual) == len(expected), path
        for index, value in enumerate(expected):
            assert_complete(actual[index], value, f"{path}[{index}]")
    elif isinstance(expected, bool):
        assert actual is expected, path
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-10, abs=1e-12), path
    elif isinstance(expected, str) and isinstance(actual, str) and actual != expected:
        try:
            left, right = Decimal(actual), Decimal(expected)
        except InvalidOperation:
            assert actual == expected, path
        else:
            assert left.is_finite() and right.is_finite(), path
            with localcontext() as context:
                context.prec = 100
                assert abs(left - right) <= Decimal("1e-18"), path
    else:
        assert actual == expected, path


def forbid_python_business(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("shipping assessment invoked Python analytics")

    for name in (
        "compute_market_truth",
        "compute_condition_values",
        "condition_market_state",
        "condition_confidence",
        "condition_depth",
        "condition_reference_close",
        "_compute_market_truth",
        "spot_truth",
        "spot_window_graph",
        "relative_truth",
        "derivatives_truth",
        "liquidity_truth",
        "supply_truth",
        "support_features",
        "enrich_evidence",
        "data_confidence",
        "select_move",
        "decisive_facts",
        "bounded_market_snapshot",
    ):
        monkeypatch.setattr(truth, name, forbidden)
    monkeypatch.setattr(truth.compute_workers, "compute_market_truth", forbidden)


class CountingNative:
    def __init__(self, client):
        self.client = client
        self.requests = []

    def __getattr__(self, name):
        return getattr(self.client, name)

    async def rows(self, statement, params=None):
        self.requests.append(statement)
        return await self.client.rows(statement, params)


@pytest.mark.integration
@pytest.mark.parametrize("case", ["empty", "supply", "fresh", "no_anchor", "late", "future"])
async def test_one_native_select_complete_frozen_original(
    engine_client, original_truth, monkeypatch, case
):
    packet = _supply_snapshot() if case == "supply" else _fresh_snapshot()[0] if case == "fresh" else _snapshot()
    packet.pop("_primary", None)
    if case == "no_anchor":
        packet["anchor"] = None
    elif case in ("late", "future"):
        row = {
            "kind": "FLOW",
            "classification": "TREASURY_INTERNAL",
            "qty_native": "12.25",
            "ts": packet["cutoff"],
        }
        if case == "late":
            row["received_ts"] = iso(parse_time(packet["assembled_at"]) + timedelta(seconds=1))
        else:
            row["ts"] = iso(parse_time(packet["cutoff"]) + timedelta(seconds=1))
        packet["supply"].append(row)
    config = {"config_version": "engine-frozen", "code_version": "engine-frozen"}
    expected = original_truth.compute_market_truth(deepcopy(packet), config=config)
    expected["measurement_context"]["numeric_policy"] = native.NUMERIC_POLICY
    forbid_python_business(monkeypatch)
    counted = CountingNative(engine_client)
    actual = await native.evaluate(counted, packet, config)
    assert len(counted.requests) == 1
    assert_complete(actual, expected)
    assert actual["measurement_metadata"]["relative.anchor.ref_price"]["cutoff"] == packet["cutoff"]


@pytest.mark.integration
async def test_native_support_consumes_post_residual_fields(engine_client, original_truth, monkeypatch):
    from test_truth_sql_conditions import _physical_snapshot

    packet = _physical_snapshot(minutes=120)
    for definition in packet["instruments"]:
        packet["semantics"].append({
            "venue": definition["venue"], "market_type": "SPOT",
            "metric_family": "aggressor", "trade_aggregation": "PER_FILL",
        })
    for bar in packet["flow_bars"]:
        bar["size_buckets"] = {
            "B3": {"buy_notional": "30", "sell_notional": "5"},
            "B4": {"buy_notional": "5", "sell_notional": "0"},
        }
    packet.pop("_primary", None)
    config = {"history_min_days": 0}
    expected = original_truth.compute_market_truth(deepcopy(packet), config=config)
    expected["measurement_context"]["numeric_policy"] = native.NUMERIC_POLICY
    forbid_python_business(monkeypatch)
    result = await native.evaluate(engine_client, packet, config)
    assert_complete(result, expected)
    assert result["spot"]["EVENT"]["size_profile"]["large_ticket_buy_majority"] is True
    assert result["derivatives"]["windows"]["EVENT"]["support_features"]["large_ticket_buy_majority"] is True
    for window, pack in result["derivatives"]["windows"].items():
        features = pack.get("support_features")
        if features is not None:
            profile = result["spot"][window].get("size_profile") or {}
            assert features["large_ticket_buy_majority"] == profile.get("large_ticket_buy_majority")
            assert features["large_ticket_sell_share_rising"] == profile.get("sell_share_rising")


@pytest.mark.integration
@pytest.mark.postgres
async def test_actual_service_native_query_postgres_immutable_readback(
    original_truth, postgres_template, monkeypatch
):
    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "owned disposable PostgreSQL is required"
    packet = _supply_snapshot()
    packet.pop("_primary", None)
    event_id = "EVT-NATIVE-ENGINE"
    event = {"event_id": event_id, "public_at": packet["anchor"]["ts"], "public_at_prec": "EXACT"}
    config = {"config_version": "engine-pg", "code_version": "engine-pg"}
    expected_packet = deepcopy(packet)
    expected_packet.update(event_id=event_id, expected_direction="LOSER")
    expected_packet["anchor"] = truth.event_anchor(event, at=packet["cutoff"])
    expected_packet.update(truth.propagate_timestamp(event, "public_at").fields("event_public_at"))
    expected = original_truth.compute_market_truth(deepcopy(expected_packet), config=config)
    expected["measurement_context"]["numeric_policy"] = native.NUMERIC_POLICY

    class Market:
        async def snapshot(self, token_id, cutoff, anchor=None):
            assert token_id == packet["token_id"] and cutoff == packet["cutoff"]
            result = deepcopy(packet)
            result["anchor"] = anchor
            return result

    async with postgres_template.repository(dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None, "native CI ClickHouse is required"
        counted = CountingNative(repo.clickhouse)
        await repo.put("evt.events", event_id, event)
        for version, role in ((1, "BENEFICIARY"), (2, "LOSER")):
            key = f"BFE-NATIVE-{version}"
            await repo.put("evt.beneficiary_edges", key, {
                "edge_id": key, "event_id": event_id, "token_id": packet["token_id"],
                "map_version": version, "role": role,
            })
        repo.clickhouse = counted
        service = truth.MarketTruthService(repo, SimpleNamespace(**config), Market())
        forbid_python_business(monkeypatch)
        result = await service.assess(event_id, packet["token_id"], packet["cutoff"])
        assert len(counted.requests) == 1
        assert result["mta_id"].startswith("MTA")
        assert_complete({key: value for key, value in result.items() if key != "mta_id"}, expected)
        stored = await repo.get("ana.market_truth", result["mta_id"])
        assert stored == result
        assert await repo.put("ana.market_truth", result["mta_id"], result, immutable=True) == result


@pytest.mark.integration
@pytest.mark.parametrize("amount", ["1e25", "0.0000000000000000000000001"])
async def test_native_admission_refuses_unrepresentable_amounts(engine_client, amount):
    packet = _supply_snapshot()
    packet["supply"] = [{
        "kind": "FLOW", "ts": iso(parse_time(packet["cutoff"]) - timedelta(seconds=1)),
        "qty_native": amount,
    }]
    with pytest.raises(RuntimeError, match="native financial range"):
        await native.evaluate(engine_client, packet)


@pytest.mark.integration
@pytest.mark.postgres
async def test_real_market_loader_native_assessment_and_durable_return(
    original_truth, postgres_template, monkeypatch, tmp_path
):
    import httpx
    from test_truth_sql_conditions import _physical_snapshot

    from tank.market.service import COLLECTIONS, MarketService
    from tank.storage import ImmutableRecordError

    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "owned disposable PostgreSQL is required"
    assert os.environ.get("CLICKHOUSE_TEST_URL"), "owned native ClickHouse is required"
    packet = _physical_snapshot(minutes=60)
    registry = {"tokens": [{"token_id": "T", "symbol": "T"}], "instruments": packet["instruments"]}
    settings = SimpleNamespace(
        config_version="engine-loader", code_version="engine-loader", data_dir=tmp_path,
        user_agent="tank-native-engine-test",
        load_config=lambda key: registry if key == "registry" else {},
    )
    captured = []
    async with postgres_template.repository(dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None
        for family, (collection, _) in COLLECTIONS.items():
            if packet.get(family):
                await repo.put_market_batch(collection, packet[family])
        counted = CountingNative(repo.clickhouse)
        repo.clickhouse = counted
        async with httpx.AsyncClient() as http:
            market = MarketService(repo, settings, http=http)
            original_snapshot = market.snapshot

            async def observed_snapshot(*args, **kwargs):
                result = await original_snapshot(*args, **kwargs)
                captured.append(deepcopy(result))
                return result

            monkeypatch.setattr(market, "snapshot", observed_snapshot)
            forbid_python_business(monkeypatch)
            service = truth.MarketTruthService(repo, settings, market)
            result = await service.assess(None, "T", packet["cutoff"])
        assert len(captured) == 1
        assert len(captured[0]["flow_bars"]) == len(packet["flow_bars"])
        assert len(captured[0]["books"]) == len(packet["books"])
        assert len([query for query in counted.requests if " AS eng_raw," in query]) == 1
        snapshot = dict(captured[0], event_id=None, token_id="T")
        expected = original_truth.compute_market_truth(snapshot, config={
            "config_version": settings.config_version, "code_version": settings.code_version,
        })
        expected["measurement_context"]["numeric_policy"] = native.NUMERIC_POLICY
        assert_complete({key: value for key, value in result.items() if key != "mta_id"}, expected)
        assert await repo.get("ana.market_truth", result["mta_id"]) == result
        with pytest.raises(ImmutableRecordError):
            await repo.put("ana.market_truth", result["mta_id"], dict(result, move_class="tampered"))
        assert await repo.get("ana.market_truth", result["mta_id"]) == result


@pytest.mark.integration
@pytest.mark.parametrize("metric", ["spot.classification", "mt.move_class", "mt.divergence_state"])
@pytest.mark.parametrize("future_anchor", [False, True])
async def test_service_condition_path_is_native_and_transient(
    engine_client, original_truth, monkeypatch, metric, future_anchor
):
    from test_truth_sql_conditions import _compare_caches, _condition_contract_oracle

    packet = _snapshot()
    packet.pop("_primary", None)
    if future_anchor:
        packet["anchor"] = {"ts": iso(parse_time(packet["cutoff"]) + timedelta(hours=1))}
    requirements = [{"metric": metric, "window": "W1H"}]
    config = {"config_version": "condition-native", "code_version": "condition-native"}
    expected_packet = dict(packet, event_id=None, token_id=packet["token_id"])
    oracle_cache = {}
    expected = _condition_contract_oracle(original_truth, expected_packet, requirements, config=config, cache=oracle_cache)
    counted = CountingNative(engine_client)

    class Repo:
        clickhouse = counted

        async def get(self, *args):
            raise AssertionError("no event lookup for an absent event")

        async def put(self, *args, **kwargs):
            raise AssertionError("condition values must remain transient")

    class Market:
        async def condition_snapshot(self, token_id, cutoff, *, anchor, requirements):
            assert anchor is None
            return deepcopy(packet)

    forbid_python_business(monkeypatch)
    service = truth.MarketTruthService(Repo(), SimpleNamespace(**config), Market())
    actual = await service.condition_values(None, packet["token_id"], packet["cutoff"], requirements)
    assert len(counted.requests) == 1
    assert_complete(actual, expected)
    _compare_caches(service._condition_cache, oracle_cache, original_truth)
    assert actual["_condition_evaluation"]["transient"] is True
    assert "mta_id" not in actual


@pytest.mark.integration
@pytest.mark.parametrize("field,value", [("cutoff", "not-a-date"), ("cutoff", "2500-01-01T00:00:00Z"), ("assembled_at", "not-a-date")])
async def test_assessment_rejects_invalid_native_timestamps(engine_client, field, value):
    packet = _snapshot()
    packet[field] = value
    with pytest.raises((RuntimeError, ValueError), match="timestamp outside native supported range|anchor must precede cutoff|Invalid isoformat"):
        await native.evaluate(engine_client, packet)


@pytest.mark.asyncio
async def test_missing_native_provider_fails_without_python_fallback(monkeypatch):
    forbid_python_business(monkeypatch)
    with pytest.raises(RuntimeError, match="ClickHouse"):
        await native.evaluate(None, _snapshot())


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [{"result": "{}"}], [{"result": "null", "provenance_snapshot": "{}"}]])
async def test_native_transport_rejects_incomplete_result_protocol(rows):
    class Transport:
        async def rows(self, statement, params=None):
            return rows

    with pytest.raises(ValueError, match="native Market Truth"):
        await native.evaluate(Transport(), _snapshot())


def test_query_values_remain_typed_parameters_and_arrays_keep_order():
    from array import array

    packet = _snapshot()
    marker = "hostile'); SELECT 42 --"
    packet["token_id"] = marker
    packet["stat_price_series"] = {
        "z-first": {"times_us": array("q", [1, 2]), "values": array("d", [3, 4]), "complete": bytearray([1, 0])},
        "a-last": {},
    }
    statement, parameters = native.query(packet)
    assert marker not in statement
    payload = json.loads(parameters["eng_snapshot"][0])
    assert payload["token_id"] == marker
    assert list(payload["stat_price_series"]) == ["z-first", "a-last"]
    assert payload["stat_price_series"]["z-first"]["complete"] == [1, 0]
    assert all(isinstance(value, tuple) and len(value) == 2 for value in parameters.values())
    assert statement.index("eng_enriched AS (") < statement.index("sup_support_stage AS (")
    assert statement.index("sup_support_stage AS (") < statement.index("res_gate_inputs AS (")


def test_decimal_wire_restoration_is_not_identifier_coercion():
    identifier = "1.000000000000000000000000"
    result = native.decode_decimals({"instrument_id": identifier, "cvd": identifier, "value": identifier})
    assert result["instrument_id"] == identifier
    assert result["cvd"] == Decimal(identifier)
    assert result["value"] == Decimal(identifier)


def test_decimal_wire_restoration_preserves_source_payload_types():
    amount = "1.000000000000000000000000"
    result = native.decode_decimals({
        "boundary_segments": [{"aggregates": [{"buy_qty": amount}]}],
        "exchange_inventory": {"kind": "INVENTORY", "qty_native": amount},
        "size_profile": {"large_ticket_buy_notional": amount},
    })
    assert result["boundary_segments"][0]["aggregates"][0]["buy_qty"] == amount
    assert result["exchange_inventory"]["qty_native"] == amount
    assert result["size_profile"]["large_ticket_buy_notional"] == Decimal(amount)


@pytest.mark.asyncio
async def test_service_retains_per_token_lock_without_python_workers(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    count = 0

    async def evaluate(client, snapshot, config=None):
        nonlocal count
        count += 1
        entered.set()
        await release.wait()
        return {"token_id": snapshot["token_id"], "config_version": config["config_version"]}

    class Repo:
        clickhouse = object()

        async def get(self, collection, key):
            return None

        async def put(self, collection, key, record, **kwargs):
            assert kwargs == {"immutable": True}
            return record

    class Market:
        async def snapshot(self, token_id, cutoff, anchor=None):
            return {"token_id": token_id, "cutoff": cutoff, "anchor": anchor}

    monkeypatch.setattr(native, "evaluate", evaluate)
    forbid_python_business(monkeypatch)
    service = truth.MarketTruthService(Repo(), SimpleNamespace(config_version="v", code_version="c"), Market())
    first = asyncio.create_task(service.assess(None, "T", _snapshot()["cutoff"]))
    await entered.wait()
    second = asyncio.create_task(service.assess(None, "T", _snapshot()["cutoff"]))
    await asyncio.sleep(0)
    assert count == 1
    release.set()
    left, right = await asyncio.gather(first, second)
    assert left["config_version"] == right["config_version"] == "v"
    assert left["mta_id"] != right["mta_id"]
    assert count == 2


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["sql", "transport", "cache_envelope", "proof_envelope", "cancel"])
async def test_failed_or_cancelled_native_query_does_not_publish_cache(engine_client, original_truth, failure):
    from test_truth_sql_conditions import _cached_evaluate, _physical_snapshot, _requirements

    packet = _physical_snapshot(minutes=0)
    requirements = _requirements(("liq.spread_ratio",))
    cache, expected_cache, baselines = {}, {}, {}
    await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, expected_cache, baselines)
    held = deepcopy(cache)
    packet["domain_dependency_fingerprints"] = {"depth": "new-unpublished-head"}
    entered = asyncio.Event()
    release = asyncio.Event()

    class FailingTransport:
        async def rows(self, statement, parameters=None):
            if failure == "sql":
                return await engine_client.rows("SELECT throwIf(1, 'cache-publication-refusal') AS result")
            rows = await engine_client.rows(statement, parameters)
            if failure == "transport":
                raise RuntimeError("native transport failed after query completion")
            if failure == "cache_envelope":
                rows[0]["condition_cache"] = '[{"key":"invalid","value":{}}]'
            elif failure == "proof_envelope":
                result = json.loads(rows[0]["result"])
                result["_condition_evaluation"]["proofs"][0].pop("reused")
                rows[0]["result"] = json.dumps(result)
            elif failure == "cancel":
                entered.set()
                await release.wait()
            return rows

    if failure == "cancel":
        task = asyncio.create_task(native.evaluate_conditions(FailingTransport(), packet, requirements, cache=cache))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises((RuntimeError, ValueError)):
            await native.evaluate_conditions(FailingTransport(), packet, requirements, cache=cache)
    assert cache == held
    await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, expected_cache, baselines)


@pytest.mark.integration
@pytest.mark.postgres
async def test_service_condition_cache_native_control_preimages_and_no_postgres_persistence(
    original_truth, postgres_template, monkeypatch
):
    from test_truth_sql_conditions import _compare_caches, _physical_snapshot, _requirements

    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "owned disposable PostgreSQL is required"
    packet = _physical_snapshot(minutes=15)
    marker = 'δ\\quoted\\n-literal\nline\ttab\rcarriage\x00nul"終'
    packet["domain_dependency_fingerprints"] = {"spot": {"source": marker, "order": ["B", "A"]}}
    packet["dependency_fingerprint"] = {"frontier": marker}
    config = {"config_version": marker, "code_version": "native-cache-controls", "description": marker}
    requirements = _requirements(("spot.classification", "spot.cohort.direction"))
    oracle_cache, baselines = {}, {}

    class Market:
        async def condition_snapshot(self, token_id, cutoff, *, anchor, requirements):
            assert anchor is None and token_id == packet["token_id"]
            return deepcopy(packet)

    settings = SimpleNamespace(
        config_version=config["config_version"], code_version=config["code_version"],
        load_config=lambda name: {"description": marker},
    )
    async with postgres_template.repository(dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None, "native CI ClickHouse is required"
        before = await repo.list("ana.market_truth", limit=10000)
        counted = CountingNative(repo.clickhouse)
        repo.clickhouse = counted
        service = truth.MarketTruthService(repo, settings, Market())
        forbid_python_business(monkeypatch)
        actual = None
        for repeat in range(2):
            expected_packet = dict(deepcopy(packet), event_id=None)
            expected = original_truth.compute_condition_values(
                expected_packet, requirements, config=deepcopy(config), cache=oracle_cache, baseline_cache=baselines
            )
            actual = await service.condition_values(None, packet["token_id"], packet["cutoff"], requirements)
            assert_complete(actual, expected)
            _compare_caches(service._condition_cache, oracle_cache, original_truth)
            assert len(counted.requests) == repeat + 1
            assert "mta_id" not in actual and not any(key.startswith("_native_") for key in actual)
        assert actual is not None
        assert all(proof["reused"] for proof in actual["_condition_evaluation"]["proofs"])
        assert await repo.list("ana.market_truth", limit=10000) == before
        assert len(counted.requests) == 2


def _corrupt_condition_envelope(rows, failure, domain):
    records = json.loads(rows[0]["condition_cache"])
    result = json.loads(rows[0]["result"])
    keys = [record["key"] for record in records]
    if failure in {"raw_empty", "raw_nested", "key_component"}:
        record = next(record for record in records if tuple(json.loads(record["key"])[:2]) == domain)
        if failure == "raw_empty":
            record["value"] = {}
        elif failure == "raw_nested":
            if domain[0] == "state":
                record["value"]["derivatives"]["windows"][record["value"]["primary_window"]] = 17
            elif domain == ("condition-state-node", "spot"):
                record["value"]["spots"]["W1H"] = 17
            elif domain == ("condition-state-node", "supply"):
                record["value"]["flows"] = [17]
            else:
                record["value"]["coverage"] = 17
        else:
            parts = json.loads(record["key"])
            parts[2] = True
            record["key"] = json.dumps(parts)
    elif failure == "public_domain":
        result["spot"]["W1H"]["coverage"] = 17
    elif failure == "observations":
        result["_condition_evaluation"]["proofs"][0]["observations"] = [17]
    elif failure == "observation_field":
        result["_condition_evaluation"]["proofs"][0]["observations"][0]["cadence_s"] = "17"
    elif failure == "coverage":
        result["_condition_evaluation"]["proofs"][0]["coverage"] = 17
    elif failure == "pending":
        result["_condition_evaluation"]["unavailable_metrics"] = [{"metric": "unknown", "window": "W1H", "reason": 17}]
    elif failure == "cutoff":
        result["cutoff"] = 17
    elif failure == "missing_proof":
        result["_condition_evaluation"]["proofs"].pop()
    elif failure == "nonfinite":
        result["spot"]["W1H"]["coverage"]["covered_share_est"] = float("nan")
    elif failure == "duplicate":
        rows[0]["result"] = json.dumps(result)[:-1] + ',"cutoff":17}'
        return
    else:
        raise AssertionError(failure)
    if failure != "key_component":
        assert [record["key"] for record in records] == keys
    rows[0]["result"] = json.dumps(result)
    rows[0]["condition_cache"] = json.dumps(records)


@pytest.mark.integration
@pytest.mark.parametrize("metric,domain,failure", [
    ("spot.classification", ("spot", "W1H"), "raw_empty"),
    ("deriv.oi_class", ("oi", "W1H"), "raw_empty"),
    ("deriv.funding_vs_normal", ("funding", "W1H"), "raw_empty"),
    ("price.close_vs_level", ("close", "W1H"), "raw_empty"),
    ("dc.class", ("dc", "W1H"), "raw_empty"),
    ("liq.spread_ratio", ("depth", "W1H"), "raw_empty"),
    ("price.repricing_band", ("relative", "W1H"), "raw_empty"),
    ("mt.move_class", ("state", "W1H"), "raw_empty"),
    ("mt.move_class", ("condition-state-node", "spot"), "raw_empty"),
    ("mt.move_class", ("condition-state-node", "relative"), "raw_empty"),
    ("mt.move_class", ("condition-state-node", "derivatives"), "raw_empty"),
    ("mt.move_class", ("condition-state-node", "supply"), "raw_empty"),
    ("spot.classification", ("spot", "W1H"), "raw_nested"),
    ("mt.move_class", ("state", "W1H"), "raw_nested"),
    ("mt.move_class", ("condition-state-node", "spot"), "raw_nested"),
    ("mt.move_class", ("condition-state-node", "supply"), "raw_nested"),
    ("spot.classification", ("spot", "W1H"), "key_component"),
    ("mt.move_class", ("condition-state-node", "spot"), "key_component"),
    ("spot.classification", ("spot", "W1H"), "public_domain"),
    ("liq.spread_ratio", ("depth", "W1H"), "observations"),
    ("liq.spread_ratio", ("depth", "W1H"), "observation_field"),
    ("spot.classification", ("spot", "W1H"), "coverage"),
    ("spot.classification", ("spot", "W1H"), "pending"),
    ("spot.classification", ("spot", "W1H"), "cutoff"),
    ("spot.classification", ("spot", "W1H"), "missing_proof"),
    ("spot.classification", ("spot", "W1H"), "nonfinite"),
    ("spot.classification", ("spot", "W1H"), "duplicate"),
])
async def test_real_native_whole_envelope_refusal_preserves_ordered_cache(
    engine_client, original_truth, metric, domain, failure
):
    from test_truth_sql_conditions import _cache_snapshot, _cached_evaluate, _requirements

    packet = _cache_snapshot()
    packet["domain_dependency_fingerprints"] = {
        name: "retained-envelope-head" for name in ("spot", "oi", "funding", "close", "dc", "depth", "relative", "state", "derivatives", "supply")
    }
    requirements = _requirements((metric,)) + [{"metric": "unknown", "window": "W1H"}]
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    candidate = deepcopy(packet)
    candidate["domain_dependency_fingerprints"] = dict.fromkeys(packet["domain_dependency_fingerprints"], "unpublished-envelope-head")

    class CorruptNative:
        async def rows(self, statement, parameters=None):
            rows = await engine_client.rows(statement, parameters)
            assert len(rows) == 1 and set(rows[0]) == {"result", "condition_cache"}
            _corrupt_condition_envelope(rows, failure, domain)
            return rows

    with pytest.raises(ValueError, match="native Market Truth"):
        await native.evaluate_conditions(CorruptNative(), candidate, requirements, cache=cache)
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
    if metric == "spot.classification":
        for row in packet["flow_bars"]:
            row.update(buy_qty="1", sell_qty="9", buy_notional="10", sell_notional="90")
        cold = original_truth.compute_condition_values(deepcopy(packet), requirements, cache={}, baseline_cache={})
        assert cold["spot"] != first["spot"]
    hit = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in hit["_condition_evaluation"]["proofs"])
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
    assert_complete({key: value for key, value in hit.items() if key != "_condition_evaluation"}, {
        key: value for key, value in first.items() if key != "_condition_evaluation"
    })


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("metric,domain,failure", [
    ("spot.classification", ("spot", "W1H"), "raw_empty"),
    ("mt.move_class", ("condition-state-node", "spot"), "raw_empty"),
    ("spot.classification", ("spot", "W1H"), "public_domain"),
    ("liq.spread_ratio", ("depth", "W1H"), "observations"),
    ("spot.classification", ("spot", "W1H"), "coverage"),
    ("spot.classification", ("spot", "W1H"), "pending"),
    ("spot.classification", ("spot", "W1H"), "cutoff"),
    ("spot.classification", ("spot", "W1H"), "missing_proof"),
])
async def test_actual_service_rejects_real_native_envelope_without_cache_or_postgres_publication(
    original_truth, postgres_template, monkeypatch, metric, domain, failure
):
    from test_truth_sql_conditions import _cache_snapshot, _compare_caches, _requirements

    dsn = os.environ.get("TANK_TEST_DATABASE_URL")
    assert dsn, "owned disposable PostgreSQL is required"
    packet = _cache_snapshot()
    packet["domain_dependency_fingerprints"] = dict.fromkeys(
        ("spot", "oi", "funding", "close", "dc", "depth", "relative", "state", "derivatives", "supply"), "retained-service-head"
    )
    requirements = _requirements((metric,)) + [{"metric": "unknown", "window": "W1H"}]
    config = {"config_version": "envelope-service", "code_version": "envelope-service"}
    oracle_cache, baselines = {}, {}

    class Market:
        async def condition_snapshot(self, token_id, cutoff, *, anchor, requirements):
            assert anchor is None and token_id == packet["token_id"] and cutoff == packet["cutoff"]
            return deepcopy(packet)

    async with postgres_template.repository(dsn) as repo:
        assert getattr(repo, "clickhouse", None) is not None
        before = await repo.list("ana.market_truth", limit=10000)
        real_native = repo.clickhouse
        corrupt = False
        calls = 0

        class NativeBoundary:
            async def rows(self, statement, parameters=None):
                nonlocal calls
                calls += 1
                rows = await real_native.rows(statement, parameters)
                if corrupt:
                    _corrupt_condition_envelope(rows, failure, domain)
                return rows

        repo.clickhouse = NativeBoundary()
        service = truth.MarketTruthService(repo, SimpleNamespace(**config), Market())
        forbid_python_business(monkeypatch)
        expected = original_truth.compute_condition_values(
            dict(deepcopy(packet), event_id=None), requirements, config=config, cache=oracle_cache, baseline_cache=baselines
        )
        first = await service.condition_values(None, packet["token_id"], packet["cutoff"], requirements)
        assert_complete(first, expected)
        _compare_caches(service._condition_cache, oracle_cache, original_truth)
        held = json.dumps(list(service._condition_cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
        packet["domain_dependency_fingerprints"] = dict.fromkeys(packet["domain_dependency_fingerprints"], "unpublished-service-head")
        corrupt = True
        with pytest.raises(ValueError, match="native Market Truth"):
            await service.condition_values(None, packet["token_id"], packet["cutoff"], requirements)
        assert json.dumps(list(service._condition_cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
        assert await repo.list("ana.market_truth", limit=10000) == before
        packet["domain_dependency_fingerprints"] = dict.fromkeys(packet["domain_dependency_fingerprints"], "retained-service-head")
        corrupt = False
        if metric == "spot.classification":
            for row in packet["flow_bars"]:
                row.update(buy_qty="1", sell_qty="9", buy_notional="10", sell_notional="90")
            cold = original_truth.compute_condition_values(dict(deepcopy(packet), event_id=None), requirements, config=config)
            assert cold["spot"] != first["spot"]
        expected = original_truth.compute_condition_values(
            dict(deepcopy(packet), event_id=None), requirements, config=config, cache=oracle_cache, baseline_cache=baselines
        )
        hit = await service.condition_values(None, packet["token_id"], packet["cutoff"], requirements)
        assert_complete(hit, expected)
        _compare_caches(service._condition_cache, oracle_cache, original_truth)
        assert all(proof["reused"] for proof in hit["_condition_evaluation"]["proofs"])
        assert json.dumps(list(service._condition_cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
        assert await repo.list("ana.market_truth", limit=10000) == before
        assert calls == 3 and "mta_id" not in hit


@pytest.mark.integration
async def test_native_replacement_copy_failure_keeps_old_cache(engine_client, original_truth, monkeypatch):
    from test_truth_sql_conditions import _cached_evaluate, _physical_snapshot, _requirements

    packet = _physical_snapshot(minutes=15)
    packet["domain_dependency_fingerprints"] = {"spot": "retained-copy-head"}
    requirements = _requirements(("spot.classification",))
    cache, oracle_cache, baselines = {}, {}, {}
    await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    candidate = deepcopy(packet)
    candidate["domain_dependency_fingerprints"] = {"spot": "unpublished-copy-head"}

    def failed_copy(value):
        raise MemoryError("replacement copy refused")

    with monkeypatch.context() as patch:
        class CopyFailureBoundary:
            async def rows(self, statement, parameters=None):
                rows = await engine_client.rows(statement, parameters)
                patch.setattr(native, "deepcopy", failed_copy)
                return rows

        with pytest.raises(MemoryError, match="replacement copy refused"):
            await native.evaluate_conditions(CopyFailureBoundary(), candidate, requirements, cache=cache)
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
    hit = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert hit["_condition_evaluation"]["proofs"][0]["reused"] is True
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


@pytest.mark.integration
@pytest.mark.parametrize("failure", [
    "raw_empty", "raw_confidence", "raw_derivatives", "move_missing", "divergence", "confidence",
    "primary_window", "derivative_window", "relative", "proof_available", "proof_reason",
    "proof_observations", "proof_coverage", "missing_proof", "pending", "unknown_field",
])
async def test_minimal_anchor_real_native_whole_envelope_refusal_and_reuse(engine_client, original_truth, failure):
    from test_truth_sql_conditions import CUTOFF, _cached_evaluate, _requirements, _snapshot

    packet = _snapshot()
    packet["anchor"]["ts"] = iso(CUTOFF + timedelta(seconds=60))
    requirements = _requirements(("mt.move_class", "mt.divergence_state"))
    cache, oracle_cache, baselines = {}, {}, {}
    await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    candidate = deepcopy(packet)
    candidate["domain_dependency_fingerprints"] = {"state": "unpublished-minimal-head"}

    class CorruptNative:
        async def rows(self, statement, parameters=None):
            rows = await engine_client.rows(statement, parameters)
            assert len(rows) == 1 and set(rows[0]) == {"result", "condition_cache"}
            records = json.loads(rows[0]["condition_cache"])
            result = json.loads(rows[0]["result"])
            keys = [record["key"] for record in records]
            assert all(json.loads(key)[0] == "state" for key in keys)
            raw = records[-1]["value"]
            proof = result["_condition_evaluation"]["proofs"][0]
            if failure == "raw_empty":
                records[-1]["value"] = {}
            elif failure == "raw_confidence":
                raw["data_confidence"] = {"class": "HIGH"}
            elif failure == "raw_derivatives":
                raw["derivatives"] = {"windows": {}}
            elif failure == "move_missing":
                result.pop("move_class")
            elif failure == "divergence":
                result["divergence"] = {"state": "UNKNOWN", "unexpected": True}
            elif failure == "confidence":
                result["data_confidence"] = {"class": "HIGH"}
            elif failure == "primary_window":
                result["primary_window"] = "W4H"
            elif failure == "derivative_window":
                result["derivatives"]["windows"]["W1H"] = {"per_venue": [], "synchronized": None}
            elif failure == "relative":
                result["relative"] = {"unexpected": "value"}
            elif failure == "proof_available":
                proof.update(available=False, reason="INCOMPLETE")
                result["_condition_evaluation"]["unavailable_metrics"] = [{
                    "metric": proof["metric"], "window": proof["window"], "reason": "INCOMPLETE",
                }]
            elif failure == "proof_reason":
                proof["reason"] = "INCOMPLETE"
            elif failure == "proof_observations":
                proof["observations"] = [{"instrument_id": "unexpected"}]
            elif failure == "proof_coverage":
                proof["coverage"] = {}
            elif failure == "missing_proof":
                result["_condition_evaluation"]["proofs"].pop()
            elif failure == "pending":
                result["_condition_evaluation"]["unavailable_metrics"] = [{
                    "metric": proof["metric"], "window": proof["window"], "reason": "INCOMPLETE",
                }]
            elif failure == "unknown_field":
                result["_native_condition_cache"] = []
            else:
                raise AssertionError(failure)
            assert [record["key"] for record in records] == keys
            rows[0]["condition_cache"] = json.dumps(records)
            rows[0]["result"] = json.dumps(result)
            return rows

    with pytest.raises(ValueError, match="native Market Truth"):
        await native.evaluate_conditions(CorruptNative(), candidate, requirements, cache=cache)
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held
    recovered = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in recovered["_condition_evaluation"]["proofs"])
    assert recovered["spot"] == recovered["relative"] == {}
    assert recovered["derivatives"] == {"windows": {}}
    assert recovered["_condition_evaluation"]["unavailable_metrics"] == []
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


@pytest.mark.integration
@pytest.mark.parametrize("seconds", [0, 60])
async def test_nonpast_anchor_retained_raw_state_hit_is_not_recomputed_minimal_state(engine_client, original_truth, seconds):
    from test_truth_sql_conditions import (
        CUTOFF,
        _cached_evaluate,
        _compare_caches,
        _condition_contract_oracle,
        _requirements,
        _snapshot,
    )

    packet = _snapshot()
    packet.pop("anchor")
    packet["domain_dependency_fingerprints"] = {"state": "authoritative-retained-anchor-head"}
    requirements = _requirements(("mt.move_class", "mt.divergence_state"))
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    packet["anchor"] = {"ts": iso(CUTOFF + timedelta(seconds=seconds)), "type": "KNOWN_TO_US_AT"}
    expected = original_truth.compute_condition_values(
        deepcopy(packet), requirements, cache=oracle_cache, baseline_cache=baselines
    )
    cold_cache = {}
    cold = _condition_contract_oracle(original_truth, packet, requirements, cache=cold_cache)
    actual = await native.evaluate_conditions(engine_client, packet, requirements, cache=cache)
    assert_complete(actual, expected)
    _compare_caches(cache, oracle_cache, original_truth)
    assert all(proof["reused"] for proof in actual["_condition_evaluation"]["proofs"])
    assert actual["spot"] == first["spot"] and actual["spot"] != cold["spot"]
    assert actual["derivatives"] == first["derivatives"] and actual["derivatives"] != cold["derivatives"]
    assert cold["spot"] == cold["relative"] == {} and cold["derivatives"] == {"windows": {}}
    assert len(cold_cache) == 1 and all(key[0] == "state" for key in cold_cache)
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


def _liquidation_failure_snapshot(case):
    from test_truth_sql_conditions import CUTOFF, _physical_snapshot

    packet = _physical_snapshot()
    packet["instruments"].append({
        "instrument_id": "binance:perp:T", "venue": "binance", "token_id": "T", "market_type": "PERP_LINEAR",
    })
    packet["liquidations"] = [{
        "instrument_id": "binance:perp:T", "token_id": "T", "ts": iso(CUTOFF - timedelta(minutes=5)),
        "ts_prec": "EXACT", "side": "LONG_LIQ", "notional_usd": "1", "qty_native": "0.1",
    }]
    if case == "limiting":
        packet["instruments"].pop()
        for row in packet["tickers"]:
            row["volume_native"] = "300"
    if case not in {"no_cadence", "limiting"}:
        entry = {"cadence_s": 60, "coverage_status_by_instrument": {"binance:perp:T": "DISCONNECTED"}}
        if case == "watermark":
            entry["observed_through"] = {"binance:perp:T": iso(CUTOFF - timedelta(minutes=2))}
        packet["capture_metadata"] = {"collections": {"liquidations": entry}}
    return packet


@pytest.mark.integration
@pytest.mark.parametrize("metric", ["dc.class", "mt.move_class"])
@pytest.mark.parametrize("case", ["no_cadence", "stale", "watermark", "limiting"])
async def test_native_liquidation_confidence_failure_fields_survive_frozen_roundtrip(engine_client, original_truth, metric, case):
    from test_truth_sql_conditions import _cached_evaluate, _requirements

    packet = _liquidation_failure_snapshot(case)
    requirements = _requirements((metric,))
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    row = next(row for row in first["data_confidence"]["failed"] if row.get("collection") == "liquidations")
    assert row["freshness_basis"] == ("STREAM_WATERMARK" if case == "watermark" else "LAST_EVENT")
    assert row["coverage_status"] == (None if case in {"no_cadence", "limiting"} else "DISCONNECTED")
    assert row["detail"] == (None if case == "watermark" else "STREAM_CONNECTION_UNPROVEN")
    if case == "limiting":
        assert first["data_confidence"]["limiting"] == row
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    hit = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in hit["_condition_evaluation"]["proofs"])
    assert hit["data_confidence"] == first["data_confidence"]
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


async def _native_shape_refusal_matrix(client, original, packet, requirements, paths, location, failure, *, confidence=False):
    from test_truth_sql_conditions import _cached_evaluate

    packet["domain_dependency_fingerprints"] = {"state": "retained-shape", "dc": "retained-shape"}
    cache, oracle_cache, baselines = {}, {}, {}
    await _cached_evaluate(client, original, packet, requirements, cache, oracle_cache, baselines)
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    candidate = deepcopy(packet)
    candidate["domain_dependency_fingerprints"] = {"state": "unpublished-shape", "dc": "unpublished-shape"}
    captured = []

    class CaptureNative:
        async def rows(self, statement, parameters=None):
            rows = await client.rows(statement, parameters)
            captured.append(deepcopy(rows))
            return rows

    await _cached_evaluate(CaptureNative(), original, candidate, requirements, deepcopy(cache), deepcopy(oracle_cache), {})
    assert len(captured) == 1
    genuine = captured[0]
    original_keys = [record["key"] for record in json.loads(genuine[0]["condition_cache"])]
    for path in paths:
        rows = deepcopy(genuine)
        records = json.loads(rows[0]["condition_cache"])
        public = json.loads(rows[0]["result"])
        record = next(record for record in reversed(records) if json.loads(record["key"])[0] in {"state", "dc"})
        target = public if location == "public" else record["value"]
        if confidence:
            target = target.get("data_confidence", target)
            if path[0] == "limiting":
                assert target["limiting"]["collection"] == "liquidations"
        else:
            target = target["liquidity"]
        for component in path[:-1]:
            target = next(row for row in target if row.get("collection") == "liquidations") if component == "liquidation_row" else target[component]
        if failure == "missing":
            del target[path[-1]]
        elif failure == "unknown":
            pack = target if path[-1] == "self" else target[path[-1]]
            pack["unexpected_derived_field"] = True
        elif failure == "nested":
            target[path[-1]] = [True, False] if path[-1] == "normal_range" else [17]
        elif failure == "pair_width":
            target[path[-1]] = [["binance"]]
        else:
            target[path[-1]] = [] if path[-1] == "range_covered_pct" else {"poison": True}
        assert [record["key"] for record in records] == original_keys
        if location == "raw":
            assert public == json.loads(genuine[0]["result"])
        else:
            assert records == json.loads(genuine[0]["condition_cache"])
        rows[0]["result"] = json.dumps(public)
        rows[0]["condition_cache"] = json.dumps(records)

        class CorruptNative:
            async def rows(self, statement, parameters=None):
                return deepcopy(rows)

        with pytest.raises(ValueError, match="native Market Truth"):
            await native.evaluate_conditions(CorruptNative(), candidate, requirements, cache=cache)
        assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held, path
    recovered = await _cached_evaluate(client, original, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in recovered["_condition_evaluation"]["proofs"])
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


@pytest.mark.integration
@pytest.mark.parametrize("metric", ["dc.class", "mt.move_class"])
@pytest.mark.parametrize("location", ["public", "raw"])
@pytest.mark.parametrize("failure", ["wrong_type", "missing"])
async def test_native_liquidation_failure_field_refusal_and_recovery(engine_client, original_truth, metric, location, failure):
    from test_truth_sql_conditions import _requirements

    packet = _liquidation_failure_snapshot("limiting")
    paths = [(group, "liquidation_row", field) for group in ("failed", "freshness") for field in ("freshness_basis", "coverage_status", "detail")]
    paths += [("limiting", field) for field in ("freshness_basis", "coverage_status", "detail")]
    await _native_shape_refusal_matrix(engine_client, original_truth, packet, _requirements((metric,)), paths, location, failure, confidence=True)


def _liquidity_shape_paths():
    paths: list[tuple[str | int, ...]] = [(key,) for key in (
        "depth_measurement_band_pct", "per_venue", "aggregate_depth_p1_usd", "aggregate_bid_depth_p1_usd",
        "aggregate_ask_depth_p1_usd", "pre_event_depth_p1_usd", "pre_event_depth_window",
        "pre_event_side_depth_p1_usd", "depth_drop_by_side", "depth_drop", "depth_majority_lost",
        "depth_drop_cohort", "spread_ratio", "replenishment", "elasticity", "vol_compression", "liquidity_state",
        "best_single_venue_slippage_bps", "split_slippage_bps", "fragmentation",
    )]
    for key in ("pre_event_side_depth_p1_usd", "depth_drop_by_side", "depth_drop_cohort"):
        paths.extend((key, side) for side in ("bid", "ask"))
    paths.extend(("pre_event_depth_window", key) for key in ("start", "cutoff", "anchor_type"))
    paths.extend(("per_venue", 0, key) for key in (
        "venue", "coverage", "observed_at", "precision", "spread_bps", "depth_p1_usd", "bid_depth_p1_usd",
        "ask_depth_p1_usd", "range_covered_pct", "tier_notional", "slippage_bps",
    ))
    paths.extend(("per_venue", 0, "coverage", key) for key in ("venue", "range_covered_pct", "integrity_ok"))
    for key in ("tier_notional", "slippage_bps"):
        paths.extend(("per_venue", 0, key, tier) for tier in ("T1", "T2", "T3"))
    for key in ("best_single_venue_slippage_bps", "split_slippage_bps"):
        paths.extend((key, tier) for tier in ("T1", "T2", "T3"))
    paths.extend(("replenishment", key) for key in ("available", "bursts", "replenished", "resilient_bursts", "median_recovery_s", "observations"))
    paths.extend(("replenishment", "observations", 0, key) for key in ("venue", "ts", "replenished", "recovery_s", "resilient"))
    paths.extend(("elasticity", key) for key in (
        "bps_per_depth_unit", "weak_response", "excess_response", "hit_side", "depth_unit", "net_flow",
        "hit_side_depth_lower_bound", "hit_side_covered_venues", "hit_side_coverage_complete",
        "pre_event_hit_side_depth_lower_bound", "pre_event_hit_side_covered_venues", "hit_side_depth_p1_usd",
        "pre_event_hit_side_depth_p1_usd", "avg_hit_side_depth_p1_usd",
    ))
    paths.extend(("vol_compression", key) for key in ("window", "cutoff", "rv24", "normal_range", "state", "reason"))
    paths.extend(("fragmentation", key) for key in (
        "window", "cutoff", "persistent_gap", "validated", "price_path_validated", "funding_path_validated",
        "funding_beyond_normal", "funding_normal_range_8h", "top_sign_flips", "gap_closed_quickly", "venues",
        "cross_quote_not_evaluated", "quote_conversion_uncertainty_bps", "complete_venues", "reason",
    ))
    return paths


@pytest.mark.integration
@pytest.mark.parametrize("location", ["public", "raw"])
@pytest.mark.parametrize("failure", ["wrong_type", "missing", "unknown", "nested", "pair_width"])
async def test_native_full_liquidity_derived_field_matrix_refusal_and_recovery(engine_client, original_truth, location, failure):
    from test_truth_sql_conditions import CUTOFF, _book, _cache_snapshot, _requirements

    packet = _cache_snapshot()
    start = CUTOFF - timedelta(hours=1)
    packet["books"].extend([_book("binance", start - timedelta(seconds=5)), _book("binance", start + timedelta(seconds=30))])
    for row in packet["flow_bars"]:
        row["low"] = "99"
        if row["instrument_id"] == "binance:spot:T" and row["minute_start"] == iso(start):
            row.update(buy_notional="0", sell_notional="1000", buy_qty="0", sell_qty="100")
    paths = _liquidity_shape_paths()
    if failure == "unknown":
        paths = [(key,) for key in ("self", "pre_event_depth_window", "pre_event_side_depth_p1_usd", "depth_drop_by_side", "depth_drop_cohort", "best_single_venue_slippage_bps", "split_slippage_bps", "replenishment", "elasticity", "vol_compression", "fragmentation")]
        paths += [("per_venue", 0), ("per_venue", 0, "coverage"), ("per_venue", 0, "tier_notional"), ("per_venue", 0, "slippage_bps"), ("replenishment", "observations", 0)]
    elif failure == "nested":
        paths = [("per_venue",), ("replenishment", "observations"), ("depth_drop_cohort", "bid"), ("depth_drop_cohort", "ask"), ("elasticity", "hit_side_covered_venues"), ("elasticity", "pre_event_hit_side_covered_venues"), ("vol_compression", "normal_range"), ("fragmentation", "funding_normal_range_8h"), ("fragmentation", "venues"), ("fragmentation", "cross_quote_not_evaluated"), ("fragmentation", "complete_venues")]
    elif failure == "pair_width":
        paths = [("fragmentation", "venues"), ("fragmentation", "cross_quote_not_evaluated")]
    await _native_shape_refusal_matrix(engine_client, original_truth, packet, _requirements(("mt.move_class",)), paths, location, failure)


@pytest.mark.integration
@pytest.mark.parametrize("location", ["public", "raw"])
@pytest.mark.parametrize("failure", ["wrong_type", "missing"])
async def test_native_liquidity_absent_anchor_window_variant_refusal(engine_client, original_truth, location, failure):
    from test_truth_sql_conditions import _physical_snapshot, _requirements

    packet = _physical_snapshot()
    packet.pop("anchor")
    paths = [("pre_event_depth_window", field) for field in ("start", "cutoff", "reason")]
    await _native_shape_refusal_matrix(engine_client, original_truth, packet, _requirements(("mt.move_class",)), paths, location, failure)


@pytest.mark.integration
@pytest.mark.parametrize("case", ["empty", "missing_side", "missing_baseline", "no_anchor", "scalar_source", "object_source", "null_source", "string_source"])
async def test_native_full_liquidity_null_cohorts_and_source_wrappers(engine_client, original_truth, case):
    from test_truth_sql_conditions import _cached_evaluate, _physical_snapshot, _requirements, _snapshot

    packet = _snapshot() if case == "empty" else _physical_snapshot()
    if case == "missing_side":
        for row in packet["books"]:
            row["depth"]["ask"]["p1"] = {}
    elif case == "missing_baseline":
        packet["books"] = [row for row in packet["books"] if row["exchange_ts"] == packet["cutoff"]]
    elif case == "no_anchor":
        packet.pop("anchor")
    elif case in {"scalar_source", "object_source", "null_source", "string_source"}:
        for row in packet["books"]:
            row["range_covered_pct"] = {"scalar_source": 2, "object_source": {"bid": 2, "ask": 2, "source_annotation": {"retained": [True, None]}}, "null_source": None, "string_source": "2"}[case]
    requirements = _requirements(("mt.move_class",))
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    if case == "empty":
        liquidity = first["liquidity"]
        assert liquidity["depth_drop"] is liquidity["depth_majority_lost"] is None
        assert liquidity["depth_drop_by_side"] == {"bid": None, "ask": None}
        assert liquidity["pre_event_side_depth_p1_usd"] == {"bid": None, "ask": None}
        assert liquidity["depth_drop_cohort"] == {"bid": [], "ask": []}
    if case in {"scalar_source", "object_source", "null_source", "string_source"}:
        expected = {"scalar_source": 2, "object_source": {"bid": 2, "ask": 2, "source_annotation": {"retained": [True, None]}}, "null_source": None, "string_source": "2"}[case]
        assert first["liquidity"]["per_venue"][0]["range_covered_pct"] == expected
        assert first["liquidity"]["per_venue"][0]["coverage"]["range_covered_pct"] == expected
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    hit = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in hit["_condition_evaluation"]["proofs"])
    assert hit["liquidity"] == first["liquidity"]
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


def _freshness_inventory_snapshot(collection):
    from test_truth_sql_conditions import CUTOFF

    packet = _liquidation_failure_snapshot("limiting")
    if collection == "tickers":
        packet["liquidations"] = []
        for row in packet["tickers"]:
            row["ts"] = iso(CUTOFF - timedelta(minutes=5))
    return packet


@pytest.mark.integration
@pytest.mark.parametrize("metric", ["dc.class", "mt.move_class"])
@pytest.mark.parametrize("location", ["public", "raw"])
@pytest.mark.parametrize("collection", ["liquidations", "tickers"])
@pytest.mark.parametrize("failure", ["missing", "timestamp_type", "present_null"])
async def test_native_freshness_complete_inventory_refusal_and_recovery(
    engine_client, original_truth, metric, location, collection, failure
):
    from test_truth_sql_conditions import _cached_evaluate, _requirements

    packet = _freshness_inventory_snapshot(collection)
    packet["domain_dependency_fingerprints"] = {"state": "healthy-inventory", "dc": "healthy-inventory"}
    requirements = _requirements((metric,))
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert first["data_confidence"]["limiting"]["collection"] == collection
    assert first["data_confidence"]["limiting"]["condition"] == "freshness"
    assert "metric_family" not in first["data_confidence"]["limiting"]
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    candidate = deepcopy(packet)
    candidate["domain_dependency_fingerprints"] = {"state": "candidate-inventory", "dc": "candidate-inventory"}
    captured = []

    class CaptureNative:
        async def rows(self, statement, parameters=None):
            rows = await engine_client.rows(statement, parameters)
            captured.append(deepcopy(rows))
            return rows

    await _cached_evaluate(CaptureNative(), original_truth, candidate, requirements, deepcopy(cache), deepcopy(oracle_cache), {})
    assert len(captured) == 1
    genuine = captured[0]
    fields = (
        "collection", "instrument_id", "metric_key", "kind", "chain", "cadence_s", "cadence_source", "age_s",
        "observed_at", "observed_at_prec", "observed_at_prec_reason", "reason",
    ) if failure == "missing" else (("observed_at", "observed_at_prec", "observed_at_prec_reason", "observed_at_bucket_anchor") if failure == "timestamp_type" else ("observed_at",))
    for group in ("failed", "freshness", "limiting"):
        group_fields = (*fields, "fresh" if group == "freshness" else "condition") if failure == "missing" else fields
        for field in group_fields:
            rows = deepcopy(genuine)
            records = json.loads(rows[0]["condition_cache"])
            public = json.loads(rows[0]["result"])
            record = next(record for record in reversed(records) if json.loads(record["key"])[0] == ("dc" if metric == "dc.class" else "state"))
            target = public if location == "public" else record["value"]
            confidence = target.get("data_confidence", target)
            row = confidence[group] if group == "limiting" else next(row for row in confidence[group] if row.get("collection") == collection and "metric_family" not in row)
            if failure == "missing":
                del row[field]
            elif failure == "present_null":
                row.update(observed_at=None, observed_at_prec=None, observed_at_prec_reason="INCOMPLETE")
                row.pop("observed_at_bucket_anchor", None)
            else:
                row[field] = {"poison": True}
            assert [record["key"] for record in records] == [record["key"] for record in json.loads(genuine[0]["condition_cache"])]
            if location == "public":
                assert records == json.loads(genuine[0]["condition_cache"])
            else:
                assert public == json.loads(genuine[0]["result"])
            rows[0]["result"] = json.dumps(public)
            rows[0]["condition_cache"] = json.dumps(records)

            class CorruptNative:
                async def rows(self, statement, parameters=None):
                    return deepcopy(rows)

            if failure == "present_null":
                admitted_cache = deepcopy(cache)
                admitted = await native.evaluate_conditions(CorruptNative(), candidate, requirements, cache=admitted_cache)
                admitted_record = next(value for key, value in reversed(list(admitted_cache.items())) if json.loads(key)[0] == ("dc" if metric == "dc.class" else "state"))
                admitted_confidence = admitted["data_confidence"] if location == "public" else admitted_record.get("data_confidence", admitted_record)
                admitted_row = admitted_confidence[group] if group == "limiting" else next(row for row in admitted_confidence[group] if row.get("collection") == collection and "metric_family" not in row)
                assert "observed_at" in admitted_row and admitted_row["observed_at"] is None
            else:
                with pytest.raises(ValueError, match="native Market Truth"):
                    await native.evaluate_conditions(CorruptNative(), candidate, requirements, cache=cache)
            assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held, (group, field)
    recovered = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in recovered["_condition_evaluation"]["proofs"])
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


@pytest.mark.integration
@pytest.mark.parametrize("metric", ["dc.class", "mt.move_class"])
@pytest.mark.parametrize("case", ["coverage_required", "bucket_anchor"])
async def test_native_confidence_short_and_bucket_variants_remain_admitted(engine_client, original_truth, metric, case):
    from test_truth_sql_conditions import _cached_evaluate, _requirements

    packet = _liquidation_failure_snapshot("no_cadence" if case == "coverage_required" else "limiting")
    if case == "bucket_anchor":
        packet["liquidations"][0].update(ts_prec="BUCKET(1m)", ts_bucket_anchor="START")
    requirements = _requirements((metric,))
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    failed = first["data_confidence"]["failed"]
    if case == "coverage_required":
        assert {row["condition"] for row in failed} >= {"spot_coverage", "derivatives_coverage", "freshness"}
        for row in failed:
            if row["condition"] != "freshness":
                assert set(row) == {"condition", "reason"}
        required = [row for row in failed if "metric_family" in row]
        assert {row["metric_family"] for row in required} >= {"oi", "funding"}
        assert all(set(row) == {"condition", "reason", "collection", "instrument_id", "metric_family", "cadence_s", "cadence_source"} for row in required)
    else:
        row = next(row for row in failed if row.get("collection") == "liquidations")
        assert row["observed_at_bucket_anchor"] == "START"
        assert row["observed_at_prec"] == "BUCKET(1m)"
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    hit = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in hit["_condition_evaluation"]["proofs"])
    assert hit["data_confidence"] == first["data_confidence"]
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


@pytest.mark.parametrize("collection", ["liquidations", "tickers"])
def test_confidence_present_null_timestamp_and_measurements_remain_admitted(collection):
    row = {
        "condition": "freshness", "reason": "NOT_ESTIMABLE", "collection": collection,
        "instrument_id": None, "metric_key": None, "kind": None, "chain": None,
        "cadence_s": None, "cadence_source": "CADENCE_NOT_CONFIGURED", "age_s": None,
        "observed_at": None, "observed_at_prec": None, "observed_at_prec_reason": "INCOMPLETE",
    }
    if collection == "liquidations":
        row.update(freshness_basis="STREAM_WATERMARK", coverage_status=None, detail=None)
    confidence = {
        "class": "MEDIUM", "failed": [deepcopy(row)], "limiting": deepcopy(row),
        "freshness": [{**{key: value for key, value in row.items() if key != "condition"}, "fresh": False}],
        "derivatives_native_coverage_ready": True, "derivatives_classification_ready": True,
    }
    expected = deepcopy(confidence)
    native._condition_confidence(confidence)
    assert confidence == expected


def _supplemental_oi_snapshot(case="quantities"):
    from test_truth_sql_conditions import CUTOFF, _cache_snapshot

    packet = _cache_snapshot()
    if case != "no_exclusions":
        row = deepcopy(next(row for row in packet["oi"] if row["instrument_id"] == "binance:perp:T"))
        row.update(ts=iso(CUTOFF - timedelta(seconds=30)), ts_prec="BUCKET(1m)", ts_bucket_anchor="UNSPECIFIED", oi_native="123.25", oi_usd="1232.5")
        if case == "null_money":
            row.update(oi_native=None, oi_usd=None)
            row.pop("mark_price")
        packet["oi"] = [row] if case == "missing_endpoints" else [*packet["oi"], row]
    return packet


def _supplemental_oi_venue(value):
    return next(row for row in value["derivatives"]["windows"][value["primary_window"]]["per_venue"] if row["instrument_id"] == "binance:perp:T")


@pytest.mark.integration
@pytest.mark.parametrize("case", ["quantities", "null_money", "missing_endpoints", "no_exclusions"])
async def test_native_supplemental_oi_complete_frozen_roundtrip_and_hit(engine_client, original_truth, case):
    from test_truth_sql_conditions import _cached_evaluate, _requirements

    packet = _supplemental_oi_snapshot(case)
    requirements = _requirements(("mt.move_class",))
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    venue = _supplemental_oi_venue(first)
    raw = next(value for key, value in reversed(list(cache.items())) if json.loads(key)[0] == "state")
    assert _supplemental_oi_venue(raw) == venue
    if case == "no_exclusions":
        assert all("oi_temporal_exclusions" not in row for row in first["derivatives"]["windows"][first["primary_window"]]["per_venue"])
    else:
        exclusion = venue["oi_temporal_exclusions"]
        assert exclusion["count"] == 1
        assert exclusion["reason"] == "INCOMPLETE"
        assert exclusion["detail"] == "OI_BUCKET_EDGE_UNSPECIFIED"
        latest = exclusion["latest_observation"]
        assert set(latest) == {"ts", "ts_prec", "ts_prec_reason", "ts_bucket_anchor", "oi_native", "oi_usd", "scope"}
        assert latest["scope"] == "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"
        assert latest["ts"] == packet["oi"][-1]["ts"]
        assert latest["ts_prec"] == "BUCKET(1m)"
        assert latest["ts_prec_reason"] is None
        assert latest["ts_bucket_anchor"] == "UNSPECIFIED"
        if case == "null_money":
            assert latest["oi_native"] is latest["oi_usd"] is None
        else:
            assert Decimal(latest["oi_native"]) == Decimal("123.25")
            assert Decimal(latest["oi_usd"]) == Decimal("1232.5")
        if case == "missing_endpoints":
            assert venue["oi_class"] is None
            assert venue["reason"] == "INCOMPLETE"
        else:
            assert "decomposition" in venue
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    hit = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in hit["_condition_evaluation"]["proofs"])
    assert hit["derivatives"] == first["derivatives"]
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held


@pytest.mark.integration
@pytest.mark.parametrize("location", ["public", "raw"])
async def test_native_supplemental_oi_shape_refusal_nonpublication_and_true_hit_recovery(engine_client, original_truth, location):
    from test_truth_sql_conditions import _cached_evaluate, _requirements

    packet = _supplemental_oi_snapshot()
    packet["domain_dependency_fingerprints"] = {"state": "healthy-supplemental"}
    requirements = _requirements(("mt.move_class",))
    cache, oracle_cache, baselines = {}, {}, {}
    first = await _cached_evaluate(engine_client, original_truth, packet, requirements, cache, oracle_cache, baselines)
    retained = _supplemental_oi_venue(first)["oi_temporal_exclusions"]
    assert retained["count"] == 1
    assert retained["latest_observation"]["scope"] == "SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"
    assert Decimal(retained["latest_observation"]["oi_native"]) == Decimal("123.25")
    assert Decimal(retained["latest_observation"]["oi_usd"]) == Decimal("1232.5")
    held = json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode()
    candidate = deepcopy(packet)
    candidate["domain_dependency_fingerprints"] = {"state": "candidate-supplemental"}
    captured = []

    class CaptureNative:
        async def rows(self, statement, parameters=None):
            rows = await engine_client.rows(statement, parameters)
            captured.append(deepcopy(rows))
            return rows

    await _cached_evaluate(CaptureNative(), original_truth, candidate, requirements, deepcopy(cache), deepcopy(oracle_cache), {})
    assert len(captured) == 1
    genuine = captured[0]
    genuine_records = json.loads(genuine[0]["condition_cache"])
    genuine_public = json.loads(genuine[0]["result"])
    record_index = next(index for index in reversed(range(len(genuine_records))) if json.loads(genuine_records[index]["key"])[0] == "state")
    assert _supplemental_oi_venue(genuine_records[record_index]["value"])["oi_temporal_exclusions"] == retained
    assert _supplemental_oi_venue(genuine_public)["oi_temporal_exclusions"] == retained
    fields = ("ts", "ts_prec", "ts_prec_reason", "ts_bucket_anchor", "oi_native", "oi_usd", "scope")
    mutations = [("missing", field, None) for field in fields]
    mutations += [("value", field, {"poison": True}) for field in fields]
    mutations += [("value", field, value) for field in ("oi_native", "oi_usd") for value in (True, 17, 1.25, "NaN", "1", "1.0", "1e2", [])]
    mutations += [("value", "scope", value) for value in (None, "WINDOW_ENDPOINT", "")]
    mutations += [("value", "ts", "2026-13-01T00:00:00Z"), ("value", "ts_prec", "BUCKET(0m)"), ("value", "ts_prec", "EXACT"), ("value", "ts_bucket_anchor", "INVALID"), ("value", "ts_bucket_anchor", None)]
    mutations += [("unknown", "latest_observation", True), ("unknown", "exclusion", True)]
    for operation, field, value in mutations:
        rows = deepcopy(genuine)
        records = json.loads(rows[0]["condition_cache"])
        public = json.loads(rows[0]["result"])
        target = public if location == "public" else records[record_index]["value"]
        exclusion = _supplemental_oi_venue(target)["oi_temporal_exclusions"]
        latest = exclusion["latest_observation"]
        assert exclusion["count"] == 1 and latest == retained["latest_observation"]
        if operation == "missing":
            del latest[field]
        elif operation == "unknown":
            (latest if field == "latest_observation" else exclusion)["unexpected_derived_field"] = value
        else:
            latest[field] = value
        assert [record["key"] for record in records] == [record["key"] for record in genuine_records]
        if location == "public":
            assert records == genuine_records
            untouched = deepcopy(public)
            _supplemental_oi_venue(untouched)["oi_temporal_exclusions"] = deepcopy(retained)
            assert untouched == genuine_public
        else:
            assert public == genuine_public
            untouched = deepcopy(records)
            _supplemental_oi_venue(untouched[record_index]["value"])["oi_temporal_exclusions"] = deepcopy(retained)
            assert untouched == genuine_records
        rows[0]["result"] = json.dumps(public)
        rows[0]["condition_cache"] = json.dumps(records)

        class CorruptNative:
            async def rows(self, statement, parameters=None):
                return deepcopy(rows)

        with pytest.raises(ValueError):
            await native.evaluate_conditions(CorruptNative(), candidate, requirements, cache=cache)
        assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held, (operation, field, value)
    recovery = deepcopy(packet)
    recovery["oi"][-1].update(oi_native="777", oi_usd="7770")
    cold = original_truth.jsonable(original_truth.compute_condition_values(deepcopy(recovery), requirements, cache={}, baseline_cache={}))
    assert _supplemental_oi_venue(cold)["oi_temporal_exclusions"] != retained
    recovered = await _cached_evaluate(engine_client, original_truth, recovery, requirements, cache, oracle_cache, baselines)
    assert all(proof["reused"] for proof in recovered["_condition_evaluation"]["proofs"])
    assert _supplemental_oi_venue(recovered)["oi_temporal_exclusions"] == retained
    assert json.dumps(list(cache.items()), ensure_ascii=False, separators=(",", ":")).encode() == held

