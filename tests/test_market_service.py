import asyncio
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import yaml
from source_support import fixture_http_admission, fixture_versions

from tank.market.service import MarketService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import clock_iso, iso, parse_time, utcnow


def test_market_redaction_covers_nested_keys_and_values_and_current_credentials() -> None:
    from types import SimpleNamespace

    market = object.__new__(MarketService)
    market.settings = SimpleNamespace(telegram_token="private-token")
    value = {"private-token": [{"url": "https://example.test/private-token"}, (7, None)]}
    assert market._redact(value) == {"[REDACTED]": [{"url": "https://example.test/[REDACTED]"}, [7, None]]}
    assert "private-token" in value
    market.settings.telegram_token = "rotated-token"
    assert market._redact({"key": "rotated-token"}) == {"key": "[REDACTED]"}


@pytest.fixture
def setup(tmp_path: Path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "thresholds.yaml").write_bytes(Settings().read_config_bytes("thresholds.yaml"))

    def inst(token, symbol):
        return {
            "instrument_id": "binance:spot:" + symbol,
            "token_id": token,
            "venue": "binance",
            "market_type": "SPOT",
            "venue_symbol": symbol,
            "quote_asset": "USD",
            "active": True,
            "contract_multiplier": "1",
            "tick_size": ".01",
            "lot_size": ".01",
        }

    target = inst("test:ABC", "ABCUSD")
    benchmark = inst("bitcoin:BTC", "BTCUSD")
    registry = {
        "tokens": [
            {"token_id": "test:ABC", "symbol": "ABC", "sector": "A"},
            {"token_id": "bitcoin:BTC", "symbol": "BTC", "sector": "BENCHMARK"},
        ],
        "instruments": [target, benchmark],
    }
    (config / "registry.yaml").write_text(yaml.safe_dump(registry))
    (config / "semantics.yaml").write_text(
        yaml.safe_dump(
            {
                "metrics": [
                    {
                        "metric_key": "binance.spot.aggTrade",
                        "semantic_confidence": "UNKNOWN",
                        "critical": True,
                    }
                ]
            }
        )
    )
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data", replay=True)
    return MemoryRepository(), settings, target


@pytest.mark.asyncio
async def test_snapshot_exact_cutoff_anchor_and_benchmark_history(setup) -> None:
    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    cutoff = "2026-06-17T00:02:00Z"
    for iid, minute in [
        (inst["instrument_id"], "00:00"),
        (inst["instrument_id"], "00:02"),
        ("binance:spot:BTCUSD", "00:01"),
    ]:
        row = {
            "instrument_id": iid,
            "token_id": inst["token_id"] if iid == inst["instrument_id"] else "bitcoin:BTC",
            "open_time": "2026-06-17T" + minute + ":00Z",
            "c": "2",
            "complete": True,
        }
        await repo.put("mkt.klines_1m", iid + minute, row)
    await repo.put(
        "mkt.oi_obs",
        "before",
        {
            "instrument_id": inst["instrument_id"],
            "metric_key": "binance.usdm.openInterest",
            "ts": "2026-06-17T00:01:00Z",
            "oi_native": "3",
        },
    )
    await repo.put(
        "mkt.oi_obs",
        "future",
        {
            "instrument_id": inst["instrument_id"],
            "metric_key": "binance.usdm.openInterest",
            "ts": "2026-06-17T00:03:00Z",
            "oi_native": "9",
        },
    )
    anchor = {"ts": "2026-06-16T23:00:00Z", "type": "T0"}
    snapshot = await service.snapshot(inst["token_id"], cutoff, anchor)
    assert snapshot["anchor"]["ts"] == anchor["ts"] and snapshot["anchor"]["type"] == anchor["type"]
    assert snapshot["anchor"]["ts_prec"] is None and snapshot["anchor"]["ts_prec_reason"] == "INCOMPLETE"
    assert snapshot["assembled_at_prec"] == "COLLECTION_ONLY" and snapshot["requested_cutoff_prec"] == "EXACT"
    assert len(snapshot["klines"]) == 2 and len(snapshot["oi"]) == 1
    from tank.time_precision import Timestamp

    uncertain = await service.snapshot(inst["token_id"], Timestamp(cutoff, "BUCKET(1h)"), anchor)
    assert uncertain["cutoff_prec"] == uncertain["requested_cutoff_prec"] == "BUCKET(1h)"
    assert any(i["token_id"] == "bitcoin:BTC" for i in snapshot["instruments"])
    assert (await service.snapshot(inst["token_id"], cutoff))["anchor"] is None
    await http.aclose()


@pytest.mark.asyncio
async def test_fundamentals_use_registered_slugs_only_and_partial_holders_revenue(setup) -> None:
    from tank.market.supply import SupplyCapture

    repo, settings, _inst = setup
    token = {
        "token_id": "test:ABC",
        "symbol": "ABC",
        "contracts": [{"chain": "Ethereum", "address": "0xabc"}],
        "defillama_protocol_slugs": ["abc-v1", "abc-v2"],
    }
    paths = []

    def handler(request: httpx.Request):
        path = request.url.path
        paths.append(path)

        assert path.startswith("/summary/fees/")
        metric = request.url.params["dataType"]
        if path.endswith("abc-v2") and metric == "dailyHoldersRevenue":
            return httpx.Response(404, json={"error": "unsupported"})
        value = "3" if metric == "dailyHoldersRevenue" else "10"
        return httpx.Response(
            200, json={"total24h": value, "total30d": value, "totalDataChart": [[1781654400, value]]}
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    capture = SupplyCapture(service)
    result = await capture.fundamentals(token)
    assert result["metrics"]["dailyFees"]["total24h"] == "20"
    assert result["metrics"]["dailyHoldersRevenue"]["total24h"] == "3"
    assert not result["metrics"]["dailyHoldersRevenue"]["complete"]
    assert result["coverage_errors"][0]["metric"] == "dailyHoldersRevenue"
    assert len(paths) == 6
    paths.clear()
    unregistered = await capture.fundamentals(dict(token, token_id="test:XYZ", defillama_protocol_slugs=[]))
    assert unregistered["available"] is False
    assert unregistered["reason"] == "NO_REGISTERED_DEFILLAMA_PROTOCOL_SLUG"
    assert paths == []
    await http.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("secret_name", ["etherscan_api_key", "foundry_api_key", "codex_gateway_key"])
async def test_market_transport_and_recursive_metadata_never_persist_api_key(setup, secret_name) -> None:
    from tank.market.supply import SupplyCapture

    repo, settings, _inst = setup
    secret = "secret-test-provider-key"
    setattr(settings, secret_name, secret)

    def handler(request: httpx.Request) -> None:
        raise httpx.ConnectError("Connection failed: " + str(request.url), request=request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    with pytest.raises(RuntimeError) as captured:
        await service._request("GET", "https://api.etherscan.io/v2/api", params={"apikey": secret})
    assert secret not in str(captured.value)
    capture = SupplyCapture(service)
    token = {"token_id": "test:ABC"}
    await capture.record(
        token,
        "EXCHANGE_INVENTORY",
        coverage_errors=[
            {
                "reason": "nested url?apikey=" + secret,
                "context": {"key": secret},
            }
        ],
    )
    await service.flush_market()
    persisted = await repo.list("mkt.supply_obs")
    assert len(persisted) == 1
    assert secret not in str(persisted)
    assert persisted[0]["coverage_errors"] == [
        {"reason": "nested url?apikey=[REDACTED]", "context": {"key": "[REDACTED]"}}
    ]
    assert not (settings.data_dir / "raw/market_frames").exists()
    await http.aclose()


@pytest.mark.asyncio
async def test_hot_escalation_priority_age_repeat_and_event_specific_demote(setup) -> None:
    repo, settings, _inst = setup
    http = httpx.AsyncClient()
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    thresholds = settings.load_config("thresholds")
    thresholds["hot_token_cap"] = 1
    (settings.config_dir / "thresholds.yaml").write_text(yaml.safe_dump(thresholds))
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    await repo.put(
        "evt.events",
        "old",
        {"event_id": "old", "public_at": "2026-10-10T00:00:00Z", "current_ems_id": "major"},
    )
    await repo.put(
        "evt.ems_assessments",
        "major",
        {
            "ems_id": "major",
            "event_id": "old",
            "class": "MAJOR",
            "version": 1,
            "cutoff": "2026-10-10T00:00:00Z",
        },
    )
    await service.escalate("old", ["test:ABC"])
    original = await repo.get("mkt.hot_tokens", "test:ABC:old")
    await service.escalate("old", ["test:ABC"])
    assert (
        (await repo.get("mkt.hot_tokens", "test:ABC:old"))["since"]
        == original["since"]
        == "2026-10-10T00:00:00Z"
    )
    await repo.put(
        "evt.events",
        "new",
        {"event_id": "new", "public_at": "2026-10-11T00:00:00Z", "current_ems_id": "shock"},
    )
    await repo.put(
        "evt.ems_assessments",
        "shock",
        {
            "ems_id": "shock",
            "event_id": "new",
            "class": "STRUCTURAL_SHOCK",
            "version": 1,
            "cutoff": "2026-10-11T00:00:00Z",
        },
    )
    await service.escalate("new", ["bitcoin:BTC"])
    hot = await repo.list("mkt.hot_tokens")
    assert len(hot) == 1 and hot[0]["token_id"] == "bitcoin:BTC"
    thresholds["hot_token_cap"] = 2
    (settings.config_dir / "thresholds.yaml").write_text(yaml.safe_dump(thresholds))
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    await service.escalate("old", ["bitcoin:BTC"])
    await service.demote("bitcoin:BTC", "old")
    assert len(await repo.list("mkt.hot_tokens")) == 1

    await http.aclose()


def add_perp_instrument(settings, spot):
    registry = settings.load_config("registry")
    instrument = dict(spot, instrument_id="binance:perp:ABCUSD", market_type="PERP_LINEAR")
    registry["instruments"].append(instrument)
    (settings.config_dir / "registry.yaml").write_text(yaml.safe_dump(registry))
    return instrument


@pytest.mark.asyncio
async def test_capture_tier_uses_durable_activation_and_demotion_history_not_future_hot_state(
    setup, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tank.market.service as module
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    perp = add_perp_instrument(settings, inst)
    clock = parse_time("2026-06-17T11:00:00Z")
    monkeypatch.setattr(module, "utcnow", lambda: clock)
    async with httpx.AsyncClient() as http:
        service = MarketService(
            repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
        )
        token = inst["token_id"]
        assert (await service._capture_metadata(token, clock))["tier"] == "UNKNOWN"
        await repo.put(
            "mkt.oi_obs",
            "unknown-tier-oi",
            {
                "instrument_id": perp["instrument_id"],
                "token_id": token,
                "ts": iso(clock - timedelta(seconds=90)),
                "metric_key": "binance.usdm.openInterest",
                "oi_native": "1",
                "oi_usd": "2",
            },
        )
        from tank.analysis.truth import compute_market_truth

        unknown_truth = compute_market_truth(await service.snapshot(token, clock))
        check = next(
            row for row in unknown_truth["data_confidence"]["freshness"] if row["collection"] == "oi"
        )
        assert check["cadence_s"] is None and check["reason"] == "NOT_ESTIMABLE"
        assert check["cadence_source"] == "CAPTURE_TIER_HISTORY_UNAVAILABLE"
        await service._record_capture_tier(token, "BASE", "CAPTURE_POLICY_STARTED")
        baseline = clock
        clock += timedelta(hours=1)
        await repo.put("evt.events", "tier-event", {"event_id": "tier-event", "public_at": iso(clock)})
        await service.escalate("tier-event", [token])
        active = clock
        assert (await service._capture_metadata(token, active - timedelta(minutes=1)))["tier"] == "BASE"
        assert (await service._capture_metadata(token, active))["tier"] == "HOT"
        clock += timedelta(minutes=1)
        await service.demote(token)
        restarted = MarketService(
            repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
        )
        assert (await restarted._capture_metadata(token, active))["tier"] == "HOT"
        assert (await restarted._capture_metadata(token, clock))["tier"] == "BASE"
        assert (await restarted._capture_metadata(token, baseline - timedelta(seconds=1)))[
            "tier"
        ] == "UNKNOWN"


@pytest.mark.asyncio
async def test_actual_producer_tier_metadata_controls_90second_oi_and_funding_freshness(
    setup, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tank.analysis.truth import compute_market_truth
    from tank.util import iso, utcnow

    repo, settings, spot = setup
    perp = add_perp_instrument(settings, spot)
    http = httpx.AsyncClient()
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    cutoff = utcnow().replace(second=0, microsecond=0)
    stamp = iso(cutoff - timedelta(seconds=90))
    iid = perp["instrument_id"]
    await repo.put(
        "mkt.oi_obs",
        "oi",
        {
            "instrument_id": iid,
            "token_id": spot["token_id"],
            "ts": stamp,
            "metric_key": "binance.usdm.openInterest",
            "oi_native": "100",
            "oi_usd": "200",
        },
    )
    await repo.put(
        "mkt.funding_obs",
        "funding",
        {
            "instrument_id": iid,
            "token_id": spot["token_id"],
            "as_of": stamp,
            "kind": "CURRENT_ESTIMATE",
            "metric_key": "binance.usdm.funding",
            "rate": "0.0001",
            "interval_h": "8",
            "rate_8h_eq": "0.0001",
        },
    )
    await repo.put(
        "mkt.hot_tokens",
        spot["token_id"] + ":event",
        {"token_id": spot["token_id"], "event_id": "event", "since": stamp, "priority": 2},
    )
    import tank.market.service as service_module

    clock = cutoff - timedelta(seconds=1)
    monkeypatch.setattr(service_module, "utcnow", lambda: clock)
    await service._record_capture_tier(spot["token_id"], "HOT", "TEST_ACTUAL_POLICY_ACTIVATED")
    hot_snapshot = await service.snapshot(spot["token_id"], cutoff)
    assert hot_snapshot["capture_tier"] == hot_snapshot["capture_metadata"]["tier"] == "HOT"
    hot = compute_market_truth(hot_snapshot)
    checks = [
        c
        for c in hot["data_confidence"]["freshness"]
        if c["instrument_id"] == iid and c["collection"] in ("oi", "funding")
    ]
    assert len(checks) == 2 and all(
        c["age_s"] == 90
        and c["cadence_s"] == 60
        and c["cadence_source"] == "CAPTURE_METADATA"
        and not c["fresh"]
        for c in checks
    )
    assert hot_snapshot["capture_metadata"]["collections"]["supply"]["by_kind"]["EXCHANGE_INVENTORY"] == 900
    assert hot_snapshot["capture_metadata"]["collections"]["supply"]["by_kind"]["FUNDAMENTALS"] == 21600
    clock = cutoff + timedelta(seconds=1)
    await service.demote(spot["token_id"], "event")
    assert (
        await MarketService(
            repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
        ).snapshot(spot["token_id"], cutoff)
    )["capture_tier"] == "HOT"
    base_snapshot = await service.snapshot(spot["token_id"], clock)
    assert base_snapshot["capture_tier"] == "BASE"
    base = compute_market_truth(base_snapshot)
    checks = [
        c
        for c in base["data_confidence"]["freshness"]
        if c["instrument_id"] == iid and c["collection"] in ("oi", "funding")
    ]
    assert len(checks) == 2 and all(c["cadence_s"] == 300 and c["fresh"] for c in checks)
    assert (
        base_snapshot["capture_metadata"]["collections"]["supply"]["by_kind"]["EXCHANGE_INVENTORY"] == 21600
    )
    await http.aclose()


def test_verified_exact_event_time_selects_t0() -> None:
    from tank.market.capture import event_anchor

    event = {
        "event_time": {"value": "2026-01-01T00:00:00Z", "precision": "EXACT"},
        "state_verified": True,
        "public_at": "2026-01-01T00:01:00Z",
    }
    assert event_anchor(event).to_wire() == {
        "type": "T0",
        "ts": "2026-01-01T00:00:00Z",
        "ts_prec": "EXACT",
        "ts_prec_reason": None,
    }
    event["state_verified"] = False
    assert event_anchor(event).type == "T1"


def test_missing_oi_field_is_data_error_without_runtime_semantic_promotion() -> None:
    from tank.market import normalize

    inst = {
        "venue": "binance",
        "market_type": "PERP_LINEAR",
        "instrument_id": "binance:perp:T",
        "token_id": "T",
    }
    with pytest.raises((KeyError, ValueError)):
        normalize.oi(inst, {"unexpected_open_interest_v2": "123"})


@pytest.mark.asyncio
async def test_capture_wait_is_bounded_and_late_completion_supersedes_timeout(setup) -> None:
    import asyncio

    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    from tank.market.capture import event_anchor

    event = {"event_id": "E-wait", "public_at": clock_iso(), "public_at_prec": "EXACT"}
    await repo.put("evt.events", "E-wait", event)
    run = await service._schedule_event_backfill("E-wait", inst["token_id"], event_anchor(event).timestamp)
    row = await repo.get("mkt.backfill_runs", run.backfill_id)
    row.update(
        status="RUNNING",
        started_at=clock_iso(),
        started_at_prec="COLLECTION_ONLY",
        started_at_prec_reason=None,
    )
    await repo.put("mkt.backfill_runs", run.backfill_id, row)
    waited = await service.wait_capture("E-wait", inst["token_id"], timeout_s=0)
    assert waited.status == "INCOMPLETE" and waited.reason == "BOUNDED_PREREQUISITE_WAIT_EXPIRED"

    async def finish() -> None:
        await asyncio.sleep(0.01)
        row.update(
            status="COMPLETE",
            reason=None,
            finished_at=clock_iso(),
            finished_at_prec="COLLECTION_ONLY",
            finished_at_prec_reason=None,
            result={"instruments": [], "benchmark_klines": []},
        )
        await repo.put("mkt.backfill_runs", row["backfill_id"], row)

    worker = asyncio.create_task(finish())
    done = await service.wait_capture("E-wait", inst["token_id"], timeout_s=1)
    await worker
    assert done.status == "COMPLETE" and done.reason is None
    snapshot = await service.snapshot(inst["token_id"], clock_iso())
    assert snapshot["capture_status"]["status"] == "COMPLETE"
    assert snapshot["capture_status"]["bounded_wait_s"] > 0
    await service.close()


@pytest.mark.asyncio
async def test_hot_expiry_protection_is_scoped_to_event_and_token(setup) -> None:
    from tank.util import iso

    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    with pytest.raises(ValueError, match="missing event"):
        await service.escalate("E-protected", [inst["token_id"]])
    assert await repo.list("mkt.hot_tokens") == []
    await repo.put(
        "evt.events",
        "E-protected",
        {"event_id": "E-protected", "known_to_us_at": clock_iso(), "known_to_us_at_prec": "COLLECTION_ONLY"},
    )
    await repo.put(
        "ana.calls",
        "other",
        {"call_id": "other", "event_id": "OTHER", "token_id": inst["token_id"], "status": "OPEN"},
    )
    await service.escalate("E-protected", [inst["token_id"]])
    key = inst["token_id"] + ":E-protected"
    assert (await repo.get("mkt.hot_tokens", key))["expires_at"] is not None
    await repo.put(
        "ana.calls",
        "own",
        {"call_id": "own", "event_id": "E-protected", "token_id": inst["token_id"], "status": "OPEN"},
    )
    await service.escalate("E-protected", [inst["token_id"]])
    assert (await repo.get("mkt.hot_tokens", key))["expires_at"] is None
    await repo.put(
        "ana.calls",
        "own",
        {"call_id": "own", "event_id": "E-protected", "token_id": inst["token_id"], "status": "CLOSED"},
    )
    await repo.put(
        "ana.watch_conditions",
        "armed",
        {
            "cond_id": "armed",
            "event_id": "E-protected",
            "token_id": inst["token_id"],
            "status": "ARMED",
            "expires_at": iso(utcnow() + timedelta(hours=1)),
        },
    )
    await service.escalate("E-protected", [inst["token_id"]])
    assert (await repo.get("mkt.hot_tokens", key))["expires_at"] is None
    condition = await repo.get("ana.watch_conditions", "armed")
    condition["expires_at"] = iso(utcnow() - timedelta(seconds=1))
    await repo.put("ana.watch_conditions", "armed", condition)
    await service.refresh_hot_expiry()
    restored = await repo.get("mkt.hot_tokens", key)
    from tank.util import iso, parse_time

    assert restored["expires_at"] == iso(parse_time(restored["since"]) + timedelta(hours=72))
    await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,url,kwargs",
    [
        ("GET", "https://api.binance.com/api/v3/aggTrades", {"params": {"symbol": "BTCUSDT"}}),
        (
            "POST",
            "https://api.hyperliquid.xyz/info",
            {"json": {"type": "fundingHistory", "coin": "BTC", "startTime": 0}},
        ),
        ("GET", "https://api.binance.com/api/v3/depth", {"params": {"limit": 1000}, "_priority": 2}),
    ],
)
async def test_exchange_rest_is_rejected_before_python_http(setup, method, url, kwargs) -> None:
    repo, settings, _ = setup
    requests = []

    def record(request: httpx.Request):
        requests.append(request)
        return httpx.Response(200, json=[])

    async with httpx.AsyncClient(transport=httpx.MockTransport(record)) as client:
        service = MarketService(
            repo, settings, http=client, versions=fixture_versions(), admission=fixture_http_admission()
        )
        with pytest.raises(RuntimeError, match="exchange REST belongs to tank-capture"):
            await service._request(method, url, **kwargs)
    assert requests == []


@pytest.mark.asyncio
async def test_capture_changes_are_durable_bounded_and_stable(setup) -> None:
    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    for key, status, finished in [
        ("a", "COMPLETE", "2026-01-01T01:00:00Z"),
        ("b", "PARTIAL", "2026-01-01T02:00:00Z"),
        ("c", "RUNNING", None),
    ]:
        await repo.put(
            "mkt.backfill_runs",
            key,
            {
                "backfill_id": key,
                "event_id": "E-change",
                "token_id": inst["token_id"],
                "anchor": "2026-01-01T00:00:00Z",
                "started_at": "2026-01-01T00:00:00Z",
                "finished_at": finished,
                "status": status,
            },
        )
    changes = [r async for r in service.capture_changes("2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z")]
    assert len(changes) == 1 and changes[0]["backfill_id"] == "a"
    restarted = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    again = [r async for r in restarted.capture_changes("2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z")]
    assert again[0]["change_id"] == changes[0]["change_id"]
    await service.close()
    await restarted.close()


@pytest.mark.asyncio
async def test_cold_bybit_oi_uses_registry_without_runtime_probe(setup) -> None:
    from tank.analysis.truth import derivatives_truth
    from tank.market import normalize as norm

    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    inst = {
        **inst,
        "instrument_id": "bybit:perp_linear:ABCUSD",
        "venue": "bybit",
        "market_type": "PERP_LINEAR",
    }
    key = "bybit.linear.open-interest"
    service.semantics = [
        {
            "metric_key": key,
            "metric_family": "oi",
            "timestamp_semantics": "EXCHANGE_EVENT_TIME",
            "unit": "BASE_QTY",
            "sidedness": "SINGLE_SIDED",
            "aggregation_class": "oi_native_single_sided",
        }
    ]
    end = utcnow().replace(second=0, microsecond=0)
    start = end - timedelta(hours=4)
    endpoints = [
        norm.oi(inst, {"singleOpenInterest": "123.45", "timestamp": str(int(t.timestamp() * 1000))})
        for t in (start, end)
    ]
    definitions = service._instrument_semantics([inst])
    actual = derivatives_truth(
        {"token_id": inst["token_id"], "instruments": [inst], "oi": endpoints, "semantics": definitions},
        start,
        end,
    )
    assert actual["synchronized"] is not None
    assert actual["per_venue"][0]["oi_native_chg_pct"] == 0
    assert all(row["ts_prec"] == "EXACT" and row["synchronized_eligible"] for row in endpoints)
    assert not hasattr(service, "self_test")
    assert not await repo.list("qa.reviews", filters={"subject": "METRIC_SEMANTICS"})
    await service.close()


@pytest.mark.asyncio
async def test_bybit_legacy_temporal_proof_not_reused_by_condition_loader(setup) -> None:
    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    inst = {**inst, "venue": "bybit", "market_type": "PERP_LINEAR"}
    service.instruments = {inst["instrument_id"]: inst}
    key = "bybit.linear.open-interest"
    definition = {
        "metric_key": key,
        "timestamp_semantics": "EXCHANGE_EVENT_TIME",
    }
    service.semantics = [definition]
    await repo.put(
        "qa.reviews",
        "old-bybit-proof",
        {
            "subject": "METRIC_SEMANTICS",
            "instrument_id": inst["instrument_id"],
            "metric_key": key,
            "at": utcnow().isoformat(),
            "semantics": {
                **definition,
                "timestamp_semantics": "BUCKET_START",
                "semantic_confidence": "VERIFIED",
            },
        },
    )
    snapshot = await service.condition_snapshot(
        inst["token_id"], utcnow(), requirements=[{"metric": "deriv.native_oi_chg_pct", "window": "W4H"}]
    )
    assert len(snapshot["semantics"]) == 1
    assert "semantic_confidence" not in snapshot["semantics"][0]
    assert snapshot["semantics"][0]["timestamp_semantics"] == "EXCHANGE_EVENT_TIME"
    assert await repo.get("qa.reviews", "old-bybit-proof")
    await service.close()


@pytest.mark.asyncio
async def test_series_counts_survive_an_import_evicting_a_counted_day(
    setup, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tank.market import service as service_module

    repo, settings, instrument = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    iid = instrument["instrument_id"]
    cutoff = utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    start = cutoff - timedelta(days=3, hours=1)

    async def day_counts(repo, ids, lo, hi, assembled):
        return {(iid, lo + timedelta(days=n)): 1440 for n in range((hi - lo).days)}

    async def edge_counts(ids, lo, hi, assembled):
        service._kline_day_counts.clear()
        return {}

    monkeypatch.setattr(service_module, "market_series_day_counts", day_counts)
    monkeypatch.setattr(repo, "market_series_counts", edge_counts)
    assert await service._series_counts([iid], start, cutoff, clock_iso()) == {iid: 2 * 1440}
    await service.close()


@pytest.mark.asyncio
async def test_snapshot_keeps_book_levels_only_on_the_newest_rows(setup) -> None:
    """Slippage walks only the newest integrity-checked book; older rows drop their levels."""
    from tank.market.service import BOOK_LEVEL_ROWS
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    start = parse_time("2026-06-17T00:00:00Z")
    count = BOOK_LEVEL_ROWS + 10
    levels = [["2", "1"], ["3", "1"]]
    for i in range(count):
        await repo.put(
            "mkt.book_summaries",
            f"book{i:03d}",
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "exchange_ts": iso(start + timedelta(seconds=10 * i)),
                "integrity_ok": i <= 5,
                "mid": "2",
                "spread_bps": "1",
                "depth": {"bid": {"p1": {"usd": "4"}}, "ask": {"p1": {"usd": "6"}}},
                "bids": levels,
                "asks": levels,
            },
        )
    snapshot = await service.snapshot(inst["token_id"], iso(start + timedelta(seconds=10 * count)))
    books = snapshot["books"]
    assert len(books) == count
    assert [i for i, b in enumerate(books) if "asks" in b] == [5, *range(10, count)]
    assert all("bids" not in b for i, b in enumerate(books) if i not in [5, *range(10, count)])
    assert all(b["depth"]["ask"]["p1"]["usd"] == "6" and b["mid"] == "2" for b in books)
    assert books[5]["asks"] == levels
    await http.aclose()


@pytest.mark.asyncio
async def test_bucketed_oi_cadence_is_bounded_by_its_source(setup) -> None:
    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    await service._record_capture_tier(inst["token_id"], "HOT", "TEST")
    oi = (await service._capture_metadata(inst["token_id"], utcnow()))["collections"]["oi"]
    assert oi["cadence_s"] == 60.0
    assert oi["cadence_s_by_metric"]["bybit.linear.open-interest"] == 300.0
    assert oi["cadence_basis_by_metric"]["bybit.linear.open-interest"] == "SOURCE_BUCKET_300S"
    await service.close()


@pytest.mark.asyncio
async def test_condition_heads_are_reused_until_their_scope_is_written(
    setup, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tank.market import service as service_module
    from tank.util import iso

    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    iid = inst["instrument_id"]
    now = utcnow().replace(microsecond=0)
    row = {
        "instrument_id": iid,
        "token_id": "test:ABC",
        "metric_key": "binance.usdm.openInterest",
        "ts": iso(now - timedelta(minutes=5)),
        "oi_native": "1",
        "recorded_at": iso(now - timedelta(minutes=4)),
    }
    await repo.put_market_batch("mkt.oi_obs", [row])
    calls = []
    real = service_module.market_dependency_head

    async def counted(*args, **kwargs):
        calls.append(kwargs.get("orders"))
        return await real(*args, **kwargs)

    monkeypatch.setattr(service_module, "market_dependency_head", counted)
    window = ("mkt.oi_obs", "ts", now - timedelta(days=1), now)
    assembled = utcnow()
    first = await service._condition_heads(*window, iso(assembled), {"instrument_id": iid})
    again = await service._condition_heads(
        *window, iso(assembled + timedelta(seconds=30)), {"instrument_id": iid}
    )
    assert (
        first == again
        and len(calls) == 1
        and calls[0] == ("availability", "semantic_latest", "semantic_earliest")
    )
    singles = [
        await real(
            repo,
            "mkt.oi_obs",
            "ts",
            now,
            start=now - timedelta(days=1),
            filters={"instrument_id": iid},
            assembled_at=iso(assembled),
            order=o,
        )
        for o in ("availability", "semantic_latest", "semantic_earliest")
    ]
    assert first == singles and first[0]["storage_key"]
    await repo.put_market_batch(
        "mkt.oi_obs", [dict(row, ts=iso(now - timedelta(minutes=1)), recorded_at=iso(utcnow()))]
    )
    later = await service._condition_heads(
        *window, iso(utcnow() + timedelta(seconds=1)), {"instrument_id": iid}
    )
    assert len(calls) == 2 and later[1]["ts"] == iso(now - timedelta(minutes=1))
    await service.close()


async def test_snapshot_anchors_yield_widest_first_and_equal_single_snapshots(setup) -> None:
    """PRE windows of one token and cutoff share their reads; each snapshot equals its own call."""
    from tank.market.service import BOOK_LEVEL_ROWS
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    cutoff = parse_time("2026-06-17T12:00:00Z")
    levels = [["2", "1"], ["3", "1"]]
    for i in range(BOOK_LEVEL_ROWS + 300):
        await repo.put(
            "mkt.book_summaries",
            f"book{i:04d}",
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "exchange_ts": iso(cutoff - timedelta(minutes=10 * i)),
                "integrity_ok": i % 7 == 3,
                "mid": "2",
                "bids": levels,
                "asks": levels,
            },
        )
    anchors = [{"type": "T1", "ts": iso(cutoff - timedelta(hours=h)), "ts_prec": "EXACT"} for h in (1, 30, 4)]

    def stable(snapshot):
        out = {k: v for k, v in snapshot.items() if not k.startswith("assembled_at")}
        out["capture_status"] = {k: v for k, v in out["capture_status"].items() if k != "observed_at"}
        return out

    shared = [
        (a, stable(s)) async for a, s in service.snapshot_anchors(inst["token_id"], iso(cutoff), anchors)
    ]

    assert [a["ts"] for a, _ in shared] == [anchors[1]["ts"], anchors[0]["ts"], anchors[2]["ts"]]
    for anchor, snapshot in shared:
        single = stable(await service.snapshot(inst["token_id"], iso(cutoff), anchor=anchor))
        assert snapshot == single
        assert sum("asks" in b for b in snapshot["books"]) <= BOOK_LEVEL_ROWS + 1
    assert len(shared[0][1]["books"]) > len(shared[-1][1]["books"])
    await http.aclose()


@pytest.mark.asyncio
async def test_snapshot_never_queues_behind_a_running_flush(setup) -> None:
    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    flushes = []
    real = service.flush_market

    async def counted(**kwargs):
        flushes.append(kwargs)
        return await real(**kwargs)

    service.flush_market = counted

    async with service._market_flush_lock:
        locked = await asyncio.wait_for(service.snapshot(inst["token_id"], utcnow()), 1)
    assert flushes == []
    unlocked = await service.snapshot(inst["token_id"], utcnow())
    assert flushes == []
    assert locked["token_id"] == unlocked["token_id"] == inst["token_id"]
    await service.close()


@pytest.mark.asyncio
async def test_loop_watchdog_logs_the_stack_that_holds_the_loop(
    setup, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import time as clock

    from tank.market import service as service_module

    repo, settings, _ = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    service._stop = asyncio.Event()
    monkeypatch.setattr(service_module, "LOOP_STALL_S", 1)
    watchdog = asyncio.create_task(service._loop_watchdog())
    await asyncio.sleep(0.1)

    def hogging_the_loop() -> None:
        clock.sleep(2.5)

    with caplog.at_level("WARNING", logger="tank.market.service"):
        hogging_the_loop()
        await asyncio.sleep(0.1)
    service._stop.set()
    await watchdog
    stalls = [r.getMessage() for r in caplog.records if "event loop stalled" in r.getMessage()]
    assert any("hogging_the_loop" in stall for stall in stalls)
    await service.close()


@pytest.mark.asyncio
async def test_reaction_snapshot_reads_books_and_tickers_only_at_the_window_bounds(
    setup, monkeypatch: pytest.MonkeyPatch
) -> None:
    """r110: one BTC reaction capture read ~1M rows (days of 10 s books and tickers). A REACTION
    snapshot reads books/tickers only around its window bounds and the rest of the window
    collections from the earlier of the anchor and cutoff - 24 h, never 2 x the window."""
    import tank.market.service as service_module
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    cutoff = parse_time("2026-06-17T12:00:00Z")
    anchor = cutoff - timedelta(hours=72)
    for i in range(0, 7 * 24 * 60, 20):
        ts = iso(cutoff - timedelta(minutes=i))
        await repo.put(
            "mkt.book_summaries",
            f"b{i}",
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "exchange_ts": ts,
                "integrity_ok": True,
                "mid": "2",
            },
        )
        await repo.put(
            "mkt.tickers_24h",
            f"t{i}",
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "ts": ts,
                "mark_price": "2",
            },
        )
    reads = []
    original = service_module.market_range_by_instrument

    async def spy(repo_, collection, field, ids, start, end, *args, **kwargs):
        reads.append((collection, parse_time(start), parse_time(end)))
        return await original(repo_, collection, field, ids, start, end, *args, **kwargs)

    monkeypatch.setattr(service_module, "market_range_by_instrument", spy)
    snapshot = await service.snapshot(
        inst["token_id"], iso(cutoff), anchor={"type": "T1", "ts": iso(anchor)}, scope="REACTION"
    )
    near = [
        (anchor - timedelta(minutes=5), anchor + timedelta(minutes=5)),
        (cutoff - timedelta(minutes=15), cutoff + timedelta(seconds=1)),
    ]

    def inside(t):
        return any(low <= t <= high for low, high in near)

    for collection, start, end in reads:
        if collection in ("mkt.book_summaries", "mkt.tickers_24h"):
            assert inside(start) and inside(end), (collection, start, end)
        elif collection != "mkt.funding_obs":
            assert start >= anchor - timedelta(minutes=1), (collection, start)
    assert snapshot["books"] and all(inside(parse_time(b["exchange_ts"])) for b in snapshot["books"])
    assert snapshot["tickers"] and all(inside(parse_time(t["ts"])) for t in snapshot["tickers"])
    full = await service.snapshot(inst["token_id"], iso(cutoff), anchor={"type": "T1", "ts": iso(anchor)})
    assert len(full["books"]) > 10 * len(snapshot["books"])
    await http.aclose()


async def test_market_truth_snapshot_reads_one_day_of_books_and_tickers_at_window_bounds(
    setup, monkeypatch: pytest.MonkeyPatch
) -> None:
    """E2E #5: the default snapshot read two days of books and tickers. Market Truth needs books
    over the last 24 h and the hour before the anchor, and tickers only around the window
    bounds (latest at or before each bound, marks next to the OI points)."""
    import tank.market.service as service_module
    from tank.util import iso, parse_time

    repo, settings, inst = setup
    http = httpx.AsyncClient()
    service = MarketService(
        repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
    )
    cutoff = parse_time("2026-06-17T12:00:00Z")
    anchor = cutoff - timedelta(minutes=30)
    for i in range(0, 3 * 24 * 60, 10):
        ts = iso(cutoff - timedelta(minutes=i))
        await repo.put(
            "mkt.book_summaries",
            f"b{i}",
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "exchange_ts": ts,
                "integrity_ok": True,
            },
        )
        await repo.put(
            "mkt.tickers_24h",
            f"t{i}",
            {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "ts": ts,
                "mark_price": "2",
            },
        )
    reads = []
    original = service_module.market_range_by_instrument

    async def spy(repo_, collection, field, ids, start, end, *args, **kwargs):
        reads.append((collection, parse_time(start), parse_time(end)))
        return await original(repo_, collection, field, ids, start, end, *args, **kwargs)

    monkeypatch.setattr(service_module, "market_range_by_instrument", spy)
    snapshot = await service.snapshot(inst["token_id"], iso(cutoff), anchor={"type": "T1", "ts": iso(anchor)})
    edges = [cutoff - timedelta(seconds=s) for s in (0, 900, 3600, 14400, 86400)] + [anchor]
    book_reads = [(a, z) for c, a, z in reads if c == "mkt.book_summaries"]
    assert book_reads and min(a for a, _ in book_reads) >= cutoff - timedelta(days=1, seconds=11)

    def near_edge(t):
        return any(
            edge - timedelta(minutes=15) <= t <= edge + timedelta(minutes=2, seconds=1) for edge in edges
        )

    ticker_reads = [(a, z) for c, a, z in reads if c == "mkt.tickers_24h"]
    assert ticker_reads and all(near_edge(a) and near_edge(z) for a, z in ticker_reads)
    assert sum((z - a).total_seconds() for a, z in ticker_reads) < 6 * 17 * 60
    assert all(
        cutoff - timedelta(days=1, seconds=11) <= parse_time(b["exchange_ts"]) for b in snapshot["books"]
    )
    assert all(
        any(
            edge - timedelta(minutes=15) <= parse_time(t["ts"]) <= edge + timedelta(minutes=2)
            for edge in edges
        )
        for t in snapshot["tickers"]
    )
    await http.aclose()


@pytest.mark.asyncio
async def test_past_day_kline_counts_survive_a_restart_unless_their_day_was_rewritten(
    setup, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tank.market import service as service_module

    repo, settings, inst = setup
    iid = inst["instrument_id"]
    today = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    earlier = iso(utcnow() - timedelta(hours=2))

    def kline(opened, recorded_at):
        return {
            "instrument_id": iid,
            "token_id": inst["token_id"],
            "open_time": iso(opened),
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "complete": True,
            "c": "10",
            "volume_native": "1",
            "recorded_at": recorded_at,
        }

    opens = [today - timedelta(days=day) + timedelta(minutes=m) for day in (3, 2, 1) for m in range(5)]
    await repo.put_market_batch(
        "mkt.klines_1m", [kline(at, earlier) for at in opens], keys=[iid + ":" + iso(at) for at in opens]
    )
    counted = []
    original = service_module.market_series_day_counts

    async def counting(repo, ids, start, end, assembled_at=None):
        counted.append((start, end))
        return await original(repo, ids, start, end, assembled_at)

    monkeypatch.setattr(service_module, "market_series_day_counts", counting)
    start = today - timedelta(days=3)
    first = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    assert (await first._series_counts([iid], start, utcnow(), clock_iso()))[iid] == 15
    assert counted == [(start, today)]
    await first._save_day_counts(force=True)

    rewritten = today - timedelta(days=2) + timedelta(minutes=10)
    await repo.put_market_batch(
        "mkt.klines_1m", [kline(rewritten, clock_iso())], keys=[iid + ":" + iso(rewritten)]
    )
    restarted = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    assert await restarted._load_day_counts() == 2
    counted.clear()
    assert (await restarted._series_counts([iid], start, utcnow(), clock_iso()))[iid] == 16
    assert counted == [(today - timedelta(days=2), today - timedelta(days=1))]

    checked = utcnow()
    appended = today - timedelta(days=1) + timedelta(minutes=10)
    await repo.put_market_batch(
        "mkt.klines_1m", [kline(appended, clock_iso())], keys=[iid + ":" + iso(appended)]
    )
    await restarted._evict_recorded_days(checked)
    assert (iid, today - timedelta(days=1)) not in restarted._kline_day_counts
    assert (await restarted._series_counts([iid], start, utcnow(), clock_iso()))[iid] == 17


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", ["mkt.flow_bars_1m", "mkt.book_summaries"])
async def test_window_cache_reads_equal_fresh_reads_through_appends_rewrites_and_late_rows(
    setup, collection
) -> None:
    from tank.market import service as service_module

    repo, settings, inst = setup
    service = MarketService(repo, settings, versions=fixture_versions(), admission=fixture_http_admission())
    books = collection == "mkt.book_summaries"
    field = "exchange_ts" if books else "minute_start"
    base = (utcnow() - timedelta(hours=3)).replace(second=0, microsecond=0)

    def row(minute, value, recorded_at=None):
        point = {
            "instrument_id": inst["instrument_id"],
            "token_id": inst["token_id"],
            field: iso(base + timedelta(minutes=minute)),
            "recorded_at": clock_iso() if recorded_at is None else recorded_at,
        }
        if books:
            point.update(mid=value, integrity_ok=minute % 3 == 0, bids=[[value, "1"]], asks=[[value, "2"]])
        else:
            point.update(buy_qty=value, sell_qty="0", complete=True)
        return point

    async def put(rows) -> None:
        await repo.put_market_batch(collection, rows, keys=[iso_key(r) for r in rows])

    def iso_key(point):
        return point["instrument_id"] + ":" + point[field]

    async def both(start, end):
        cached = await service._ranged(collection, field, [inst["instrument_id"]], start, end, books=books)
        fresh = await service._ranged_read(
            collection, field, [inst["instrument_id"]], start, end, books=books
        )
        return cached, fresh

    await put([row(m, str(m)) for m in range(0, 120, 2)])
    start, end = base, base + timedelta(minutes=100)
    cached, fresh = await both(start, end)
    assert cached == fresh
    reads = []
    original = service_module.market_range_by_instrument

    async def spy(repo_, collection_, field_, ids, start_, end_, *args, **kwargs):
        reads.append((parse_time(start_), parse_time(end_)))
        return await original(repo_, collection_, field_, ids, start_, end_, *args, **kwargs)

    service_module.market_range_by_instrument = spy
    try:
        await put([row(10, "999"), row(11, "777")])
        cached = await service._ranged(
            collection,
            field,
            [inst["instrument_id"]],
            start + timedelta(minutes=5),
            end + timedelta(minutes=10),
            books=books,
        )
        assert reads == [(end, end + timedelta(minutes=10))]
    finally:
        service_module.market_range_by_instrument = original
    fresh = await service._ranged_read(
        collection,
        field,
        [inst["instrument_id"]],
        start + timedelta(minutes=5),
        end + timedelta(minutes=10),
        books=books,
    )
    assert cached == fresh
    held = cached[inst["instrument_id"]]
    assert any(r.get("mid", r.get("buy_qty")) == "999" for r in held)
    assert any(r[field] == iso(base + timedelta(minutes=11)) for r in held)
    if books:
        assert sum("bids" in r for r in held) <= service_module.BOOK_LEVEL_ROWS + 1
    await service.close()


def test_window_cache_is_bounded_by_age_and_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from tank.market import service as service_module

    clock = [1000.0]
    monkeypatch.setattr(service_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(service_module, "WINDOW_ROWS", 100)

    def lane(used, size):
        return {"used": used, "size": size, "rows": {}}

    windows = {
        ("old",): [lane(1000.0 - service_module.WINDOW_TTL_S - 1, 10)],
        ("a",): [lane(990.0, 60), lane(995.0, 30)],
        ("b",): [lane(999.0, 40)],
    }
    service_module._bound_windows(windows)
    assert ("old",) not in windows

    assert [w["used"] for w in windows[("a",)]] == [995.0] and windows[("b",)][0]["size"] == 40


@pytest.mark.asyncio
async def test_generic_market_write_cannot_bypass_capture_control_contract(setup) -> None:
    repo, settings, _inst = setup
    async with httpx.AsyncClient() as http:
        service = MarketService(
            repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
        )
        with pytest.raises(ValueError, match="validated capture repository"):
            await service._put("mkt.backfill_runs", "BF-invalid", {})
    assert await repo.get("mkt.backfill_runs", "BF-invalid") is None
