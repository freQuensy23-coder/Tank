"""Regressions from authenticated Etherscan coverage probes, using synthetic responses."""

import os
import uuid
from decimal import Decimal
from urllib.parse import urlsplit, urlunsplit

import httpx
import pytest
import yaml

from tank.market.service import MarketService
from tank.market.supply import SupplyCapture
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository


def lending_market(tmp_path, transport, *, catalog=True, repo=None, tokens=None):
    token = {
        "token_id": "test:T",
        "contracts": [{"chain": "Base", "address": "0x" + "1" * 40}],
    }
    identities = {
        "source_url": "https://yields.llama.fi/pools",
        "captured_at": "2026-10-20T00:00:00+00:00",
        "source_sha256": "public-fixture",
        "pools": [
            {
                "pool_id": iid,
                "project": project,
                "chain": chain,
                "underlying_tokens": ["0x" + "1" * 40],
            }
            for iid, project, chain in [
                ("allowed", "morpho-blue", "Base"),
                ("other-chain", "aave-v3", "Ethereum"),
                ("other-project", "compound-v3", "Base"),
                ("changed-identity", "euler-v2", "Base"),
            ]
        ],
    }
    config = tmp_path / "config"
    config.mkdir()
    for name, document in {
        "registry": {
            "tokens": tokens or [token],
            "instruments": [],
            **({"lending_pool_catalog": identities} if catalog else {}),
        },
        "semantics": {"metrics": []},
    }.items():
        (config / (name + ".yaml")).write_text(yaml.safe_dump(document))
    settings = Settings(_env_file=None, config_dir=config, data_dir=tmp_path / "data")
    return token, MarketService(
        repo or MemoryRepository(), settings, http=httpx.AsyncClient(transport=httpx.MockTransport(transport))
    )


async def test_lending_runtime_uses_only_free_endpoint_and_exact_registered_identities(tmp_path):
    calls = []

    def respond(request):
        calls.append(str(request.url))
        assert str(request.url) == "https://yields.llama.fi/lendBorrow"
        return httpx.Response(
            200,
            json=[
                {
                    "pool": iid,
                    "underlyingTokens": ["0x" + ("2" if iid == "changed-identity" else "1") * 40],
                    "totalSupplyUsd": "100000000000000000000.00000000000000001",
                    "totalBorrowUsd": "30000000000000000000.000000000000000003",
                    "apyBaseBorrow": "2.125",
                }
                for iid in ("allowed", "other-chain", "other-project", "changed-identity", "new-unmapped")
            ],
        )

    token, market = lending_market(tmp_path, respond)
    capture = SupplyCapture(market)
    try:
        result = await capture.lending(token)
        cached = await capture.lending(dict(token, token_id="second:T"))
        assert result["available"] is True
        assert result["pools"] == [
            {
                "pool_id": "allowed",
                "project": "morpho-blue",
                "chain": "Base",
                "utilization": Decimal("0.3"),
                "borrow_apy": "2.125",
                "total_supply_usd": "100000000000000000000.00000000000000001",
                "total_borrow_usd": "30000000000000000000.000000000000000003",
            }
        ]
        assert cached["pools"] == result["pools"]
        assert result["identity_evidence"]["unmapped_pool_ids"] == ["new-unmapped"]
        assert result["identity_evidence"]["pool_identity_source"]["source_sha256"] == "public-fixture"
        from tank.market_storage import encode

        encode("mkt.supply_obs", result)
        assert len(calls) == 1
    finally:
        await market.http.aclose()


async def test_lending_unknown_identity_remains_explicitly_unavailable_without_catalog(tmp_path):
    calls = []

    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(
            200,
            json=[
                {
                    "pool": "unknown",
                    "underlyingTokens": ["0x" + "1" * 40],
                    "totalSupplyUsd": 100,
                    "totalBorrowUsd": 50,
                }
            ],
        )

    token, market = lending_market(tmp_path, respond, catalog=False)
    try:
        result = await SupplyCapture(market).lending(token)
        assert result["available"] is False
        assert result["pools"] == []
        assert result["reason"] == "NO_MATCHED_LENDING_POOLS"
        assert result["identity_evidence"]["unmapped_pool_ids"] == ["unknown"]
        from tank.market_storage import encode

        encode("mkt.supply_obs", result)
        assert calls == ["https://yields.llama.fi/lendBorrow"]
    finally:
        await market.http.aclose()


async def test_lending_invalid_free_endpoint_schema_is_not_cached(tmp_path):
    from tank.market.normalize import SemanticError

    token, market = lending_market(tmp_path, lambda _: httpx.Response(200, json={"data": []}))
    try:
        with pytest.raises(SemanticError, match="lendBorrow schema absent"):
            await SupplyCapture(market).lending(token)
        assert getattr(market, "_lend_cache", None) is None
    finally:
        await market.http.aclose()


@pytest.fixture(params=["memory", "postgres"])
async def lending_repository(request):
    if request.param == "memory":
        yield await MemoryRepository().open()
        return
    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL; creates and drops its own lending proof database")
    import psycopg
    from psycopg import sql

    name = "tank_lending_boundary_" + uuid.uuid4().hex[:12]
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    repo = None
    created = False
    try:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        created = True
        target = urlunsplit(urlsplit(dsn)._replace(path="/" + name))
        repo = await PostgresRepository(target).open()
        await repo.migrate()
        yield repo
    finally:
        if repo is not None:
            await repo.close()
        if created:
            await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def test_lending_collect_flush_typed_storage_and_22_token_cache(tmp_path, lending_repository):
    calls = []

    def respond(request):
        calls.append(str(request.url))
        assert str(request.url) == "https://yields.llama.fi/lendBorrow"
        return httpx.Response(
            200,
            json=[
                {
                    "pool": pool_id,
                    "underlyingTokens": ["0x" + digit * 40],
                    "totalSupplyUsd": "10.000000000000000001",
                    "totalBorrowUsd": "3.0000000000000000003",
                    "apyBaseBorrow": "2.125",
                }
                for pool_id, digit in [("allowed", "1"), ("unregistered", "2")]
            ],
        )

    tokens = [
        {
            "token_id": f"token:{index}",
            "contracts": [{"chain": "Base", "address": "0x" + str(index + 1) * 40}] if index < 2 else [],
        }
        for index in range(22)
    ]
    _, market = lending_market(tmp_path, respond, repo=lending_repository, tokens=tokens)
    try:
        capture = SupplyCapture(market)
        for token in market.tokens.values():
            await capture.lending(token)
        assert len(market._pending_market["mkt.supply_obs"]) == 22
        await market.flush_market()
        assert not market._pending_market
        rows = await lending_repository.list("mkt.supply_obs", filters={"kind": "LENDING"}, limit=100)
        assert len(rows) == 22
        by_token = {row["token_id"]: row for row in rows}
        measured = by_token["token:0"]
        assert measured["available"] is True
        assert measured["pools"][0]["utilization"] == "0.3"
        assert measured["pools"][0]["total_supply_usd"] == "10.000000000000000001"
        assert measured["identity_evidence"]["unmapped_pool_ids"] == []
        unknown = by_token["token:1"]
        assert unknown["available"] is False and unknown["pools"] == []
        assert unknown["reason"] == "NO_MATCHED_LENDING_POOLS"
        assert unknown["identity_evidence"]["unmapped_pool_ids"] == ["unregistered"]
        assert all(
            row["identity_evidence"]["pool_identity_source"]["source_sha256"] == "public-fixture"
            for row in rows
        )
        assert all("unmapped_pool_ids" not in row and "pool_identity_source" not in row for row in rows)
        assert calls == ["https://yields.llama.fi/lendBorrow"]
    finally:
        await market.http.aclose()


@pytest.mark.parametrize("failure", ["plan_restriction", "invalid_decimals", "transport"])
async def test_contract_failure_preserves_other_chains_and_uses_registry_chain_id(tmp_path, failure):
    token = {
        "token_id": "test:T",
        "contracts": [
            {"chain": "Ethereum", "chain_id": 1, "address": "0x" + "1" * 40},
            {"chain": "Base", "chain_id": 8453, "address": "0x" + "2" * 40},
            {"chain": "World Chain", "chain_id": 480, "address": "0x" + "3" * 40},
        ],
    }
    config = tmp_path / "config"
    config.mkdir()
    for name, document in {
        "registry": {"tokens": [token], "instruments": []},
        "semantics": {"metrics": []},
        "exchange_addresses": {
            "wallet_lists": [{"venue": "test", "commit": "synthetic", "addresses": ["0x" + "4" * 40]}]
        },
    }.items():
        (config / (name + ".yaml")).write_text(yaml.safe_dump(document))
    settings = Settings(
        _env_file=None, config_dir=config, data_dir=tmp_path / "data", etherscan_api_key="FAKE-only"
    )
    calls = []

    def respond(request):
        chain, action = int(request.url.params["chainid"]), request.url.params["action"]
        calls.append((chain, action))
        if chain == 8453:
            if failure == "transport":
                raise httpx.ConnectError("synthetic unavailable", request=request)
            if failure == "invalid_decimals":
                return httpx.Response(200, json={"result": "0x"})
            return httpx.Response(
                200,
                json={
                    "status": "0",
                    "message": "NOTOK",
                    "result": "Free API access is not supported for this chain",
                },
            )
        if action == "eth_call":
            return httpx.Response(200, json={"result": "0x06"})
        if action == "tokenbalance":
            return httpx.Response(200, json={"status": "1", "result": "2000000"})
        return httpx.Response(200, json={"status": "0", "message": "No transactions found", "result": []})

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        market = MarketService(repo, settings, http=http)
        result = await SupplyCapture(market).inventory(token)
        await market.close()
    assert result["available"] is True
    assert [(b["chainid"], b["qty_native"]) for b in result["balances"]] == [(1, "2"), (480, "2")]
    assert len(result["coverage_errors"]) == 1
    assert result["coverage_errors"][0]["chainid"] == 8453
    assert (480, "eth_call") in calls and (480, "tokentx") in calls
    flow = (await repo.list("mkt.supply_obs", filters={"kind": "WALLET_FLOWS"}))[0]
    assert flow["available"] is True and flow["flows"] == []
    assert flow["transfer_queries_succeeded"] == 2


async def test_failed_transfer_reads_do_not_become_observed_zero_flows(tmp_path):
    token = {
        "token_id": "test:T",
        "contracts": [{"chain": "Ethereum", "chain_id": 1, "address": "0x" + "1" * 40, "decimals": 6}],
    }
    config = tmp_path / "config"
    config.mkdir()
    for name, document in {
        "registry": {"tokens": [token], "instruments": []},
        "semantics": {"metrics": []},
        "exchange_addresses": {
            "wallet_lists": [{"venue": "test", "commit": "synthetic", "addresses": ["0x" + "4" * 40]}]
        },
    }.items():
        (config / (name + ".yaml")).write_text(yaml.safe_dump(document))
    settings = Settings(
        _env_file=None, config_dir=config, data_dir=tmp_path / "data", etherscan_api_key="FAKE-only"
    )

    def respond(request):
        if request.url.params["action"] == "tokenbalance":
            return httpx.Response(200, json={"status": "1", "result": "0"})
        return httpx.Response(200, json={"status": "0", "message": "NOTOK", "result": "Query unavailable"})

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        market = MarketService(repo, settings, http=http)
        inventory = await SupplyCapture(market).inventory(token)
        await market.close()
    flow = (await repo.list("mkt.supply_obs", filters={"kind": "WALLET_FLOWS"}))[0]
    assert inventory["available"] is True and inventory["balances"][0]["qty_native"] == "0"
    assert flow["available"] is False and flow["reason"] == "SOURCE_FAILED"
    assert flow["transfer_queries_succeeded"] == 0


async def test_chain_liquidity_maps_defillama_chain_names_and_isolates_chain_gaps(tmp_path):
    paths = []

    def transport(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/Polygon"):
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(
            200, json=[{"date": "1792368000", "totalCirculatingUSD": {"peggedUSD": 7, "peggedEUR": 3}}]
        )

    token, market = lending_market(tmp_path, transport)
    token["contracts"] = [
        {"chain": chain, "address": "0x" + "1" * 40} for chain in ("BNB Chain", "Polygon", "Ethereum")
    ]
    await SupplyCapture(market).chain_liquidity(token)
    await market.flush_market()
    assert paths == [
        "/stablecoincharts/BSC",
        "/stablecoincharts/Ethereum",
        "/stablecoincharts/Polygon",
    ]
    rows = {row["chain"]: row for row in await market.repo.list("mkt.supply_obs", limit=100)}
    assert rows["BNB Chain"]["available"] and rows["BNB Chain"]["series"][0]["supply_usd"] == "10"
    assert rows["Ethereum"]["available"]
    assert rows["Polygon"]["available"] is False and "404" in rows["Polygon"]["reason"]
    await market.http.aclose()


async def test_restart_resumes_supply_cadences_from_stored_observations(tmp_path):
    """A restart must not restart the 22-token cycle (03 §5.6 refresh periods)."""
    calls = []

    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(200, json=[])

    token = {"token_id": "test:T", "contracts": [], "fundamentals_status": "NOT_SUPPORTED"}
    token, first = lending_market(tmp_path, respond, tokens=[token])
    await SupplyCapture(first).poll(set())
    await first.flush_market()
    assert calls == ["https://yields.llama.fi/lendBorrow"]
    stored = {r["kind"] for r in await first.repo.list("mkt.supply_obs", limit=100)}
    assert {"UNLOCKS", "EXCHANGE_INVENTORY", "LENDING", "CHAIN_LIQUIDITY", "FUNDAMENTALS"} <= stored
    restarted = MarketService(first.repo, first.settings, http=first.http)
    capture = SupplyCapture(restarted)
    await capture.poll(set())
    assert calls == ["https://yields.llama.fi/lendBorrow"]
    assert set(restarted._supply_refresh) == {
        ("test:T", kind) for kind in ("unlocks", "inventory", "lending", "chain_liquidity", "fundamentals")
    }
    await first.http.aclose()


def test_supply_kind_is_refreshed_before_its_age_reaches_the_cadence():
    """E2E #9: FUNDAMENTALS observed 22126 s old against its 21600 s cadence, because a kind was
    refreshed only once its age reached the cadence and a pass takes minutes. Refresh at
    0.9 x cadence, or two poll cycles (longest pass + loop sleep) before it, whichever is earlier."""
    from tank.market.supply import refresh_due

    cycle = 120 + 60
    assert refresh_due(19440, 21600, cycle) and not refresh_due(19439, 21600, cycle)
    assert refresh_due(540, 900, cycle) and not refresh_due(539, 900, cycle)
    assert refresh_due(1440, 1800, cycle) and not refresh_due(1439, 1800, cycle)
    assert refresh_due(0, 900, 600)


async def test_poll_refreshes_a_kind_before_its_cadence_elapses(tmp_path):
    from datetime import timedelta

    from tank.util import utcnow

    calls = []

    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(200, json=[])

    token = {"token_id": "test:T", "contracts": [], "fundamentals_status": "NOT_SUPPORTED"}
    token, market = lending_market(tmp_path, respond, tokens=[token])
    capture = SupplyCapture(market)
    await capture.poll(set())
    assert calls == ["https://yields.llama.fi/lendBorrow"]
    now = utcnow()
    market._supply_longest_pass_s = 120.0
    stale = market._supply_refresh["test:T", "lending"] = now - timedelta(seconds=1700)
    before = market._supply_refresh["test:T", "fundamentals"] = now - timedelta(seconds=19000)
    await capture.poll(set())
    assert market._supply_refresh["test:T", "lending"] > stale
    assert market._supply_refresh["test:T", "fundamentals"] == before
    await market.http.aclose()
