"""Regressions from authenticated Etherscan coverage probes, using synthetic responses."""

import httpx
import pytest
import yaml

from tank.market.service import MarketService
from tank.market.supply import SupplyCapture
from tank.settings import Settings
from tank.storage import MemoryRepository


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
