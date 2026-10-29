"""The Etherscan daily budget of config/api_limits.yaml is enforced by supply capture (03 §5.6)."""

import logging
from datetime import UTC, datetime, timedelta

import pytest
import yaml

from tank.market.service import MarketService
from tank.market.supply import (
    ETHERSCAN_BUDGET_EXHAUSTED,
    EtherscanBudgetDeferred,
    SupplyCapture,
)
from tank.settings import Settings
from tank.sources import rate_limits
from tank.storage import MemoryRepository
from tank.util import utcnow

WALLET = "0x" + "4" * 40


def evm_token(index):
    return {
        "token_id": f"evm:T{index}",
        "contracts": [{"chain": "Ethereum", "chain_id": 1, "address": f"0x{index:040x}", "decimals": 6}],
        "fundamentals_status": "NOT_SUPPORTED",
    }


@pytest.fixture
def budgets(monkeypatch):
    fresh = rate_limits.SharedRequestBudgets()
    monkeypatch.setattr(rate_limits, "shared_request_budgets", fresh)
    return fresh


def budget_market(tmp_path, tokens, *, per_day):
    config = tmp_path / "config"
    config.mkdir()
    documents = {
        "registry": {"tokens": tokens, "instruments": []},
        "semantics": {"metrics": []},
        "exchange_addresses": {
            "wallet_lists": [{"venue": "test", "commit": "synthetic", "addresses": [WALLET]}]
        },
        "api_limits": {
            "limits": [{"provider": "etherscan", "tank_max_requests_per_day": per_day}]
            if per_day is not None
            else []
        },
    }
    for name, document in documents.items():
        (config / (name + ".yaml")).write_text(yaml.safe_dump(document))
    settings = Settings(
        _env_file=None, config_dir=config, data_dir=tmp_path / "data", etherscan_api_key="FAKE-only"
    )
    market = MarketService(MemoryRepository(), settings)
    calls = []

    async def request(method, url, **kwargs):
        params = kwargs.get("params", {})
        if "etherscan" not in url:
            return []
        calls.append((params.get("contractaddress"), params["action"]))
        if market.etherscan_reply is not None:
            return market.etherscan_reply
        if params["action"] == "tokenbalance":
            return {"status": "1", "result": "1000000"}
        return {"status": "0", "message": "No transactions found", "result": []}

    market._request = request
    market.etherscan_reply = None
    return market, calls


def refreshed_inventory(market):
    return {token_id for (token_id, kind) in market._supply_refresh if kind == "inventory"}


async def inventory_rows(market):
    return {
        row["token_id"]
        for row in await market.repo.list("mkt.supply_obs", limit=1000)
        if row["kind"] in ("EXCHANGE_INVENTORY", "WALLET_FLOWS")
    }


async def test_try_acquire_admits_within_the_window_and_never_waits(budgets):
    assert await budgets.try_acquire("etherscan", 3, 3600, weight=2) == 0
    wait = await budgets.try_acquire("etherscan", 3, 3600, weight=2)
    assert 3599 < wait <= 3600
    assert await budgets.try_acquire("etherscan", 3, 3600, weight=1) == 0
    assert sum(weight for _, weight in budgets.windows["etherscan", 3600.0, 3.0]) == 3
    with pytest.raises(ValueError):
        await budgets.try_acquire("etherscan", 3, 3600, weight=4)


async def test_poll_stops_reserving_before_the_hourly_share_of_the_daily_budget_is_spent(
    tmp_path, budgets, caplog
):
    """Prod 27-28 Oct: 680 calls per token poll at a 15 min cadence spent Etherscan's 100k/day
    by 11:00 UTC. A pass now admits only what fits a 24th of tank_max_requests_per_day."""
    tokens = [evm_token(i) for i in range(3)]
    market, calls = budget_market(tmp_path, tokens, per_day=24 * 5)
    capture = SupplyCapture(market)
    with caplog.at_level(logging.WARNING, logger="tank.market.supply"):
        await capture.poll(set())
    await market.flush_market()
    assert len(calls) == 4
    assert await inventory_rows(market) == {"evm:T0", "evm:T1"}
    assert refreshed_inventory(market) == {"evm:T0", "evm:T1"}
    assert ("evm:T2", "lending") in market._supply_refresh
    assert ETHERSCAN_BUDGET_EXHAUSTED in caplog.text and "evm:T2" in caplog.text

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="tank.market.supply"):
        await capture.poll(set())
    assert len(calls) == 4
    assert caplog.text == ""


async def test_most_overdue_token_relative_to_its_cadence_gets_the_budget_first(tmp_path, budgets):
    """A hot token one hour old (4 cadences of 15 min) outranks a base token due at 6 h."""
    tokens = [evm_token(0), evm_token(1)]
    market, calls = budget_market(tmp_path, tokens, per_day=24 * 2)
    capture = SupplyCapture(market)
    now = utcnow()
    market._supply_refresh_resumed = True
    market._supply_refresh = {
        ("evm:T0", "inventory"): now - timedelta(hours=6),
        ("evm:T1", "inventory"): now - timedelta(hours=1),
    }
    await capture.poll({"evm:T1"})
    assert {contract for contract, _ in calls} == {tokens[1]["contracts"][0]["address"]}
    assert market._supply_refresh["evm:T0", "inventory"] == now - timedelta(hours=6)
    assert market._supply_refresh["evm:T1", "inventory"] > now - timedelta(minutes=1)


async def test_etherscan_daily_limit_stops_calls_until_utc_midnight_and_records_nothing(tmp_path, budgets):
    tokens = [evm_token(0), evm_token(1)]
    market, calls = budget_market(tmp_path, tokens, per_day=None)
    market.etherscan_reply = {
        "status": "0",
        "message": "NOTOK",
        "result": "Max daily rate limit reached. 100000 (100%) of 100000 day/limit",
    }
    capture = SupplyCapture(market)
    await capture.poll(set())
    await capture.poll(set())
    await market.flush_market()
    assert len(calls) == 1
    assert await inventory_rows(market) == set()
    assert refreshed_inventory(market) == set()
    midnight = datetime.combine(utcnow().date() + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
    assert capture.budget.blocked_until == midnight

    capture.budget.blocked_until = utcnow() - timedelta(seconds=1)
    market.etherscan_reply = None
    await capture.poll(set())
    await market.flush_market()
    assert await inventory_rows(market) == {"evm:T0", "evm:T1"}


async def test_extra_transfer_pages_beyond_the_budget_are_partial_coverage_not_overspend(
    tmp_path, budgets, monkeypatch
):
    token = evm_token(0)
    market, calls = budget_market(tmp_path, [token], per_day=24 * 2)
    recent = str(int(utcnow().timestamp()))
    page = [
        {
            "hash": f"0x{i:064x}",
            "logIndex": "0",
            "from": "0x" + "5" * 40,
            "to": WALLET,
            "timeStamp": recent,
            "value": "1000000",
            "tokenDecimal": "6",
        }
        for i in range(100)
    ]

    async def request(method, url, **kwargs):
        calls.append(kwargs["params"]["action"])
        if kwargs["params"]["action"] == "tokenbalance":
            return {"status": "1", "result": "1000000"}
        return {"status": "1", "result": page}

    market._request = request
    capture = SupplyCapture(market)
    result = await capture.inventory(token)
    assert calls == ["tokenbalance", "tokentx"]
    assert result["available"] is True
    assert result["coverage_errors"] == [
        {"wallet": WALLET, "chainid": 1, "reason": ETHERSCAN_BUDGET_EXHAUSTED}
    ]


async def test_decimals_lookup_is_charged_and_deferred_before_any_wallet_call(tmp_path, budgets):
    token = evm_token(0)
    del token["contracts"][0]["decimals"]
    market, calls = budget_market(tmp_path, [token], per_day=24 * 2)
    market.etherscan_reply = {"result": "0x06"}
    capture = SupplyCapture(market)
    with pytest.raises(EtherscanBudgetDeferred) as deferred:
        await capture.inventory(token)
    assert deferred.value.reason == ETHERSCAN_BUDGET_EXHAUSTED
    assert calls == [(None, "eth_call")]
    assert token["contracts"][0]["decimals"] == 6


async def test_each_contract_is_reserved_separately_so_no_call_runs_uncharged(tmp_path, budgets):
    token = evm_token(0)
    token["contracts"].append(
        {"chain": "Arbitrum", "chain_id": 42161, "address": "0x" + "9" * 40, "decimals": 6}
    )
    market, calls = budget_market(tmp_path, [token], per_day=24 * 2)
    result = await SupplyCapture(market).inventory(token)
    assert {contract for contract, _ in calls} == {token["contracts"][0]["address"]}
    assert len(calls) == 2
    assert [b["chainid"] for b in result["balances"]] == [1]
    assert result["coverage_errors"] == [
        {
            "chain": "Arbitrum",
            "chainid": 42161,
            "contract": "0x" + "9" * 40,
            "operation": "reserve",
            "reason": ETHERSCAN_BUDGET_EXHAUSTED,
        }
    ]
    assert sum(weight for _, weight in budgets.windows["api.etherscan.io", 3600.0, 2.0]) == 2


async def test_extra_pages_of_one_wallet_never_take_another_wallets_reserved_calls(tmp_path, budgets):
    token = evm_token(0)
    market, calls = budget_market(tmp_path, [token], per_day=24 * 4)
    other = "0x" + "6" * 40
    (tmp_path / "config" / "exchange_addresses.yaml").write_text(
        yaml.safe_dump(
            {"wallet_lists": [{"venue": "test", "commit": "synthetic", "addresses": [WALLET, other]}]}
        )
    )
    recent = str(int(utcnow().timestamp()))
    full_page = [
        {
            "hash": f"0x{i:064x}",
            "logIndex": "0",
            "from": "0x" + "5" * 40,
            "to": WALLET,
            "timeStamp": recent,
            "value": "1",
            "tokenDecimal": "6",
        }
        for i in range(100)
    ]

    async def request(method, url, **kwargs):
        params = kwargs["params"]
        calls.append((params["address"], params["action"]))
        if params["action"] == "tokenbalance":
            return {"status": "1", "result": "1000000"}
        if params["address"] == WALLET:
            return {"status": "1", "result": full_page}
        return {"status": "0", "message": "No transactions found", "result": []}

    market._request = request
    result = await SupplyCapture(market).inventory(token)
    assert calls == [
        (WALLET, "tokenbalance"),
        (WALLET, "tokentx"),
        (other, "tokenbalance"),
        (other, "tokentx"),
    ]
    assert result["balances"][0]["observed_wallets"] == 2
    assert result["coverage_errors"] == [
        {"wallet": WALLET, "chainid": 1, "reason": ETHERSCAN_BUDGET_EXHAUSTED}
    ]


async def test_an_http_retry_after_429_is_charged_before_it_is_sent(tmp_path, budgets):
    import httpx

    sent = []

    def respond(request):
        sent.append(request.url.path)
        return httpx.Response(429, headers={"Retry-After": "1"})

    market, _ = budget_market(tmp_path, [], per_day=None)
    del market._request
    market.http = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    admitted = []

    async def admit():
        admitted.append(len(sent))
        raise EtherscanBudgetDeferred(ETHERSCAN_BUDGET_EXHAUSTED, 1.0)

    try:
        with pytest.raises(EtherscanBudgetDeferred):
            await market._request("GET", "https://api.etherscan.io/v2/api", _admit_retry=admit)
    finally:
        await market.http.aclose()
    assert sent == ["/v2/api"]
    assert admitted == [1]
