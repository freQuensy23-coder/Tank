"""Supply failure leaves redact credentials without changing retained coverage facts."""

import json
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.contracts.json import decode_json
from tank.market.supply import SupplyCapture
from tank.settings import Settings
from tank.storage import MemoryRepository

WALLET_GOOD = "0x" + "4" * 40
WALLET_BAD = "0x" + "5" * 40
CONTRACT_A = "0x" + "1" * 40
CONTRACT_B = "0x" + "2" * 40


class SupplySettings(Settings):
    model_config = SettingsConfigDict(env_file=None)

    def load_config(self, name: str) -> dict[str, JsonValue]:
        if name == "exchange_addresses":
            return {
                "wallet_lists": [
                    {
                        "venue": "synthetic-exchange",
                        "commit": "declared-fixture",
                        "addresses": [WALLET_GOOD, WALLET_BAD],
                    }
                ]
            }
        return super().load_config(name)


@pytest.fixture
def supply_settings(tmp_path: Path) -> SupplySettings:
    return SupplySettings(
        data_dir=tmp_path,
        openma_api_key="openma-private-value",
        foundry_api_key="foundry-private-value",
        telegram_token="telegram-private-value",
        etherscan_api_key="etherscan-private-value",
        codex_gateway_key="gateway-private-value",
        apify_api_token="apify-private-value",
        tank_db_password="tank-db-private-value",
        oma_db_password="oma-db-private-value",
        database_url="postgresql://fixture:database-private-value@localhost/tank_test_supply",
        openma_database_url="postgresql://fixture:openma-dsn-private-value@localhost/tank_test_supply",
        clickhouse_url="http://fixture:clickhouse-private-value@localhost:8123/tank_test_supply",
    )


def message(settings: SupplySettings) -> str:
    return (
        "supply request failed | "
        + " | ".join(sorted(settings.configured_secrets()))
        + " | https://example.invalid/read?api_key=unregistered-query-value"
        + " | https://reader:unregistered-url-password@example.invalid/read"
        + " | Bearer unregisteredBearerValue"
        + " | https://api.telegram.org/botunregisteredBotValue/sendMessage"
    )


def safe_reason(value: JsonValue, settings: SupplySettings) -> str:
    assert isinstance(value, str)
    assert all(secret not in value for secret in settings.configured_secrets())
    for secret in (
        "unregistered-query-value",
        "unregistered-url-password",
        "unregisteredBearerValue",
        "unregisteredBotValue",
    ):
        assert secret not in value
    assert "supply request failed" in value and "[REDACTED]" in value
    return value


class SupplyMarket:
    """Only the external request and persistence ports; SupplyCapture stays real."""

    def __init__(self, settings: SupplySettings, http: httpx.AsyncClient) -> None:
        self.settings = settings
        self.http = http
        self.repo = MemoryRepository()
        self.tokens: dict[str, dict[str, JsonValue]] = {}
        self.raw_responses: list[bytes] = []
        self._supply_refresh: dict[tuple[str, str], datetime] = {}
        self._supply_refresh_resumed = True
        self._supply_longest_pass_s = 0.0

    async def _request(
        self, method: str, url: str, *, params: Mapping[str, str | int] | None = None
    ) -> JsonValue:
        response = await self.http.request(method, url, params=params)
        self.raw_responses.append(response.content)
        response.raise_for_status()
        return decode_json(response.content)

    async def _put(self, collection: str, key: str, row: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return await self.repo.put(collection, key, row)


@asynccontextmanager
async def capture(
    settings: SupplySettings, respond: Callable[[httpx.Request], httpx.Response]
) -> AsyncIterator[tuple[SupplyCapture, SupplyMarket]]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        market = SupplyMarket(settings, http)
        yield SupplyCapture(market), market


async def test_record_redacts_only_diagnostic_reason_and_preserves_identity_and_zero(
    supply_settings: SupplySettings,
) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Record must not issue HTTP")

    async with capture(supply_settings, refuse) as (supply, market):
        identity = supply_settings.etherscan_api_key
        assert isinstance(identity, str)
        result = await supply.record(
            {"token_id": "test:T"},
            "FUNDAMENTALS",
            available=False,
            reason=message(supply_settings),
            chain=identity,
            identity_evidence={
                name: identity for name in ("wallet", "contract", "operation", "slug", "metric")
            },
            transfer_queries_succeeded=0,
        )
        safe_reason(result["reason"], supply_settings)
        assert result["chain"] == identity
        assert result["identity_evidence"] == {
            name: identity for name in ("wallet", "contract", "operation", "slug", "metric")
        }
        assert result["transfer_queries_succeeded"] == 0 and result["available"] is False
        [stored] = await market.repo.list("mkt.supply_obs")
        assert stored["reason"] == result["reason"]
        assert stored["chain"] == identity
        assert stored["identity_evidence"] == result["identity_evidence"]


@pytest.mark.parametrize("blank", [False, True])
async def test_poll_retains_sanitized_exception_or_actual_blank_reason(
    supply_settings: SupplySettings, monkeypatch: pytest.MonkeyPatch, blank: bool
) -> None:
    now = datetime(2026, 10, 26, tzinfo=UTC)
    monkeypatch.setattr("tank.market.supply.utcnow", lambda: now)

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/emissions/supply-fixture")
        raise RuntimeError("" if blank else message(supply_settings))

    async with capture(supply_settings, respond) as (supply, market):
        token: dict[str, JsonValue] = {"token_id": "test:T", "unlock_slug": "supply-fixture"}
        market.tokens = {"test:T": token}
        market._supply_refresh = {
            ("test:T", kind): now for kind in ("inventory", "lending", "chain_liquidity", "fundamentals")
        }
        await supply.poll(set())
        [stored] = await market.repo.list("mkt.supply_obs")
        assert stored["kind"] == "UNLOCKS" and stored["available"] is False
        if blank:
            assert stored["reason"] == ""
        else:
            safe_reason(stored["reason"], supply_settings)
        assert ("test:T", "unlocks") in market._supply_refresh


async def test_inventory_keeps_contract_wallet_and_partial_success_coverage(
    supply_settings: SupplySettings,
) -> None:
    error_body = json.dumps({"status": "0", "message": "NOTOK", "result": message(supply_settings)}).encode()

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.etherscan.io"
        assert request.url.params["apikey"] == supply_settings.etherscan_api_key
        if request.url.params["action"] == "eth_call":
            return httpx.Response(200, content=error_body)
        if request.url.params["address"] == WALLET_BAD:
            raise RuntimeError(message(supply_settings))
        result: JsonValue = "7" if request.url.params["action"] == "tokenbalance" else []
        return httpx.Response(200, json={"result": result})

    token: dict[str, JsonValue] = {
        "token_id": "test:T",
        "contracts": [
            {"chain": "Ethereum", "chain_id": 1, "address": CONTRACT_A},
            {"chain": "Base", "chain_id": 8453, "address": CONTRACT_B, "decimals": 0},
        ],
    }
    async with capture(supply_settings, respond) as (supply, market):
        result = await supply.inventory(token)
        errors = result["coverage_errors"]
        assert errors[0]["chain"] == "Ethereum" and errors[0]["chainid"] == 1
        assert errors[0]["contract"] == CONTRACT_A and errors[0]["operation"] == "decimals"
        assert errors[1]["wallet"] == WALLET_BAD and errors[1]["chainid"] == 8453
        for error in errors:
            safe_reason(error["reason"], supply_settings)
        assert result["available"] is True
        assert result["balances"] == [
            {
                "venue": "synthetic-exchange",
                "chainid": 8453,
                "contract": CONTRACT_B,
                "qty_native": "7",
                "observed_wallets": 1,
                "expected_wallets": 2,
                "complete": False,
                "wallet_list_commit": "declared-fixture",
            }
        ]
        rows = await market.repo.list("mkt.supply_obs", filters={"kind": "WALLET_FLOWS"})
        assert len(rows) == 1 and rows[0]["available"] is True
        assert rows[0]["transfer_queries_succeeded"] == 1 and rows[0]["flows"] == []
        assert rows[0]["coverage_errors"] == result["coverage_errors"]
        assert market.raw_responses[0] == error_body
        key = supply_settings.foundry_api_key
        assert isinstance(key, str) and key.encode() in market.raw_responses[0]


async def test_chain_failure_keeps_chain_identity_and_real_zero_series(
    supply_settings: SupplySettings,
) -> None:
    raw = b'[{"date":1767225600,"totalCirculatingUSD":{"peggedUSD":"0"},"opaque":"provider-original"}]'

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/Ethereum"):
            raise RuntimeError(message(supply_settings))
        assert request.url.path.endswith("/Base")
        return httpx.Response(200, content=raw)

    async with capture(supply_settings, respond) as (supply, market):
        await supply.chain_liquidity({"token_id": "test:T", "settlement_chains": ["Ethereum", "Base"]})
        rows = await market.repo.list("mkt.supply_obs")
        assert len(rows) == 2
        by_chain = {row["chain"]: row for row in rows}
        assert by_chain["Ethereum"]["available"] is False
        safe_reason(by_chain["Ethereum"]["reason"], supply_settings)
        assert by_chain["Base"]["available"] is True
        series = by_chain["Base"]["series"]
        assert isinstance(series, list) and len(series) == 1
        point = series[0]
        assert isinstance(point, dict) and point["supply_usd"] == "0"
        assert market.raw_responses == [raw]


async def test_fundamentals_retains_metric_and_slug_errors_blank_and_success_bytes(
    supply_settings: SupplySettings,
) -> None:
    raw = b'{"total24h":"1.25","total30d":"12.50","totalDataChart":[[1767225600,"3.00"]]}'
    zero = b'{"total24h":"0","total30d":"0","totalDataChart":[]}'

    def respond(request: httpx.Request) -> httpx.Response:
        slug = request.url.path.rsplit("/", 1)[1]
        metric = request.url.params["dataType"]
        if slug == "good" and metric == "dailyFees":
            return httpx.Response(200, content=raw)
        if metric == "dailyRevenue":
            return httpx.Response(200, content=zero)
        raise RuntimeError("" if slug == "good" else message(supply_settings))

    async with capture(supply_settings, respond) as (supply, market):
        result = await supply.fundamentals(
            {
                "token_id": "test:T",
                "defillama_protocol_slugs": ["good", "broken"],
            }
        )
        errors = result["coverage_errors"]
        assert [(error["slug"], error["metric"]) for error in errors] == [
            ("good", "dailyHoldersRevenue"),
            ("broken", "dailyFees"),
            ("broken", "dailyHoldersRevenue"),
        ]
        assert errors[0]["reason"] == ""
        for error in errors[1:]:
            safe_reason(error["reason"], supply_settings)
        assert result["protocols"]["good"]["dailyHoldersRevenue"]["reason"] == ""
        assert result["protocols"]["broken"]["dailyFees"]["reason"] == errors[1]["reason"]
        fees = result["metrics"]["dailyFees"]
        assert fees["total24h"] == "1.25" and fees["total30d"] == "12.50"
        assert fees["totalDataChart"] == [[1767225600, "3.00"]] and fees["complete"] is False
        assert result["metrics"]["dailyRevenue"]["total24h"] == "0"
        assert result["metrics"]["dailyRevenue"]["complete"] is True
        assert result["available"] is True
        assert market.raw_responses == [raw, zero, zero]
        [stored] = await market.repo.list("mkt.supply_obs")
        assert stored["coverage_errors"] == errors
