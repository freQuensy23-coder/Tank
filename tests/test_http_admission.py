"""Shared HTTP admission uses actual scoped budgets without fabricated capacities."""

import asyncio
import os
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from psycopg import AsyncConnection
from source_support import fixture_versions

import tank.request_budget as budget_module
from tank.contracts.http_policy import HttpAdmitted, HttpRefused, HttpUnconfigured
from tank.contracts.sources import HttpFetchPlan, SourceApiLimitsConfiguration
from tank.http_admission import ConfiguredHttpAdmission
from tank.request_budget import BudgetConstraint, RequestLedger, SharedRequestBudgets, normalized_constraints
from tank.sources.http_client import SourceHttpTransport


def test_atomic_group_requires_distinct_configured_bucket_identities() -> None:
    constraint = BudgetConstraint(key="test", capacity=2, window_s=60, weight=1)
    with pytest.raises(ValueError, match="at least one"):
        normalized_constraints([])
    with pytest.raises(ValueError, match="repeat"):
        normalized_constraints([constraint, constraint])


@pytest.mark.asyncio
async def test_atomic_memory_group_does_not_spend_other_window_while_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    sleeping = asyncio.Event()
    release = asyncio.Event()

    async def pause(delay: float) -> None:
        assert delay > 0
        sleeping.set()
        await release.wait()

    monkeypatch.setattr(budget_module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(asyncio, "sleep", pause)
    engine = SharedRequestBudgets()
    long = BudgetConstraint(key="long", capacity=3, window_s=100, weight=1)
    short = BudgetConstraint(key="short", capacity=1, window_s=10, weight=1)
    await engine.acquire_many([short, long])
    pending = asyncio.create_task(engine.acquire_many([short, long]))
    try:
        await asyncio.wait_for(sleeping.wait(), timeout=1)
        assert len(engine.windows[long.bucket()]) == 1
        assert len(engine.windows[short.bucket()]) == 1
        clock[0] = 10.0
        release.set()
        await asyncio.wait_for(pending, timeout=1)
        assert [row.at for row in engine.windows[long.bucket()]] == [0.0, 10.0]
        assert [row.at for row in engine.windows[short.bucket()]] == [10.0]
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


def configured_admission() -> tuple[ConfiguredHttpAdmission, SharedRequestBudgets]:
    import yaml

    policy = SourceApiLimitsConfiguration.model_validate(
        yaml.safe_load(Path("config/api_limits.yaml").read_text())
    )
    engine = SharedRequestBudgets()
    return ConfiguredHttpAdmission(policy, fixture_versions(), engine), engine


@pytest.mark.asyncio
async def test_actual_built_query_identity_shares_all_chains_and_both_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(budget_module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    admission, engine = configured_admission()
    async with httpx.AsyncClient(params={"apikey": " Exact Key+é "}) as client:
        first = client.build_request("GET", "https://api.etherscan.io/v2/api", params={"chainid": "1"})
        result = await admission.admit(first)
    assert isinstance(result, HttpAdmitted)
    scope = "etherscan:api-key:" + sha256(" Exact Key+é ".encode()).hexdigest()
    assert {bucket.key for bucket in engine.windows} == {scope}
    assert {bucket.window_s for bucket in engine.windows} == {1.0, 86400.0}
    for rows in engine.windows.values():
        assert len(rows) == 1
    second = httpx.Request(
        "GET",
        "https://api.etherscan.io/v2/api",
        params={"apikey": " Exact Key+é ", "chainid": "56"},
    )
    clock[0] = 1.0
    await admission.admit(second)
    assert {bucket.key for bucket in engine.windows} == {scope}
    assert {bucket.window_s: len(rows) for bucket, rows in engine.windows.items()} == {1.0: 1, 86400.0: 2}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix",
    [
        "/api?apikey=secret",
        "/v2/api",
        "/v2/api?apikey=",
        "/v2/api?apikey=secret&apikey=second",
        "/v2/api?apikey=%FF",
    ],
)
async def test_api_refusal_never_reserves_or_exposes_credentials(suffix: str) -> None:
    admission, engine = configured_admission()
    result = await admission.admit(httpx.Request("GET", "https://api.etherscan.io" + suffix))
    assert isinstance(result, HttpRefused)
    assert not engine.windows
    assert "secret" not in result.model_dump_json()
    assert "second" not in result.model_dump_json()


@pytest.mark.asyncio
async def test_public_explorer_has_explicit_unknown_policy_without_api_scope() -> None:
    admission, engine = configured_admission()
    for host in ("etherscan.io", "www.etherscan.io"):
        result = await admission.admit(httpx.Request("GET", f"https://{host}/address/test"))
        assert isinstance(result, HttpUnconfigured)
    assert not engine.windows


@pytest.mark.asyncio
async def test_sec_subdomains_share_declared_ip_budget() -> None:
    admission, engine = configured_admission()
    for host in ("www.sec.gov", "data.sec.gov"):
        result = await admission.admit(httpx.Request("GET", f"https://{host}/synthetic"))
        assert isinstance(result, HttpAdmitted)
    assert len(engine.windows) == 1
    bucket, rows = next(iter(engine.windows.items()))
    assert bucket.key == "sec.gov"
    assert len(rows) == 2


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_group_rolls_back_tentative_spend_in_other_window(
    postgres_template: PostgresTemplate,
) -> None:
    database_url = os.environ["TANK_SOURCE_TEST_DATABASE_URL"]
    async with postgres_template.repository(database_url) as repo:
        ledger = RequestLedger(repo.database_url, owner="synthetic-atomic-proof")
        long = BudgetConstraint(key="a-long", capacity=3, window_s=86400, weight=1)
        short = BudgetConstraint(key="z-short", capacity=1, window_s=60, weight=1)
        try:
            assert await ledger.wait_many([short, long]) == 0
            assert await ledger.wait_many([long, short]) > 0
            async with await AsyncConnection.connect(repo.database_url) as connection:
                rows = await (
                    await connection.execute(
                        "SELECT bucket, count(*) FROM ops.request_budget_ledger GROUP BY bucket ORDER BY bucket"
                    )
                ).fetchall()
            assert rows == [
                ("a-long|86400.0|3.0", 1),
                ("z-short|60.0|1.0", 1),
            ]
        finally:
            await ledger.shutdown()


@pytest.mark.asyncio
async def test_source_transport_admits_each_actual_redirect_destination() -> None:
    admission, engine = configured_admission()
    seen: list[str] = []

    async def admit(feed: HttpFetchPlan, request: httpx.Request) -> None:
        result = await admission.admit(request)
        assert isinstance(result, HttpAdmitted)

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.host)
        if request.url.host == "www.sec.gov":
            return httpx.Response(302, headers={"Location": "https://data.sec.gov/synthetic"})
        return httpx.Response(200, content=b"retained")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        transport = SourceHttpTransport(client, admit)
        response = await transport.request(HttpFetchPlan(url="https://www.sec.gov/synthetic"), {}, None)
    assert response.content == b"retained"
    assert seen == ["www.sec.gov", "data.sec.gov"]
    assert len(next(iter(engine.windows.values()))) == 2


@pytest.mark.asyncio
async def test_source_redirect_cannot_restore_python_exchange_transport() -> None:
    seen: list[str] = []

    async def admit(feed: HttpFetchPlan, request: httpx.Request) -> None:
        seen.append(request.url.host)

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "https://api.binance.com/api/v3/exchangeInfo"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        transport = SourceHttpTransport(client, admit)
        with pytest.raises(ValueError, match="exchange capture transport"):
            await transport.request(HttpFetchPlan(url="https://example.org/feed"), {}, None)
    assert seen == ["example.org"]
