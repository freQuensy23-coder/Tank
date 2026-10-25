"""Shared HTTP admission uses actual scoped budgets without fabricated capacities."""

import asyncio
import os
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from psycopg import AsyncConnection
from source_support import FixtureBudgetClock, fixture_versions

import tank.request_budget as budget_module
from tank.contracts.http_policy import HttpAdmitted, HttpRefused, HttpUnconfigured
from tank.contracts.sources import HttpFetchPlan, SourceApiLimitsConfiguration
from tank.http_admission import ConfiguredHttpAdmission
from tank.request_budget import (
    BudgetConstraint,
    MemoryBudgetBackend,
    RequestBudgetScope,
    RequestLedger,
    SharedRequestBudgets,
    SystemBudgetClock,
    normalized_constraints,
)
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
    backend = MemoryBudgetBackend(SystemBudgetClock())
    engine = SharedRequestBudgets(backend)
    long = BudgetConstraint(key="long", capacity=3, window_s=100, weight=1)
    short = BudgetConstraint(key="short", capacity=1, window_s=10, weight=1)
    await engine.acquire_many([short, long])
    pending = asyncio.create_task(engine.acquire_many([short, long]))
    try:
        await asyncio.wait_for(sleeping.wait(), timeout=1)
        assert len(backend.windows[long.bucket()]) == 1
        assert len(backend.windows[short.bucket()]) == 1
        clock[0] = 10.0
        release.set()
        await asyncio.wait_for(pending, timeout=1)
        assert [row.at for row in backend.windows[long.bucket()]] == [0.0, 10.0]
        assert [row.at for row in backend.windows[short.bucket()]] == [10.0]
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


def configured_admission() -> tuple[ConfiguredHttpAdmission, MemoryBudgetBackend]:
    import yaml

    policy = SourceApiLimitsConfiguration.model_validate(
        yaml.safe_load(Path("config/api_limits.yaml").read_text())
    )
    backend = MemoryBudgetBackend(SystemBudgetClock())
    engine = SharedRequestBudgets(backend)
    return ConfiguredHttpAdmission(policy, fixture_versions(), engine), backend


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


@pytest.mark.asyncio
async def test_unbound_scope_refuses_acquisition_and_blocking() -> None:
    scope = RequestBudgetScope()
    constraint = BudgetConstraint(key="configured", capacity=1, window_s=60, weight=1)
    with pytest.raises(RuntimeError, match="has not been bound"):
        await scope.acquire_many([constraint])
    with pytest.raises(RuntimeError, match="has not been bound"):
        await scope.block("api.example", FixtureBudgetClock().now(), "provider refusal")


@pytest.mark.asyncio
async def test_releasing_old_runtime_binding_preserves_replacement_ownership() -> None:
    scope = RequestBudgetScope()
    old_backend = MemoryBudgetBackend(FixtureBudgetClock())
    old = scope.bind(old_backend)
    new_backend = MemoryBudgetBackend(FixtureBudgetClock())
    constraint = BudgetConstraint(key="configured", capacity=1, window_s=60, weight=1)
    with pytest.raises(RuntimeError, match="already has an active binding"):
        scope.bind(new_backend)
    assert scope.require_binding() is old
    await scope.acquire_many([constraint])
    assert len(old_backend.windows[constraint.bucket()]) == 1
    assert not new_backend.windows
    await scope.release(old)
    replacement = scope.bind(new_backend)
    await scope.release(old)
    await scope.acquire_many([constraint])
    assert old_backend.closed
    assert len(new_backend.windows[constraint.bucket()]) == 1
    assert scope.require_binding() is replacement
    await scope.release(replacement)
    with pytest.raises(RuntimeError, match="has not been bound"):
        await scope.acquire_many([constraint])


@pytest.mark.asyncio
async def test_memory_backend_enforces_provider_host_block_without_spending_other_windows() -> None:
    clock = FixtureBudgetClock()
    backend = MemoryBudgetBackend(clock)
    engine = SharedRequestBudgets(backend)
    first = BudgetConstraint(key="first", capacity=2, window_s=60, weight=1, host="api.example")
    second = BudgetConstraint(key="second", capacity=2, window_s=60, weight=1, host="api.example")
    await engine.block("api.example", clock.now() + timedelta(seconds=10), "provider Retry-After")
    assert await backend.wait_many([second, first], ready=lambda: True) == 10
    assert not any(backend.windows.values())
    await engine.acquire_many([second, first])
    assert clock.monotonic() == 10
    assert [row.at for row in backend.windows[first.bucket()]] == [10]
    assert [row.at for row in backend.windows[second.bucket()]] == [10]


@pytest.mark.asyncio
async def test_invalid_constructed_constraint_cannot_bypass_required_capacity() -> None:
    from pydantic import JsonValue, ValidationError

    wire: dict[str, JsonValue] = {"key": "configured", "capacity": None, "window_s": 60, "weight": 1}
    invalid = BudgetConstraint.model_construct(set(wire), **wire)
    backend = MemoryBudgetBackend(FixtureBudgetClock())
    engine = SharedRequestBudgets(backend)
    with pytest.raises(ValidationError):
        await engine.acquire_many([invalid])
    assert not backend.windows


@pytest.mark.asyncio
async def test_unregistered_public_host_waits_for_empirical_block_without_invented_quota() -> None:
    from source_support import fixture_http_admission, fixture_request_budgets

    clock = FixtureBudgetClock()
    engine = fixture_request_budgets(clock=clock)
    assert isinstance(engine.backend, MemoryBudgetBackend)
    admission = fixture_http_admission(budgets=engine)
    request = httpx.Request("GET", "https://public.example/ordinary")
    await admission.block(request, clock.now() + timedelta(seconds=12), "observed Retry-After")
    result = await admission.admit(request)
    assert isinstance(result, HttpUnconfigured)
    assert clock.monotonic() == 12
    assert not engine.backend.windows


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_host_block_is_visible_without_any_provider_quota(
    postgres_template: PostgresTemplate,
) -> None:
    from tank.util import utcnow

    database_url = os.environ["TANK_SOURCE_TEST_DATABASE_URL"]
    async with postgres_template.repository(database_url) as repo:
        writer = RequestLedger(repo.database_url, owner="synthetic-host-writer")
        reader = RequestLedger(repo.database_url, owner="synthetic-host-reader")
        try:
            assert await reader.host_delay("public.example") == 0
            await writer.block("public.example", utcnow() + timedelta(seconds=60), "observed Retry-After")
            assert 0 < await reader.host_delay("public.example") <= 60
            assert await reader.host_delay("other.example") == 0
            async with await AsyncConnection.connect(repo.database_url) as connection:
                row = await (
                    await connection.execute("SELECT count(*) FROM ops.request_budget_ledger")
                ).fetchone()
            assert row == (0,)
        finally:
            await writer.shutdown()
            await reader.shutdown()


def test_budget_composition_preserves_explicit_memory_and_unopened_postgres_resources() -> None:
    from tank.request_budget import build_budget_backend
    from tank.storage import MemoryRepository, PostgresRepository

    memory = build_budget_backend(MemoryRepository(), owner="synthetic-memory")
    assert isinstance(memory, MemoryBudgetBackend)
    physical = build_budget_backend(
        PostgresRepository("postgresql://synthetic:synthetic@127.0.0.1/tank_test_unopened"),
        owner="synthetic-postgres",
    )
    assert isinstance(physical, RequestLedger)
    assert physical.connection is None
