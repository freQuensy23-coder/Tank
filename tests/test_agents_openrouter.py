"""Offline provider integration; no environment credentials or live requests."""

import json
from decimal import Decimal

import httpx
import pytest

from tank.agents import ConfigurationError, Pipeline
from tank.agents.jev import classify
from tank.agents.pipeline import FILTER
from tank.settings import Settings
from tank.storage import MemoryRepository


def settings(tmp_path, **values):
    return Settings(_env_file=None, secrets_dir=tmp_path / "empty-secrets", **values)


def response(model="typesafe/jev-1.13-20260917", **usage):
    return {
        "id": "gen-dec-fake",
        "provider": "TypeSafe",
        "model": model,
        "answers": {"relevant": {"type": "noul", "noul": 0.97}},
        "usage": {"input_tokens": 100, "output_tokens": 22, **usage},
    }


@pytest.mark.asyncio
async def test_real_sdk_openrouter_wire_and_billed_cost_are_retained(tmp_path):
    repo = MemoryRepository()
    opts = settings(tmp_path, openrouter_api_key="FAKE_OR_KEY_ONLY", jev_provider="openrouter")
    calls = []
    item = {
        "item_id": "i",
        "source": "Issuer",
        "author": "Issuer",
        "text": "ETH crypto asset",
        "quoted": None,
    }

    def fake(request):
        calls.append(request)
        assert str(request.url) == "https://openrouter.ai/api/v1/systemone"
        assert request.headers["Authorization"] == "Bearer FAKE_OR_KEY_ONLY"
        body = json.loads(request.content)
        assert body == {
            "model": "jev-1.13",
            "state": Pipeline.state(item),
            "questions": {
                "relevant": {
                    "type": "noul",
                    "instructions": FILTER,
                    "criteria": {"true": "About crypto", "false": "Not about crypto"},
                }
            },
        }
        return httpx.Response(
            200, json=response(cost=0.123456789, cost_details={"upstream_inference_cost": 0.12})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(repo, opts, http=http)
        assert await pipeline.score(item, "relevant", pipeline.state(item)) == 0.97
        assert await pipeline.score(item, "relevant", pipeline.state(item)) == 0.97
    assert len(calls) == 1
    row = (await repo.list("ops.llm_usage"))[0]
    assert row["cost_usd"] == "0.123456789" and row["billing_basis"] == "OPENROUTER_REPORTED"
    assert row["usage"]["cost_details"] == {"upstream_inference_cost": 0.12}
    assert (
        row["resolved_model_id"] == "typesafe/jev-1.13-20260917" and row["semantic_model_id"] == "jev-1.13.0"
    )
    assert row["requested_model_id"] == "jev-1.13" and row["response_id"] == "gen-dec-fake"
    assert (
        row["upstream_provider"] == "TypeSafe" and "patch suffix is not exposed" in row["model_pin_evidence"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("provider,expected", [("openrouter", "OPENROUTER"), ("typesafe", "TYPESAFE")])
async def test_selected_provider_never_falls_back_to_other_credentials(tmp_path, provider, expected):
    other_key = (
        {"typesafe_api_key": "FAKE_WRONG_PROVIDER"}
        if provider == "openrouter"
        else {"openrouter_api_key": "FAKE_WRONG_PROVIDER"}
    )
    opts = settings(tmp_path, jev_provider=provider, **other_key)
    repo = MemoryRepository()
    calls = []

    def prohibited(request):
        calls.append(request)
        raise AssertionError("Missing selected provider must not make a request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(prohibited)) as http:
        with pytest.raises(ConfigurationError, match=expected):
            await Pipeline(repo, opts, http=http).score({"item_id": "i"}, "relevant", {"text": "data"})
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolved", ["typesafe/jev-1.14-20261014", "~typesafe/jev-latest", "jev-latest", "typesafe/jev-1.13"]
)
async def test_openrouter_resolved_pin_cannot_silently_upgrade_or_lose_snapshot(tmp_path, resolved):
    opts = settings(tmp_path, openrouter_api_key="FAKE_OR_KEY_ONLY")
    repo = MemoryRepository()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=response(model=resolved, cost=0.01))
        )
    ) as http:
        with pytest.raises(ValueError, match="unexpected resolved model"):
            await Pipeline(repo, opts, http=http).score({"item_id": "i"}, "relevant", {"text": "data"})
    assert await repo.list("ops.llm_usage") == []


@pytest.mark.asyncio
async def test_missing_openrouter_cost_stays_unknown_without_direct_rate_estimate(tmp_path):
    opts = settings(tmp_path, openrouter_api_key="FAKE_OR_KEY_ONLY")
    repo = MemoryRepository()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response()))
    ) as http:
        await Pipeline(repo, opts, http=http).score({"item_id": "i"}, "relevant", {"text": "data"})
    row = (await repo.list("ops.llm_usage"))[0]
    assert row["cost_usd"] is None and row["cost_reason"] == "OpenRouter omitted billed usage.cost"


@pytest.mark.asyncio
async def test_provider_change_has_distinct_exact_wire_cache_identity(tmp_path):
    repo = MemoryRepository()
    item = {"item_id": "i"}
    state = {"text": "same data"}
    calls = []

    def fake(request):
        calls.append(request.url.host)
        return httpx.Response(
            200,
            json=response(
                model="jev-1.13.0" if request.url.host == "api.typesafe.ai" else "typesafe/jev-1.13-20260917",
                cost=0.01,
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        direct = Pipeline(
            repo, settings(tmp_path, jev_provider="typesafe", typesafe_api_key="FAKE_DIRECT_KEY"), http=http
        )
        routed = Pipeline(
            repo, settings(tmp_path, jev_provider="openrouter", openrouter_api_key="FAKE_OR_KEY"), http=http
        )
        await direct.score(item, "relevant", state)
        await routed.score(item, "relevant", state)
        await direct.score(item, "relevant", state)
        await routed.score(item, "relevant", state)
    rows = await repo.list("ops.llm_usage")
    assert calls == ["api.typesafe.ai", "openrouter.ai"] and len(rows) == 2
    assert len({row["request_fingerprint"] for row in rows}) == 2
    assert {row["jev_provider"] for row in rows} == {"typesafe", "openrouter"}
    assert sum(Decimal(row["cost_usd"]) for row in rows) == Decimal(".0100042")


@pytest.mark.asyncio
async def test_sdk_openrouter_retains_exact_reported_zero_cost():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response(cost=0)))
    ) as http:
        actual = await classify(
            http,
            api_key="FAKE_OR_KEY",
            provider="openrouter",
            state="data",
            questions={"relevant": {"type": "noul", "instructions": FILTER}},
        )
    assert actual["usage"]["cost"] == 0 and actual["id"] == "gen-dec-fake"


@pytest.mark.asyncio
async def test_filter_item_records_actual_model_provenance_on_first_and_cached_calls(tmp_path):
    class FixtureSettings(Settings):
        def load_config(self, name):
            return {"filter_cutoff": 0.7}

    opts = FixtureSettings(_env_file=None, secrets_dir=tmp_path / "empty", openrouter_api_key="FAKE_OR_KEY")
    repo = MemoryRepository()
    calls = []
    await repo.put(
        "ingest.source_items", "i", {"item_id": "i", "source_id": "S30", "text": "ETH is a crypto asset"}
    )

    def fake(request):
        calls.append(request)
        return httpx.Response(200, json=response(cost=0.0000042))

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(repo, opts, http=http)
        first = await pipeline.filter_item({"item_id": "i"})
        second = await pipeline.filter_item({"item_id": "i"})
    assert len(calls) == 1 and first["relevance"] == second["relevance"]
    relevance = first["relevance"]
    assert relevance["model"] == "typesafe/jev-1.13-20260917"
    assert relevance["provider"] == "openrouter" and relevance["requested_model"] == "jev-1.13"
    assert relevance["semantic_model_id"] == "jev-1.13.0"
    assert "patch suffix is not exposed" in relevance["model_pin_evidence"]
    assert (await repo.get("ops.llm_usage", relevance["usage_id"]))["model_id"] == relevance["model"]
