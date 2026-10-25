"""Offline Foundry Decision-1 provider contracts and persistence."""

import json

import httpx
import pytest
from agent_source_fixtures import (
    PipelineFixtureSettings,
    declared_versions_fixture,
    pipeline_settings_fixture,
    source_item_fixture,
)
from pydantic import JsonValue

from tank.agents import Pipeline
from tank.agents.decision import DecisionItem, DecisionQuestion, DecisionState, binding, classify
from tank.agents.pipeline import FILTER, FILTER_CRITERIA
from tank.ops_progress import DisabledSourceProgress
from tank.storage import MemoryRepository

URL = "https://synthetic.services.ai.azure.com/providers/microsoft/v1/systemone"


def settings(*, foundry_api_key: str | None = "FAKE_FOUNDRY_KEY") -> PipelineFixtureSettings:
    return pipeline_settings_fixture(
        {"registry": {}, "thresholds": {"filter_cutoff": 0.7}}, foundry_api_key=foundry_api_key
    )


def response(model: str = "microsoft-decision-1", **usage: JsonValue) -> dict[str, JsonValue]:
    return {
        "model": model,
        "answers": {"relevant": {"type": "noul", "noul": 0.97}},
        "usage": {"input_tokens": 100, "output_tokens": 22, **usage},
    }


ITEM = source_item_fixture("i", source_id="S30", source="Issuer", author="Issuer", text="ETH crypto asset")


@pytest.mark.asyncio
async def test_configured_key_deployment_exact_wire_and_cache() -> None:
    repo = MemoryRepository()
    calls: list[httpx.Request] = []

    def fake(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert str(request.url) == URL and "authorization" not in request.headers
        assert request.headers["api-key"] == "FAKE_FOUNDRY_KEY"
        assert json.loads(request.content) == {
            "model": "decision-1",
            "state": Pipeline.state(ITEM).to_wire(),
            "questions": {"relevant": {"type": "noul", "instructions": FILTER, "criteria": FILTER_CRITERIA}},
        }
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(
            repo,
            settings(),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
    assert len(calls) == 1
    row = (await repo.list("ops.llm_usage"))[0]
    assert row["at"] == row["created_at"] and row["at_prec"] == "COLLECTION_ONLY"
    assert row["classifier_provider"] == "azure-foundry" and row["endpoint"] == URL
    assert row["requested_model_id"] == "decision-1" and row["resolved_model_id"] == "microsoft-decision-1"
    assert (
        row["semantic_model_id"] == "microsoft-decision-1:1"
        and row["billing_basis"] == "AZURE_FOUNDRY_REPORTED"
    )
    assert row["cost_usd"] is None and row["cost_reason"] == "Azure Foundry does not report usage.cost"
    missing_fields = row["missing_fields"]
    assert isinstance(missing_fields, dict)
    assert missing_fields["cost_usd"] == row["cost_reason"]


@pytest.mark.asyncio
async def test_reported_cost_and_key_are_preserved() -> None:
    seen: list[httpx.Request] = []

    def fake(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert request.headers["api-key"] == "FAKE_FOUNDRY_KEY"
        assert json.loads(request.content)["model"] == "decision-1"
        return httpx.Response(200, json=response(model="microsoft-decision-1", cost=0.0000042))

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(
            repo,
            settings(foundry_api_key="FAKE_FOUNDRY_KEY"),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        assert await pipeline.score(ITEM, "relevant", pipeline.state(ITEM)) == 0.97
    row = (await repo.list("ops.llm_usage"))[0]
    assert len(seen) == 1 and row["requested_model_id"] == "decision-1" and row["cost_usd"] == "0.0000042"


@pytest.mark.asyncio
@pytest.mark.parametrize("resolved", ["jev-1.14", "jev-latest", "decision-1", "typesafe/jev-1.13-20260917"])
async def test_resolved_model_cannot_silently_change(resolved: str) -> None:
    repo = MemoryRepository()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response(model=resolved)))
    ) as http:
        with pytest.raises(ValueError, match="unexpected resolved model"):
            await Pipeline(
                repo,
                settings(),
                progress=DisabledSourceProgress(),
                http=http,
                versions=declared_versions_fixture(),
            ).score(ITEM, "relevant", DecisionState(item=DecisionItem(source="Fixture", text="data")))
    assert await repo.list("ops.llm_usage") == []


@pytest.mark.asyncio
async def test_filter_item_records_actual_model_provenance_on_first_and_cached_calls() -> None:
    repo = MemoryRepository()
    calls: list[httpx.Request] = []
    await repo.put(
        "ingest.source_items",
        "i",
        source_item_fixture("i", source_id="S30", text="ETH is a crypto asset").storage_record(),
    )

    def fake(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as http:
        pipeline = Pipeline(
            repo,
            settings(),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        first = await pipeline.filter_item_wire({"item_id": "i"})
        second = await pipeline.filter_item_wire({"item_id": "i"})
    assert len(calls) == 1 and first["relevance"] == second["relevance"]
    relevance = first["relevance"]
    assert isinstance(relevance, dict)
    assert relevance["model"] == "microsoft-decision-1" and relevance["requested_model"] == "decision-1"
    assert (
        relevance["provider"] == "azure-foundry"
        and relevance["semantic_model_id"] == "microsoft-decision-1:1"
    )
    usage_id = relevance["usage_id"]
    assert isinstance(usage_id, str)
    stored = await repo.get("ops.llm_usage", usage_id)
    assert stored is not None and stored["model_id"] == relevance["model"]


@pytest.mark.parametrize(
    "status,body", [(429, {"error": {"message": "rate limited"}}), (400, {"error": {"message": "invalid"}})]
)
async def test_provider_errors_are_not_retried_or_chat_wrapped(
    status: int, body: dict[str, JsonValue]
) -> None:
    calls: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/providers/microsoft/v1/systemone"
        assert "messages" not in json.loads(request.content)
        return httpx.Response(status, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await classify(
                http,
                api_key="FAKE_FOUNDRY_KEY",
                pinned=binding(settings()),
                state=DecisionState(item=DecisionItem(source="Fixture", text="data")),
                questions={"relevant": DecisionQuestion(instructions=FILTER)},
            )
    assert len(calls) == 1


@pytest.mark.parametrize(
    "answer",
    [{"type": "choice", "noul": 0.9}, {"type": "noul", "noul": True}, {"type": "noul", "noul": "0.9"}],
)
async def test_malformed_typed_answer_is_rejected(answer: dict[str, JsonValue]) -> None:
    data = response()
    answers = data["answers"]
    assert isinstance(answers, dict)
    answers["relevant"] = answer
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=data))
    ) as http:
        with pytest.raises(ValueError, match="invalid noul"):
            await classify(
                http,
                api_key="FAKE_FOUNDRY_KEY",
                pinned=binding(settings()),
                state=DecisionState(item=DecisionItem(source="Fixture", text="data")),
                questions={"relevant": DecisionQuestion(instructions=FILTER)},
            )


@pytest.mark.parametrize("key", [None, "", " "])
async def test_missing_key_refuses_before_network(key: str | None) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("network call"))
    ) as http:
        with pytest.raises(ValueError, match="TANK_FOUNDRY_API_KEY"):
            await classify(
                http,
                api_key=key,
                pinned=binding(settings()),
                state=DecisionState(item=DecisionItem(source="Fixture")),
                questions={"relevant": DecisionQuestion(instructions=FILTER)},
            )


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://example.com",
        "https://example.com",
        "https://secret@a.services.ai.azure.com",
        "https://a.services.ai.azure.com/path",
        "https://a.services.ai.azure.com?api-key=secret",
    ],
)
def test_endpoint_refuses_untrusted_origin(endpoint: str) -> None:
    opts = settings()
    opts.foundry_endpoint = endpoint
    with pytest.raises(ValueError, match="resource origin"):
        binding(opts)


async def test_redirect_cannot_forward_key() -> None:
    requests: list[httpx.Request] = []

    def redirect(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(307, headers={"Location": "https://other.example/systemone"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(redirect), follow_redirects=True) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await classify(
                http,
                api_key="FAKE_FOUNDRY_KEY",
                pinned=binding(settings()),
                state=DecisionState(item=DecisionItem(source="Fixture")),
                questions={"relevant": DecisionQuestion(instructions=FILTER)},
            )
    assert len(requests) == 1


async def test_raw_duplicate_answers_refuse_before_score_persistence() -> None:
    repo = MemoryRepository()
    raw = b'{"model":"microsoft-decision-1","answers":{"relevant":{"type":"noul","noul":0.1,"noul":0.9}},"usage":{}}'
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=raw))
    ) as http:
        pipeline = Pipeline(
            repo,
            settings(),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(ValueError, match="keys must be unique"):
            await pipeline.score(ITEM, "relevant", pipeline.state(ITEM))
    assert await repo.list("ops.llm_usage") == []


async def test_request_identity_isolated_by_resource_deployment_and_semantic_model() -> None:
    opts = settings()
    async with httpx.AsyncClient() as http:
        pipeline = Pipeline(
            MemoryRepository(),
            opts,
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        state = pipeline.state(ITEM)
        initial = pipeline.score_identity(ITEM, "relevant", state)
        assert initial.binding.semantic_model == "microsoft-decision-1:1"
        opts.foundry_deployment = "other-deployment"
        assert pipeline.score_identity(ITEM, "relevant", state).usage_id != initial.usage_id
        opts.foundry_deployment = "decision-1"
        opts.foundry_endpoint = "https://other.services.ai.azure.com"
        assert pipeline.score_identity(ITEM, "relevant", state).usage_id != initial.usage_id


async def test_replay_requires_exact_record_without_live_request() -> None:
    opts = settings()
    opts.replay = True
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: pytest.fail("live replay call"))
    ) as http:
        pipeline = Pipeline(
            MemoryRepository(),
            opts,
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        with pytest.raises(ValueError, match="Replay requires stored Decision-1 score"):
            await pipeline.score(ITEM, "relevant", pipeline.state(ITEM))


async def test_old_provider_cache_cannot_answer_new_request_and_zero_cost_is_known() -> None:
    from tank.util import sha256

    opts = settings()
    repo = MemoryRepository()
    requests: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=response(cost=0))

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        pipeline = Pipeline(
            repo, opts, progress=DisabledSourceProgress(), http=http, versions=declared_versions_fixture()
        )
        state = pipeline.state(ITEM)
        identity = pipeline.score_identity(ITEM, "relevant", state)
        old_key = sha256(
            {
                "item_id": ITEM.item_id,
                "model": "jev-1.13-free",
                "provider": "opencode",
                "base_url": "https://opencode.ai/inference/systemone",
                "prompt_version": "jev-v2",
                "questions": {name: question.to_wire() for name, question in identity.questions.items()},
                "state_fingerprint": identity.state_fingerprint,
            }
        )
        await repo.put(
            "ops.llm_usage", old_key, {"usage_id": old_key, "score": 0.01, "model_id": "jev-1.13-free"}
        )
        assert old_key != identity.usage_id
        assert await pipeline.score(ITEM, "relevant", state) == 0.97
        record = await pipeline.scores.get(identity.usage_id)
        assert record is not None and record.cost_usd == 0
        assert record.cost_reason is None and record.missing_fields.cost_usd is None
        assert len(requests) == 1
        assert await repo.get("ops.llm_usage", old_key) == {
            "usage_id": old_key,
            "score": 0.01,
            "model_id": "jev-1.13-free",
        }


async def test_prompt_change_has_distinct_cache_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    from tank.agents import classification

    async with httpx.AsyncClient() as http:
        pipeline = Pipeline(
            MemoryRepository(),
            settings(),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        state = pipeline.state(ITEM)
        initial = pipeline.score_identity(ITEM, "relevant", state)
        monkeypatch.setattr(classification, "DECISION_PROMPT_VERSION", "declared-next-prompt")
        changed = pipeline.score_identity(ITEM, "relevant", state)
    assert changed.usage_id != initial.usage_id
    assert changed.state_fingerprint == initial.state_fingerprint


async def test_actual_optional_provider_metadata_is_preserved() -> None:
    repo = MemoryRepository()
    payload = response()
    payload.update(id="reported-response", provider="reported-upstream")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as http:
        pipeline = Pipeline(
            repo,
            settings(),
            progress=DisabledSourceProgress(),
            http=http,
            versions=declared_versions_fixture(),
        )
        record = await pipeline._score_record(ITEM, "relevant", pipeline.state(ITEM))
    assert record.response_id == "reported-response"
    assert record.upstream_provider == "reported-upstream"
    assert "does not attest its version" in record.model_pin_evidence
