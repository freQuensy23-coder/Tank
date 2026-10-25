"""Producer provenance is a required validated dependency, not a settings marker fallback."""

from dataclasses import dataclass
from typing import Literal

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture, pipeline_settings_fixture, source_item_fixture
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from tank.agents.classification import DecisionClassifier
from tank.agents.decision import DecisionItem, DecisionState
from tank.agents.pipeline import Pipeline
from tank.agents.pipeline_contracts import DecisionScoreRecord
from tank.agents.sessions import SessionService
from tank.bootstrap import sync_registry
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import Collection, Repository
from tank.ops_progress import DisabledSourceProgress
from tank.settings import Settings
from tank.storage import MemoryRepository


class UnreadSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)

    def read_config_bytes(self, relative: str) -> bytes:
        raise AssertionError("Invalid provenance must fail before configuration reads")


def invalid_identity() -> ResolvedVersionIdentity:
    return ResolvedVersionIdentity.model_construct(config_version="UNKNOWN", code_version="fixture-code")


@pytest.mark.parametrize("service", ["pipeline", "session"])
def test_invalid_provenance_fails_before_allocating_owned_http_client(
    service: Literal["pipeline", "session"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def client_factory(*, timeout: int) -> httpx.AsyncClient:
        raise AssertionError("Invalid provenance must fail before client allocation")

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    with pytest.raises(ValidationError):
        if service == "pipeline":
            Pipeline(
                MemoryRepository(),
                pipeline_settings_fixture({"registry": {}, "thresholds": {}}),
                progress=DisabledSourceProgress(),
                versions=invalid_identity(),
            )
        else:
            SessionService(MemoryRepository(), UnreadSettings(), None, versions=invalid_identity())


@pytest.mark.asyncio
async def test_invalid_bootstrap_provenance_fails_before_reads_or_mutation() -> None:
    store = MemoryRepository()
    with pytest.raises(ValidationError):
        await sync_registry(store, UnreadSettings(), versions=invalid_identity())
    assert await store.list("reg.sources") == []


@dataclass(frozen=True)
class ClassificationConfig:
    foundry_api_key: str | None
    replay: bool
    foundry_endpoint: str = "https://synthetic.services.ai.azure.com"
    foundry_deployment: str = "decision-1"


@pytest.mark.asyncio
async def test_classification_writes_explicit_identity_without_raw_settings_version_fields() -> None:
    store = MemoryRepository()
    scores = Repository(store, Collection("ops.llm_usage", DecisionScoreRecord))

    def provider(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "model": "microsoft-decision-1",
                "answers": {"relevant": {"type": "noul", "noul": 0.97}},
                "usage": {"input_tokens": 10, "output_tokens": 0},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        classifier = DecisionClassifier(
            scores,
            ClassificationConfig("FAKE_FOUNDRY_KEY", False),
            http,
            versions=declared_versions_fixture("explicit-config", "explicit-code"),
        )
        item = source_item_fixture("provenance", source_id="S30", text="ETH announcement")
        result = await classifier._score_record(
            item, "relevant", DecisionState(item=DecisionItem(source=item.source, text=item.text))
        )
    assert result.config_version == "explicit-config" and result.code_version == "explicit-code"
