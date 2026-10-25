"""Readable reference domains and strict ownership facts at the context boundary."""

import copy
from typing import Literal

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.context import ModelContext
from tank.agents.context_records import ClaimReference, DocumentReference
from tank.storage import MemoryRepository


class ReferenceConfiguration:
    def load_config(self, name: str) -> dict[str, JsonValue]:
        if name == "registry":
            return {"tokens": [], "entities": [], "instruments": []}
        if name == "sources":
            return {"sources": []}
        raise KeyError(name)

    def read_config_json(self, name: str) -> JsonValue:
        raise AssertionError("Reference projection never loads tool schemas")


class ReferenceRepository(MemoryRepository):
    def __init__(self) -> None:
        super().__init__()
        self.reads: list[tuple[str, str]] = []

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None:
        self.reads.append((collection, key))
        return await super().get(collection, key)


def boundary(repo: MemoryRepository) -> ModelContext:
    return ModelContext(repo, ReferenceConfiguration(), {"session_id": "session", "event_id": "event"})


@pytest.mark.asyncio
async def test_reference_cache_retains_closed_records_and_real_unknown_absence() -> None:
    repo = ReferenceRepository()
    source: dict[str, JsonValue] = {
        "item_id": "source",
        "url": "https://example.invalid/source",
        "text": "Original captured text",
    }
    claim: dict[str, JsonValue] = {
        "claim_id": "claim",
        "event_id": "event",
        "quote": "Original captured text",
        "verified": False,
        "evidence": [{"item_id": "source", "quote": "Original captured text"}],
    }
    await repo.put("ingest.source_items", "source", source)
    await repo.put("evt.claims", "claim", claim)
    original = copy.deepcopy(claim)
    context = boundary(repo)
    reference = await context.reference("claim", "claim")
    assert isinstance(reference, ClaimReference)
    assert reference.subject is None and reference.verified is False
    assert reference.sources is not None and len(reference.sources) == 1
    document = reference.sources[0]
    assert isinstance(document, DocumentReference)
    assert document.title is None and document.observed_at is None
    assert document.url == source["url"] and document.excerpt is None
    assert await context.reference("claim", "claim") is reference
    assert repo.reads.count(("evt.claims", "claim")) == 1
    assert repo.reads.count(("ingest.source_items", "source")) == 1
    assert await repo.get("evt.claims", "claim") == original
    unavailable = await context.describe_claim({"quote": "Historical quote"})
    assert unavailable.sources is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["claim", "condition"])
async def test_missing_reference_event_ownership_refuses_instead_of_inheriting_current(
    kind: Literal["claim", "condition"],
) -> None:
    repo = MemoryRepository()
    table = "evt.claims" if kind == "claim" else "ana.watch_conditions"
    await repo.put(table, "unowned", {"quote": "A historical record without declared ownership"})
    with pytest.raises(ValidationError, match="event_id"):
        await boundary(repo).check("set_assessment", "$.evidence[0]", kind, "unowned")


@pytest.mark.asyncio
async def test_projection_preserves_declared_empty_identity_lists_and_refuses_wrong_scalars() -> None:
    context = boundary(MemoryRepository())
    assert await context.project({"token_ids": []}) == {"tokens": []}
    invalid_identities: dict[str, JsonValue] = {"token_ids": [42]}
    with pytest.raises(ValueError, match="nonempty string"):
        await context.project(invalid_identities)
    with pytest.raises(ValueError, match="nonempty string"):
        await context.project({"item_id": ""})
    with pytest.raises(ValidationError, match="url"):
        await context.project({"item_id": "item", "url": 42})


@pytest.mark.asyncio
async def test_text_excerpt_requires_real_text_and_does_not_invent_title_or_time() -> None:
    context = boundary(MemoryRepository())
    reference = await context.describe("item", {"text": "Captured content"})
    assert isinstance(reference, DocumentReference)
    assert reference.excerpt == "Captured content"
    assert reference.url is None and reference.title is None and reference.observed_at is None
    with pytest.raises(ValueError, match="text must be a string"):
        await context.describe("item", {"text": {"unrecognized": "payload"}})
