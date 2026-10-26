import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httpx
import pytest
from agent_source_fixtures import source_record_fixture
from postgres_helpers import PostgresTemplate
from test_event_tools import extraction, unknown_assessment
from test_preliminary_cards import deliver, preliminary_services

from tank.event_tools import LAYERS, EventToolService
from tank.models import Record
from tank.storage import MemoryRepository, Repository
from tank.time_precision import Timestamp, collection_clock
from tank.util import TimeInput, clock_iso, sha256

FIXTURE_CONTEXT: Record = {
    "model_id": "declared-fixture-model",
    "prompt_version": "declared-fixture-prompt",
    "role": "system",
}


@dataclass(frozen=True, slots=True)
class FrozenPublicationClock:
    actual: Timestamp

    def __call__(self, value: TimeInput | None = None, *, policy: str = "COLLECTION_ONLY") -> Timestamp:
        return collection_clock(self.actual.value if value is None else value, policy=policy)


async def store_authored_source(service: EventToolService, item_id: str, text: str) -> None:
    path = service.settings.data_dir / (item_id + ".txt")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    digest = sha256(path.read_bytes())
    published = clock_iso()
    source = source_record_fixture(
        {
            "item_id": item_id,
            "source_id": "S01",
            "text": text,
            "title": text,
            "source_class": "PRIMARY",
            "published_at": published,
            "published_prec": "EXACT",
            "published_prec_reason": None,
            "observed_at": clock_iso(),
            "url": "https://example.org/" + item_id,
            "raw_ref": str(path),
            "raw_sha256": digest,
            "content_hash": digest,
        }
    )
    await service.repo.put("ingest.source_items", item_id, source)


async def actual_material_lead(service: EventToolService) -> Record:
    await store_authored_source(service, "i", "Product announced")
    authored = extraction()
    authored.update(certainty="LOW", certainty_reason="The authored lead is not confirmed")
    authored["claims"][0].update(verified=False, verification_missing="No counterparty confirmation")
    created = await service.dispatch("record_event", authored, {**FIXTURE_CONTEXT, "session_id": "s"})
    event_id = created["event_id"]
    claims = [row async for row in service.repo.iter_all("evt.claims", filters={"event_id": event_id})]
    assert claims
    claim_id = claims[0]["claim_id"]
    assert isinstance(claim_id, str)
    assessed = await service.dispatch(
        "set_assessment",
        {
            "event_id": event_id,
            "assessment": unknown_assessment(criterion="MATERIAL", klass="MATERIAL", claim=claim_id),
        },
        FIXTURE_CONTEXT,
    )
    assert assessed["class"] == "MATERIAL"
    return {
        "event_id": event_id,
        "kind": "PRELIMINARY",
        "what": "The repository contains an integration adapter.",
        "verdict": "An integration adapter appeared",
        "why": ["It could connect the protocol to the counterparty.", "Actual use remains unknown."],
        "unconfirmed": "The counterparty has not confirmed adoption or a launch date.",
    }


async def actual_denial(service: EventToolService, event_id: str) -> Record:
    quote = "The partner denies any integration"
    await store_authored_source(service, "denial", quote)
    authored = extraction()
    authored["claims"][0].update(verified=True, quote=quote)
    await service.dispatch(
        "add_observation",
        {
            "event_id": event_id,
            "item_id": "denial",
            "link": "DENIAL",
            "certainty": "HIGH",
            "certainty_reason": "Official statement in the authored primary-source fixture",
            "claims": authored["claims"],
            "public_at": authored["public_at"],
            "freshness": "FRESH",
        },
        FIXTURE_CONTEXT,
    )
    event = await service.repo.get("evt.events", event_id)
    assert event is not None
    return event


async def promote_with_actual_analysis(service: EventToolService, event_id: str) -> None:
    event = await service.repo.get("evt.events", event_id)
    assert event is not None
    event["certainty"] = "MEDIUM"
    await service.repo.put("evt.events", event_id, event)
    claims = [row async for row in service.repo.iter_all("evt.claims", filters={"event_id": event_id})]
    assert claims
    claim_id = claims[0]["claim_id"]
    assert isinstance(claim_id, str)
    assessment = await service.dispatch(
        "set_assessment",
        {"event_id": event_id, "assessment": unknown_assessment(claim=claim_id)},
        FIXTURE_CONTEXT,
    )
    assert assessment["class"] == "MAJOR"
    mapped = await service.dispatch(
        "set_beneficiaries",
        {
            "event_id": event_id,
            "edges": [
                {
                    "entity_id": "world",
                    "token_id": "world:WLD",
                    "role": "PRIMARY",
                    "layer": "ISSUER_PRODUCT",
                    "direction": "POSITIVE",
                    "causal_chain": "Declared fixture access proposal",
                    "directionality": "BOTH",
                    "exposure_score": 6,
                    "edge_certainty": "MEDIUM",
                    "evidence": [claim_id],
                }
            ],
            "layer_reasons": {layer: "No intermediary evidence in the authored fixture" for layer in LAYERS},
        },
        FIXTURE_CONTEXT,
    )
    assert mapped["analysis_set"] == ["world:WLD"]
    truth = await service.truth.assess(event_id, "world:WLD", clock_iso())
    assert truth["data_confidence"]["class"] == "LOW"
    decision = await service.dispatch(
        "get_decision", {"event_id": event_id, "token_id": "world:WLD"}, FIXTURE_CONTEXT
    )
    assert decision["action"] == "WAIT"


async def exercise_actual_revision_eleven(
    repo: Repository,
    directory: Path,
    monkeypatch: pytest.MonkeyPatch,
    final_kind: Literal["FULL", "PRELIMINARY_DENIAL"],
) -> None:
    async with preliminary_services(repo, directory) as service:
        arguments = await actual_material_lead(service)
        event_id = arguments["event_id"]
        actual_clock = collection_clock()
        monkeypatch.setattr("tank.event_tools.collection_clock", FrozenPublicationClock(actual_clock))
        published: list[Record] = []
        for revision in range(1, 10):
            result = await service.dispatch(
                "publish_card",
                {**arguments, "what": f"The repository contains an adapter; authored revision {revision}."},
                FIXTURE_CONTEXT,
            )
            assert result["published"] and result["version"] == revision
            published.append(result)
        requests: list[Record] = []

        def telegram(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            requests.append(payload)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 100 + len(requests)}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
            await deliver(repo, service.settings, http, published[-1])
            tenth = await service.dispatch(
                "publish_card",
                {**arguments, "what": "The repository contains an adapter; authored revision 10."},
                FIXTURE_CONTEXT,
            )
            assert tenth["published"] and tenth["version"] == 10
            alert_id = tenth["alert_id"]
            ninth_row = await repo.get("out.alerts", f"{alert_id}:9")
            tenth_row = await repo.get("out.alerts", f"{alert_id}:10")
            assert ninth_row is not None and tenth_row is not None
            assert ninth_row["generated_at"] == tenth_row["generated_at"] == actual_clock.value
            originals = (sha256(ninth_row), sha256(tenth_row))
            if final_kind == "FULL":
                await promote_with_actual_analysis(service, event_id)
                final_arguments = {
                    **arguments,
                    "kind": "FULL",
                    "what": "The authored current analysis remains uncertain.",
                }
                del final_arguments["unconfirmed"]
            else:
                invalidated = await actual_denial(service, event_id)
                assert invalidated["lifecycle"] == "INVALIDATED"
                final_arguments = {
                    **arguments,
                    "verdict": "The partner denied the integration",
                    "why": ["The partner officially denied the integration."],
                }
                del final_arguments["unconfirmed"]
            eleventh = await service.dispatch("publish_card", final_arguments, FIXTURE_CONTEXT)
            assert eleventh["published"] and eleventh["version"] == 11
            assert eleventh["alert_id"] == alert_id
            final_row = await repo.get("out.alerts", f"{alert_id}:11")
            assert final_row is not None and final_row["generated_at"] == actual_clock.value
            assert (
                sha256(await repo.get("out.alerts", f"{alert_id}:9")),
                sha256(await repo.get("out.alerts", f"{alert_id}:10")),
            ) == originals
            await deliver(repo, service.settings, http, eleventh)
            assert len(requests) == 2
            assert requests[0].get("reply_parameters") is None
            assert requests[1]["reply_parameters"]["message_id"] == 101
            if final_kind == "FULL":
                denied = await service.dispatch("publish_card", arguments, FIXTURE_CONTEXT)
                assert denied["reason"] == "ORDINARY_CARD_EXISTS"
                first = await service.first_full_alert(event_id)
                assert first is not None and first["version"] == 11
            else:
                assert final_row["terminal_correction"] is True and final_row["delivery_immediate"] is True
                event = await repo.get("evt.events", event_id)
                assert event is not None and event["terminal_alert_version"] == 11
                assert event["session_finalized_at"]


@pytest.mark.parametrize("final_kind", ["FULL", "PRELIMINARY_DENIAL"])
async def test_actual_memory_publisher_equal_clock_revision_eleven_preserves_history_and_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    final_kind: Literal["FULL", "PRELIMINARY_DENIAL"],
) -> None:
    repo = MemoryRepository()
    await repo.open()
    try:
        await exercise_actual_revision_eleven(repo, tmp_path, monkeypatch, final_kind)
    finally:
        await repo.close()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("final_kind", ["FULL", "PRELIMINARY_DENIAL"])
async def test_actual_postgres_publisher_equal_clock_revision_eleven_preserves_history_and_thread(
    tmp_path: Path,
    postgres_template: PostgresTemplate,
    monkeypatch: pytest.MonkeyPatch,
    final_kind: Literal["FULL", "PRELIMINARY_DENIAL"],
) -> None:
    url = os.environ["TANK_TEST_DATABASE_URL"]
    async with postgres_template.repository(url) as repo:
        await exercise_actual_revision_eleven(repo, tmp_path, monkeypatch, final_kind)
