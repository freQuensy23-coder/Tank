import os
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Literal

import pytest
from agent_source_fixtures import source_record_fixture
from test_event_tools import complete_assessment_fixture, extraction
from test_event_tools import repository_lifetime as repository_lifetime
from test_event_tools import service as service

from tank.event_tools import LAYERS, EventToolService
from tank.time_precision import collection_clock

if TYPE_CHECKING:
    from postgres_helpers import PostgresTemplate


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.parametrize(
    "ownership",
    [
        "matching",
        "missing",
        "ambiguous",
        "foreign_session",
        "foreign_call",
        "attempt_mismatch",
        "missing_item",
        "non_source",
    ],
)
async def test_publication_dispatch_retains_persisted_call_trigger_after_active_input_changes(
    service: EventToolService,
    ownership: Literal[
        "matching",
        "missing",
        "ambiguous",
        "foreign_session",
        "foreign_call",
        "attempt_mismatch",
        "missing_item",
        "non_source",
    ],
    backend: Literal["memory", "postgres"],
    postgres_template: "PostgresTemplate",
    repository_lifetime: AsyncExitStack,
) -> None:
    """A recovered call owns its journal input even after a later timer is active."""
    if backend == "postgres":
        url = os.environ["TANK_TEST_DATABASE_URL"]
        repo = await repository_lifetime.enter_async_context(postgres_template.repository(url))
        service = EventToolService(
            repo,
            service.settings,
            service.truth,
            service.engine,
            versions=service.versions,
            scheduler=service.scheduler,
            pages=service.pages,
        )
    extracted = extraction()
    await service.repo.put(
        "ingest.source_items",
        "i",
        source_record_fixture(
            {
                "item_id": "i",
                "source_id": "S01",
                "text": "Product announced",
                "source_class": "PRIMARY",
                "published_prec": "COLLECTION_ONLY",
            }
        ),
    )
    created = await service.dispatch("record_event", extracted, {})
    eid = created["event_id"]
    ems = eid + "-ems"
    event = await service.repo.get("evt.events", eid)
    await service.repo.put("evt.events", eid, {**event, "current_ems_id": ems})
    await service.repo.put("evt.ems_assessments", ems, complete_assessment_fixture(ems, eid))
    await service.dispatch(
        "set_beneficiaries",
        {
            "event_id": eid,
            "edges": [],
            "layer_reasons": {layer: "No supported token link is asserted." for layer in LAYERS},
        },
        {},
    )
    publication_arguments = {
        "event_id": eid,
        "kind": "FULL",
        "what": "Product announced",
        "verdict": "Official announcement",
        "why": ["Official fact", "Defined access"],
    }
    stamp = collection_clock()
    await service.repo.put(
        "ops.agent_sessions",
        "owned-session",
        {
            "session_id": "owned-session",
            "event_id": eid,
            "opened_by_item": "i",
            "agent_version": None,
            "agent_version_reason": "Synthetic scope does not report a native agent version",
            "status": "OPEN",
            "last_event_seq": 0,
            "last_item_at": None,
            "last_item_at_reason": "Synthetic scope has no recorded accepted-input occurrence",
            **stamp.fields("opened_at"),
            "active_message_id": "later-timer",
            "message_log": [
                {
                    "message_id": "original-input",
                    "retry_count": 0,
                    "payload": {"kind": "NEW_ITEM", "item_id": "i"},
                },
                {"message_id": "later-timer", "payload": {"kind": "CONDITION_MET"}},
            ],
        },
    )
    await service.repo.put(
        "ops.tool_calls",
        "owned-session:publication-call",
        {
            "session_id": "owned-session",
            "call_id": "publication-call",
            "tool": "publish_card",
            "status": "PENDING",
            "input": publication_arguments,
            **stamp.fields("requested_at"),
            "native_turn_message_id": "original-input",
            "native_turn_retry_count": 0,
        },
    )
    prior_deliveries = await service.repo.list("ops.jobs", filters={"type": "DELIVER"})
    if ownership == "non_source":
        session = await service.repo.get("ops.agent_sessions", "owned-session")
        session["message_log"][0]["payload"] = {"kind": "CONDITION_MET"}
        await service.repo.put("ops.agent_sessions", "owned-session", session)
    if ownership not in {"matching", "non_source"}:
        session = await service.repo.get("ops.agent_sessions", "owned-session")
        call = await service.repo.get("ops.tool_calls", "owned-session:publication-call")
        if ownership == "missing_item":
            session["message_log"][0]["payload"].pop("item_id")
        elif ownership == "missing":
            call.pop("native_turn_message_id")
        elif ownership == "ambiguous":
            session["message_log"].append(session["message_log"][0])
        elif ownership == "foreign_session":
            await service.repo.put(
                "ops.agent_sessions",
                "foreign-session",
                {**session, "session_id": "foreign-session", "event_id": None},
            )
            call["session_id"] = "foreign-session"
        elif ownership == "foreign_call":
            call["call_id"] = "foreign-call"
        else:
            session["message_log"][0]["retry_count"] = 1
        await service.repo.put("ops.agent_sessions", "owned-session", session)
        await service.repo.put("ops.tool_calls", "owned-session:publication-call", call)
        with pytest.raises(ValueError):
            await service.dispatch(
                "publish_card",
                {
                    "event_id": eid,
                    "kind": "FULL",
                    "what": "Product announced",
                    "verdict": "Official announcement",
                    "why": ["Official fact", "Defined access"],
                },
                {"session_id": "owned-session", "call_id": "publication-call", "role": "analyst"},
            )
        assert await service.repo.list("out.alerts", filters={"event_id": eid}) == []
        assert await service.repo.list("ops.jobs", filters={"type": "DELIVER"}) == prior_deliveries
        return
    result = await service.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
            "what": "Product announced",
            "verdict": "Official announcement",
            "why": ["Official fact", "Defined access"],
        },
        {"session_id": "owned-session", "call_id": "publication-call", "role": "analyst"},
    )
    assert result["published"] is True
    if ownership == "matching":
        assert result["publication_receipt"]["trigger_item_id"] == "i"
        assert result["publication_receipt"]["trigger_freshness"] == "FRESH"
    else:
        assert "trigger_item_id" not in result["publication_receipt"]
        assert "trigger_freshness" not in result["publication_receipt"]
    alerts = await service.repo.list("out.alerts", filters={"event_id": eid})
    assert len(alerts) == 1
    assert alerts[0]["session_id"] == "owned-session"
    deliveries = await service.repo.list("ops.jobs", filters={"type": "DELIVER"})
    assert sum(row["payload"].get("alert_id") == result["alert_id"] for row in deliveries) == 1
