from test_event_tools import service as service

from test_event_tools import complete_assessment_fixture, seed

from tank.event_tools import LAYERS


async def test_publication_dispatch_retains_persisted_call_trigger_after_active_input_changes(service):
    """A recovered call owns its journal input even after a later timer is active."""
    extracted = await seed(service)
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
    await service.repo.put(
        "ops.agent_sessions",
        "owned-session",
        {
            "session_id": "owned-session",
            "event_id": eid,
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
            "native_turn_message_id": "original-input",
            "native_turn_retry_count": 0,
        },
    )
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
    assert result["publication_receipt"]["trigger_item_id"] == "i"
    assert result["publication_receipt"]["trigger_freshness"] == "FRESH"
    alerts = await service.repo.list("out.alerts", filters={"event_id": eid})
    assert len(alerts) == 1
    assert alerts[0]["session_id"] == "owned-session"
    assert alerts[0]["tool_call_ids"] == ["publication-call"]
