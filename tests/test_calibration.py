"""`tank calibrate`: one offline pass with the service's exact Jev inputs (03 §3.1, 01 V0-B)."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest

from tank.agents.pipeline import FILTER, ROUTE
from tank.calibration import (
    CalibrationError,
    acquire,
    filter_cutoff,
    load_packet,
    routing_cutoff,
    routing_metrics,
)
from tank.util import iso

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def item(index, *, hours, event_id, source_id="S31", relevant=True, title="Ethereum upgrade"):
    return {
        "item_id": f"i{index}",
        "source_id": source_id,
        "clock": iso(T0 + timedelta(hours=hours)),
        "relevant": relevant,
        "event_id": event_id,
        "source_item": {
            "item_id": f"i{index}",
            "source_id": source_id,
            "source": "Archive",
            "author": "Desk",
            "title": title,
            "text": f"Body {index}",
        },
    }


def event(event_id):
    return {
        "event_id": event_id,
        "statement": f"Statement {event_id}",
        "entities": [{"entity_id": "ethereum"}],
        "tokens_mentioned": [],
        "event_state": "PROPOSED",
    }


def packet(items, events):
    return {
        "dataset_id": "unit",
        "status": "FROZEN",
        "labeler": "unit",
        "labeled_at": "2026-10-20T00:00:00Z",
        "routing_window_days": 3,
        "items": items,
        "events": [event(e) for e in events],
    }


def test_history_is_replayed_in_clock_order_not_list_order():
    rows, events, _ = load_packet(
        packet([item(1, hours=5, event_id="e1", source_id="S30"), item(0, hours=1, event_id="e1")], ["e1"])
    )
    assert [r["item_id"] for r in rows] == ["i0", "i1"]
    assert rows[0]["expected_event_id"] is None and rows[1]["expected_event_id"] == "e1"
    assert rows[1]["cross_source_repeat"] is True
    assert events["e1"]["known_to_us_at"] == iso(T0 + timedelta(hours=1))


def test_labels_that_contradict_the_routing_window_are_refused():
    with pytest.raises(CalibrationError, match="routing window"):
        load_packet(packet([item(0, hours=0, event_id="e1"), item(1, hours=73, event_id="e1")], ["e1"]))
    with pytest.raises(CalibrationError, match="routed items"):
        load_packet(packet([item(0, hours=0, event_id="e1", source_id="S30", relevant=False)], ["e1"]))


def test_filter_cutoff_is_highest_value_keeping_95_percent_of_relevant():
    rows = [{"relevant": True, "filter_score": s / 100} for s in range(80, 100)]
    rows += [{"relevant": False, "filter_score": 0.99}]
    assert filter_cutoff(rows) == 0.81


def test_routing_cutoff_is_highest_meeting_both_targets_or_none():
    def row(target, *candidates):
        return {
            "item_id": "x",
            "routed": True,
            "expected_event_id": target,
            "candidates": [{"event_id": e, "score": s} for e, s in candidates],
        }

    rows = [row("a", ("a", 0.9), ("b", 0.4)) for _ in range(9)] + [row(None, ("c", 0.6))]
    curve = [routing_metrics(rows, c) for c in (0.95, 0.9, 0.6, 0.4)]
    assert routing_cutoff(curve) == 0.9
    assert routing_metrics(rows, 0.6)["false_positive"] == 1
    assert routing_cutoff([routing_metrics(rows, 0.95)]) is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_acquire_scores_the_service_inputs_at_the_historical_clock(tmp_path):
    items = [
        item(0, hours=0, event_id="old"),
        item(1, hours=100, event_id="e1"),
        item(2, hours=101, event_id="e1", source_id="S30"),
        item(3, hours=102, event_id=None, title="Weather report", source_id="S30", relevant=False),
    ]
    path = tmp_path / "packet.json"
    path.write_text(json.dumps(packet(items, ["old", "e1"])))
    calls = []

    def jev(request):
        body = json.loads(request.content)
        calls.append(body)
        assert (
            str(request.url)
            == "https://alexeym-4644-resource.services.ai.azure.com/providers/microsoft/v1/systemone"
        )
        assert set(body) == {"model", "state", "questions"} and body["model"] == "decision-1"
        name = next(iter(body["questions"]))
        assert body["questions"][name]["instructions"] == (FILTER if name == "relevant" else ROUTE)
        assert set(body["state"]) <= {"item", "event"}
        assert not {"relevant", "event_id", "expected_event_id"} & body["state"]["item"].keys()
        score = 0.95 if name == "relevant" and "Weather" not in body["state"]["item"]["title"] else 0.1
        if name == "same_event":
            score = 0.92
        return httpx.Response(
            200,
            json={
                "model": "microsoft-decision-1",
                "usage": {"input_tokens": 9},
                "answers": {name: {"type": "noul", "noul": score}},
            },
        )

    settings = SimpleNamespace(
        foundry_api_key="FAKE",
        replay=False,
        config_version="cfg",
        code_version="code",
        load_config=lambda name: {
            "registry": {"entities": [{"entity_id": "ethereum", "aliases": ["Ethereum"]}]},
            "thresholds": {"routing_window_days": 3},
        }[name],
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(jev)) as http:
        report = await acquire(path, tmp_path / "out", settings=settings, http=http)
        routing_calls = [c for c in calls if "same_event" in c["questions"]]

        assert len(routing_calls) == 1 and routing_calls[0]["state"]["event"]["statement"] == "Statement e1"
        first = len(calls)
        await acquire(path, tmp_path / "out", settings=settings, http=http)
        assert len(calls) == first
    assert report["counts"]["repeats"] == 1 and report["selected"]["routing_cutoff"] == 0.92
    assert report["filter"]["crypto_reaching_full_analysis"] == 3

    assert report["status"] == "FAIL" and report["rejection_reasons"] == ["sample_minimums"]
    assert "routing_cutoff" not in (tmp_path / "out" / "thresholds.proposed.yaml").read_text()
