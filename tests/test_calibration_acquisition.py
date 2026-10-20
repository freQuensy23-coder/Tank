"""One bounded calibration command uses production Jev inputs, never label prompts."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from click.testing import CliRunner

from tank.agents.pipeline import FILTER, ROUTE
from tank.calibration import CalibrationError, acquire
from tank.util import iso, sha256


def packet():
    rows = []
    for index in range(200):
        repeat = index % 5 != 0
        stamp = datetime(2026, 10, 18, tzinfo=UTC) + timedelta(minutes=index)
        source_id = "S31" if repeat else "S30"
        item = {
            "item_id": f"i{index}",
            "source_id": source_id,
            "source": "Independent archive",
            "author": "Reporter",
            "title": "Ethereum release",
            "text": f"Original news {index}",
            "observed_at": iso(stamp),
            "content_hash": sha256(f"content{index}"),
            "raw_sha256": sha256(f"raw{index}"),
        }
        event = {
            "event_id": f"e{index // 5}",
            "statement": f"Ethereum release {index // 5}",
            "entities": ["ethereum"],
            "tokens_mentioned": [],
            "event_state": "BUILT",
            "known_to_us_at": iso(stamp - timedelta(minutes=1)),
            "lifecycle": "TRACKED",
        }
        rows.append(
            {
                "item_id": item["item_id"],
                "source_id": source_id,
                "label_group": event["event_id"],
                "relevant": True,
                "cross_source_repeat": repeat,
                "expected_event_id": event["event_id"] if repeat else None,
                "source_item": item,
                "prefix": {
                    "events": [event] if repeat else [],
                    "sessions": [
                        {
                            "session_id": event["event_id"],
                            "event_id": event["event_id"],
                            "last_item_at": event["known_to_us_at"],
                        }
                    ]
                    if repeat
                    else [],
                },
            }
        )
    return {
        "dataset_id": "synthetic-unit-news",
        "status": "FROZEN",
        "synthetic": True,
        "labeler": "Independent unit annotation",
        "labeled_at": "2026-10-19T00:00:00Z",
        "candidate_set_description": "Complete preceding captured event/session states",
        "export_provenance": "Synthetic unit prefix export, never acceptance evidence",
        "complete_prefix_export": True,
        "items": rows,
    }


def settings():
    return SimpleNamespace(
        jev_provider="openrouter",
        openrouter_api_key="FAKE_UNIT_SECRET",
        replay=False,
        config_version="unit-config",
        code_version="unit-code",
        load_config=lambda name: {
            "registry": {"entities": [{"entity_id": "ethereum", "aliases": ["Ethereum"]}]},
            "thresholds": {"routing_window_days": 3, "routing_selection_objective": "max_f1"},
        }[name],
    )


def save(tmp_path, value):
    path = tmp_path / "packet.json"
    path.write_text(json.dumps(value))
    return path


def provider_response(request):
    body = json.loads(request.content)
    assert str(request.url) == "https://openrouter.ai/api/v1/systemone"
    assert body["model"] == "typesafe/jev-1.13"
    assert set(body) == {"state", "questions", "model"}
    name = next(iter(body["questions"]))
    assert body["questions"][name]["instructions"] == (FILTER if name == "relevant" else ROUTE)
    assert not {"expected_event_id", "label_group", "relevant", "cross_source_repeat"} & body["state"].keys()
    return httpx.Response(
        200,
        json={
            "model": "typesafe/jev-1.13-20260917",
            "usage": {"input_tokens": 10, "cost": 0.000001},
            "answers": {name: {"type": "noul", "noul": 0.8 if name == "relevant" else 0.9}},
        },
    )


@pytest.mark.asyncio
async def test_200_news_one_workflow_exact_runtime_scoring_and_resumable_evidence(tmp_path):
    source = save(tmp_path, packet())
    calls = []

    def transport(request):
        calls.append(request)
        return provider_response(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        report = await acquire(source, tmp_path / "out", settings=settings(), http=http)
        assert len(calls) == 360
        assert report["selected"] == {"filter_cutoff": 0.8, "routing_cutoff": 0.9}
        assert report["routing_selection_objective"] == "precision_recall_90"
        assert report["counts"]["items"] == 200 and report["counts"]["cross_source_repeats"] == 160
        assert report["routing_target_90_90_met"]
        assert not report["production_eligible"]  # Explicit synthetic test evidence.
        assert await acquire(source, tmp_path / "out", settings=settings(), http=http) == report
        assert len(calls) == 360
    assert len(list((tmp_path / "out" / "usage").glob("*.json"))) == 360
    assert len(list((tmp_path / "out" / "attempts").glob("*/response.json"))) == 360
    for path in (tmp_path / "out").rglob("*.json"):
        assert "FAKE_UNIT_SECRET" not in path.read_text()
    assert source.read_text() == json.dumps(packet())


@pytest.mark.asyncio
async def test_preflight_rejects_insufficient_or_future_evidence_before_any_billing(tmp_path):
    values = []
    small = packet()
    small["items"] = small["items"][:199]
    values.append(small)
    future_event = packet()
    future_event["items"][1]["prefix"]["events"][0]["known_to_us_at"] = "2027-01-01T00:00:00Z"
    values.append(future_event)
    future_session = packet()
    future_session["items"][1]["prefix"]["sessions"][0]["last_item_at"] = "2027-01-01T00:00:00Z"
    values.append(future_session)
    missing_prefix = packet()
    missing_prefix["complete_prefix_export"] = False
    values.append(missing_prefix)
    mismatch = packet()
    mismatch["items"][0]["source_item"]["item_id"] = "wrong"
    values.append(mismatch)

    def forbidden(request):
        raise AssertionError("Invalid packet reached provider")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        for index, value in enumerate(values):
            with pytest.raises(CalibrationError):
                await acquire(
                    save(tmp_path, value), tmp_path / f"out-{index}", settings=settings(), http=http
                )


@pytest.mark.asyncio
async def test_missing_true_candidate_stays_a_measured_miss_not_injected_from_label(tmp_path):
    value = packet()
    for row in value["items"]:
        row["prefix"] = {"events": [], "sessions": []}
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider_response)) as http:
        report = await acquire(save(tmp_path, value), tmp_path / "out", settings=settings(), http=http)
    assert report["selected"]["routing_cutoff"] is None
    assert report["routing_target_90_90_met"] is False
    assert report["best_measured_routing"]["metrics"]["false_negative"] == 160
    assert report["best_measured_routing"]["automatic_apply"] is False
    assert report["status"] == "REJECTED"


@pytest.mark.asyncio
async def test_provider_failure_retains_evidence_and_resume_reuses_only_successes(tmp_path):
    source = save(tmp_path, packet())
    calls = []
    failed = False

    def transport(request):
        nonlocal failed
        calls.append(request)
        if len(calls) == 3 and not failed:
            failed = True
            return httpx.Response(503, json={"error": "unit provider failure"})
        return provider_response(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await acquire(source, tmp_path / "out", settings=settings(), http=http)
        assert len(list((tmp_path / "out" / "usage").glob("*.json"))) == 2
        assert json.loads((tmp_path / "out" / "failures.jsonl").read_text())["completed_items"] == 1
        report = await acquire(source, tmp_path / "out", settings=settings(), http=http)
        assert len(calls) == 361 and report["counts"]["items"] == 200
        changed = settings()
        changed.code_version = "other-revision"
        with pytest.raises(CalibrationError, match="changed"):
            await acquire(source, tmp_path / "out", settings=changed, http=http)
        assert len(calls) == 361


def test_cli_exposes_one_acquisition_form_and_no_alternate_modes():
    from tank.cli import main

    result = CliRunner().invoke(main, ["calibrate", "--help"])
    assert result.exit_code == 0
    assert "--output-dir" in result.output
    for removed in ("--raw-items", "--history", "--response-cache", "--labels-sha256"):
        assert removed not in result.output


@pytest.mark.asyncio
async def test_parallel_route_failure_retains_other_scores_without_partial_item(tmp_path):
    import asyncio
    from copy import deepcopy

    value = packet()
    prefix = value["items"][1]["prefix"]
    for suffix in ("b", "c"):
        event = deepcopy(prefix["events"][0])
        event["event_id"] += suffix
        prefix["events"].append(event)
        prefix["sessions"].append(
            {
                "session_id": event["event_id"],
                "event_id": event["event_id"],
                "last_item_at": event["known_to_us_at"],
            }
        )
    # Distinct statements make exact wire identities distinct too.
    for event in prefix["events"]:
        event["statement"] = event["event_id"]
    source = save(tmp_path, value)
    arrived = set()
    ready = asyncio.Event()
    calls = []
    retry = False

    async def transport(request):
        calls.append(request)
        body = json.loads(request.content)
        if "event" in body["state"] and body["state"]["text"].endswith("Original news 1"):
            statement = body["state"]["event"]["statement"]
            arrived.add(statement)
            if len(arrived) == 3:
                ready.set()
            await asyncio.wait_for(ready.wait(), 1)
            if statement == "e0b" and not retry:
                return httpx.Response(503, json={"error": "unit error"})
        return provider_response(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await acquire(source, tmp_path / "out", settings=settings(), http=http)
        assert arrived == {"e0", "e0b", "e0c"}
        assert len(list((tmp_path / "out" / "usage").glob("*.json"))) == 4
        assert not (tmp_path / "out" / "rows" / "00001.json").exists()
        retry = True
        await acquire(source, tmp_path / "out", settings=settings(), http=http)
    assert len(calls) == 363
