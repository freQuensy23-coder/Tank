"""Calibration measures historical labels through typed shared service inputs."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import httpx
import pytest
from pydantic import ValidationError

from tank.agents.classification import FILTER, ROUTE
from tank.agents.jev import JevQuestion, JevState
from tank.agents.pipeline_contracts import ClassificationName
from tank.analysis.calibration_contracts import (
    CalibrationEntity,
    CalibrationEvent,
    CalibrationLabel,
    CalibrationPacket,
    CalibrationSource,
    CandidateScore,
    FilterEvaluation,
    RoutingEvaluation,
)
from tank.calibration import (
    CalibrationError,
    _Ledger,
    acquire,
    filter_cutoff,
    load_packet,
    routing_cutoff,
    routing_metrics,
)
from tank.contracts import DomainModel
from tank.contracts.json import decode_json, encode_json
from tank.settings import Settings, resolve_versions
from tank.util import iso

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def item(
    index: int,
    *,
    hours: int,
    event_id: str | None,
    source_id: str = "S31",
    relevant: bool = True,
    title: str = "Ethereum upgrade",
) -> CalibrationLabel:
    identity = f"i{index}"
    return CalibrationLabel(
        item_id=identity,
        source_id=source_id,
        clock=iso(T0 + timedelta(hours=hours)),
        relevant=relevant,
        event_id=event_id,
        source_item=CalibrationSource(
            item_id=identity,
            source_id=source_id,
            source="Archive",
            author="Desk",
            title=title,
            text=f"Body {index}",
        ),
    )


def event(event_id: str) -> CalibrationEvent:
    return CalibrationEvent(
        event_id=event_id,
        statement=f"Statement {event_id}",
        entities=[CalibrationEntity(entity_id="ethereum")],
        tokens_mentioned=[],
        event_state="PROPOSED",
    )


def packet(items: Sequence[CalibrationLabel], events: Sequence[str]) -> CalibrationPacket:
    return CalibrationPacket(
        dataset_id="unit",
        status="FROZEN",
        labeler="unit",
        labeled_at="2026-10-20T00:00:00Z",
        routing_window_days=3,
        items=list(items),
        events=[event(identity) for identity in events],
    )


def test_history_is_replayed_in_clock_order_not_list_order() -> None:
    rows, events, _ = load_packet(
        packet([item(1, hours=5, event_id="e1", source_id="S30"), item(0, hours=1, event_id="e1")], ["e1"])
    )
    assert [row.label.item_id for row in rows] == ["i0", "i1"]
    assert rows[0].expected_event_id is None and rows[1].expected_event_id == "e1"
    assert rows[1].cross_source_repeat is True
    assert events["e1"].known_to_us_at == T0 + timedelta(hours=1)


def test_labels_that_contradict_the_routing_window_are_refused() -> None:
    with pytest.raises(CalibrationError, match="routing window"):
        load_packet(packet([item(0, hours=0, event_id="e1"), item(1, hours=73, event_id="e1")], ["e1"]))
    with pytest.raises(CalibrationError, match="routed items"):
        load_packet(packet([item(0, hours=0, event_id="e1", source_id="S30", relevant=False)], ["e1"]))


def test_filter_cutoff_is_highest_value_keeping_95_percent_of_relevant() -> None:
    rows = [FilterEvaluation(relevant=True, filter_score=score / 100) for score in range(80, 100)]
    rows.append(FilterEvaluation(relevant=False, filter_score=0.99))
    assert filter_cutoff(rows) == 0.81


def test_routing_cutoff_is_highest_meeting_both_targets_or_none() -> None:
    def row(target: str | None, *candidates: tuple[str, float]) -> RoutingEvaluation:
        return RoutingEvaluation(
            item_id="x",
            routed=True,
            expected_event_id=target,
            candidates=[CandidateScore(event_id=identity, score=score) for identity, score in candidates],
        )

    rows = [row("a", ("a", 0.9), ("b", 0.4)) for _ in range(9)] + [row(None, ("c", 0.6))]
    curve = [routing_metrics(rows, cutoff) for cutoff in (0.95, 0.9, 0.6, 0.4)]
    assert routing_cutoff(curve) == 0.9
    assert routing_metrics(rows, 0.6).false_positive == 1
    assert routing_cutoff([routing_metrics(rows, 0.95)]) is None


class CapturedJevRequest(DomainModel):
    model: Literal["jev-1.13-free"]
    state: JevState
    questions: dict[ClassificationName, JevQuestion]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_acquire_scores_the_service_inputs_at_the_historical_clock(tmp_path: Path) -> None:
    items = [
        item(0, hours=0, event_id="old"),
        item(1, hours=100, event_id="e1"),
        item(2, hours=101, event_id="e1", source_id="S30"),
        item(3, hours=102, event_id=None, title="Weather report", source_id="S30", relevant=False),
    ]
    path = tmp_path / "packet.json"
    path.write_text(packet(items, ["old", "e1"]).model_dump_json(exclude_unset=True))
    calls: list[CapturedJevRequest] = []

    def jev(request: httpx.Request) -> httpx.Response:
        body = CapturedJevRequest.model_validate_json(request.content, strict=True)
        calls.append(body)
        assert str(request.url) == "https://opencode.ai/inference/systemone/v1/systemone"
        (name,) = body.questions
        assert body.questions[name].instructions == (FILTER if name == "relevant" else ROUTE)
        title = body.state.item.title
        assert title is not None
        score = 0.95 if name == "relevant" and "Weather" not in title else 0.1
        if name == "same_event":
            score = 0.92
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "usage": {"input_tokens": 9},
                "answers": {name: {"type": "noul", "noul": score}},
            },
        )

    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_bytes(
        encode_json(
            {"entities": [{"entity_id": "ethereum", "aliases": ["Ethereum"]}], "tokens": [], "x_accounts": []}
        )
    )
    (config / "thresholds.yaml").write_bytes(Settings().read_config_bytes("thresholds.yaml"))
    settings = resolve_versions(
        Settings(
            config_dir=config,
            data_dir=tmp_path / "data",
            opencode_api_key=None,
            replay=False,
            config_version="cfg",
            code_version="code",
        )
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(jev)) as http:
        result = await acquire(path, tmp_path / "out", settings=settings, http=http)
        routing_calls = [call for call in calls if "same_event" in call.questions]
        assert len(routing_calls) == 1
        candidate = routing_calls[0].state.event
        assert candidate is not None and candidate.statement == "Statement e1"
        first = len(calls)
        await acquire(path, tmp_path / "out", settings=settings, http=http)
        assert len(calls) == first
    assert result.counts.repeats == 1 and result.selected.routing_cutoff == 0.92
    assert result.filter.crypto_reaching_full_analysis == 3
    assert result.status == "FAIL" and result.rejection_reasons == ["sample_minimums"]
    assert "routing_cutoff" not in (tmp_path / "out" / "thresholds.proposed.yaml").read_text()
    usage_path = next((tmp_path / "out" / "usage").glob("*.json"))
    invalid_usage = decode_json(usage_path.read_bytes())
    assert isinstance(invalid_usage, dict)
    invalid_usage["score"] = 1.5
    usage_path.write_bytes(encode_json(invalid_usage))
    with pytest.raises(ValidationError):
        await _Ledger(usage_path.parent).get(usage_path.stem)


def test_wrong_existing_event_counts_as_both_false_positive_and_false_negative() -> None:
    result = routing_metrics(
        [
            RoutingEvaluation(
                item_id="wrong",
                routed=True,
                expected_event_id="expected",
                candidates=[CandidateScore(event_id="other", score=0.9)],
            )
        ],
        0.5,
    )
    assert result.true_positive == 0 and result.false_positive == result.false_negative == 1
    assert result.precision == result.recall == 0.0
    assert result.errors[0].predicted == "other" and result.errors[0].expected == "expected"


def test_no_selected_candidate_keeps_undefined_precision_separate_from_zero_recall() -> None:
    result = routing_metrics(
        [RoutingEvaluation(item_id="missing", routed=True, expected_event_id="expected", candidates=[])], 0.5
    )
    assert result.precision is None and result.recall == 0.0
    assert routing_cutoff([result]) is None


def test_packet_rejects_identity_mismatch_and_nonboolean_relevance() -> None:
    wire = decode_json(packet([item(0, hours=0, event_id="e1")], ["e1"]).model_dump_json())
    assert isinstance(wire, dict)
    rows = wire["items"]
    assert isinstance(rows, list) and isinstance(rows[0], dict)
    rows[0]["relevant"] = 1
    with pytest.raises(ValidationError):
        CalibrationPacket.model_validate(wire)
    rows[0]["relevant"] = True
    rows[0]["source_id"] = "S30"
    with pytest.raises(ValidationError, match="identity"):
        CalibrationPacket.model_validate(wire)


def test_frozen_reference_packet_validates_without_rewriting_its_evidence() -> None:
    import gzip

    path = Path(__file__).parents[1] / "docs/verification/v0b-calibration-2026-10-20/packet.json.gz"
    original = path.read_bytes()
    raw = gzip.decompress(original)
    packet = CalibrationPacket.model_validate_json(raw, strict=True)
    assert packet.model_dump(mode="json", exclude_unset=True) == decode_json(raw)
    assert path.read_bytes() == original
    rows, events, window = load_packet(packet)
    assert len(rows) == 1401 and len(events) == 1101 and window == timedelta(days=3)
    labeled_events = [label.event_id for label in packet.items if label.event_id is not None]
    assert sum(row.expected_event_id is not None for row in rows) == len(labeled_events) - len(
        set(labeled_events)
    )


@pytest.mark.parametrize("source_id", ["S25", "S29"])
def test_policy_dependent_source_refuses_uncaptured_filter_decision(source_id: str) -> None:
    wire = item(1, hours=0, event_id=None).source_item.model_dump()
    wire["source_id"] = source_id
    with pytest.raises(ValidationError, match="requires captured"):
        CalibrationSource.model_validate(wire)


def test_github_subtype_drives_actual_calibration_filter_policy() -> None:
    from tank.calibration import requires_filter

    wire = item(1, hours=0, event_id=None).source_item.model_dump()
    wire.update(source_id="S25", subtype="commits")
    assert requires_filter(CalibrationSource.model_validate(wire))
    wire["subtype"] = "releases"
    assert not requires_filter(CalibrationSource.model_validate(wire))


@pytest.mark.parametrize(
    "field,value",
    [("published_at", "2026-10-01T12:00:00"), ("observed_at", "1781654400000"), ("raw_sha256", "uncaptured")],
)
def test_captured_source_metadata_refuses_invalid_clock_or_digest(field: str, value: str) -> None:
    wire = item(1, hours=0, event_id=None).source_item.model_dump()
    wire[field] = value
    with pytest.raises(ValidationError):
        CalibrationSource.model_validate(wire)
