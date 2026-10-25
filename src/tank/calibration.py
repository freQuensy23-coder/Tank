"""Offline calibration of the same typed classification and routing use cases."""

import asyncio
import hashlib
import math
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol

import httpx
import yaml
from pydantic import JsonValue, ValidationError, field_validator

from tank.agents.classification import JEV_PROMPT_VERSION, ClassificationSettings, JevClassifier
from tank.agents.jev import JevState, binding
from tank.agents.pipeline_contracts import (
    ClassificationName,
    JevScoreRecord,
    RoutingCandidate,
    RoutingRegistry,
    RoutingSession,
)
from tank.agents.routing import RoutingItem, RoutingSelector, item_state
from tank.analysis.calibration_contracts import (
    BestMeasuredRouting,
    CalibrationChecks,
    CalibrationCounts,
    CalibrationEvent,
    CalibrationGeneration,
    CalibrationHistory,
    CalibrationLabel,
    CalibrationPacket,
    CalibrationRejection,
    CalibrationReport,
    CalibrationRow,
    CalibrationSource,
    CandidateScore,
    FailedThresholdProposal,
    FilterEvaluation,
    FilterMetrics,
    HistoricalCalibrationEvent,
    PassedThresholdProposal,
    RoutingError,
    RoutingEvaluation,
    RoutingMetrics,
    RoutingSummary,
    ScoredCalibrationRow,
    ScoredCalibrationWire,
    SelectedThresholds,
)
from tank.contracts.configuration import Thresholds
from tank.contracts.json import decode_json, encode_json
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import Collection
from tank.contracts.source_items import PendingRelevance
from tank.settings import Settings, resolve_versions
from tank.sources.policy import initial_relevance
from tank.storage import MemoryRepository
from tank.util import iso, parse_time, sha256

MIN_ITEMS = 200
MIN_REPEATS = 30
MIN_CROSS_SOURCE_REPEATS = 10
FILTER_RECALL = 0.95
ROUTING_TARGET = 0.90
CONCURRENCY = 8
JEV_ENDPOINT: Literal["https://opencode.ai/inference/systemone/v1/systemone"] = (
    "https://opencode.ai/inference/systemone/v1/systemone"
)
_LABELS = Collection("calibration.labels", CalibrationLabel)
_CANDIDATE_SCORES = Collection("calibration.candidate_scores", CandidateScore)
_SCORED = Collection("calibration.scored", ScoredCalibrationWire)
_ROUTING_EVENTS = Collection("calibration.routing_events", RoutingCandidate)


def _classification_endpoint(
    base_url: str,
) -> Literal["https://opencode.ai/inference/systemone/v1/systemone"]:
    if base_url + "/v1/systemone" != JEV_ENDPOINT:
        raise CalibrationError("Unsupported calibration classification endpoint")
    return JEV_ENDPOINT


class CalibrationError(ValueError):
    """Invalid or insufficient labeled calibration packet."""


class CalibrationSettings(ClassificationSettings, Protocol):
    def require_versions(self) -> ResolvedVersionIdentity: ...

    def thresholds(self) -> Thresholds: ...

    def load_config(self, name: str) -> dict[str, JsonValue]: ...


def requires_filter(item: CalibrationSource) -> bool:
    policy = initial_relevance(item.source_id, item.subtype, item.author_role)
    if (
        isinstance(policy, PendingRelevance)
        and policy.pending_reason == "SOURCE_INGESTION_POLICY_NOT_CAPTURED"
    ):
        raise CalibrationError(f"{item.item_id}: source outside S01–S34")
    return policy.decision != "ALWAYS"


def load_packet(
    packet: CalibrationPacket,
) -> tuple[list[CalibrationRow], dict[str, HistoricalCalibrationEvent], timedelta]:
    events = {event.event_id: event for event in packet.events}
    window = timedelta(days=packet.routing_window_days)
    history: dict[str, CalibrationHistory] = {}
    ordered = sorted(enumerate(packet.items), key=lambda entry: (parse_time(entry[1].clock), entry[0]))
    rows = []
    for index, label in ordered:
        at = parse_time(label.clock)
        filtered = requires_filter(label.source_item)
        routed = label.relevant or not filtered
        event_id = label.event_id
        expected: str | None = None
        cross_source: bool | None = None
        if event_id is not None:
            if not routed or event_id not in events:
                raise CalibrationError(
                    f"{label.item_id}: only routed items (crypto or ALWAYS) carry a known event_id"
                )
            if event_id in history:
                previous = history[event_id]
                if at - previous.last_at > window:
                    raise CalibrationError(
                        f"{label.item_id}: joins {event_id} after the routing window (FR-E-02)"
                    )
                expected = event_id
                cross_source = label.source_id not in previous.sources
                history[event_id] = CalibrationHistory(
                    known_at=previous.known_at,
                    last_at=at,
                    sources=previous.sources | frozenset({label.source_id}),
                )
            else:
                history[event_id] = CalibrationHistory(
                    known_at=at, last_at=at, sources=frozenset({label.source_id})
                )
        rows.append(
            CalibrationRow(
                label=label,
                index=index,
                at=at,
                filtered=filtered,
                routed=routed,
                expected_event_id=expected,
                cross_source_repeat=cross_source,
            )
        )
    historical = {
        event_id: HistoricalCalibrationEvent(
            label=event, known_to_us_at=history[event_id].known_at if event_id in history else None
        )
        for event_id, event in events.items()
    }
    return rows, historical, window


def filter_cutoff(rows: Sequence[FilterEvaluation]) -> float:
    relevant = [row.filter_score for row in rows if row.relevant]
    if not relevant:
        raise CalibrationError("No items labeled relevant")
    return max(
        cutoff
        for cutoff in {0.0, *relevant}
        if sum(score >= cutoff for score in relevant) >= FILTER_RECALL * len(relevant)
    )


def routing_metrics(rows: Sequence[RoutingEvaluation], cutoff: float) -> RoutingMetrics:
    if not math.isfinite(cutoff) or not 0 <= cutoff <= 1:
        raise CalibrationError("Routing cutoff requires a finite probability")
    tp = fp = fn = 0
    errors = []
    for row in rows:
        if not row.routed:
            continue
        best: str | None = None
        best_score: float | None = None
        for candidate in row.candidates:
            if candidate.score >= cutoff and (best_score is None or candidate.score > best_score):
                best, best_score = candidate.event_id, candidate.score
        target = row.expected_event_id
        tp += best is not None and best == target
        fp += best is not None and best != target
        fn += target is not None and best != target
        if best != target:
            errors.append(
                RoutingError(item_id=row.item_id, predicted=best, expected=target, predicted_score=best_score)
            )
    return RoutingMetrics(
        cutoff=cutoff,
        true_positive=tp,
        false_positive=fp,
        false_negative=fn,
        precision=tp / (tp + fp) if tp + fp else None,
        recall=tp / (tp + fn) if tp + fn else None,
        errors=errors,
    )


def routing_cutoff(curve: Sequence[RoutingSummary]) -> float | None:
    passing = [
        metric.cutoff
        for metric in curve
        if metric.precision is not None
        and metric.recall is not None
        and metric.precision >= ROUTING_TARGET
        and metric.recall >= ROUTING_TARGET
    ]
    return max(passing) if passing else None


def report(
    packet: CalibrationPacket, rows: Sequence[ScoredCalibrationRow], generation: CalibrationGeneration
) -> CalibrationReport:
    counts = CalibrationCounts(
        items=len(rows),
        filter_path_items=sum(row.row.filtered for row in rows),
        relevant=sum(row.row.label.relevant for row in rows),
        routed_items=sum(row.row.routed for row in rows),
        repeats=sum(row.row.expected_event_id is not None for row in rows),
        cross_source_repeats=sum(row.row.cross_source_repeat is True for row in rows),
        candidate_pairs=sum(len(row.candidates) for row in rows),
        sources=sorted({row.row.label.source_id for row in rows}),
    )
    f_cut = filter_cutoff([row.filter_evaluation() for row in rows])
    crypto = [row for row in rows if row.row.label.relevant]
    reached = sum(not row.row.filtered or row.filter_score >= f_cut for row in crypto)
    cutoffs = sorted({0.0, *(candidate.score for row in rows for candidate in row.candidates)}, reverse=True)
    evaluation = [row.routing_evaluation() for row in rows]
    curve = [routing_metrics(evaluation, cutoff) for cutoff in cutoffs]
    r_cut = routing_cutoff(curve)
    selected = next((metric for metric in curve if metric.cutoff == r_cut), None)
    checks = CalibrationChecks(
        sample_minimums=counts.items >= MIN_ITEMS
        and counts.repeats >= MIN_REPEATS
        and counts.cross_source_repeats >= MIN_CROSS_SOURCE_REPEATS,
        crypto_reach_full_analysis_95=reached >= FILTER_RECALL * len(crypto),
        routing_precision_recall_90=r_cut is not None,
    )
    reasons: list[CalibrationRejection] = []
    if not checks.sample_minimums:
        reasons.append("sample_minimums")
    if not checks.crypto_reach_full_analysis_95:
        reasons.append("crypto_reach_full_analysis_95")
    if not checks.routing_precision_recall_90:
        reasons.append("routing_precision_recall_90")
    best_f1 = max(
        curve,
        key=lambda metric: (
            2
            * metric.true_positive
            / max(1, 2 * metric.true_positive + metric.false_positive + metric.false_negative),
            metric.cutoff,
        ),
    )
    return CalibrationReport(
        dataset_id=packet.dataset_id,
        generation=generation,
        status="FAIL" if reasons else "PASS",
        rejection_reasons=reasons,
        counts=counts,
        selected=SelectedThresholds(filter_cutoff=f_cut, routing_cutoff=r_cut),
        routing_target_90_90_met=r_cut is not None,
        v0b_checks=checks,
        filter=FilterMetrics(
            cutoff=f_cut,
            relevant_passed=sum(row.filter_score >= f_cut for row in crypto),
            relevant=len(crypto),
            crypto_reaching_full_analysis=reached,
            filter_path_irrelevant_skipped=sum(
                row.row.filtered and not row.row.label.relevant and row.filter_score < f_cut for row in rows
            ),
            filter_path_irrelevant=sum(row.row.filtered and not row.row.label.relevant for row in rows),
        ),
        routing=selected,
        best_measured_routing=BestMeasuredRouting(
            cutoff=best_f1.cutoff, metrics=best_f1, automatic_apply=False
        ),
        routing_curve=[metric.summary() for metric in curve],
    )


class _Ledger:
    """Content-addressed immutable score records at the filesystem boundary."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    async def get(self, key: str) -> JevScoreRecord | None:
        path = self.root / (key + ".json")
        if not path.exists():
            return None
        record = JevScoreRecord.model_validate_json(path.read_bytes(), strict=True)
        if record.usage_id != key:
            raise CalibrationError("Cached classification identity differs from its durable filename")
        return record

    async def put(self, key: str, value: JevScoreRecord, *, immutable: bool = False) -> JevScoreRecord:
        if not immutable:
            raise CalibrationError("Calibration classification records require immutable writes")
        if value.usage_id != key:
            raise CalibrationError("Classification identity differs from its durable filename")
        existing = await self.get(key)
        if existing is not None:
            return existing
        path = self.root / (key + ".json")
        temporary = path.with_suffix(".tmp")
        temporary.write_text(value.model_dump_json())
        temporary.rename(path)
        stored = await self.get(key)
        if stored is None:
            raise CalibrationError("Classification ledger write has no durable record")
        return stored


class _CalibrationSession(RoutingSession):
    event_id: str
    status: Literal["OPEN"]
    last_item_at: str

    @field_validator("last_item_at")
    @classmethod
    def valid_clock(cls, value: str) -> str:
        parse_time(value)
        return value


_ROUTING_SESSIONS = Collection("calibration.routing_sessions", _CalibrationSession)


def _routing_item(item: CalibrationSource) -> RoutingItem:
    return RoutingItem(
        item_id=item.item_id,
        source=item.source,
        author=item.author,
        title=item.title,
        text=item.text,
        quoted=item.quoted,
        author_organization=item.author_organization,
        identity_hints=item.identity_hints,
        monitored_author=item.monitored_author,
    )


def _routing_candidate(event: CalibrationEvent) -> RoutingCandidate:
    return RoutingCandidate(
        event_id=event.event_id,
        statement=event.statement,
        lifecycle="DETECTED",
        entities=[entity if isinstance(entity, str) else entity.entity_id for entity in event.entities],
        tokens_mentioned=event.tokens_mentioned,
        event_state=event.event_state,
    )


def _scored_wire(row: ScoredCalibrationRow) -> ScoredCalibrationWire:
    label = _LABELS.encode(row.row.label, exclude_unset=True)
    del label["source_item"]
    return ScoredCalibrationWire.model_validate(
        {
            **label,
            "index": row.row.index,
            "at": iso(row.row.at),
            "filtered": row.row.filtered,
            "routed": row.row.routed,
            "expected_event_id": row.row.expected_event_id,
            "cross_source_repeat": row.row.cross_source_repeat,
            "filter_score": row.filter_score,
            "candidates": [_CANDIDATE_SCORES.encode(candidate) for candidate in row.candidates],
        }
    )


async def acquire(
    input_path: str | Path,
    output_dir: str | Path,
    *,
    settings: CalibrationSettings | None = None,
    http: httpx.AsyncClient | None = None,
) -> CalibrationReport:
    """Score one frozen packet and write validated report/proposal artifacts."""
    raw = Path(input_path).read_bytes()
    try:
        packet = CalibrationPacket.model_validate_json(raw, strict=True)
    except ValidationError as error:
        raise CalibrationError(str(error)) from error
    rows, events, window = load_packet(packet)
    configuration = resolve_versions(Settings()) if settings is None else settings
    thresholds = configuration.thresholds()
    if thresholds.routing_window_days != window.days:
        raise CalibrationError("Packet routing window differs from config routing_window_days")
    versions = configuration.require_versions()
    registry_wire = configuration.load_config("registry")
    registry = RoutingRegistry.from_wire(registry_wire)
    pinned = binding(configuration.opencode_api_key)
    generation = CalibrationGeneration(
        packet_sha256=hashlib.sha256(raw).hexdigest(),
        code_version=versions.code_version,
        config_version=versions.config_version,
        prompt_version=JEV_PROMPT_VERSION,
        registry_sha256=sha256(registry_wire),
        routing_window_days=window.days,
        jev_endpoint=_classification_endpoint(pinned.base_url),
        jev_request_model=pinned.request_model,
        jev_resolved_models=list(pinned.resolved_models),
    )
    output = Path(output_dir)
    owned = http is None
    client = httpx.AsyncClient(timeout=30) if http is None else http
    scorer = JevClassifier(_Ledger(output / "usage"), configuration, client, versions=versions)
    repository = MemoryRepository()
    selector = RoutingSelector(repository, registry)
    capacity = asyncio.Semaphore(CONCURRENCY)

    async def score(item: RoutingItem, name: ClassificationName, state: JevState) -> float:
        async with capacity:
            return await scorer.score(item, name, state)

    async def candidates(row: CalibrationRow, sessions: dict[str, datetime]) -> tuple[str, ...]:
        for event_id, last_at in sessions.items():
            if last_at >= row.at - window:
                await repository.put(
                    "evt.events", event_id, _ROUTING_EVENTS.encode(_routing_candidate(events[event_id].label))
                )
                await repository.put(
                    "ops.agent_sessions",
                    event_id,
                    _ROUTING_SESSIONS.encode(
                        _CalibrationSession(event_id=event_id, status="OPEN", last_item_at=iso(last_at))
                    ),
                )
        found = await selector.candidate_events(
            selector.identities(_routing_item(row.label.source_item)), iso(row.at - window), row.at
        )
        return tuple(event.event_id for event in found)

    async def score_row(row: CalibrationRow, candidate_ids: tuple[str, ...]) -> ScoredCalibrationRow:
        item = _routing_item(row.label.source_item)
        filtered_score = await score(item, "relevant", item_state(item))
        values = await asyncio.gather(
            *(
                score(
                    item,
                    "same_event",
                    selector.routing_state(item, _routing_candidate(events[event_id].label)),
                )
                for event_id in candidate_ids
            )
        )
        return ScoredCalibrationRow(
            row=row,
            filter_score=filtered_score,
            candidates=[
                CandidateScore(event_id=event_id, score=value)
                for event_id, value in zip(candidate_ids, values, strict=True)
            ],
        )

    try:
        sessions: dict[str, datetime] = {}
        candidate_sets: dict[str, tuple[str, ...]] = {}
        for row in rows:
            candidate_sets[row.label.item_id] = await candidates(row, sessions) if row.routed else ()
            if row.label.event_id is not None:
                sessions[row.label.event_id] = row.at
        tasks = [asyncio.create_task(score_row(row, candidate_sets[row.label.item_id])) for row in rows]
        try:
            scored = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    finally:
        if owned:
            await client.aclose()
    result = report(packet, scored, generation)
    output.mkdir(parents=True, exist_ok=True)
    scored_documents: list[JsonValue] = [
        _SCORED.encode(_scored_wire(row), exclude_unset=True) for row in scored
    ]
    (output / "scored-items.json").write_bytes(encode_json(scored_documents))
    (output / "calibration-report.json").write_text(result.model_dump_json(indent=2) + "\n")
    proposal: PassedThresholdProposal | FailedThresholdProposal
    if result.status == "PASS":
        selected = result.selected.routing_cutoff
        if selected is None:
            raise CalibrationError("Passing report has no routing cutoff")
        proposal = PassedThresholdProposal(
            status="PASS",
            packet_sha256=generation.packet_sha256,
            filter_cutoff=result.selected.filter_cutoff,
            routing_cutoff=selected,
        )
    else:
        proposal = FailedThresholdProposal(status="FAIL", packet_sha256=generation.packet_sha256)
    (output / "thresholds.proposed.yaml").write_text(
        yaml.safe_dump(decode_json(proposal.model_dump_json()), sort_keys=False)
    )
    return result
