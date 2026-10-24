"""`tank calibrate`: offline Jev calibration of the filter and routing cutoffs (03 §3.1, 01 V0-B, D6).

One pass over one frozen labeled packet (docs/OFFLINE_CALIBRATION.md). Every
item is scored exactly as the service scores it - Pipeline state, questions,
registry identities, candidate builder and the pinned OpenCode Jev binding -
at the item's historical clock, against the labeled event history before it.
Production configuration is never changed: the command writes a report and a
proposed thresholds patch, which is recorded in config by a D6 checkpoint.
"""

import asyncio
import hashlib
import json
from collections.abc import Mapping
from datetime import timedelta
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import JsonValue

from tank.contracts.source_items import PendingRelevance
from tank.sources.policy import initial_relevance
from tank.util import iso, parse_time, sha256

MINIMUMS = {"items": 200, "repeats": 30, "cross_source_repeats": 10}
FILTER_RECALL = 0.95
ROUTING_TARGET = 0.90
CONCURRENCY = 8


class CalibrationError(ValueError):
    """Invalid or insufficient labeled calibration packet."""


def _source_text(value: JsonValue, field: str, *, required: bool) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value):
        raise CalibrationError(f"source_item.{field} requires text")
    return value


def requires_filter(item: Mapping[str, JsonValue]) -> bool:
    source_id = _source_text(item["source_id"], "source_id", required=True)
    item_id = _source_text(item["item_id"], "item_id", required=True)
    policy = initial_relevance(
        source_id,
        _source_text(item.get("subtype"), "subtype", required=False),
        _source_text(item.get("author_role"), "author_role", required=False),
    )
    if (
        isinstance(policy, PendingRelevance)
        and policy.pending_reason == "SOURCE_INGESTION_POLICY_NOT_CAPTURED"
    ):
        raise CalibrationError(f"{item_id}: source outside S01–S34")
    return policy.decision != "ALWAYS"


def load_packet(packet):
    """Validate the frozen packet and return items in clock order with derived routing labels.

    A routed item's `event_id` is its labeled event: the first item of an event opens it,
    every later one is a repeat or development that must join it (FR-E-02). A routed
    item without one opens a session in which no event is extracted (03 §3.1).
    """
    if packet.get("status") != "FROZEN" or not all(
        packet.get(k) for k in ("dataset_id", "labeler", "labeled_at", "items", "events")
    ):
        raise CalibrationError(
            "Require a FROZEN labeled packet with dataset_id, labeler, labeled_at, items, events"
        )
    events = {}
    for event in packet["events"]:
        if not event.get("event_id") or event["event_id"] in events or not event.get("statement"):
            raise CalibrationError("Events need unique event_id and a statement")
        events[event["event_id"]] = event
    rows, seen = [], set()
    for index, row in enumerate(packet["items"]):
        item = row.get("source_item") or {}
        clock = parse_time(row.get("clock"))
        if not row.get("item_id") or row["item_id"] in seen or item.get("item_id") != row["item_id"]:
            raise CalibrationError("Items need unique item_id matching source_item")
        if clock is None or type(row.get("relevant")) is not bool:
            raise CalibrationError(f"{row['item_id']}: historical clock and boolean relevant label required")
        if not (item.get("title") or item.get("text") or item.get("body")):
            raise CalibrationError(f"{row['item_id']}: original item text required")
        seen.add(row["item_id"])
        routed = row["relevant"] or not requires_filter(item)
        if row.get("event_id") is not None and (not routed or row["event_id"] not in events):
            raise CalibrationError(
                f"{row['item_id']}: only routed items (crypto or ALWAYS) carry a known event_id"
            )
        rows.append({**row, "index": index, "at": clock, "filtered": requires_filter(item), "routed": routed})
    rows.sort(key=lambda r: (r["at"], r["index"]))
    window = timedelta(days=packet.get("routing_window_days", 3))
    history = {}
    for row in rows:
        event_id = row.get("event_id")
        row["expected_event_id"] = row["cross_source_repeat"] = None
        if event_id is None:
            continue
        previous = history.get(event_id)
        if previous:
            if row["at"] - previous["last_at"] > window:
                raise CalibrationError(
                    f"{row['item_id']}: joins {event_id} after the routing window (FR-E-02)"
                )
            row["expected_event_id"] = event_id
            row["cross_source_repeat"] = row["source_id"] not in previous["sources"]
            previous["last_at"] = row["at"]
            previous["sources"].add(row["source_id"])
        else:
            history[event_id] = {
                "opened_by": row["item_id"],
                "known_at": row["at"],
                "last_at": row["at"],
                "sources": {row["source_id"]},
            }
    for event_id, opened in history.items():
        events[event_id] = {**events[event_id], "known_to_us_at": iso(opened["known_at"])}
    return rows, events, window


def filter_cutoff(rows):
    relevant = [r["filter_score"] for r in rows if r["relevant"]]
    if not relevant:
        raise CalibrationError("No items labeled relevant")
    return max(c for c in {0.0, *relevant} if sum(s >= c for s in relevant) >= FILTER_RECALL * len(relevant))


def routing_metrics(rows, cutoff):

    tp = fp = fn = 0
    errors = []
    for row in rows:
        if not row["routed"]:
            continue
        best, best_score = None, -1.0
        for candidate in row["candidates"]:
            if candidate["score"] >= cutoff and candidate["score"] > best_score:
                best, best_score = candidate["event_id"], candidate["score"]
        target = row["expected_event_id"]
        tp += best is not None and best == target
        fp += best is not None and best != target
        fn += target is not None and best != target
        if best != target:
            errors.append(
                {
                    "item_id": row["item_id"],
                    "predicted": best,
                    "expected": target,
                    "predicted_score": best_score if best else None,
                }
            )
    return {
        "cutoff": cutoff,
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "errors": errors,
    }


def routing_cutoff(curve):
    passing = [
        m["cutoff"]
        for m in curve
        if (m["precision"] or 0) >= ROUTING_TARGET and (m["recall"] or 0) >= ROUTING_TARGET
    ]
    return max(passing) if passing else None


def report(packet, rows, generation):
    counts = {
        "items": len(rows),
        "filter_path_items": sum(r["filtered"] for r in rows),
        "relevant": sum(r["relevant"] for r in rows),
        "routed_items": sum(r["routed"] for r in rows),
        "repeats": sum(r["expected_event_id"] is not None for r in rows),
        "cross_source_repeats": sum(bool(r["cross_source_repeat"]) for r in rows),
        "candidate_pairs": sum(len(r["candidates"]) for r in rows),
        "sources": sorted({r["source_id"] for r in rows}),
    }
    f_cut = filter_cutoff(rows)
    crypto = [r for r in rows if r["relevant"]]
    reached = sum(not r["filtered"] or r["filter_score"] >= f_cut for r in crypto)
    cutoffs = sorted({0.0, *(c["score"] for r in rows for c in r["candidates"])}, reverse=True)
    curve = [routing_metrics(rows, c) for c in cutoffs]
    r_cut = routing_cutoff(curve)
    selected = next((m for m in curve if m["cutoff"] == r_cut), None)
    checks = {
        "sample_minimums": all(counts[k] >= n for k, n in MINIMUMS.items()),
        "crypto_reach_full_analysis_95": reached >= FILTER_RECALL * len(crypto),
        "routing_precision_recall_90": r_cut is not None,
    }
    reasons = [name for name, ok in checks.items() if not ok]
    status = "PASS" if not reasons else "FAIL"
    best_f1 = max(
        curve,
        key=lambda m: (
            2
            * m["true_positive"]
            / max(1, 2 * m["true_positive"] + m["false_positive"] + m["false_negative"]),
            m["cutoff"],
        ),
    )
    return {
        "dataset_id": packet["dataset_id"],
        "generation": generation,
        "status": status,
        "rejection_reasons": reasons,
        "counts": counts,
        "selected": {"filter_cutoff": f_cut, "routing_cutoff": r_cut},
        "routing_target_90_90_met": r_cut is not None,
        "v0b_checks": checks,
        "filter": {
            "cutoff": f_cut,
            "relevant_passed": sum(r["filter_score"] >= f_cut for r in crypto),
            "relevant": len(crypto),
            "crypto_reaching_full_analysis": reached,
            "filter_path_irrelevant_skipped": sum(
                r["filtered"] and not r["relevant"] and r["filter_score"] < f_cut for r in rows
            ),
            "filter_path_irrelevant": sum(r["filtered"] and not r["relevant"] for r in rows),
        },
        "routing": selected,
        "best_measured_routing": {"cutoff": best_f1["cutoff"], "metrics": best_f1, "automatic_apply": False},
        "routing_curve": [{k: v for k, v in m.items() if k != "errors"} for m in curve],
    }


class _Ledger:
    """Content-addressed, immutable Jev usage rows: an exact request is paid for once."""

    def __init__(self, root):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    async def get(self, collection, key):
        path = self.root / (key + ".json")
        return json.loads(path.read_text()) if path.exists() else None

    async def put(self, collection, key, value, *, immutable=False):
        path = self.root / (key + ".json")
        if not path.exists():
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(value, sort_keys=True))
            temporary.rename(path)
        return json.loads(path.read_text())


class _CachedConfig:
    """Settings proxy that parses each config file once for the whole pass."""

    def __init__(self, settings):
        self._settings = settings
        self.load_config = lru_cache(maxsize=None)(settings.load_config)

    def __getattr__(self, name):
        return getattr(self._settings, name)


async def acquire(input_path, output_dir, *, settings=None, http=None):
    """Score the frozen packet with Jev and write the calibration report and proposal."""
    import httpx

    from tank.agents.jev import binding
    from tank.agents.pipeline import JEV_PROMPT_VERSION, Pipeline
    from tank.settings import Settings, resolve_versions
    from tank.storage import MemoryRepository

    raw = Path(input_path).read_bytes()
    packet = json.loads(raw)
    rows, events, window = load_packet(packet)
    settings = _CachedConfig(settings or resolve_versions(Settings()))
    if settings.load_config("thresholds").get("routing_window_days", 3) != window.days:
        raise CalibrationError("Packet routing window differs from config routing_window_days")
    output = Path(output_dir)
    pinned = binding(getattr(settings, "opencode_api_key", None))
    generation = {
        "packet_sha256": hashlib.sha256(raw).hexdigest(),
        "code_version": settings.code_version,
        "config_version": settings.config_version,
        "prompt_version": JEV_PROMPT_VERSION,
        "registry_sha256": sha256(settings.load_config("registry")),
        "routing_window_days": window.days,
        "jev_endpoint": pinned.base_url + "/v1/systemone",
        "jev_request_model": pinned.request_model,
        "jev_resolved_models": list(pinned.resolved_models),
    }
    owned = http is None
    http = http or httpx.AsyncClient(timeout=30)
    scorer = Pipeline(_Ledger(output / "usage"), settings, http=http)
    capacity = asyncio.Semaphore(CONCURRENCY)

    async def score(row, name, state):
        async with capacity:
            return (await scorer._score_record(row["source_item"], name, state))["score"]

    async def candidates(row, sessions):
        repository = MemoryRepository()
        for event_id, last_at in sessions.items():
            if last_at >= row["at"] - window:
                await repository.put("evt.events", event_id, {**events[event_id], "lifecycle": "OPEN"})
                await repository.put(
                    "ops.agent_sessions",
                    event_id,
                    {"session_id": event_id, "event_id": event_id, "last_item_at": iso(last_at)},
                )
        selector = Pipeline(repository, settings, http=http)
        found = await selector._candidate_events(
            selector.identities(row["source_item"]), iso(row["at"] - window), row["at"]
        )
        return [event["event_id"] for event in found]

    try:
        sessions = {}
        for row in rows:
            row["candidate_ids"] = await candidates(row, sessions) if row["routed"] else []
            if row.get("event_id"):
                sessions[row["event_id"]] = row["at"]

        async def score_row(row):
            item = row["source_item"]
            row["filter_score"] = await score(row, "relevant", Pipeline.state(item))
            scores = await asyncio.gather(
                *(
                    score(row, "same_event", scorer.routing_state(item, events[event_id]))
                    for event_id in row["candidate_ids"]
                )
            )
            row["candidates"] = [
                {"event_id": e, "score": s} for e, s in zip(row["candidate_ids"], scores, strict=False)
            ]

        tasks = [asyncio.create_task(score_row(row)) for row in rows]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    finally:
        if owned:
            await http.aclose()
    result = report(packet, rows, generation)
    output.mkdir(parents=True, exist_ok=True)
    (output / "scored-items.json").write_text(
        json.dumps(
            [
                {
                    k: (iso(v) if k == "at" else v)
                    for k, v in row.items()
                    if k not in {"source_item", "candidate_ids"}
                }
                for row in rows
            ],
            indent=1,
        )
        + "\n"
    )
    (output / "calibration-report.json").write_text(json.dumps(result, indent=2) + "\n")
    proposal = {
        "status": result["status"],
        "packet_sha256": generation["packet_sha256"],
        **(
            {
                "filter_cutoff": result["selected"]["filter_cutoff"],
                "routing_cutoff": result["selected"]["routing_cutoff"],
            }
            if result["status"] == "PASS"
            else {}
        ),
    }
    (output / "thresholds.proposed.yaml").write_text(yaml.safe_dump(proposal, sort_keys=False))
    return result
