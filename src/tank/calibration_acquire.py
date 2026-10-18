"""Acquire real calibration scores after independent labels have been frozen.

Run: python -m tank.calibration_acquire RAW_JSONL LABELS_JSON OUTPUT --labels-sha256 SHA
Only Jev is called; this command has no production event/message repository.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import timedelta
from pathlib import Path

import httpx

from tank.agents.pipeline import JEV_PROMPT_VERSION, Pipeline
from tank.agents.security import redact_tool_arguments
from tank.calibration import CalibrationError, write_report
from tank.settings import Settings, resolve_versions
from tank.util import parse_time


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class CaptureLedger:
    """Durable append-only score records, reusable after interrupted acquisition."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    async def get(self, collection, key):
        if collection != "ops.llm_usage":
            raise ValueError("Calibration only stores Jev usage")
        path = self.path / (key + ".json")
        return json.loads(path.read_text()) if path.exists() else None

    async def put(self, collection, key, record, *, immutable=False):
        existing = await self.get(collection, key)
        if existing is not None:
            if existing != record:
                raise CalibrationError("Immutable acquisition collision")
            return existing
        with (self.path / (key + ".json")).open("x") as stream:
            json.dump(record, stream, indent=2)
        return record


async def acquire(raw_path, labels_path, output_dir, labels_sha256, *, settings=None, http=None):
    if digest(labels_path) != labels_sha256:
        raise CalibrationError("Frozen labels SHA256 mismatch")
    labels = json.loads(Path(labels_path).read_text())
    if labels.get("status") != "FROZEN":
        raise CalibrationError("Independent label artifact must explicitly be FROZEN")
    raw_rows = list(map(json.loads, Path(raw_path).read_text().splitlines()))
    raw = {r["item_id"]: r for r in raw_rows}
    if len(raw) != len(raw_rows):
        raise CalibrationError("Duplicate captured item IDs")
    groups = {}
    for row in labels["items"]:
        if row["split"] not in {"calibration", "heldout"} or type(row["relevant"]) is not bool:
            raise CalibrationError("Independent labels and preassigned splits required")
        group = row["label_group"]
        if group in groups and groups[group] != row["split"]:
            raise CalibrationError("Group leakage before acquisition")
        groups[group] = row["split"]
        if row["item_id"] not in raw:
            raise CalibrationError("Labeled item absent from captured sources")
        item = raw[row["item_id"]]
        for field in ("source_id", "content_hash", "raw_sha256"):
            if not row.get(field) or row[field] != item.get(field):
                raise CalibrationError("Frozen label does not match captured " + field)
        if row.get("known_to_us_at") != item.get("observed_at"):
            raise CalibrationError("Frozen label observation time mismatch")
        index = row.get("raw_index")
        if (
            type(index) is not int
            or not 0 <= index < len(raw_rows)
            or raw_rows[index]["item_id"] != row["item_id"]
        ):
            raise CalibrationError("Frozen raw_index mismatch")
        if type(row.get("is_repeat")) is not bool:
            raise CalibrationError("Explicit independent is_repeat boolean required")
    excluded = [row for row in labels["items"] if row.get("ambiguity_excluded")]
    labels["items"] = [row for row in labels["items"] if not row.get("ambiguity_excluded")]
    if not labels["items"]:
        raise CalibrationError("No unambiguous frozen labels")
    events = {e["label_group"]: e for e in labels["events"]}
    for group, event in events.items():
        event_identities = {
            e["entity_id"] if isinstance(e, dict) else e for e in event.get("entities", [])
        } | set(event.get("tokens_mentioned", []))
        if "candidate_identity_keys" in event and set(event["candidate_identity_keys"]) != event_identities:
            raise CalibrationError("Candidate keys differ from production event entity/token universe")
        if not event.get("statement") or event.get("representative_item_id") not in raw:
            raise CalibrationError("Reviewed real candidate statement/representative required")
        if event["representative_item_id"] not in {
            r["item_id"] for r in labels["items"] if r["label_group"] == group
        }:
            raise CalibrationError("Candidate representative outside its reviewed group")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"labels_sha256": labels_sha256, "raw_sha256": digest(raw_path)}
    manifest_path = output / "inputs.json"
    if manifest_path.exists() and json.loads(manifest_path.read_text()) != manifest:
        raise CalibrationError("Acquisition inputs changed; create a new experiment")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    settings = settings or resolve_versions(Settings())
    owned_http = http is None
    if owned_http:

        async def capture_response(response):
            await response.aread()
            if response.request.url.host not in {"openrouter.ai", "api.typesafe.ai"}:
                raise CalibrationError("Unexpected calibration provider endpoint")
            request = json.loads(response.request.content)
            capture = {
                "url": str(response.request.url),
                "request": request,
                "status": response.status_code,
                "response": response.json(),
            }
            capture, refusal = redact_tool_arguments(capture, settings)
            if refusal:
                raise CalibrationError("Provider capture contained credentials; acquisition refused")
            wire_dir = output / "wire"
            wire_dir.mkdir(exist_ok=True)
            key = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
            (wire_dir / (key + ".json")).write_text(json.dumps(capture, indent=2) + "\n")

        http = httpx.AsyncClient(timeout=15, event_hooks={"response": [capture_response]})
    pipeline = Pipeline(CaptureLedger(output / "usage"), settings, http=http)
    active = {}
    rows, records = [], []
    ordered = sorted(
        labels["items"], key=lambda r: (parse_time(raw[r["item_id"]]["observed_at"]), r["raw_index"])
    )
    try:
        for label in ordered:
            item = raw[label["item_id"]]
            now = parse_time(item["observed_at"])
            identities = pipeline.identities(item)
            state = pipeline.state(item)
            score = await pipeline._score_record(item, "relevant", state)
            records.append(score)
            candidates = []
            target = None
            for event_id, candidate in sorted(active.items()):
                event = candidate["event"]
                if now - candidate["last_at"] > timedelta(days=3) or not identities.intersection(
                    candidate["identities"]
                ):
                    continue
                event_state = dict(
                    state,
                    event={
                        "statement": event["statement"],
                        "entities": event.get("entities", []),
                        "tokens": event.get("tokens_mentioned", []),
                        "stage": event.get("event_state"),
                        "known_to_us_at": candidate["known_at"],
                    },
                )
                result = await pipeline._score_record(item, "same_event", event_state)
                records.append(result)
                candidates.append(
                    {
                        "event_id": event_id,
                        "score": result["score"],
                        "usage_id": result["usage_id"],
                        "state": event_state,
                    }
                )
                if label["relevant"] and label["is_repeat"] and candidate["group"] == label["label_group"]:
                    target = event_id
            row = dict(
                label,
                source_id=item["source_id"],
                filter_score=score["score"],
                filter_usage_id=score["usage_id"],
                filter_state=state,
                candidates=candidates,
                expected_event_id=target,
            )
            # Independent repeat labels remain explicit even if actual candidate prefilter misses them.
            if label["relevant"] and label.get("is_repeat") and target is None:
                row["expected_event_id"] = events[label["label_group"]]["event_id"]
            rows.append(row)
            group = label["label_group"]
            if label["relevant"] and group in events:
                event = events[group]
                if event["event_id"] in active:
                    active[event["event_id"]]["last_at"] = now
                elif item["item_id"] == event["representative_item_id"]:
                    active[event["event_id"]] = {
                        "event": event,
                        "group": group,
                        "last_at": now,
                        "known_at": item["observed_at"],
                        "identities": set(event["candidate_identity_keys"])
                        if "candidate_identity_keys" in event
                        else {e["entity_id"] if isinstance(e, dict) else e for e in event.get("entities", [])}
                        | set(event.get("tokens_mentioned", [])),
                    }
            (output / "progress.json").write_text(
                json.dumps({"completed": len(rows), "total": len(ordered)}) + "\n"
            )
    finally:
        if owned_http:
            await pipeline.http.aclose()
    first = records[0]
    dataset = {k: v for k, v in labels.items() if k not in {"events", "items"}}
    dataset.update(
        candidate_events=list(events.values()),
        exclusions=excluded,
        items=rows,
        score_provenance={
            "jev_provider": first["jev_provider"],
            "model": first["resolved_model_id"],
            "resolved_model_id": first["resolved_model_id"],
            "requested_model_id": first["requested_model_id"],
            "model_pin_evidence": first["model_pin_evidence"],
            "prompt_version": JEV_PROMPT_VERSION,
            "acquisition": "actual Pipeline._score_record; frozen independent labels; chronological observed_at; reviewed candidate states",
            "code_version": settings.code_version,
            "config_version": settings.config_version,
            "synthetic": False,
            **manifest,
        },
    )
    path = output / "scored-dataset.json"
    path.write_text(json.dumps(dataset, indent=2) + "\n")
    return write_report(path, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_items")
    parser.add_argument("labels")
    parser.add_argument("output")
    parser.add_argument("--labels-sha256", required=True)
    args = parser.parse_args()
    report = asyncio.run(acquire(args.raw_items, args.labels, args.output, args.labels_sha256))
    print(
        json.dumps({"status": report["status"], "counts": report["counts"], "selected": report["selected"]})
    )


if __name__ == "__main__":
    main()
