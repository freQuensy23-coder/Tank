"""Offline whole-packet threshold selection under a recorded checkpoint objective.

Input scores must be acquired with Pipeline.score and its exact prompts/model.
The command calls the exact production Jev path; no production configuration is modified.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import yaml

from tank.agents.jev import provider_binding
from tank.sources.policy import initial_relevance
from tank.util import sha256


class CalibrationError(ValueError):
    """Invalid or insufficient operator-labeled calibration evidence."""


def _probability(value):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 <= value <= 1
    ):
        raise CalibrationError("Scores must be finite probabilities in [0,1]")
    return float(value)


def derive_ingest_policy(item):
    """Source-path metadata only; never infer crypto relevance or event truth."""
    metadata = {key: item.get(key) for key in ("source_id", "subtype", "author_role")}
    if metadata["source_id"] == "S25" and metadata["subtype"] not in {"commits", "releases"}:
        raise CalibrationError("S25 calibration needs original releases/commits subtype metadata")
    policy = initial_relevance(**metadata)
    if (
        policy["decision"] not in {None, "ALWAYS"}
        or policy.get("pending_reason") == "SOURCE_INGESTION_POLICY_NOT_CAPTURED"
    ):
        raise CalibrationError("Source ingestion policy cannot be established")
    return {
        "decision": "ALWAYS" if policy["decision"] == "ALWAYS" else "FILTER",
        "requires_filter": policy["decision"] != "ALWAYS",
        "source_metadata": metadata,
        "source_metadata_sha256": sha256(metadata),
    }


def route_population(row):
    # FILTER relevance remains a semantic label; ALWAYS is a source path, not a
    # forced positive relevance label. Genuine LOW events still compete.
    return row["relevant"] or not derive_ingest_policy(row)["requires_filter"]


def policy_reach_metrics(rows, filter_cutoff):
    routed = [
        row
        for row in rows
        if not derive_ingest_policy(row)["requires_filter"] or row["filter_score"] >= filter_cutoff
    ]
    relevant = [row for row in rows if row["relevant"]]
    return {
        "always_items": sum(not derive_ingest_policy(row)["requires_filter"] for row in rows),
        "filter_items": sum(derive_ingest_policy(row)["requires_filter"] for row in rows),
        "routed_items": len(routed),
        "crypto_items": len(relevant),
        "crypto_items_routed": sum(row["relevant"] for row in routed),
        "routing_reach_only_not_full_analysis_or_new_acceptance_gate": True,
    }


def _validate(dataset, *, _test_minimums=None):
    required = [
        "dataset_id",
        "labeler",
        "labeled_at",
        "candidate_set_description",
        "score_provenance",
        "items",
    ]
    if any(not dataset.get(k) for k in required):
        raise CalibrationError(
            "Require dataset/labeler/time/candidate-set/score provenance and labeled items"
        )
    provenance = dataset["score_provenance"]
    provider = provenance.get("jev_provider", provenance.get("provider", "openrouter"))
    if not isinstance(provider, str) or provenance.get("provider", provider) != provider:
        raise CalibrationError("Conflicting or invalid Jev provider provenance")
    try:
        binding = provider_binding(provider)
    except ValueError as exc:
        raise CalibrationError("Score provenance must identify a supported explicit Jev provider") from exc
    model = provenance.get("model")
    if model not in binding.resolved_models or provenance.get("prompt_version") != "jev-v1":
        raise CalibrationError("Scores must use the pinned Jev provider/model binding and jev-v1 prompts")
    if provenance.get("resolved_model_id", model) != model:
        raise CalibrationError("Conflicting resolved model provenance")
    if provenance.get("requested_model_id", binding.request_model) != binding.request_model:
        raise CalibrationError("Score provenance has an unpinned requested model")
    if provider == "openrouter" and (
        provenance.get("requested_model_id") != binding.request_model
        or provenance.get("resolved_model_id") != model
        or not isinstance(provenance.get("model_pin_evidence"), str)
        or not provenance["model_pin_evidence"].strip()
    ):
        raise CalibrationError("OpenRouter scores require requested/resolved model IDs and pin evidence")
    if not provenance.get("acquisition") or not provenance.get("code_version"):
        raise CalibrationError("Record score acquisition method and exact code version")
    rows = dataset["items"]
    ids = set()
    for row in rows:
        if not row.get("item_id") or row["item_id"] in ids:
            raise CalibrationError("Item IDs must be present and unique")
        ids.add(row["item_id"])
        if row.get("source_id") not in {f"S{i:02}" for i in range(1, 35)}:
            raise CalibrationError("Source outside S01–S34")
        if not row.get("label_group"):
            raise CalibrationError("Each item needs its independently labeled event group")
        if type(row.get("relevant")) is not bool or type(row.get("cross_source_repeat")) is not bool:
            raise CalibrationError("Labels must be explicit booleans")
        if (
            "expected_event_id" not in row
            or row["expected_event_id"] is not None
            and not isinstance(row["expected_event_id"], str)
        ):
            raise CalibrationError("Expected existing event ID or explicit null required")
        policy = derive_ingest_policy(row)
        if row.get("ingest_policy") is not None and row["ingest_policy"] != policy:
            raise CalibrationError("Derived ingest policy contradicts original source metadata")
        if row["expected_event_id"] is not None and not route_population(row):
            raise CalibrationError("Filtered noncrypto items cannot be routing repeat targets")
        if row["cross_source_repeat"] and row["expected_event_id"] is None:
            raise CalibrationError("Cross-source repeat must name its true event")
        if type(row.get("relation_scorable", True)) is not bool:
            raise CalibrationError("Routing scorable flag must be boolean")
        if not row.get("relation_scorable", True) and (
            row.get("split") != "calibration"
            or not row.get("relation_mask_reason")
            or not dataset.get("routing_adjudication")
        ):
            raise CalibrationError("Routing masks require prior adjudication, reason and TRAIN-only scope")
        _probability(row.get("filter_score"))
        candidates = row.get("candidates")
        if not isinstance(candidates, list):
            raise CalibrationError("Ordered candidate list required, including empty lists")
        seen = set()
        for candidate in candidates:
            if not candidate.get("event_id") or candidate["event_id"] in seen:
                raise CalibrationError("Candidate IDs must be unique")
            seen.add(candidate["event_id"])
            _probability(candidate.get("score"))
    minimums = _test_minimums or (200, 30, 10)
    counts = (
        sum(r.get("relation_scorable", True) for r in rows),
        sum(r["expected_event_id"] is not None and r.get("relation_scorable", True) for r in rows),
        sum(r["cross_source_repeat"] and r.get("relation_scorable", True) for r in rows),
    )
    if any(n < minimum for n, minimum in zip(counts, minimums)):
        raise CalibrationError(
            "V0-B requires >=200 labeled source items, >=30 repeats, >=10 cross-source repeats"
        )
    return rows


def filter_metrics(rows, cutoff):
    positives = sum(r["relevant"] for r in rows)
    passed = sum(r["relevant"] and r["filter_score"] >= cutoff for r in rows)
    return {
        "relevant": positives,
        "passed_relevant": passed,
        "recall": passed / positives if positives else None,
        "skipped_irrelevant": sum(not r["relevant"] and r["filter_score"] < cutoff for r in rows),
    }


def routing_metrics(rows, cutoff, *, include_unscorable=False):
    # Exact online policy: highest score; equal-score ties retain original candidate order.
    tp = fp = fn = 0
    decisions = []
    for row in rows:
        if not route_population(row) or (not include_unscorable and not row.get("relation_scorable", True)):
            continue
        best = None
        best_score = -1
        for candidate in row["candidates"]:
            if candidate["score"] >= cutoff and candidate["score"] > best_score:
                best, best_score = candidate["event_id"], candidate["score"]
        target = row["expected_event_id"]
        if best is not None:
            if best == target:
                tp += 1
            else:
                fp += 1
        if target is not None and best != target:
            fn += 1
        decisions.append(
            {
                "item_id": row["item_id"],
                "predicted_event_id": best,
                "expected_event_id": target,
                "correct": best == target,
            }
        )
    return {
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "f1": 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0,
        "decisions": decisions,
    }


def select_training_cutoffs(train, *, routing_objective="precision_recall_90"):
    """Select from measured scores under the recorded checkpoint objective."""
    if routing_objective not in {"precision_recall_90", "max_f1"}:
        raise CalibrationError("Unknown routing calibration objective")
    filter_cutoff = max(
        c
        for c in {0.0, 1.0, *[r["filter_score"] for r in train]}
        if filter_metrics(train, c)["recall"] >= 0.95
    )
    candidates = sorted(
        {0.0, 1.0, *[c["score"] for r in train if route_population(r) for c in r["candidates"]]}, reverse=True
    )
    routing_cutoff = None
    if routing_objective == "max_f1":

        def quality(cutoff):
            metrics = routing_metrics(train, cutoff)
            tp, fp, fn = (metrics[key] for key in ("true_positive", "false_positive", "false_negative"))
            denominator = 2 * tp + fp + fn
            f1 = 2 * tp / denominator if denominator else 0.0
            return f1, metrics["precision"] or 0.0, metrics["recall"] or 0.0, cutoff

        return filter_cutoff, max(candidates, key=quality)
    for cutoff in candidates:
        metrics = routing_metrics(train, cutoff)
        if (
            metrics["precision"] is not None
            and metrics["recall"] is not None
            and metrics["precision"] >= 0.90
            and metrics["recall"] >= 0.90
        ):
            routing_cutoff = cutoff
            break
    return filter_cutoff, routing_cutoff


def calibrate(dataset, *, routing_objective="precision_recall_90", _test_minimums=None):
    """Return reviewable report. Private minimum override is only for unit test vectors."""
    rows = _validate(dataset, _test_minimums=_test_minimums)
    # V0-B minima and calibration apply to the complete labeled population.
    # Partition metadata is diagnostic, never an additional business gate.
    if not any(row["relevant"] for row in rows):
        raise CalibrationError("Calibration requires labeled relevant items to measure filter recall")
    train = [r for r in rows if r.get("split") == "calibration"]
    heldout = [r for r in rows if r.get("split") == "heldout"]
    filter_cutoff, routing_cutoff = select_training_cutoffs(rows, routing_objective=routing_objective)
    held_filter = filter_metrics(heldout, filter_cutoff) if heldout else None
    held_route = routing_metrics(heldout, routing_cutoff) if heldout and routing_cutoff is not None else None
    passed = routing_cutoff is not None
    selected_routing = routing_metrics(rows, routing_cutoff) if passed else None
    synthetic = dataset["score_provenance"].get("synthetic") is not False
    rejections = []
    if synthetic:
        rejections.append(
            "Synthetic status is true or not explicitly false; production score evidence required"
        )
    if routing_cutoff is None:
        rejections.append("No routing cutoff meets precision and recall >=0.90 on the total V0-B population")
    status = (
        "REVIEW_REQUIRED"
        if passed and not synthetic and _test_minimums is None
        else "TEST_ONLY"
        if _test_minimums is not None
        else "REJECTED"
    )
    best_cutoff = select_training_cutoffs(rows, routing_objective="max_f1")[1]
    canonical = json.dumps(dataset, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "dataset_id": dataset["dataset_id"],
        "dataset_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
        "status": status,
        "production_eligible": status == "REVIEW_REQUIRED",
        "rejection_reasons": rejections,
        "provenance": {k: v for k, v in dataset.items() if k != "items"},
        "counts": {
            "items": len(rows),
            "repeats": sum(r["expected_event_id"] is not None for r in rows),
            "cross_source_repeats": sum(r["cross_source_repeat"] for r in rows),
            "sources_present": sorted({r["source_id"] for r in rows}),
            "calibration": len(train),
            "heldout": len(heldout),
        },
        "routing_coverage": {
            "all_items": len(rows),
            "scorable_items": sum(r.get("relation_scorable", True) for r in rows),
            "masked": [
                {"item_id": r["item_id"], "reason": r["relation_mask_reason"]}
                for r in rows
                if not r.get("relation_scorable", True)
            ],
            "filter_denominator_unchanged": True,
        },
        "original_gold_all_items_sensitivity": {
            split: routing_metrics(subset, routing_cutoff, include_unscorable=True)
            if routing_cutoff is not None
            else None
            for split, subset in (("calibration", train), ("heldout", heldout), ("all", rows))
        },
        "selected": {"filter_cutoff": filter_cutoff, "routing_cutoff": routing_cutoff},
        "best_measured_routing": {
            "cutoff": best_cutoff,
            "metrics": routing_metrics(rows, best_cutoff),
            "automatic_apply": False,
        },
        "calibration": {
            "filter": filter_metrics(rows, filter_cutoff),
            "routing": selected_routing,
        },
        "routing_target_90_90_met": bool(
            selected_routing
            and (selected_routing["precision"] or 0.0) >= 0.90
            and (selected_routing["recall"] or 0.0) >= 0.90
        ),
        "selection_scope": "TOTAL_V0_B_POPULATION",
        "routing_selection_objective": routing_objective,
        "routing_selection_tiebreak": "precision, recall, higher cutoff"
        if routing_objective == "max_f1"
        else "highest cutoff meeting precision and recall >=0.90",
        "routing_population": "ALWAYS_PLUS_FILTER_GOLD_RELEVANT_COMPONENT",
        "actual_source_policy_reach_diagnostic": policy_reach_metrics(rows, filter_cutoff),
        "filter_selection_denominator": "ALL_LABELED_RELEVANT_UNCHANGED_LITERAL_TDD_3_1",
        "candidate_context_scope": dataset.get("context_scope", "DECLARED_CANDIDATE_SET"),
        "partition_diagnostics": {
            "calibration": {
                "filter": filter_metrics(train, filter_cutoff) if train else None,
                "routing": routing_metrics(train, routing_cutoff)
                if train and routing_cutoff is not None
                else None,
            },
            "heldout": {"filter": held_filter, "routing": held_route},
            "used_in_selection_population": True,
            "independent_validation_claim": False,
        },
        "d6_independent_retest": {"status": "NOT_ESTABLISHED_BY_THIS_SELECTION", "automatic_apply": False},
        "limitations": [
            "No requirement that every S01–S34 appears is inferred; source distribution is reported for reviewer judgment.",
            "Routing evaluates one highest candidate per item, not independent candidate pairs. Wrong existing event counts as both FP and FN.",
            "All items select the literal V0-B cutoffs; partition diagnostics are not independent validation. D6 retesting on cases not motivating the change remains separate.",
            "Filter and routing are separately measured; evaluate end-to-end pipeline latency and event outcomes at V0-B too.",
            "No threshold is applied automatically. Synthetic or undeclared-synthetic scores cannot yield a production config patch.",
        ],
    }


def write_report(input_path, output_dir, *, routing_objective="precision_recall_90"):
    """Production entry point: strict sample gates, report and proposed YAML patch only."""
    dataset = json.loads(Path(input_path).read_text())
    report = calibrate(dataset, routing_objective=routing_objective)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / "calibration-report.json").write_text(json.dumps(report, indent=2) + "\n")
    patch = {
        "status": report["status"],
        "dataset_sha256": report["dataset_sha256"],
        "requires_operator_review": True,
        "thresholds": report["selected"] if report["production_eligible"] else None,
        "routing_selection_objective": routing_objective,
    }
    (output / "thresholds.proposed.yaml").write_text(yaml.safe_dump(patch, sort_keys=False))
    return report


def _immutable_json(path, value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    if path.exists():
        if path.read_text() != encoded:
            raise CalibrationError("Calibration evidence changed; use a new output directory")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x") as stream:
            stream.write(encoded)


class _UsageLedger:
    """Only successful exact-wire scores are reused after an interrupted run."""

    def __init__(self, root):
        self.root = root

    async def get(self, collection, key):
        if collection != "ops.llm_usage":
            raise CalibrationError("Calibration can only persist Jev usage")
        path = self.root / (key + ".json")
        return json.loads(path.read_text()) if path.exists() else None

    async def put(self, collection, key, value, *, immutable=False):
        previous = await self.get(collection, key)
        if previous is not None:
            if previous != value:
                raise CalibrationError("Immutable calibration usage collision")
            return previous
        _immutable_json(self.root / (key + ".json"), value)
        return value


class _CapturedHTTP:
    def __init__(self, http, root, settings):
        self.http, self.root, self.settings = http, root, settings

    async def post(self, url, **kwargs):
        import uuid

        if url != "https://openrouter.ai/api/v1/systemone":
            raise CalibrationError("Only the OpenRouter Jev endpoint is allowed")
        # Never save headers: they contain the bearer credential.
        attempt = self.root / uuid.uuid4().hex
        _immutable_json(attempt / "request.json", {"url": url, "body": kwargs["json"]})
        try:
            response = await self.http.post(url, **kwargs)
            from tank.agents.security import redact_tool_arguments

            evidence, refusal = redact_tool_arguments(
                {"status": response.status_code, "body": response.json()}, self.settings
            )
            _immutable_json(attempt / "response.json", evidence)
            if refusal:
                raise CalibrationError("Provider response contained credentials; sanitized evidence retained")
            return response
        except BaseException as error:
            _immutable_json(attempt / "failure.json", {"error_type": type(error).__name__})
            raise


def _packet_rows(packet):
    """Validate frozen inputs before billing; labels never enter a Jev state."""
    from tank.util import parse_time

    required = ("dataset_id", "labeler", "labeled_at", "candidate_set_description", "export_provenance")
    if packet.get("status") != "FROZEN" or any(not packet.get(k) for k in required):
        raise CalibrationError("Require a FROZEN independently labeled packet with export provenance")
    if packet.get("complete_prefix_export") is not True:
        raise CalibrationError("Each news item needs the complete preceding event/session export")
    rows = packet.get("items")
    if not isinstance(rows, list) or not rows:
        raise CalibrationError("Require independently labeled news items")
    validated = []
    for row in rows:
        if row.get("relation_scorable", True) is not True:
            raise CalibrationError(
                "Every frozen label remains in the calibration population; masks are unsupported"
            )
        item, prefix = row.get("source_item"), row.get("prefix")
        if not isinstance(item, dict) or not isinstance(prefix, dict):
            raise CalibrationError("Each label needs its source_item and frozen prefix")
        if item.get("item_id") != row.get("item_id") or item.get("source_id") != row.get("source_id"):
            raise CalibrationError("Label/source identity mismatch")
        if any(not item.get(k) for k in ("observed_at", "content_hash", "raw_sha256")):
            raise CalibrationError("Original source observation clock and content/raw hashes required")
        if not item.get("title") and not item.get("text") and not item.get("body"):
            raise CalibrationError("Original news text required")
        now = parse_time(item["observed_at"])
        if now is None:
            raise CalibrationError("Invalid source observation clock")
        events, sessions = prefix.get("events"), prefix.get("sessions")
        if not isinstance(events, list) or not isinstance(sessions, list):
            raise CalibrationError("Frozen prefix needs explicit events and sessions, including empty lists")
        seen = set()
        for event in events:
            event_id = event.get("event_id")
            known_at = parse_time(event.get("known_to_us_at"))
            if not event_id or event_id in seen or known_at is None or known_at > now:
                raise CalibrationError("Duplicate or future event in frozen prefix")
            seen.add(event_id)
        session_ids = set()
        for session in sessions:
            last_at = parse_time(session.get("last_item_at"))
            if (
                not session.get("session_id")
                or session["session_id"] in session_ids
                or session.get("event_id") not in seen
                or last_at is None
                or last_at > now
            ):
                raise CalibrationError("Invalid or future session in frozen prefix")
            session_ids.add(session["session_id"])
        label = {k: v for k, v in row.items() if k not in {"source_item", "prefix"}}
        label.update(subtype=item.get("subtype"), author_role=item.get("author_role"))
        validated.append((label, item, prefix))
    # Use the same sample/label validation as final reporting, before any HTTP.
    binding = provider_binding()
    _validate(
        {
            **packet,
            "score_provenance": {
                "jev_provider": binding.provider,
                "model": binding.resolved_models[0],
                "requested_model_id": binding.request_model,
                "resolved_model_id": binding.resolved_models[0],
                "model_pin_evidence": "Pinned OpenRouter dated snapshot",
                "prompt_version": "jev-v1",
                "code_version": "INPUT_VALIDATION_ONLY",
                "acquisition": "INPUT_VALIDATION_ONLY",
            },
            "items": [{**label, "filter_score": 0.0, "candidates": []} for label, _, _ in validated],
        }
    )
    return validated


async def acquire(input_path, output_dir, *, settings=None, http=None):
    """Single calibration workflow: frozen labeled news → exact Jev calls → proposal."""
    import asyncio
    from datetime import timedelta

    import httpx

    from tank.agents.pipeline import JEV_PROMPT_VERSION, Pipeline
    from tank.settings import Settings, resolve_versions
    from tank.storage import MemoryRepository
    from tank.util import iso, parse_time

    packet = json.loads(Path(input_path).read_text())
    rows = _packet_rows(packet)
    settings = settings or resolve_versions(Settings())
    provider_binding(getattr(settings, "jev_provider", "openrouter"))
    from tank.agents.security import redact_tool_arguments

    _, refusal = redact_tool_arguments(packet, settings)
    if refusal:
        raise CalibrationError("Calibration packet contains credentials; remove them before acquisition")
    output = Path(output_dir)
    generation = {
        "input_sha256": hashlib.sha256(Path(input_path).read_bytes()).hexdigest(),
        "code_version": settings.code_version,
        "config_version": settings.config_version,
        "prompt_version": JEV_PROMPT_VERSION,
        "registry_sha256": sha256(settings.load_config("registry")),
        "routing_window_days": settings.load_config("thresholds").get("routing_window_days", 3),
    }
    _immutable_json(output / "inputs.json", packet)
    _immutable_json(output / "generation.json", generation)
    ledger = _UsageLedger(output / "usage")
    owned_http = http is None
    underlying = http or httpx.AsyncClient(timeout=15)
    pipeline = Pipeline(ledger, settings, http=_CapturedHTTP(underlying, output / "attempts", settings))
    scored, first = [], None
    try:
        for index, (label, item, prefix) in enumerate(rows):
            # Use the exact service prefilter/order; labels cannot nominate candidates.
            repository = MemoryRepository()
            for event in prefix["events"]:
                await repository.put("evt.events", event["event_id"], event)
            for session in prefix["sessions"]:
                await repository.put("ops.agent_sessions", session["session_id"], session)
            selector = Pipeline(repository, settings, http=underlying)
            now = parse_time(item["observed_at"])
            since = iso(now - timedelta(days=generation["routing_window_days"]))
            candidates = await selector._candidate_events(selector.identities(item), since, now)
            record = await pipeline._score_record(item, "relevant", pipeline.state(item))
            first = first or record
            capacity = asyncio.Semaphore(8)

            async def score_candidate(candidate, *, item=item, capacity=capacity):
                async with capacity:
                    result = await pipeline._score_record(
                        item, "same_event", pipeline.routing_state(item, candidate)
                    )
                    return {
                        "event_id": candidate["event_id"],
                        "score": result["score"],
                        "usage_id": result["usage_id"],
                    }

            # Finish every request before moving the item clock or reporting a
            # failed pair; successful exact-wire scores survive ordinary retry.
            results = await asyncio.gather(
                *(score_candidate(candidate) for candidate in candidates), return_exceptions=True
            )
            error = next((result for result in results if isinstance(result, BaseException)), None)
            if error is not None:
                raise error
            scored.append(
                {
                    **label,
                    "filter_score": record["score"],
                    "filter_usage_id": record["usage_id"],
                    "candidates": results,
                }
            )
            _immutable_json(output / "rows" / f"{index:05}.json", scored[-1])
    except BaseException as error:
        with (output / "failures.jsonl").open("a") as stream:
            stream.write(
                json.dumps(
                    {
                        "error_type": type(error).__name__,
                        "completed_items": len(scored),
                        "planned_items": len(rows),
                    }
                )
                + "\n"
            )
        raise
    finally:
        if owned_http:
            await underlying.aclose()
    dataset = {k: v for k, v in packet.items() if k != "items"}
    dataset.update(
        items=scored,
        score_provenance={
            **generation,
            "jev_provider": first["jev_provider"],
            "model": first["resolved_model_id"],
            "resolved_model_id": first["resolved_model_id"],
            "requested_model_id": first["requested_model_id"],
            "model_pin_evidence": first["model_pin_evidence"],
            "acquisition": "Exact Pipeline scoring and candidate selection on frozen source/event/session exports",
            "synthetic": packet.get("synthetic") is not False,
        },
    )
    _immutable_json(output / "scored-dataset.json", dataset)
    # New calibrations always use the specification's highest qualifying cutoff.
    # The previously authorized max-F1 deployment is a separate retained decision.
    return write_report(output / "scored-dataset.json", output)
