"""Offline V0-B threshold selection with untouched held-out validation.

Input scores must be acquired with Pipeline.score and its exact prompts/model.
This module does not call a model or modify production configuration.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import yaml

from tank.agents.jev import provider_binding


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


def _validate(dataset, *, _test_minimums=None):
    required = [
        "dataset_id",
        "labeler",
        "labeled_at",
        "split_method",
        "candidate_set_description",
        "score_provenance",
        "items",
    ]
    if any(not dataset.get(k) for k in required):
        raise CalibrationError(
            "Require dataset/labeler/time/split/candidate-set/score provenance and labeled items"
        )
    provenance = dataset["score_provenance"]
    provider = provenance.get("jev_provider", provenance.get("provider", "typesafe"))
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
    groups = {"calibration": set(), "heldout": set()}
    for row in rows:
        if not row.get("item_id") or row["item_id"] in ids:
            raise CalibrationError("Item IDs must be present and unique")
        ids.add(row["item_id"])
        if row.get("source_id") not in {f"S{i:02}" for i in range(1, 35)}:
            raise CalibrationError("Source outside S01–S34")
        if row.get("split") not in groups or not row.get("label_group"):
            raise CalibrationError("Each item needs calibration/heldout split and label_group")
        groups[row["split"]].add(row["label_group"])
        if type(row.get("relevant")) is not bool or type(row.get("cross_source_repeat")) is not bool:
            raise CalibrationError("Labels must be explicit booleans")
        if (
            "expected_event_id" not in row
            or row["expected_event_id"] is not None
            and not isinstance(row["expected_event_id"], str)
        ):
            raise CalibrationError("Expected existing event ID or explicit null required")
        if row["expected_event_id"] is not None and not row["relevant"]:
            raise CalibrationError("Known crypto event repeats must be labeled relevant")
        if row["cross_source_repeat"] and row["expected_event_id"] is None:
            raise CalibrationError("Cross-source repeat must name its true event")
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
    if groups["calibration"] & groups["heldout"]:
        raise CalibrationError("label_group leakage between calibration and heldout")
    minimums = _test_minimums or (200, 30, 10)
    counts = (
        len(rows),
        sum(r["expected_event_id"] is not None for r in rows),
        sum(r["cross_source_repeat"] for r in rows),
    )
    if any(n < minimum for n, minimum in zip(counts, minimums)):
        raise CalibrationError(
            "V0-B requires >=200 labeled source items, >=30 repeats, >=10 cross-source repeats"
        )
    for split in groups:
        subset = [r for r in rows if r["split"] == split]
        if (
            not subset
            or not any(r["relevant"] for r in subset)
            or not any(r["expected_event_id"] is not None for r in subset)
        ):
            raise CalibrationError(
                "Both calibration and heldout need relevant examples and true repeat labels"
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


def routing_metrics(rows, cutoff):
    # Exact online policy: highest score; equal-score ties retain original candidate order.
    tp = fp = fn = 0
    decisions = []
    for row in rows:
        if not row["relevant"]:
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
        "decisions": decisions,
    }


def calibrate(dataset, *, _test_minimums=None):
    """Return reviewable report. Private minimum override is only for unit test vectors."""
    rows = _validate(dataset, _test_minimums=_test_minimums)
    train = [r for r in rows if r["split"] == "calibration"]
    heldout = [r for r in rows if r["split"] == "heldout"]
    filter_cutoff = max(
        c
        for c in {0.0, 1.0, *[r["filter_score"] for r in train]}
        if filter_metrics(train, c)["recall"] >= 0.95
    )
    candidates = sorted(
        {0.0, 1.0, *[c["score"] for r in train if r["relevant"] for c in r["candidates"]]}, reverse=True
    )
    routing_cutoff = None
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
    held_filter = filter_metrics(heldout, filter_cutoff)
    held_route = routing_metrics(heldout, routing_cutoff) if routing_cutoff is not None else None
    passed = (
        held_filter["recall"] >= 0.95
        and held_route is not None
        and held_route["precision"] is not None
        and held_route["recall"] is not None
        and held_route["precision"] >= 0.90
        and held_route["recall"] >= 0.90
    )
    synthetic = dataset["score_provenance"].get("synthetic") is not False
    rejections = []
    if synthetic:
        rejections.append(
            "Synthetic status is true or not explicitly false; production score evidence required"
        )
    if routing_cutoff is None:
        rejections.append("No routing cutoff meets precision and recall >=0.90 on calibration")
    if held_filter["recall"] < 0.95:
        rejections.append("Heldout filter recall below 0.95")
    if held_route is not None and (
        held_route["precision"] is None
        or held_route["recall"] is None
        or held_route["precision"] < 0.90
        or held_route["recall"] < 0.90
    ):
        rejections.append("Heldout routing precision or recall below 0.90")
    status = (
        "REVIEW_REQUIRED"
        if passed and not synthetic and _test_minimums is None
        else "TEST_ONLY"
        if _test_minimums is not None
        else "REJECTED"
    )
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
        "selected": {"filter_cutoff": filter_cutoff, "routing_cutoff": routing_cutoff},
        "calibration": {
            "filter": filter_metrics(train, filter_cutoff),
            "routing": routing_metrics(train, routing_cutoff) if routing_cutoff is not None else None,
        },
        "heldout": {"filter": held_filter, "routing": held_route},
        "limitations": [
            "No requirement that every S01–S34 appears is inferred; source distribution is reported for reviewer judgment.",
            "Routing evaluates one highest candidate per item, not independent candidate pairs. Wrong existing event counts as both FP and FN.",
            "Heldout scores never select cutoffs. A failed heldout evaluation requires a new reviewed experiment, not tuning to that holdout.",
            "Filter and routing are separately measured; evaluate end-to-end pipeline latency and event outcomes at V0-B too.",
            "No threshold is applied automatically. Synthetic or undeclared-synthetic scores cannot yield a production config patch.",
        ],
    }


def write_report(input_path, output_dir):
    """Production entry point: strict sample gates, report and proposed YAML patch only."""
    dataset = json.loads(Path(input_path).read_text())
    report = calibrate(dataset)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / "calibration-report.json").write_text(json.dumps(report, indent=2) + "\n")
    patch = {
        "status": report["status"],
        "dataset_sha256": report["dataset_sha256"],
        "requires_operator_review": True,
        "thresholds": report["selected"] if report["production_eligible"] else None,
    }
    (output / "thresholds.proposed.yaml").write_text(yaml.safe_dump(patch, sort_keys=False))
    return report
