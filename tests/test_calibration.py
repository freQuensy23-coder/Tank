import json

import pytest

from tank.calibration import CalibrationError, calibrate, filter_metrics, routing_metrics, write_report


def dataset(n=200):
    rows = []
    for i in range(n):
        repeat = i % 5 == 0
        rows.append(
            {
                "item_id": f"i{i}",
                "source_id": f"S{i % 34 + 1:02}",
                "subtype": "releases" if i % 34 + 1 == 25 else None,
                "author_role": "ORGANIZATION" if i % 34 + 1 == 29 else None,
                "split": "calibration" if i < n // 2 else "heldout",
                "label_group": f"fact{i}",
                "relevant": True,
                "cross_source_repeat": repeat,
                "expected_event_id": f"event{i}" if repeat else None,
                "filter_score": 0.8,
                "candidates": [{"event_id": f"event{i}", "score": 0.9 if repeat else 0.3}],
            }
        )
    return {
        "dataset_id": "review-1",
        "labeler": "operator",
        "labeled_at": "2026-10-17T00:00:00Z",
        "split_method": "group-disjoint preassigned split",
        "candidate_set_description": "Frozen online candidate order, 3-day known event window",
        "score_provenance": {
            "jev_provider": "openrouter",
            "model": "typesafe/jev-1.13-20260917",
            "requested_model_id": "typesafe/jev-1.13",
            "resolved_model_id": "typesafe/jev-1.13-20260917",
            "model_pin_evidence": "OpenRouter dated 1.13 snapshot; upstream patch suffix is not exposed",
            "prompt_version": "jev-v1",
            "code_version": "test-revision",
            "acquisition": "test vectors, never production evidence",
            "synthetic": True,
        },
        "items": rows,
    }


def test_highest_filter_cutoff_at_recall_boundary():
    rows = [{"relevant": True, "filter_score": score} for score in [0.1] + [0.7] * 19] + [
        {"relevant": False, "filter_score": 0.5}
    ]
    assert filter_metrics(rows, 0.7)["recall"] == 0.95
    d = dataset()
    d["items"][:20] = [dict(d["items"][i], filter_score=score) for i, score in enumerate([0.1] + [0.7] * 19)]
    assert calibrate(d)["selected"]["filter_cutoff"] == 0.7


def test_routing_measures_selected_winner_not_candidate_pairs():
    d = dataset(4)
    for r in d["items"]:
        r.update(
            expected_event_id="correct",
            cross_source_repeat=True,
            candidates=[{"event_id": "wrong", "score": 0.99}, {"event_id": "correct", "score": 0.95}],
        )
    report = calibrate(d, _test_minimums=(4, 2, 2))
    assert report["selected"]["routing_cutoff"] is None
    m = routing_metrics(d["items"], 0.9)
    assert (m["true_positive"], m["false_positive"], m["false_negative"]) == (0, 4, 4)


def test_routing_equal_scores_preserve_pipeline_candidate_order():
    rows = [
        {
            "item_id": "a",
            "relevant": True,
            "expected_event_id": "first",
            "candidates": [{"event_id": "first", "score": 0.5}, {"event_id": "second", "score": 0.5}],
        }
    ]
    assert routing_metrics(rows, 0.5)["decisions"][0]["predicted_event_id"] == "first"


def test_authorized_f1_objective_selects_best_balance_without_claiming_90_90():
    d = dataset()
    d["score_provenance"]["synthetic"] = False
    positives = [row for row in d["items"] if row["expected_event_id"]]
    negatives = [row for row in d["items"] if not row["expected_event_id"]]
    for row in positives[:20]:
        row["candidates"][0]["score"] = 0.6
    for row in negatives[:10]:
        row["candidates"][0]["score"] = 0.7
    assert calibrate(d)["status"] == "REJECTED"
    report = calibrate(d, routing_objective="max_f1")
    assert report["selected"]["routing_cutoff"] == 0.6
    assert report["routing_selection_objective"] == "max_f1"
    assert report["calibration"]["routing"]["precision"] == 0.8
    assert report["calibration"]["routing"]["recall"] == 1.0
    assert report["routing_target_90_90_met"] is False
    assert report["production_eligible"]
    d["score_provenance"]["synthetic"] = True
    assert not calibrate(d, routing_objective="max_f1")["production_eligible"]


def test_unknown_routing_objective_is_not_silently_accepted():
    with pytest.raises(CalibrationError, match="objective"):
        calibrate(dataset(), routing_objective="anything")


def test_literal_population_includes_diagnostic_partition_and_does_not_add_heldout_gate():
    d = dataset()
    d["score_provenance"]["synthetic"] = False
    for row in d["items"][100:]:
        row["filter_score"] = 0.1
    r = calibrate(d)
    assert r["selected"]["filter_cutoff"] == 0.1
    assert r["status"] == "REVIEW_REQUIRED"
    assert r["production_eligible"]
    assert r["selection_scope"] == "TOTAL_V0_B_POPULATION"
    assert not r["partition_diagnostics"]["independent_validation_claim"]


def test_production_minima_cannot_be_disabled_through_file(tmp_path):
    d = dataset(10)
    d["_test_minimums"] = [1, 1, 1]
    p = tmp_path / "input.json"
    p.write_text(json.dumps(d))
    with pytest.raises(CalibrationError, match="200"):
        write_report(p, tmp_path / "out")


def test_synthetic_reports_never_propose_production_cutoffs(tmp_path):
    d = dataset()
    p = tmp_path / "input.json"
    p.write_text(json.dumps(d))
    r = write_report(p, tmp_path / "out")
    assert r["status"] == "REJECTED"
    assert not r["production_eligible"]
    import yaml

    assert yaml.safe_load((tmp_path / "out" / "thresholds.proposed.yaml").read_text())["thresholds"] is None


def test_reviewable_report_keeps_production_configuration_untouched(tmp_path):
    d = dataset()
    d["score_provenance"]["synthetic"] = False
    p = tmp_path / "input.json"
    p.write_text(json.dumps(d))
    r = write_report(p, tmp_path / "out")
    assert r["status"] == "REVIEW_REQUIRED"
    assert r["selected"] == {"filter_cutoff": 0.8, "routing_cutoff": 0.9}
    assert r["counts"]["repeats"] == 40
    assert r["counts"]["cross_source_repeats"] == 40
    assert len(r["dataset_sha256"]) == 64


def test_group_leakage_and_invalid_probabilities_rejected():
    d = dataset()
    d["items"][100]["label_group"] = d["items"][0]["label_group"]
    # Diagnostic partitions do not partition the literal calibration population.
    assert calibrate(d)["selected"]["routing_cutoff"] == 0.9
    d = dataset()
    d["items"][0]["filter_score"] = float("nan")
    with pytest.raises(CalibrationError, match="finite"):
        calibrate(d)


def openrouter_dataset():
    d = dataset()
    d["score_provenance"].update(
        jev_provider="openrouter",
        model="typesafe/jev-1.13-20260917",
        requested_model_id="typesafe/jev-1.13",
        resolved_model_id="typesafe/jev-1.13-20260917",
        model_pin_evidence="OpenRouter dated 1.13 snapshot; upstream patch suffix is not exposed",
    )
    return d


def test_openrouter_actual_model_provenance_preserves_calibration_and_synthetic_gates():
    d = openrouter_dataset()
    result = calibrate(d)
    assert result["selected"] == calibrate(dataset())["selected"]
    assert result["provenance"]["score_provenance"] == d["score_provenance"]
    assert result["status"] == "REJECTED" and result["production_eligible"] is False
    d["score_provenance"]["synthetic"] = False
    assert calibrate(d)["status"] == "REVIEW_REQUIRED"
    d["items"] = d["items"][:10]
    with pytest.raises(CalibrationError, match="200"):
        calibrate(d)


@pytest.mark.parametrize(
    "changes",
    [
        {"model": "jev-1.13.0"},
        {"model": "typesafe/jev-1.14", "resolved_model_id": "typesafe/jev-1.14"},
        {"resolved_model_id": "jev-1.13.0"},
        {"requested_model_id": "jev-latest"},
        {"jev_provider": "typesafe"},
        {"jev_provider": "unapproved"},
        {"provider": "typesafe"},
        {"model_pin_evidence": ""},
    ],
)
def test_calibration_rejects_false_or_unpinned_openrouter_provenance(changes):
    d = openrouter_dataset()
    d["score_provenance"].update(changes)
    with pytest.raises(CalibrationError):
        calibrate(d)


def test_prior_train_routing_mask_keeps_filter_and_original_gold_sensitivity():
    d = dataset(202)
    row = d["items"][1]
    row.update(
        relation_scorable=False,
        relation_mask_reason="independent unknown relation",
        candidates=[{"event_id": "wrong", "score": 1.0}],
    )
    d["routing_adjudication"] = {"sha256": "prior-frozen-review"}
    report = calibrate(d)
    assert report["routing_coverage"]["all_items"] == 202
    assert report["routing_coverage"]["scorable_items"] == 201
    assert report["calibration"]["filter"]["relevant"] == 202
    assert report["calibration"]["routing"]["false_positive"] == 0
    assert report["original_gold_all_items_sensitivity"]["calibration"]["false_positive"] == 1
    assert report["counts"]["heldout"] == 101


def test_heldout_routing_masks_are_never_allowed():
    d = dataset(202)
    d["items"][-1].update(relation_scorable=False, relation_mask_reason="convenient")
    d["routing_adjudication"] = {"sha256": "review"}
    with pytest.raises(CalibrationError, match="TRAIN-only"):
        calibrate(d)


def test_literal_single_population_without_partition_metadata():
    d = dataset()
    d["score_provenance"]["synthetic"] = False
    d.pop("split_method")
    for row in d["items"]:
        row.pop("split")
    report = calibrate(d)
    assert report["status"] == "REVIEW_REQUIRED"
    assert report["selected"] == {"filter_cutoff": 0.8, "routing_cutoff": 0.9}
    assert report["d6_independent_retest"]["automatic_apply"] is False


def test_unequal_diagnostic_partitions_do_not_add_a_routing_gate():
    d = dataset()
    d["score_provenance"]["synthetic"] = False
    # Thirty-nine repeats are correct; the single diagnostic heldout repeat is
    # wrong. Total39/40 meets both90% gates; old heldout-only gate rejected.
    for row in d["items"]:
        row["split"] = "calibration"
    d["items"][0]["split"] = "heldout"
    d["items"][0]["candidates"] = [{"event_id": "wrong", "score": 0.95}]
    report = calibrate(d)
    assert report["status"] == "REVIEW_REQUIRED"
    assert report["calibration"]["routing"]["precision"] == 39 / 40
    assert report["calibration"]["routing"]["recall"] == 39 / 40
    assert report["partition_diagnostics"]["heldout"]["routing"]["precision"] == 0


def test_missing_diagnostic_split_on_mask_refuses_with_calibration_error():
    d = dataset(202)
    row = d["items"][1]
    row.pop("split")
    row.update(relation_scorable=False, relation_mask_reason="prior adjudication")
    d["routing_adjudication"] = {"sha256": "prior-frozen-review"}
    with pytest.raises(CalibrationError, match="prior adjudication|TRAIN-only"):
        calibrate(d)


def test_always_noncrypto_routing_competition_is_not_dropped():
    d = dataset()
    row = d["items"][1]
    row.update(
        source_id="S05",
        relevant=False,
        filter_score=0.01,
        candidates=[{"event_id": "unrelated-stablecoin-rule", "score": 0.99}],
    )
    report = calibrate(d)
    assert routing_metrics(d["items"], 0.9)["false_positive"] == 1
    assert report["calibration"]["filter"]["relevant"] == 199
    assert report["filter_selection_denominator"] == "ALL_LABELED_RELEVANT_UNCHANGED_LITERAL_TDD_3_1"
    assert report["actual_source_policy_reach_diagnostic"][
        "routing_reach_only_not_full_analysis_or_new_acceptance_gate"
    ]
    # The same noncrypto input on the actual FILTER path is outside ROUTE.
    row["source_id"] = "S30"
    assert routing_metrics(d["items"], 0.9)["false_positive"] == 0


def test_always_noncrypto_repeat_is_valid_without_forcing_crypto_label():
    d = dataset()
    d["items"][0].update(source_id="S05", relevant=False)
    report = calibrate(d)
    assert report["calibration"]["routing"]["true_positive"] == 40
    assert d["items"][0]["relevant"] is False
    d["items"][0]["source_id"] = "S30"
    with pytest.raises(CalibrationError, match="Filtered noncrypto"):
        calibrate(d)


def test_source_policy_requires_original_metadata_and_rejects_forged_override():
    d = dataset()
    d["items"][0].update(source_id="S25", subtype=None)
    with pytest.raises(CalibrationError, match="subtype"):
        calibrate(d)
    d["items"][0].update(source_id="S05", ingest_policy={"decision": "FILTER"})
    with pytest.raises(CalibrationError, match="contradicts"):
        calibrate(d)


@pytest.mark.asyncio
async def test_normal_pipeline_always_routes_noncrypto_without_filter_request():
    from types import SimpleNamespace

    import httpx

    from tank.agents.pipeline import Pipeline
    from tank.sources.policy import initial_relevance
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    item = {
        "item_id": "fed-minutes",
        "source_id": "S05",
        "title": "Minutes of the FOMC",
        "text": "Minutes of the Federal Open Market Committee",
        "relevance": initial_relevance("S05"),
    }
    await repo.put("ingest.source_items", item["item_id"], item)
    opts = SimpleNamespace(
        load_config=lambda name: {"filter_cutoff": None, "routing_cutoff": None},
        config_version="test",
        code_version="test",
    )

    def no_http(request):
        raise AssertionError("ALWAYS must not request FILTER")

    async with httpx.AsyncClient(transport=httpx.MockTransport(no_http)) as http:
        await Pipeline(repo, opts, http=http).filter_item({"item_id": item["item_id"]})
    jobs = await repo.list("ops.jobs")
    assert len(jobs) == 1 and jobs[0]["type"] == "ROUTE_ITEM"
    assert await repo.list("ops.llm_usage") == []
    row = dict(
        dataset(1)["items"][0],
        source_id="S05",
        relevant=False,
        expected_event_id=None,
        cross_source_repeat=False,
        candidates=[{"event_id": "different-rule", "score": 0.99}],
    )
    assert routing_metrics([row], 0.9)["false_positive"] == 1
