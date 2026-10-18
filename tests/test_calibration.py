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
            "model": "jev-1.13.0",
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


def test_heldout_never_tunes_cutoff_and_failed_holdout_rejected():
    d = dataset()
    d["score_provenance"]["synthetic"] = False
    for row in d["items"][100:]:
        row["filter_score"] = 0.1
    r = calibrate(d)
    assert r["selected"]["filter_cutoff"] == 0.8
    assert r["status"] == "REJECTED"
    assert not r["production_eligible"]


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
    with pytest.raises(CalibrationError, match="leakage"):
        calibrate(d)
    d = dataset()
    d["items"][0]["filter_score"] = float("nan")
    with pytest.raises(CalibrationError, match="finite"):
        calibrate(d)


def openrouter_dataset():
    d = dataset()
    d["score_provenance"].update(
        jev_provider="openrouter",
        model="typesafe/jev-1.13-20260917",
        requested_model_id="jev-1.13",
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
