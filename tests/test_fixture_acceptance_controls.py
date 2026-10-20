"""Declared synthetic controls for the acceptance gate, not historical fixtures."""

import hashlib
import json

import pytest
from fixture_acceptance import (
    assertion_passes,
    canonical,
    check_completed,
    check_ready,
    cutoff_errors,
    digest,
)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical(value))
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def complete_case(tmp_path):
    raw = tmp_path / "public/raw.txt"
    raw.parent.mkdir()
    raw.write_text("Issuer 7 proposed a rule.")
    mapping = {"replacements": {"ExampleCorp": "Issuer 7"}}
    packet = {
        "cutoff": "2026-09-21T15:02:00Z",
        "registry": {"entities": [{"entity_id": "issuer7"}]},
        "config": {"thresholds": {"fixed_fixture_parameter": 1}},
        "code_version": "declared-test-code",
        "config_version": "declared-test-config",
        "records": [
            {
                "collection": "ingest.source_items",
                "record": {
                    "text": raw.read_text(),
                    "observed_at": "2026-09-21T15:00:00Z",
                    "raw_path": str(raw),
                    "raw_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
                },
            },
            {
                "collection": "replay.market_snapshots",
                "record": {
                    "oi": [{"oi_native": "12", "ts": "2026-09-21T15:00:00Z"}],
                },
            },
        ],
        "tool_results": [],
        "masking": {"applied": True, "map_sha256": digest(mapping)},
    }
    packet["input_sha256"] = digest(packet)
    case = {
        "fixture_id": "F-01",
        "cutoff_spec": "21 Sep 2026 15:02",
        "assertion_spec": "State PROPOSED.",
        "acceptance_input": save(tmp_path / "public/input.json", packet),
        "private_name_map": save(tmp_path / "evaluator/map.json", mapping),
    }
    sealed = {
        "fixture_id": "F-01",
        "input_sha256": digest(packet),
        "assertion_spec": case["assertion_spec"],
        "sealed_at": "2026-10-18T00:00:00Z",
        "assertions": [{"path": ["event_state"], "equals": "PROPOSED"}],
    }
    case["sealed_outcomes"] = save(tmp_path / "evaluator/sealed.json", sealed)
    output = {"event_state": "PROPOSED"}
    run = {
        "started_at": "2026-10-18T01:00:00Z",
        "frozen_output": save(
            tmp_path / "results/output.json",
            {
                "status": "COMPLETED",
                "input_sha256": digest(packet),
                "output": output,
                "output_sha256": digest(output),
            },
        ),
    }
    case["acceptance_run"] = save(tmp_path / "results/run.json", run)
    return case, packet, sealed


def test_complete_declared_control_passes(tmp_path):
    case, _, _ = complete_case(tmp_path)
    assert check_ready(tmp_path, case) == []
    assert check_completed(tmp_path, case) == []


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "missing_review",
        "wrong_input",
        "wrong_output",
        "missing_clause",
        "failed_clause",
        "changed_clause",
        "no_evidence",
        "tampered_evidence",
    ],
)
def test_required_manual_clauses_are_bound_and_cannot_be_ignored(tmp_path, failure):
    case, _, sealed = complete_case(tmp_path)
    requirement = "A durable milestone job is scheduled at the source's vote end."
    sealed["manual_predicates"] = [{"requirement": requirement}]
    case["sealed_outcomes"] = save(tmp_path / "evaluator/sealed.json", sealed)
    run = json.loads((tmp_path / "results/run.json").read_text())
    evidence_path = tmp_path / "results/jobs.json"
    review = {
        "input_sha256": sealed["input_sha256"],
        "frozen_output_sha256": run["frozen_output"]["sha256"],
        "checks": [
            {
                "requirement": requirement,
                "passed": True,
                "reason": "The retained job has the declared vote-end timestamp.",
                "evidence": [save(evidence_path, {"jobs": [{"due_at": "2026-09-24T15:00:00Z"}]})],
            }
        ],
    }
    if failure == "wrong_input":
        review["input_sha256"] = "0" * 64
    elif failure == "wrong_output":
        review["frozen_output_sha256"] = "0" * 64
    elif failure == "missing_clause":
        review["checks"] = []
    elif failure == "failed_clause":
        review["checks"][0]["passed"] = False
    elif failure == "changed_clause":
        review["checks"][0]["requirement"] = "No milestone required."
    elif failure == "no_evidence":
        review["checks"][0]["evidence"] = []
    elif failure == "tampered_evidence":
        evidence_path.write_text('{"jobs":[]}')
    if failure != "missing_review":
        run["manual_review"] = save(tmp_path / "results/manual-review.json", review)
    case["acceptance_run"] = save(tmp_path / "results/run.json", run)
    assert bool(check_completed(tmp_path, case)) is (failure is not None)


@pytest.mark.parametrize(
    "failure",
    [
        "placeholder",
        "empty_market",
        "missing_source",
        "raw_tamper",
        "unmasked_raw",
        "cutoff_shift",
        "oracle_in_input",
        "no_oracle",
        "oracle_in_public",
        "oracle_for_different_input",
    ],
)
def test_missing_or_contaminated_fixture_cannot_pass(tmp_path, failure):
    case, packet, sealed = complete_case(tmp_path)
    if failure == "placeholder":
        packet = {"fixture_status": "INSUFFICIENT_FIXTURE"}
    elif failure == "empty_market":
        packet["records"][1]["record"] = {"artifact_kind": "REFERENCE_MISSING_MARKET_INPUTS"}
    elif failure == "missing_source":
        packet["records"] = packet["records"][1:]
    elif failure in {"raw_tamper", "unmasked_raw"}:
        raw = tmp_path / "public/raw.txt"
        raw.write_text("ExampleCorp proposed a rule.")
        if failure == "unmasked_raw":
            packet["records"][0]["record"]["raw_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
    elif failure == "cutoff_shift":
        packet["cutoff"] = "2026-09-21T15:03:00Z"
    elif failure == "oracle_in_input":
        packet["config"]["hidden"] = {"expected_outputs": {"state": "PROPOSED"}}
    elif failure == "no_oracle":
        case.pop("sealed_outcomes")
    elif failure == "oracle_in_public":
        case["sealed_outcomes"] = save(tmp_path / "public/sealed.json", sealed)
    elif failure == "oracle_for_different_input":
        sealed["input_sha256"] = "0" * 64
        case["sealed_outcomes"] = save(tmp_path / "evaluator/sealed.json", sealed)
    packet["input_sha256"] = digest({k: v for k, v in packet.items() if k != "input_sha256"})
    case["acceptance_input"] = save(tmp_path / "public/input.json", packet)
    if failure not in {"no_oracle", "oracle_in_public", "oracle_for_different_input"}:
        sealed["input_sha256"] = digest(packet)
        case["sealed_outcomes"] = save(tmp_path / "evaluator/sealed.json", sealed)
    expected = {
        "placeholder": "placeholder",
        "empty_market": "populated market",
        "missing_source": "missing source items",
        "raw_tamper": "retained hash",
        "unmasked_raw": "original identities",
        "cutoff_shift": "cutoff moved",
        "oracle_in_input": "leaked",
        "no_oracle": "sealed outcomes: missing",
        "oracle_in_public": "outcomes are inside",
        "oracle_for_different_input": "not bound",
    }[failure]
    assert any(expected in error for error in check_ready(tmp_path, case))


def test_ready_fixture_does_not_mean_executed(tmp_path):
    case, _, _ = complete_case(tmp_path)
    case.pop("acceptance_run")
    assert not check_ready(tmp_path, case)
    assert check_completed(tmp_path, case)


def test_success_label_cannot_hide_a_failed_assertion(tmp_path):
    case, packet, _ = complete_case(tmp_path)
    result = {"event_state": "LIVE"}
    case["acceptance_run"] = save(
        tmp_path / "results/run.json",
        {
            "started_at": "2026-10-18T01:00:00Z",
            "passed": True,
            "frozen_output": save(
                tmp_path / "results/wrong.json",
                {
                    "status": "COMPLETED",
                    "input_sha256": digest(packet),
                    "output": result,
                    "output_sha256": digest(result),
                },
            ),
        },
    )
    assert any("assertion failed" in error for error in check_completed(tmp_path, case))


def test_post_run_oracle_is_not_a_preregistered_expectation(tmp_path):
    case, _, sealed = complete_case(tmp_path)
    sealed["sealed_at"] = "2026-10-18T02:00:00Z"
    case["sealed_outcomes"] = save(tmp_path / "evaluator/sealed.json", sealed)
    assert any("after the run" in error for error in check_completed(tmp_path, case))


def test_date_precision_is_preserved_without_inventing_a_midnight_publication():
    case = {"fixture_id": "F-18", "cutoff_spec": "13 Oct 2026"}
    packet = {
        "cutoff": "2026-10-14T00:00:00Z",
        "fixture_reference": {
            "cutoff_policy": "UTC_DAY_END_EXCLUSIVE",
            "cutoff_day": "2026-10-13",
        },
    }
    assert not cutoff_errors(case, packet)
    packet["fixture_reference"]["cutoff_day"] = "2026-10-14"
    assert cutoff_errors(case, packet)


def test_whole_minute_reference_requires_all_declared_precision_fields():
    case = {"fixture_id": "F-05", "cutoff_spec": "26 Sep 2026 03:50"}
    reference = {
        "cutoff_policy": "UTC_MINUTE_END_EXCLUSIVE",
        "cutoff_spec": case["cutoff_spec"],
        "cutoff_instant": "2026-09-26T03:51:00Z",
        "original_call_precision": "BUCKET(1m)",
        "chosen_boundary_basis": "WHOLE_NAMED_MINUTE_END_EXCLUSIVE",
    }
    packet = {"cutoff": reference["cutoff_instant"], "fixture_reference": reference}
    assert not cutoff_errors(case, packet)
    for key in reference:
        changed = dict(reference)
        changed.pop(key)
        assert cutoff_errors(case, dict(packet, fixture_reference=changed))
    assert cutoff_errors(case, dict(packet, cutoff="2026-09-26T03:52:00Z"))


def test_literal_sets_bounds_and_conditional_tactical_apply_to_every_token():
    predicate = {
        "path": ["decisions", "*"],
        "satisfies": {
            "all": [
                {"path": ["capture"], "in": ["INDIRECT", "NARRATIVE_ONLY", "NONE"]},
                {"path": ["thesis_grade"], "in": ["C", "D", "E"]},
                {"path": ["capture_score"], "lte": 5},
                {
                    "when": {"path": ["action"], "in": ["START", "ADD", "HOLD", "WATCH_TO_START"]},
                    "path": ["tactical"],
                    "equals": True,
                },
            ]
        },
    }
    row = {
        "capture": "INDIRECT",
        "thesis_grade": "C",
        "capture_score": 5,
        "action": "START",
        "tactical": True,
    }
    assert assertion_passes({"decisions": [row]}, predicate)
    assert assertion_passes({"decisions": [dict(row, action="WAIT", tactical=False)]}, predicate)
    for wrong in [
        dict(row, capture="DIRECT"),
        dict(row, thesis_grade="B"),
        dict(row, capture_score=6),
        dict(row, tactical=False),
    ]:
        assert not assertion_passes({"decisions": [row, wrong]}, predicate)
    assert not assertion_passes({"decisions": []}, predicate)
    assert not assertion_passes({"decisions": [{k: v for k, v in row.items() if k != "action"}]}, predicate)


@pytest.mark.parametrize(
    "operator,expected,value,passes",
    [
        ("not_equals", "START", "WAIT", True),
        ("not_in", ["START", "ADD"], "ADD", False),
        ("gte", 4, 4, True),
        ("gte", 1, True, False),
        ("length_equals", 0, [], True),
        ("equals", 1, True, False),
    ],
)
def test_predicates_reject_missing_values_and_bool_number_coercion(operator, expected, value, passes):
    predicate = {"path": ["value"], operator: expected}
    assert assertion_passes({"value": value}, predicate) is passes
    assert not assertion_passes({}, predicate)


def test_ambiguous_and_unknown_operators_are_not_executable():
    for predicate in [
        {"path": ["value"], "equals": 1, "lte": 2},
        {"path": ["value"], "eval": "True"},
        {"any": []},
    ]:
        with pytest.raises(ValueError):
            assertion_passes({"value": 1}, predicate)


def test_existential_token_predicate_needs_a_matching_row():
    predicate = {"path": ["edges"], "some": {"path": ["token_id"], "equals": "asset-7"}}
    assert assertion_passes({"edges": [{"token_id": "asset-2"}, {"token_id": "asset-7"}]}, predicate)
    for output in [{}, {"edges": []}, {"edges": [{"token_id": "asset-2"}]}, {"edges": [{}]}]:
        assert not assertion_passes(output, predicate)


def test_proper_name_mask_preserves_common_geographic_noun(tmp_path):
    case, packet, sealed = complete_case(tmp_path)
    mapping = {"replacements": {"World": "Issuer 7"}, "case_sensitive_names": ["World"]}
    case["private_name_map"] = save(tmp_path / "evaluator/map.json", mapping)
    packet["masking"]["map_sha256"] = digest(mapping)
    for text, leaked in [("Issuer 7 operates across the world.", False), ("World operates globally.", True)]:
        raw = tmp_path / "public/raw.txt"
        raw.write_text(text)
        packet["records"][0]["record"].update(
            text=text, raw_sha256=hashlib.sha256(raw.read_bytes()).hexdigest()
        )
        packet["input_sha256"] = digest({k: v for k, v in packet.items() if k != "input_sha256"})
        case["acceptance_input"] = save(tmp_path / "public/input.json", packet)
        sealed["input_sha256"] = digest(packet)
        case["sealed_outcomes"] = save(tmp_path / "evaluator/sealed.json", sealed)
        errors = check_ready(tmp_path, case)
        assert bool(errors) is leaked
        assert not errors or any("original identities" in error for error in errors)


def test_empty_map_only_for_exact_already_anonymous_f25(tmp_path):
    case, packet, sealed = complete_case(tmp_path)
    lead = "Yesterday we shipped X and told all the press, sorry for posting late"
    authority = save(
        tmp_path / "src/tank/resources/fixture-input-authority-v1.json",
        {"cases": [{"fixture_id": "F-25", "source_leads": [lead]}]},
    )
    case.update(fixture_id="F-25", cutoff_spec="16 Oct 2026 12:01")
    packet.update(
        cutoff="2026-10-16T12:01:00Z", fixture_reference={"authoritative_input_sha256": authority["sha256"]}
    )
    mapping = {"replacements": {}, "already_anonymous": True}
    case["private_name_map"] = save(tmp_path / "evaluator/map.json", mapping)
    packet["masking"].update(map_sha256=digest(mapping), already_anonymous=True)
    raw = tmp_path / "public/raw.txt"
    raw.write_text(lead)
    source = packet["records"][0]["record"]
    source.update(
        text=lead,
        title=lead,
        body=lead,
        author="DECLARED_SYNTHETIC_FOUNDER",
        author_label="DECLARED_SYNTHETIC_FOUNDER",
        declared_synthetic=True,
        source_id="S29",
        raw_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),
    )

    def errors():
        packet["input_sha256"] = digest({k: v for k, v in packet.items() if k != "input_sha256"})
        case["acceptance_input"] = save(tmp_path / "public/input.json", packet)
        sealed.update(fixture_id="F-25", input_sha256=digest(packet))
        case["sealed_outcomes"] = save(tmp_path / "evaluator/sealed.json", sealed)
        return check_ready(tmp_path, case)

    assert not errors()
    source["author"] = "A real named founder"
    assert errors()
    source["author"] = "DECLARED_SYNTHETIC_FOUNDER"
    source["text"] += " World is live."
    assert errors()
    source["text"] = lead
    packet["masking"].pop("already_anonymous")
    assert errors()
