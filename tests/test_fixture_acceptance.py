"""Actual V0-C/V0-D readiness, not a test that incomplete fixtures stay incomplete."""

import json
from pathlib import Path

import pytest
from fixture_acceptance import check_completed, check_ready, specification_cases

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = json.loads((ROOT / "tests/fixtures/manifest.json").read_text())
CASES = MANIFEST["fixtures"]


def test_all_27_specified_fixtures_are_present():
    assert [case["fixture_id"] for case in CASES] == [f"F-{i:02}" for i in range(1, 28)]
    specifications = specification_cases(ROOT)
    assert len(specifications) == 27
    for case in CASES:
        assert {
            key: case[key] for key in ("case_description", "cutoff_spec", "assertion_spec")
        } == specifications[case["fixture_id"]]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["fixture_id"])
def test_fixture_is_ready_for_spec_acceptance(case):
    errors = check_ready(ROOT, case)
    assert not errors, "\n" + "\n".join(errors)


def test_v0c_v0d_have_completed_acceptance_evidence():
    failures = {case["fixture_id"]: check_completed(ROOT, case) for case in CASES}
    failures = {name: errors for name, errors in failures.items() if errors}
    assert not failures, json.dumps(failures, ensure_ascii=False, indent=2)
