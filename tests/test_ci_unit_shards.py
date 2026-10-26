import hashlib
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from scripts.ci.build_unit_plan import build_plan
from scripts.ci.check_unit_results import verify
from scripts.ci.unit_contracts import (
    FileTiming,
    UnitCase,
    UnitCollection,
    UnitDeclaration,
    UnitPlan,
    UnitShard,
    UnitSummary,
    measured_assignments,
    parse_shard,
    read_model,
    shard_for,
    weight_coverage,
)
from scripts.ci.unit_shard import UnitFailure, UnitWorkerCrash, failure_from_report, pytest_runtest_logreport
from tank.contracts.json import DuplicateJsonKeyError

SOURCE = "a" * 40


@dataclass(frozen=True)
class GateFixture:
    plan_path: Path
    plan: UnitPlan
    collection: UnitCollection
    bundles: list[Path]


def write_report(path: Path, cases: list[UnitCase], seconds: str = "0.1") -> None:
    root = ET.Element("testsuite")
    for case in cases:
        ET.SubElement(root, "testcase", classname=case.classname, name=case.name, time=seconds)
    ET.ElementTree(root).write(path)


def gate_fixture(root: Path) -> GateFixture:
    collection = UnitCollection(
        schema_version="tank-unit-collection-v1",
        source_head=SOURCE,
        cases=[
            UnitCase(classname=f"tests.test_{letter}", name=f"test_{index}", file=f"tests/test_{letter}.py")
            for letter in "ab"
            for index in range(2)
        ],
    )
    plan = UnitPlan(
        schema_version="tank-unit-plan-v1",
        source_head=SOURCE,
        collection_sha256="c" * 64,
        reports_sha256=["d" * 64],
        files=[
            FileTiming(file="tests/test_a.py", cases=2, seconds=Decimal(10)),
            FileTiming(file="tests/test_b.py", cases=2, seconds=Decimal(5)),
        ],
    )
    path = root / "plan.json"
    path.write_text(plan.model_dump_json())
    measured = measured_assignments(plan.files)
    bundles: list[Path] = []
    for index in range(2):
        bundle = root / f"shard-{index}"
        bundle.mkdir()
        (bundle / "unit-collection.json").write_text(collection.model_dump_json())
        declaration = UnitDeclaration(
            schema_version="tank-unit-declaration-v1",
            source_head=SOURCE,
            shard=UnitShard(index=index, count=2),
            collection_sha256=collection.signature,
            plan_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            coverage=weight_coverage(collection, plan),
        )
        (bundle / "unit-run.json").write_text(declaration.model_dump_json())
        write_report(
            bundle / "unit.xml",
            [case for case in collection.cases if shard_for(case.file, measured) == index],
        )
        bundles.append(bundle)
    return GateFixture(path, plan, collection, bundles)


def test_actual_calibration_rejects_missing_cases_and_balances_whole_files(tmp_path: Path) -> None:
    fixture = gate_fixture(tmp_path)
    report = tmp_path / "actual.xml"
    write_report(report, fixture.collection.cases)
    collection = tmp_path / "baseline.json"
    collection.write_text(json.dumps([case.model_dump() for case in fixture.collection.cases]))
    plan = build_plan([report], collection, SOURCE)
    assert sum(file.cases for file in plan.files) == 4
    assert sum((file.seconds for file in plan.files), Decimal(0)) == Decimal("0.4")
    assignments = measured_assignments(plan.files)
    assert assignments["tests/test_a.py"] != assignments["tests/test_b.py"]
    write_report(report, fixture.collection.cases[:-1])
    with pytest.raises(ValueError, match="full unit collection"):
        build_plan([report], collection, SOURCE)


def test_partition_uses_measured_zero_and_explicit_unmeasured_hash_without_guessing_time(
    tmp_path: Path,
) -> None:
    fixture = gate_fixture(tmp_path)
    zero = [FileTiming(file=file.file, cases=file.cases, seconds=Decimal(0)) for file in fixture.plan.files]
    before = measured_assignments(zero)
    assert set(before.values()) == {0, 1}
    unknown = "tests/test_future.py"
    index = shard_for(unknown, before)
    assert index in {0, 1}
    assert unknown not in before
    assert measured_assignments(list(reversed(zero))) == before
    expanded = UnitCollection(
        schema_version="tank-unit-collection-v1",
        source_head=SOURCE,
        cases=[
            *fixture.collection.cases,
            UnitCase(classname="tests.test_future", name="test_new", file=unknown),
        ],
    )
    assert weight_coverage(expanded, fixture.plan).unmeasured_files == [unknown]
    assert shard_for(unknown, before) == index


def test_both_actual_lane_reports_and_full_union_are_required(tmp_path: Path) -> None:
    fixture = gate_fixture(tmp_path)
    result = verify(fixture.bundles, fixture.plan_path, SOURCE, None)
    assert result.cases == result.passed == 4 and result.skipped == 0
    assert result.p90_seconds == result.max_seconds == Decimal("0.1")
    for index, bundle in enumerate(fixture.bundles):
        assert verify([bundle], fixture.plan_path, SOURCE, UnitShard(index=index, count=2)).cases == 2
    for invalid in (fixture.bundles[:1], [fixture.bundles[0], fixture.bundles[0]]):
        with pytest.raises(ValueError, match="both distinct lanes"):
            verify(invalid, fixture.plan_path, SOURCE, None)


@pytest.mark.parametrize("change", ["missing", "duplicate", "swapped"])
def test_report_omission_duplicate_or_wrong_lane_is_rejected(tmp_path: Path, change: str) -> None:
    fixture = gate_fixture(tmp_path)
    report = fixture.bundles[0] / "unit.xml"
    tree = ET.parse(report)
    root = tree.getroot()
    if change == "missing":
        root.remove(root[0])
    elif change == "duplicate":
        root.append(root[0])
    else:
        root[0].set("classname", "tests.test_b")
    tree.write(report)
    with pytest.raises(ValueError, match="full selected case set"):
        verify(fixture.bundles, fixture.plan_path, SOURCE, None)


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_any_unsuccessful_phase_cannot_qualify_unit_union(tmp_path: Path, outcome: str) -> None:
    fixture = gate_fixture(tmp_path)
    report = fixture.bundles[0] / "unit.xml"
    tree = ET.parse(report)
    ET.SubElement(tree.getroot()[0], outcome)
    tree.write(report)
    with pytest.raises(ValueError, match="zero failures, errors and skips"):
        verify(fixture.bundles, fixture.plan_path, SOURCE, None)


@pytest.mark.parametrize("seconds", ["1", "10", "NaN", "Infinity", "-0.1"])
def test_timing_limits_and_invalid_duration_refuse_qualification(tmp_path: Path, seconds: str) -> None:
    fixture = gate_fixture(tmp_path)
    report = fixture.bundles[0] / "unit.xml"
    tree = ET.parse(report)
    for case in tree.getroot():
        case.set("time", seconds)
    tree.write(report)
    with pytest.raises(ValueError):
        verify(fixture.bundles, fixture.plan_path, SOURCE, None)


def test_different_source_or_plan_is_refused(tmp_path: Path) -> None:
    fixture = gate_fixture(tmp_path)
    with pytest.raises(ValueError, match="different source"):
        verify(fixture.bundles, fixture.plan_path, "b" * 40, None)
    path = fixture.bundles[0] / "unit-run.json"
    path.write_text(
        path.read_text().replace(hashlib.sha256(fixture.plan_path.read_bytes()).hexdigest(), "e" * 64)
    )
    with pytest.raises(ValueError, match="bind its collection"):
        verify(fixture.bundles, fixture.plan_path, SOURCE, None)


def test_distinct_full_collections_cannot_hide_unexecuted_cases(tmp_path: Path) -> None:
    fixture = gate_fixture(tmp_path)
    bundle = fixture.bundles[1]
    expanded = UnitCollection(
        schema_version="tank-unit-collection-v1",
        source_head=SOURCE,
        cases=[
            *fixture.collection.cases,
            UnitCase(classname="tests.test_a", name="test_hidden", file="tests/test_a.py"),
        ],
    )
    (bundle / "unit-collection.json").write_text(expanded.model_dump_json())
    declaration = UnitDeclaration(
        schema_version="tank-unit-declaration-v1",
        source_head=SOURCE,
        shard=UnitShard(index=1, count=2),
        collection_sha256=expanded.signature,
        plan_sha256=hashlib.sha256(fixture.plan_path.read_bytes()).hexdigest(),
        coverage=weight_coverage(expanded, fixture.plan),
    )
    (bundle / "unit-run.json").write_text(declaration.model_dump_json())
    with pytest.raises(ValueError, match="different full case sets"):
        verify(fixture.bundles, fixture.plan_path, SOURCE, None)


def test_json_duplicate_facts_and_scalar_coercions_are_refused(tmp_path: Path) -> None:
    fixture = gate_fixture(tmp_path)
    path = fixture.bundles[0] / "unit-run.json"
    path.write_text(
        path.read_text().replace('"source_head":', '"source_head":"' + "b" * 40 + '","source_head":', 1)
    )
    with pytest.raises(DuplicateJsonKeyError):
        read_model(path, UnitDeclaration)
    with pytest.raises(ValidationError):
        UnitShard.model_validate({"index": False, "count": 2})
    with pytest.raises(ValidationError):
        UnitShard.model_validate_json('{"index":0,"count":2.0}')
    with pytest.raises(ValidationError):
        UnitSummary.model_validate(
            {"cases": 1, "passed": 1, "skipped": False, "p90_seconds": Decimal(0), "max_seconds": Decimal(0)}
        )


@pytest.mark.parametrize("value", ["0", "0/1", "0/3", "2/2", "01/2", "a/2"])
def test_only_the_declared_two_unit_lanes_are_admitted(value: str) -> None:
    with pytest.raises(ValueError):
        parse_shard(value)


def test_failure_identity_is_flushed_without_waiting_for_final_junit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    report = pytest.TestReport(
        nodeid="tests/test_failed.py::test_contract",
        location=("tests/test_failed.py", 0, "test_contract"),
        keywords={},
        outcome="failed",
        longrepr="synthetic failure",
        when="call",
        duration=0.25,
    )
    with monkeypatch.context() as scoped:
        scoped.chdir(tmp_path)
        scoped.delenv("PYTEST_XDIST_WORKER", raising=False)
        pytest_runtest_logreport(report)
        failure = UnitFailure.model_validate_json(
            (tmp_path / ".test-results/unit-failures.jsonl").read_text()
        )
        assert failure.nodeid == report.nodeid and failure.phase == "call"
        assert (
            'UNIT_FAILURE nodeid="tests/test_failed.py::test_contract" phase=call' in capsys.readouterr().err
        )


def test_pinned_xdist_crash_marker_is_preserved_without_invented_zero_duration() -> None:
    report = pytest.TestReport(
        nodeid="tests/test_crashed.py::test_contract",
        location=("tests/test_crashed.py", 0, "test_contract"),
        keywords={},
        outcome="failed",
        longrepr="worker crashed",
        when="call",
    )
    wire = report._to_json()
    wire["when"] = "???"
    crash = pytest.TestReport._from_json(wire)
    observed = failure_from_report(crash)
    assert isinstance(observed, UnitWorkerCrash)
    assert "duration_seconds" not in observed.model_dump()


def test_current_collection_binds_recalibration_source_and_combines_both_reports(tmp_path: Path) -> None:
    fixture = gate_fixture(tmp_path)
    collection = fixture.bundles[0] / "unit-collection.json"
    reports = [bundle / "unit.xml" for bundle in fixture.bundles]
    plan = build_plan(reports, collection, SOURCE)
    assert sum(file.cases for file in plan.files) == 4
    assert len(plan.reports_sha256) == 2
    with pytest.raises(ValueError, match="Calibration source"):
        build_plan(reports, collection, "b" * 40)
