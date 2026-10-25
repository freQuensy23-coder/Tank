from pathlib import Path

import pytest

from scripts.ci import refactor_inventory as inventory_module
from scripts.ci.openma_inventory import archive_sources, changed_ranges


def test_inventory_tracks_nested_symbols_and_real_boundaries(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    (source / "candidate.py").write_text(
        "from typing import Any\n"
        "class Boundary:\n"
        "    def parse(self, payload, limit=None):\n"
        "        def inner(value: Any):\n"
        "            return value\n"
        "        return payload.get('data') or {}\n"
    )
    report = inventory_module.inventory(tmp_path)
    found = {(item.symbol, item.kind, item.expression) for item in report.findings}
    assert ("Boundary.parse", "untyped_parameter", "payload") in found
    assert ("Boundary.parse", "none_default", "None") in found
    assert ("Boundary.parse.inner", "any", "Any") in found
    assert ("Boundary.parse", "fallback", "payload.get('data')") in found
    assert report.files == 1
    assert report.counts["untyped_parameter"] == 2


def test_inventory_preserves_fixture_and_third_party_exclusions(tmp_path: Path) -> None:
    for directory in ("deploy/upstream", "src/vendor"):
        path = tmp_path / directory
        path.mkdir(parents=True)
        (path / "retained.py").write_text("this deliberately is not Python syntax")
    for directory in ("tests/goldens", "capture/testdata"):
        path = tmp_path / directory
        path.mkdir(parents=True)
        (path / "retained.json").write_text("immutable payload")
        (path / "generator.py").write_text("def generate():\n    return 1\n")
    path = tmp_path / "capture/internal"
    path.mkdir()
    (path / "boundary.go").write_text("package capture\nfunc decode(v map[string]any) {}\n")
    report = inventory_module.inventory(tmp_path)
    assert report.files == 3
    assert report.counts == {"dynamic_go": 1, "untyped_return": 2}
    paths = {finding.path for finding in report.findings}
    assert paths == {
        "capture/internal/boundary.go",
        "capture/testdata/generator.py",
        "tests/goldens/generator.py",
    }


def test_inventory_rejects_unparseable_active_source(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src/broken.py").write_text("def broken(")
    with pytest.raises(SyntaxError):
        inventory_module.inventory(tmp_path)


def test_inventory_reports_import_cycles_without_importing_modules(tmp_path: Path) -> None:
    package = tmp_path / "src/tank"
    package.mkdir(parents=True)
    (package / "a.py").write_text("import tank.b\nraise RuntimeError('must never execute')\n")
    (package / "b.py").write_text("from tank.a import value\n")
    report = inventory_module.inventory(tmp_path)
    assert report.imports == {"tank.a": ["tank.b"], "tank.b": ["tank.a"]}
    assert report.cycles == [["tank.a", "tank.b"]]


def test_inventory_distinguishes_trivial_assertions_from_observed_calls(tmp_path: Path) -> None:
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/assertions.py").write_text(
        "def proof(value, observe):\n"
        "    assert True\n"
        "    assert value == value\n"
        "    assert observe() == observe()\n"
    )
    report = inventory_module.inventory(tmp_path)
    assertions = [(item.kind, item.expression) for item in report.findings if item.kind.startswith("assert_")]
    assert assertions == [("assert_constant", "True"), ("assert_self_comparison", "value == value")]


def test_inventory_includes_explicit_embedded_python_with_original_locations(tmp_path: Path) -> None:
    snippet = inventory_module.EmbeddedPython(
        path="deploy/probe.mjs",
        line=40,
        symbol="PROBE",
        language="python",
        encoding="raw-template",
        source="def decode(payload):\n    return payload.get('value')\n",
    )
    report = inventory_module.inventory(tmp_path, (snippet,))
    assert report.embedded_python == 1
    found = {(item.path, item.line, item.symbol, item.kind) for item in report.findings}
    assert ("deploy/probe.mjs", 40, "PROBE.decode", "untyped_parameter") in found
    assert ("deploy/probe.mjs", 41, "PROBE.decode", "fallback") in found


def test_inventory_observes_exact_historical_recipe_and_rejects_changed_exemption(tmp_path: Path) -> None:
    relative = Path("capture/internal/budget/testdata/gen_vectors.py")
    source = Path(__file__).parents[1] / relative
    target = tmp_path / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(source.read_bytes())
    report = inventory_module.inventory(tmp_path)
    assert report.files == 0
    reference = report.reference_programs[str(relative)]
    assert reference.provenance.role == "historical-oracle-recipe"
    assert reference.findings
    assert any(item.kind == "untyped_parameter" for item in reference.findings)
    target.write_text(target.read_text() + "\n")
    with pytest.raises(ValueError, match="Historical reference program changed"):
        inventory_module.inventory(tmp_path)


def test_inventory_keeps_decoded_worker_locations_inside_the_actual_container(tmp_path: Path) -> None:
    snippet = inventory_module.EmbeddedPython(
        path="vendor/openma/worker.ts",
        line=1,
        symbol="WORKER",
        language="python",
        encoding="decoded-literal",
        source="import json\n\ndef decode(payload):\n    return payload.get('value')\n",
    )
    report = inventory_module.inventory(tmp_path, (snippet,))
    assert all(finding.line == 1 for finding in report.findings)
    assert any(
        finding.symbol == "WORKER.decode:decoded-python-line:4" and finding.kind == "fallback"
        for finding in report.findings
    )


def test_vendor_inventory_rejects_an_unpinned_archive_before_parsing(tmp_path: Path) -> None:
    archive = tmp_path / "not-upstream.tar.gz"
    archive.write_bytes(b"untrusted archive contents")
    with pytest.raises(ValueError, match="pinned checksum"):
        archive_sources(archive)


def test_vendor_diff_keeps_authored_lines_separate_from_preserved_signatures() -> None:
    original = "def upstream(value):\n    return value\n"
    modified = "def upstream(value):\n    captured = value.get('data')\n    return captured\n"
    ranges, removed = changed_ranges(original, modified)
    authored_lines = {line for span in ranges for line in range(span.start, span.end + 1)}
    assert 1 not in authored_lines
    assert authored_lines == {2, 3}
    assert removed == 1
    ranges, removed = changed_ranges(modified, "def upstream(value):\n")
    assert not ranges and removed == 2
