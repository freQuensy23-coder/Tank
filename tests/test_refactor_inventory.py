import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def inventory_module() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts/ci/refactor_inventory.py"
    spec = importlib.util.spec_from_file_location("refactor_inventory", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_inventory_tracks_nested_symbols_and_real_boundaries(
    inventory_module: ModuleType, tmp_path: Path
) -> None:
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


def test_inventory_preserves_fixture_and_third_party_exclusions(
    inventory_module: ModuleType, tmp_path: Path
) -> None:
    for directory in ("tests/goldens", "capture/testdata", "deploy/upstream", "src/vendor"):
        path = tmp_path / directory
        path.mkdir(parents=True)
        (path / "retained.py").write_text("this deliberately is not Python syntax")
    path = tmp_path / "capture/internal"
    path.mkdir()
    (path / "boundary.go").write_text("package capture\nfunc decode(v map[string]any) {}\n")
    report = inventory_module.inventory(tmp_path)
    assert report.files == 1
    assert report.counts == {"dynamic_go": 1}
    assert report.findings[0].path == "capture/internal/boundary.go"


def test_inventory_rejects_unparseable_active_source(inventory_module: ModuleType, tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src/broken.py").write_text("def broken(")
    with pytest.raises(SyntaxError):
        inventory_module.inventory(tmp_path)


def test_inventory_reports_import_cycles_without_importing_modules(
    inventory_module: ModuleType, tmp_path: Path
) -> None:
    package = tmp_path / "src/tank"
    package.mkdir(parents=True)
    (package / "a.py").write_text("import tank.b\nraise RuntimeError('must never execute')\n")
    (package / "b.py").write_text("from tank.a import value\n")
    report = inventory_module.inventory(tmp_path)
    assert report.imports == {"tank.a": ["tank.b"], "tank.b": ["tank.a"]}
    assert report.cycles == [["tank.a", "tank.b"]]


def test_inventory_distinguishes_trivial_assertions_from_observed_calls(
    inventory_module: ModuleType, tmp_path: Path
) -> None:
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
