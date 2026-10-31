import hashlib
from importlib.machinery import EXTENSION_SUFFIXES

import pytest
from test_market_truth_request import stage_native_module


@pytest.fixture
def native_roots(tmp_path):
    checkout, archived = tmp_path / "checkout", tmp_path / "archived"
    for root in (checkout, archived):
        source = root / "src/tank/analysis/truth.py"
        source.parent.mkdir(parents=True)
        source.write_text("def value():\n    return 1\n")
        (root / "pyproject.toml").write_text("[project]\nname = 'fixture'\n")
    binary = checkout / ("src/tank/analysis/truth" + EXTENSION_SUFFIXES[0])
    binary.write_bytes(b"native-artifact-transport-unit-fixture")
    return checkout, archived, binary


def test_native_staging_preserves_artifact_bytes_and_retains_its_digest(native_roots):
    checkout, archived, binary = native_roots
    receipt = stage_native_module(checkout, archived, binary, "tank.analysis.truth")
    assert (archived / receipt["path"]).read_bytes() == binary.read_bytes()
    assert receipt["sha256"] == hashlib.sha256(binary.read_bytes()).hexdigest()
    assert receipt["path"].startswith("src/tank/analysis/truth")


@pytest.mark.parametrize("mutation", ["source", "build", "symlink", "foreign", "occupied"])
def test_native_staging_refuses_unbound_or_unsafe_artifacts_without_changing_archive(native_roots, mutation):
    checkout, archived, binary = native_roots
    if mutation == "source":
        (archived / "src/tank/analysis/truth.py").write_text("different source")
    elif mutation == "build":
        (archived / "pyproject.toml").write_text("different build")
    elif mutation == "symlink":
        target = binary.with_name("transport-fixture")
        binary.rename(target)
        binary.symlink_to(target)
    elif mutation == "foreign":
        target = checkout.parent / binary.name
        binary.rename(target)
        binary = target
    else:
        (archived / "src/tank/analysis" / binary.name).write_bytes(b"existing-owned-file")
    before = {str(p): p.read_bytes() for p in archived.rglob("*") if p.is_file()}
    with pytest.raises(AssertionError):
        stage_native_module(checkout, archived, binary, "tank.analysis.truth")
    assert {str(p): p.read_bytes() for p in archived.rglob("*") if p.is_file()} == before


def test_native_staging_binds_companion_declaration_bytes(native_roots):
    checkout, archived, binary = native_roots
    declaration = b"cdef object value()\n"
    for root in (checkout, archived):
        (root / "src/tank/analysis/truth.pxd").write_bytes(declaration)
    receipt = stage_native_module(checkout, archived, binary, "tank.analysis.truth")
    assert receipt["declaration_sha256"] == hashlib.sha256(declaration).hexdigest()


@pytest.mark.parametrize("mutation", ["source-missing", "archive-missing", "different", "symlink"])
def test_native_staging_refuses_unbound_declarations_without_writes(native_roots, mutation):
    checkout, archived, binary = native_roots
    source, target = (root / "src/tank/analysis/truth.pxd" for root in (checkout, archived))
    source.write_bytes(b"cdef object value()\n")
    target.write_bytes(source.read_bytes())
    if mutation == "source-missing":
        source.unlink()
    elif mutation == "archive-missing":
        target.unlink()
    elif mutation == "different":
        target.write_bytes(b"cdef object different()\n")
    else:
        other = source.with_name("foreign.pxd")
        source.rename(other)
        source.symlink_to(other)
    before = {str(p): p.read_bytes() for p in archived.rglob("*") if p.is_file()}
    with pytest.raises(AssertionError):
        stage_native_module(checkout, archived, binary, "tank.analysis.truth")
    assert {str(p): p.read_bytes() for p in archived.rglob("*") if p.is_file()} == before
