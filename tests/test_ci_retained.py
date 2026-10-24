import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def retained_stage() -> ModuleType:
    source = Path(__file__).parents[1] / "scripts/ci/stage_retained_fixtures.py"
    spec = importlib.util.spec_from_file_location("stage_retained_fixtures", source)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_staging_preserves_actual_retained_bytes_and_refuses_overwrite(
    retained_stage: ModuleType, tmp_path: Path
) -> None:
    destination = tmp_path / "staged"
    report = retained_stage.stage(destination)
    outputs = [path for path in destination.rglob("*") if path.is_file()]
    assert report["files"] == len(outputs)
    assert (destination / "results.json").is_file()
    original = {path.relative_to(destination): path.read_bytes() for path in outputs}
    assert retained_stage.stage(destination) == report
    assert {path.relative_to(destination): path.read_bytes() for path in outputs} == original
    target = outputs[0]
    target.write_bytes(b"operator-owned different bytes")
    with pytest.raises(ValueError, match="refusing to overwrite"):
        retained_stage.stage(destination)
    assert target.read_bytes() == b"operator-owned different bytes"


def test_staging_rejects_destination_symlink(retained_stage: ModuleType, tmp_path: Path) -> None:
    owned = tmp_path / "owned"
    owned.mkdir()
    destination = tmp_path / "linked"
    destination.symlink_to(owned, target_is_directory=True)
    with pytest.raises(ValueError, match="must not be a symlink"):
        retained_stage.stage(destination)
    assert list(owned.iterdir()) == []
