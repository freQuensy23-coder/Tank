"""tank-capture parity: recorded exchange traffic through the Go port against the goldens.

scripts/capture/parity.py runs every fixture of capture/testdata/frames and
capture/testdata/rest through the capture-parity binary and requires the full outputs the
retired Python capture produced (scripts/capture/goldens): normalized trades, typed rows,
flow bars, fingerprints and watermarks.
"""

import importlib.util
import os
import pwd
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

ROOT = Path(__file__).resolve().parents[1]
CAPTURE = ROOT / "capture"


if TYPE_CHECKING:
    from scripts.capture import gen_schema
else:
    spec = importlib.util.spec_from_file_location("gen_schema", ROOT / "scripts/capture/gen_schema.py")
    assert spec is not None
    assert spec.loader is not None
    gen_schema = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = gen_schema
    spec.loader.exec_module(gen_schema)


def test_go_schema_copy_matches_market_schema() -> None:
    assert gen_schema.OUT.read_text() == gen_schema.render(), (
        "run: uv run python scripts/capture/gen_schema.py"
    )


def go_environment(tmp_path: Path) -> dict[str, str]:
    """The operator's Go module and build caches (pytest moved HOME and the XDG roots);
    modules and the go.mod toolchain come from the module cache, never the network."""
    probe = {name: value for name, value in os.environ.items() if not name.startswith("XDG_")}
    probe["HOME"] = pwd.getpwuid(os.getuid()).pw_dir
    module_cache, build_cache = subprocess.run(
        ["go", "env", "GOMODCACHE", "GOCACHE"], env=probe, capture_output=True, text=True, check=True
    ).stdout.split()
    return dict(
        os.environ,
        HOME=str(tmp_path),
        GOMODCACHE=module_cache,
        GOCACHE=build_cache,
        GOPROXY="off",
        GOSUMDB="off",
        GOTOOLCHAIN="local",
        GOENV="off",
        GOFLAGS="-mod=readonly",
        CGO_ENABLED="0",
    )


@pytest.fixture(scope="module")
def parity_binary(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if shutil.which("go") is None:
        pytest.fail("Go 1.25 and its prefilled module cache are required; see docs/TESTING.md")
    directory = tmp_path_factory.mktemp("capture-parity")
    binary = directory / "capture-parity"
    build = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/capture-parity"],
        check=False,
        cwd=CAPTURE,
        env=go_environment(directory),
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert build.returncode == 0, build.stderr
    return binary


def run_parity(binary: Path, fixtures: Sequence[Path]) -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "capture" / "parity.py"), "--check", "--go", str(binary)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    passed = [line for line in result.stdout.splitlines() if line.endswith(" ok")]
    assert fixtures and len(passed) == len(fixtures)


@pytest.mark.integration
@pytest.mark.process
def test_go_port_matches_the_goldens_on_recorded_frames_and_rest_polls(parity_binary: Path) -> None:
    testdata = CAPTURE / "testdata"
    run_parity(parity_binary, [*testdata.glob("frames/*.jsonl.gz"), *testdata.glob("rest/*.json.gz")])
