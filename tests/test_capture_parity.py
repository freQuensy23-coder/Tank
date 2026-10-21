"""tank-capture parity: the same recorded exchange frames through tank.market and the Go port.

scripts/capture/parity.py runs every fixture of capture/testdata/frames through the
Python reference and the capture-parity binary and requires identical normalized trades,
typed rows, flow bars, fingerprints and watermarks.
"""

import importlib.util
import os
import pwd
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CAPTURE = ROOT / "capture"


def script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / "capture" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_go_schema_copy_matches_market_schema():
    gen_schema = script("gen_schema")
    assert gen_schema.OUT.read_text() == gen_schema.render(), (
        "run: uv run python scripts/capture/gen_schema.py"
    )


def go_environment(tmp_path):
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
        GOFLAGS="-mod=readonly",
        CGO_ENABLED="0",
    )


def test_go_port_matches_python_reference_on_recorded_frames(tmp_path):
    if shutil.which("go") is None:
        pytest.skip("Go toolchain not installed")
    binary = tmp_path / "capture-parity"
    build = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/capture-parity"],
        check=False,
        cwd=CAPTURE,
        env=go_environment(tmp_path),
        capture_output=True,
        text=True,
        timeout=600,
    )
    if build.returncode and ("lookup disabled" in build.stderr or "requires go >=" in build.stderr):
        pytest.skip("Go toolchain or module cache unavailable offline: " + build.stderr.strip()[-300:])
    assert build.returncode == 0, build.stderr
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "capture" / "parity.py"), "--check", "--go", str(binary)],
        check=False,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    fixtures = sorted((CAPTURE / "testdata" / "frames").glob("*.jsonl.gz"))
    passed = [line for line in result.stdout.splitlines() if line.endswith(" ok")]
    assert fixtures and len(passed) == len(fixtures)
