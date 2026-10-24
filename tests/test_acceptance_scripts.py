"""Offline checks of the read-only acceptance measurement helpers (no database)."""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1] / "scripts" / "acceptance"


def load(name: str) -> ModuleType:
    sys.path.insert(0, str(ROOT))
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_percentile_nearest_rank() -> None:
    common = load("common")
    values = list(range(1, 101))
    assert common.percentile(values, 95) == 95
    assert common.percentile([10], 95) == 10
    assert common.percentile([], 95) is None


def test_summary_unmeasured_is_never_a_pass() -> None:
    common = load("common")
    assert common.summary([10, None], 90)["status"] == "UNVERIFIED"
    assert common.summary([], 90)["status"] == "NO_SAMPLES"
    assert common.summary([80, 85, 100], 90, "max")["status"] == "FAIL"
    assert common.summary([80, 85, 89], 90, "max")["status"] == "PASS"


def test_source_ratio_uses_counter_deltas_and_cadence() -> None:
    health = load("v0a_health")
    start = {
        "taken_at": "2026-10-21T00:00:00Z",
        "feeds": {
            "S01:0": {"reads": {"ok": 1000, "failed": 5}, "detail_reads": {"ok": 0, "failed": 0}},
        },
    }
    end = {
        "taken_at": "2026-10-21T01:00:00Z",
        "feeds": {
            "S01:0": {
                "reads": {"ok": 1059, "failed": 6},
                "detail_reads": {"ok": 0, "failed": 0},
                "status": "HEALTHY",
            },
            "S02:0": {
                "reads": {"ok": 30, "failed": 0},
                "detail_reads": {"ok": 10, "failed": 0},
                "status": "HEALTHY",
            },
        },
    }
    rows = {
        r["source_id"]: r for r in health.sources(start, end, {"S01:0": 60, "S02:0": 60, "S03:0": 60}, 3600)
    }
    assert rows["S01"]["reads"] == 60 and rows["S01"]["failures"] == 1
    assert rows["S01"]["status"] == "FAIL"
    assert rows["S02"]["status"] == "FAIL"
    assert rows["S03"]["reads"] == 0 and rows["S03"]["status"] == "FAIL"


@pytest.mark.integration
@pytest.mark.process
def test_clickhouse_parameters_remain_literal_shell_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    common = load("common")
    client = tmp_path / "clickhouse-client"
    client.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$TANK_TEST_ARGUMENTS"\nprintf "{\\"rows\\":1}\\n"\n')
    client.chmod(0o755)
    arguments = tmp_path / "arguments"
    marker = tmp_path / "injected"
    value = f"'; touch {marker}; #"
    run = subprocess.run

    def local_shell(
        args: list[str], *, input: str, capture_output: bool, text: bool, check: bool
    ) -> subprocess.CompletedProcess[str]:
        assert args[:4] == ["docker", "exec", "-i", common.CH_CONTAINER]
        return run(
            args[4:],
            input=input,
            capture_output=capture_output,
            text=text,
            check=check,
            env={
                **os.environ,
                "PATH": str(tmp_path) + os.pathsep + os.defpath,
                "TANK_DB_PASSWORD": "disposable-test-value",
                "TANK_TEST_ARGUMENTS": str(arguments),
            },
        )

    monkeypatch.setattr(common.subprocess, "run", local_shell)
    rows = common.market_query("SELECT 1", {"since": value})
    assert rows == [json.loads('{"rows":1}')]
    assert f"--param_since={value}" in arguments.read_text().splitlines()
    assert not marker.exists()
