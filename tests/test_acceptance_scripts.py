"""Offline checks of the read-only acceptance measurement helpers (no database)."""
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "scripts" / "acceptance"


def load(name):
    sys.path.insert(0, str(ROOT))
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_percentile_nearest_rank():
    common = load("common")
    values = list(range(1, 101))
    assert common.percentile(values, 95) == 95
    assert common.percentile([10], 95) == 10
    assert common.percentile([], 95) is None


def test_summary_unmeasured_is_never_a_pass():
    common = load("common")
    assert common.summary([10, None], 90)["status"] == "UNVERIFIED"
    assert common.summary([], 90)["status"] == "NO_SAMPLES"
    assert common.summary([80, 85, 100], 90, "max")["status"] == "FAIL"
    assert common.summary([80, 85, 89], 90, "max")["status"] == "PASS"


def test_source_ratio_counts_only_window_and_non_baseline(tmp_path):
    health = load("v0a_health")
    reads = tmp_path / "reads.jsonl"
    rows = [
        '{"kind": "ATTEMPT", "key": "S01:0", "source_id": "S01", "at": "2026-10-20T00:00:00Z", "baseline": true}',
        *[f'{{"kind": "ATTEMPT", "key": "S01:0", "source_id": "S01", "at": "2026-10-21T00:{m:02d}:00Z", "baseline": false}}'
          for m in range(0, 60)],
        '{"kind": "FAILURE", "key": "S01:0", "source_id": "S01", "at": "2026-10-21T00:30:05Z", "baseline": false}',
    ]
    reads.write_text("\n".join(rows) + "\n")
    out, _ = health.sources(str(reads), "2026-10-21T00:00:00Z", "2026-10-21T01:00:00Z", ["S01", "S02"], {"S01": 60})
    s01 = next(r for r in out if r["source_id"] == "S01")
    s02 = next(r for r in out if r["source_id"] == "S02")
    assert s01["attempts"] == 60 and s01["failures"] == 1
    assert s01["status"] == "FAIL"  # 1/60 > 1%
    assert s02["status"] == "FAIL" and s02["attempts"] == 0
