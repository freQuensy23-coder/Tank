"""V0-C/V0-D fixture status from tests/fixtures/manifest.json (03 §11.3), read-only.

The pytest gates over the manifest were removed by user ruling (tank-ops sweep, main 1816a0c);
this report keeps the same checks visible: per fixture, check_ready and check_completed from
tests/fixture_acceptance.py, plus readiness and the manifest's own `missing` notes.

Usage: /srv/tank/.venv/bin/python fixture_status.py [--root /srv/tank] [--json OUT]
"""
import argparse
import json
import sys
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--root", default="/srv/tank")
ap.add_argument("--json")
a = ap.parse_args()
root = Path(a.root)
sys.path.insert(0, str(root / "tests"))
sys.path.insert(0, str(root / "src"))
from fixture_acceptance import check_completed, check_ready  # noqa: E402

rows = []
for case in json.loads((root / "tests/fixtures/manifest.json").read_text())["fixtures"]:
    ready = check_ready(root, case)
    completed = check_completed(root, case) if not ready and case.get("acceptance_run") else ready or ["no acceptance run"]
    rows.append({"fixture_id": case["fixture_id"], "readiness": case.get("readiness"),
                 "ready": not ready, "passed": not completed, "errors": (ready or completed)[:5],
                 "missing": case.get("missing", [])})
print(f"{'ID':5} {'ready':5} {'pass':5} readiness")
for r in rows:
    print(f"{r['fixture_id']:5} {str(r['ready']):5} {str(r['passed']):5} {r['readiness']}")
print(f"\npassed {sum(r['passed'] for r in rows)}/27, ready {sum(r['ready'] for r in rows)}/27")
if a.json:
    Path(a.json).write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n")
