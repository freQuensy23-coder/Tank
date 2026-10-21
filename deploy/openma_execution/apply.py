"""Apply only the reviewed execution-status overlay to an exact prepared source tree."""

import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path


def verify_runtime(root):
    path = Path(__file__).resolve().parent.parent / "prepare_boxlite_runtime.py"
    spec = importlib.util.spec_from_file_location("tank_execution_native_contract", path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper.verify_runtime(root / "tank-deploy/boxlite-runtime")


def apply(root, *, replay_dns=False):
    base = Path(__file__).resolve().parent
    replacements = json.loads((base / "overlay.json").read_text())
    assets = json.loads((base / "assets.json").read_text())
    if replay_dns:
        rebase = json.loads((base / "replay-dns-rebase.json").read_text())
        manifest = json.loads((root / "tank-overlay.json").read_text())
        identity = manifest["retained_lifecycle"]["runtime_identity"]
        if not re.fullmatch(r"[a-f0-9]{64}", identity):
            raise ValueError("Execution requires the actual prepared runtime identity")
        verify_runtime(root)
        matches = [row for row in replacements if row["path"] == rebase["target"]]
        if len(matches) != 1:
            raise ValueError("Execution rebase requires one exact target")
        row = matches[0]
        if (
            row["sha256"] != rebase["original_input_sha256"]
            or row["patched_sha256"] != rebase["original_output_sha256"]
        ):
            raise ValueError("Execution original overlay changed before rebase")
        for key, expected in (("old", "input_sha256"), ("new", "patched_sha256")):
            if row[key].count(rebase["original_runtime_identity"]) != 1:
                raise ValueError("Execution rebase requires one identity literal")
            historical = row[key].replace(rebase["original_runtime_identity"], rebase["runtime_identity"])
            if hashlib.sha256(historical.encode()).hexdigest() != rebase[expected]:
                raise ValueError("Execution identity-only rebase integrity mismatch")
            row[key] = row[key].replace(rebase["original_runtime_identity"], identity)
        row["sha256"] = hashlib.sha256(row["old"].encode()).hexdigest()
        row["patched_sha256"] = hashlib.sha256(row["new"].encode()).hexdigest()
    for item in replacements:
        path = root / item["path"]
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != item["sha256"] or raw.decode() != item["old"]:
            raise ValueError("Execution overlay source guard mismatch: " + item["path"])
        if hashlib.sha256(item["new"].encode()).hexdigest() != item["patched_sha256"]:
            raise ValueError("Execution overlay replacement integrity mismatch")
    for item in assets:
        if (root / item["path"]).exists():
            raise ValueError("Execution overlay new asset already exists: " + item["path"])
        if hashlib.sha256((base / item["asset"]).read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("Execution overlay asset integrity mismatch")
    for item in replacements:
        (root / item["path"]).write_text(item["new"])
    for item in assets:
        path = root / item["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((base / item["asset"]).read_bytes())


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply.py PREPARED_OPENMA_SOURCE_DIRECTORY")
    apply(Path(sys.argv[1]).resolve())
