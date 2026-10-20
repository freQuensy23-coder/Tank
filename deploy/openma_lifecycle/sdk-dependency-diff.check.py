"""Verify the production SDK repair is one declared dependency and importer entry."""

import argparse
import hashlib
import json
from pathlib import Path


def check(old, new):
    old, new = Path(old), Path(new)
    package = "packages/control-plane-node/package.json"
    before = json.loads((old / package).read_text())
    after = json.loads((new / package).read_text())
    assert "@boxlite-ai/boxlite" not in before["dependencies"]
    assert after["dependencies"].pop("@boxlite-ai/boxlite") == "0.10.6"
    assert before == after, "Unrelated package dependency changed"
    previous = (old / "pnpm-lock.yaml").read_text()
    current = (new / "pnpm-lock.yaml").read_text()
    entry = "      '@boxlite-ai/boxlite':\n        specifier: 0.10.6\n        version: 0.10.6(playwright-core@1.59.1)\n"
    head, section = current.split("\n  packages/control-plane-node:\n", 1)
    importer, tail = section.split("\n  packages/", 1)
    assert importer.count(entry) == 1
    stripped = (
        head + "\n  packages/control-plane-node:\n" + importer.replace(entry, "", 1) + "\n  packages/" + tail
    )
    assert stripped == previous, "Lock changed beyond direct SDK importer"
    snapshot = "  '@boxlite-ai/boxlite@0.10.6(playwright-core@1.59.1)':"
    assert previous.count(snapshot) == current.count(snapshot) == 1
    old_meta = json.loads((old / "tank-retained-candidate.json").read_text())
    new_meta = json.loads((new / "tank-retained-candidate.json").read_text())
    assembly = "packages/control-plane-node/src/modules/node-managed.ts"
    assert (old / assembly).read_text().replace(
        old_meta["runtime_identity"], new_meta["runtime_identity"]
    ) == (new / assembly).read_text()
    composite = json.loads((new / "tank-overlay.json").read_text())
    files = composite["patched_file_hashes"] | composite["added_file_hashes"]
    assert len(files) == 43
    assert all(
        hashlib.sha256((new / name).read_bytes()).hexdigest() == digest for name, digest in files.items()
    )
    return {
        "status": "PASS",
        "direct_dependency": "@boxlite-ai/boxlite@0.10.6",
        "lock_importer_only": True,
        "existing_sdk_snapshot_unchanged": True,
        "assembly_identity_only": True,
        "all_composite_hashes_verified": len(files),
        "runtime_identity": new_meta["runtime_identity"],
        "native_vm_or_model_call": False,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(args.baseline, args.candidate), indent=2))
