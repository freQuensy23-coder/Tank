"""Run only isolated public local file-I/O tests; never import BoxLite or contact OpenMA."""

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

BASE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--prepared-source", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    node = subprocess.run([args.node, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    python = subprocess.run(
        [args.python, "-c", 'import sys; print(".".join(map(str,sys.version_info[:3])))'],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if node != "v24.19.0" or not python.startswith("3.12."):
        raise ValueError("Pinned Node24.19.0 and Python3.12 required for this recorded contract")
    spec = importlib.util.spec_from_file_location("file_io_overlay", BASE / "prepare_overlay.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    prepared = args.prepared_source.resolve()
    shipping_provider = prepared / module.TARGET
    if hashlib.sha256(shipping_provider.read_bytes()).hexdigest() != module.ORIGINAL_SHA256:
        raise ValueError(
            "Actual prepared shipping provider hash differs; raw lifecycle assets are not test inputs"
        )
    with tempfile.TemporaryDirectory(prefix="tank-file-io-offline-") as tmp:
        root = Path(tmp)
        target = root / module.TARGET
        target.parent.mkdir(parents=True)
        for name in ("boxlite-provider.ts", "retained-session.ts"):
            shutil.copyfile(shipping_provider.with_name(name), target.with_name(name))
        result = module.apply(root)
        shutil.copyfile(
            BASE / "provider-regression.test.mjs", target.with_name("provider-regression.test.mjs")
        )
        subprocess.run([args.node, str(BASE / "compile-test-workcopy.mjs"), str(target.parent)], check=True)
        env = {
            "PATH": os.defpath,
            "TANK_FILE_IO_TEST_WORKCOPY": str(target.parent),
            "TANK_FILE_IO_TEST_PYTHON": args.python,
        }
        command = [
            args.node,
            "--test",
            str(target.with_name("provider-regression.test.mjs")),
            str(BASE / "file-io.test.mjs"),
        ]
        completed = subprocess.run(command, capture_output=True, env=env, timeout=30, check=False)
        (args.output / "node-tests.log").write_bytes(completed.stdout + completed.stderr)
        guard = {"repeat_refused": False, "changed_source_refused": False, "raw_asset_refused": False}
        try:
            module.apply(root)
        except ValueError:
            guard["repeat_refused"] = True
        target.write_text("tampered source")
        for name in ("guest-file-io.ts", "guest-file-worker.ts"):
            target.with_name(name).unlink()
        try:
            module.apply(root)
        except ValueError:
            guard["changed_source_refused"] = True
        shutil.copyfile(BASE.parent / "openma_lifecycle" / "boxlite-provider.ts", target)
        try:
            module.apply(root)
        except ValueError:
            guard["raw_asset_refused"] = True
        assert all(guard.values())
        receipt = {
            "scope": "local real subprocess file I/O and synthetic blocked transports; NOT native VM acceptance",
            "node": node,
            "python": python,
            "tests_exit_code": completed.returncode,
            "guard": guard,
            "actual_prepared_source": str(prepared),
            "shipping_input_sha256": hashlib.sha256(shipping_provider.read_bytes()).hexdigest(),
            "stage": result,
            "asset_manifest_sha256": hashlib.sha256((BASE / "manifest.json").read_bytes()).hexdigest(),
        }
        (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(
            json.dumps(
                {"node": node, "python": python, "tests_exit_code": completed.returncode, "guard": guard}
            )
        )
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
