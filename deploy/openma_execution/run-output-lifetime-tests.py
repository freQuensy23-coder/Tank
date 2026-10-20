"""Isolated Node24 ownership regression; no SDK, native device or service."""

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

BASE = Path(__file__).resolve().parent
TARGET = "packages/control-plane-node/src/lib/node-managed-harness-runtime.ts"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def run(source, image, output):
    output.mkdir(exist_ok=False)
    (row,) = [r for r in json.loads((BASE / "overlay.json").read_text()) if r["path"] == TARGET]
    assert sha(row["old"].encode()) == row["sha256"]
    assert sha(row["new"].encode()) == row["patched_sha256"]
    assert sha((source / TARGET).read_bytes()) in {row["sha256"], row["patched_sha256"]}
    receipt = {
        "native": False,
        "models": 0,
        "target": TARGET,
        "source_sha256": row["sha256"],
        "patched_sha256": row["patched_sha256"],
        "image": image,
        "source_modules_sha256": {},
    }
    with tempfile.TemporaryDirectory(prefix="tank-output-lifetime-") as directory:
        staging = Path(directory)
        for label, body in [("before", row["old"]), ("after", row["new"])]:
            patch = staging / (label + ".ts")
            patch.write_text(body)
            command = [
                "docker",
                "--context",
                "desktop-linux",
                "run",
                "--rm",
                "--network",
                "none",
                "--memory",
                "1g",
                "--cpus",
                "2",
                "--entrypoint",
                "/usr/bin/env",
                "--workdir",
                "/app/apps/main-node",
            ]
            for name in [
                "node-managed-harness-runtime.ts",
                "node-managed-session-runtime.ts",
                "db-unhandled-rejection.ts",
            ]:
                relative = "packages/control-plane-node/src/lib/" + name
                path = patch if relative == TARGET else source / relative
                receipt["source_modules_sha256"][label + "/" + name] = sha(path.read_bytes())
                command += ["--mount", f"type=bind,src={path},dst=/app/{relative},readonly"]
            command += [
                "--mount",
                f"type=bind,src={BASE / 'output-lifetime-child.mjs'},dst=/audit/driver-child.mjs,readonly",
                image,
                "-u",
                "NODE_PATH",
                "-u",
                "NODE_ENV",
                "node",
                "--import",
                "tsx",
                "/audit/driver-child.mjs",
                "fatal" if label == "before" else "controls",
            ]
            (output / (label + "-command.json")).write_text(json.dumps(command, indent=2) + "\n")
            result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
            (output / (label + ".log")).write_text(result.stdout + result.stderr)
            if label == "before":
                assert result.returncode == 1
                assert '"op":"process.unhandled_rejection"' in result.stdout
                assert "execution fence was lost" in result.stdout
                receipt["negative_control_exit"] = result.returncode
            else:
                assert result.returncode == 0, result.stdout + result.stderr
                parsed = json.loads(result.stdout.strip())
                assert parsed["status"] == "PASS"
                receipt["checks"] = parsed["checks"]
    receipt["status"] = "PASS"
    (output / "report.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepared-source", required=True, type=Path)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.prepared_source.resolve(), args.image, args.output.resolve()), indent=2))
