"""Actual sandbox-image worker/RO mount check; no KVM, SDK, model, network or secret mounts."""

import hashlib
import json
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent
IMAGE = "sha256:a65d36f62d5d59d964b8d104beb063afd619f9512dd1dd3f6da0dada75255bd4"


def main():
    fixture = BASE / "readonly-fixture.bin"
    digest = hashlib.sha256(fixture.read_bytes()).hexdigest()
    base = [
        "docker",
        "--context",
        "desktop-linux",
        "run",
        "--pull",
        "never",
        "--rm",
        "-i",
        "--network",
        "none",
        "--read-only",
        "--memory",
        "128m",
        "--pids-limit",
        "32",
        "--volume",
        str(BASE / "guest-file-io.py") + ":/audit.py:ro",
        "--volume",
        str(fixture) + ":/fixture.bin:ro",
        "--entrypoint",
        "python3",
        IMAGE,
        "/audit.py",
    ]
    read = subprocess.run(
        base + ["read", "/fixture.bin"], input=b"A\n", capture_output=True, timeout=15, check=False
    )
    write = subprocess.run(
        base + ["write", "/fixture.bin", "1"], input=b"X", capture_output=True, timeout=15, check=False
    )
    error = json.loads(write.stdout)
    assert write.returncode == 1 and error["ok"] is False and error["errno"] == 30 and not write.stderr
    assert read.returncode == 0 and not read.stderr
    assert hashlib.sha256(fixture.read_bytes()).hexdigest() == digest
    lines = read.stdout.decode().splitlines()
    header, footer = json.loads(lines[0]), json.loads(lines[-1])
    assert header["size"] == 4 and footer["sha256"] == digest
    result = {
        "scope": "actual Python3.12 sandbox image DesktopDocker readonly mount, NOT KVM/native acceptance",
        "image_id": IMAGE,
        "read_exit": read.returncode,
        "read_frame_size": header["size"],
        "read_digest_matches": True,
        "write_exit": write.returncode,
        "write_errno": error["errno"],
        "readonly_mount_denial_observed": True,
        "fixture_unchanged": True,
        "worker_sha256": hashlib.sha256((BASE / "guest-file-io.py").read_bytes()).hexdigest(),
        "network": "none",
        "container_rootfs": "read-only",
        "provider_or_model_calls": 0,
    }
    print(json.dumps(result))
    return result


if __name__ == "__main__":
    main()
