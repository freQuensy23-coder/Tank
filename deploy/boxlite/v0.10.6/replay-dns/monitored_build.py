"""Run one owned Desktop build phase; no daemon-wide cleanup or native devices."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import time
from pathlib import Path

IMAGE = "sha256:48a010671e6b48b811bbd698efcdcafc18fca708aab61ec1983027eff43c103c"
DOCKER = ["docker"]
LABEL = "tank.replay-dns-build=20261019"
FLOOR = 8 * 1024**3


def recipe_hashes(assets):
    names = (
        "apply_patch.py",
        "build.sh",
        "prepare.sh",
        "test.sh",
        "monitored_build.py",
        "source-contract.json",
    )
    return {name: hashlib.sha256((assets / name).read_bytes()).hexdigest() for name in names}


def command(args):
    return subprocess.check_output(DOCKER + args, text=True, timeout=20).strip()


def run(
    root, assets, phase, network, seconds, attempt, scope=None, builder_image=IMAGE, architecture="arm64"
):
    root = root.resolve(strict=True)
    assets = assets.resolve(strict=True)
    scope = scope or root.name
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", builder_image):
        raise ValueError("Builder must be an immutable local image SHA256, never a mutable tag")
    if architecture not in {"amd64", "arm64"}:
        raise ValueError("Supported native Linux builders are amd64 and arm64")
    image = json.loads(command(["image", "inspect", builder_image]))[0]
    if image.get("Os") != "linux" or image.get("Architecture") != architecture:
        raise ValueError("Builder image does not match requested Linux architecture")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,59}", scope):
        raise ValueError("Build scope must be a short lowercase owned identifier")
    cache = "tank-replay-dns-" + scope + "-cache"
    receipt_phase = phase + ("-" + str(attempt) if attempt != 1 else "")
    name = "tank-replay-dns-" + scope + "-" + receipt_phase
    receipt = {
        "phase": phase,
        "network": network,
        "image": builder_image,
        "architecture": architecture,
        "samples": [],
        "recipe_sha256": recipe_hashes(assets),
    }
    if shutil.disk_usage(root).free < FLOOR:
        raise RuntimeError("Host headroom below 8 GiB before phase")
    # An existing container of this name is evidence, never permission to remove it.
    if subprocess.run(DOCKER + ["inspect", name], capture_output=True, check=False).returncode == 0:
        raise RuntimeError("Owned build phase name already exists")
    volume = subprocess.run(
        DOCKER + ["volume", "inspect", cache], capture_output=True, text=True, check=False
    )
    if volume.returncode == 0:
        existing = json.loads(volume.stdout)[0]
        if (
            existing.get("Labels", {}).get("tank.replay-dns-scope") != scope
            or existing.get("Driver") != "local"
            or existing.get("Options")
        ):
            raise RuntimeError("Existing cache volume lacks this build scope")
    else:
        command(["volume", "create", "--driver", "local", "--label", "tank.replay-dns-scope=" + scope, cache])
    cid = command(
        [
            "run",
            "-d",
            "--name",
            name,
            "--label",
            LABEL,
            "--network",
            network,
            "--cpus",
            "3",
            "--memory",
            "5g",
            "--mount",
            f"type=bind,src={root / 'source'},dst=/build/source",
            "--mount",
            f"type=bind,src={root / 'output'},dst=/output",
            "--mount",
            f"type=bind,src={assets},dst=/recipe,readonly",
            "--mount",
            f"type=volume,src={cache},dst=/cache",
            "-e",
            "CARGO_HOME=/cache/cargo",
            "-e",
            "GOMODCACHE=/cache/go-mod",
            "-e",
            "GOCACHE=/cache/go-build",
            "-e",
            "CARGO_TARGET_DIR=/cache/target",
            "-w",
            "/build/source",
            "-e",
            "TARGETARCH=" + architecture,
            builder_image,
            "bash",
            f"/recipe/{phase}.sh",
        ]
    )
    receipt["container_id"] = cid
    state = json.loads(command(["inspect", cid]))[0]
    if state["Config"]["Labels"].get("tank.replay-dns-build") != "20261019":
        raise RuntimeError("Build ownership label mismatch")
    started = time.monotonic()
    reason = None
    log_path = root / (receipt_phase + ".log")
    try:
        with log_path.open("w") as output:
            follower = subprocess.Popen(
                DOCKER + ["logs", "--follow", cid], stdout=output, stderr=subprocess.STDOUT
            )
            try:
                while True:
                    state = json.loads(command(["inspect", cid]))[0]["State"]
                    if not state["Running"]:
                        receipt["exit_code"] = state["ExitCode"]
                        break
                    disk = subprocess.run(
                        DOCKER + ["exec", cid, "df", "-Pk", "/cache"],
                        capture_output=True,
                        check=False,
                        text=True,
                        timeout=10,
                    )
                    if disk.returncode != 0:
                        # Recheck the phase exit before classifying a df race as failure.
                        if not json.loads(command(["inspect", cid]))[0]["State"]["Running"]:
                            continue
                        reason = "MONITOR_FAILED"
                    else:
                        sample = {
                            "elapsed_s": round(time.monotonic() - started, 3),
                            "host_free_bytes": shutil.disk_usage(root).free,
                            "docker_free_bytes": int(disk.stdout.splitlines()[-1].split()[3]) * 1024,
                        }
                        receipt["samples"].append(sample)
                        if min(sample["host_free_bytes"], sample["docker_free_bytes"]) < FLOOR:
                            reason = "HEADROOM_ABORT"
                    if time.monotonic() - started > seconds:
                        reason = "BUILD_PHASE_TIMEOUT"
                    if reason:
                        # The full immutable ID belongs to this phase only.
                        command(["stop", "--time", "5", cid])
                        receipt["exit_code"] = json.loads(command(["inspect", cid]))[0]["State"]["ExitCode"]
                        break
                    time.sleep(3)
            finally:
                try:
                    follower.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    follower.terminate()
                    follower.wait(timeout=5)
    finally:
        # Interruptions must not leave compilation running after its monitor exits.
        if json.loads(command(["inspect", cid]))[0]["State"]["Running"]:
            command(["stop", "--time", "5", cid])
        receipt["elapsed_s"] = round(time.monotonic() - started, 3)
        receipt["status"] = reason or ("PASS" if receipt.get("exit_code") == 0 else "FAILED")
        if receipt["status"] == "PASS" and recipe_hashes(assets) != receipt["recipe_sha256"]:
            receipt["status"] = "RECIPE_CHANGED_DURING_PHASE"
        if receipt["status"] == "PASS" and phase == "build":
            receipt["artifact_sha256"] = hashlib.sha256(
                (root / "output/boxlite-shim").read_bytes()
            ).hexdigest()
        receipt["final_state"] = json.loads(command(["inspect", cid]))[0]["State"]
        (root / (receipt_phase + "-receipt.json")).write_text(json.dumps(receipt, indent=2) + "\n")
    if receipt["status"] != "PASS":
        raise RuntimeError("Owned build phase: " + receipt["status"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("phase", choices=["prepare", "test", "build"])
    parser.add_argument("--network", choices=["none", "default"], default="none")
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--scope")
    parser.add_argument("--builder-image", default=IMAGE)
    parser.add_argument("--architecture", choices=["amd64", "arm64"], default="arm64")
    parser.add_argument("--docker-context")
    args = parser.parse_args()
    if args.docker_context:
        DOCKER = ["docker", "--context", args.docker_context]
    run(
        args.root,
        Path(__file__).resolve().parent,
        args.phase,
        args.network,
        args.seconds,
        args.attempt,
        args.scope,
        args.builder_image,
        args.architecture,
    )
