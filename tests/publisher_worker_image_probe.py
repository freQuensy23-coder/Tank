"""Exercise unchanged shell supervision with an explicitly instrumented local Docker worker."""

import json
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def docker(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *arguments], text=True, capture_output=True, check=False)


def ignore_signals() -> None:
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def prove_signal(image: str, sha: str, selected: str, requested: signal.Signals) -> None:
    with tempfile.TemporaryDirectory(prefix="publisher-supervision-") as temporary:
        host = Path(temporary)
        process = subprocess.Popen(
            [
                str(ROOT / "scripts/deploy/publisher_worker.sh"),
                image,
                sha,
                selected,
                selected,
                "prepare",
                str(host),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=ignore_signals,
        )
        state = host / "apify-state"
        lease = state / ".worker-active"
        container: str | None = None
        nonce: str | None = None
        try:
            deadline = time.monotonic() + 2
            effects = state / "actual-worker-effects"
            while not effects.exists() or not effects.stat().st_size:
                if process.poll() is not None:
                    _, error = process.communicate()
                    raise AssertionError("Actual worker exited before effects: " + error.decode())
                if time.monotonic() >= deadline:
                    raise AssertionError("Actual owned Docker worker did not begin effects")
                time.sleep(0.01)
            lines = lease.read_text().splitlines()
            assert len(lines) == 4 and lines[0] == "tank-publisher-worker-v1" and lines[3] == image
            container, nonce = lines[1:3]
            assert re.fullmatch(r"[0-9a-f]{64}", container)
            observed = docker(
                "inspect",
                "--format",
                '{{index .Config.Labels "org.tank.publisher.role"}}|{{index .Config.Labels "org.tank.publisher.owner"}}|{{index .Config.Labels "org.tank.publisher.image"}}|{{index .Config.Labels "org.tank.publisher.root"}}',
                container,
            )
            assert observed.returncode == 0 and observed.stdout.strip() == f"worker-v1|{nonce}|{image}|{host}"
            process.send_signal(requested)
            _, error = process.communicate(timeout=4)
            assert (
                process.returncode == {signal.SIGTERM: 143, signal.SIGHUP: 129, signal.SIGINT: 130}[requested]
            ), error.decode()
            stable = effects.read_bytes()
            time.sleep(0.1)
            assert effects.read_bytes() == stable
            assert not lease.exists()
            assert docker("inspect", container).returncode != 0
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=4)
            if container is not None and nonce is not None:
                observed = docker(
                    "inspect",
                    "--format",
                    '{{index .Config.Labels "org.tank.publisher.owner"}}|{{index .Config.Labels "org.tank.publisher.image"}}|{{index .Config.Labels "org.tank.publisher.root"}}',
                    container,
                )
                if observed.returncode == 0:
                    assert observed.stdout.strip() == f"{nonce}|{image}|{host}"
                    removed = docker("rm", "--force", container)
                    assert removed.returncode == 0, removed.stderr


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Explicit instrumented supervision image coordinate required")
    image = sys.argv[1]
    match = re.fullmatch(r"ghcr.io/frequensy23-coder/tank/tank:([0-9a-f]{40})", image)
    if match is None:
        raise SystemExit("Invalid explicit supervision image coordinate")
    before = docker(
        "image", "inspect", "--format", '{{index .Config.Labels "org.tank.refactor.role"}}', image
    )
    assert before.returncode == 0 and before.stdout.strip() == "publisher-supervision-instrumentation"
    for requested in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        prove_signal(image, match[1], "b" * 40, requested)
    print(
        json.dumps(
            {
                "role": "SUPERVISION_INSTRUMENTATION_ONLY",
                "signals": ["TERM", "HUP", "INT"],
                "existing_start_bound_s": 2,
                "existing_drain_bound_s": 4,
                "result": "ACTUAL_CONTAINER_EFFECTS_STOPPED",
            }
        )
    )


if __name__ == "__main__":
    main()
