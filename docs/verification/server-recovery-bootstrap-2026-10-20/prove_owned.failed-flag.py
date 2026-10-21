"""Sequential read-only restore proof; starts no Runtime/OpenMA/market worker."""

import json
import shutil
import subprocess
import time
from pathlib import Path

OUT = Path("/srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20")
assert json.loads((OUT / "restore-stage-complete.json").read_text())["semantic_proof_pending"]
COMPOSE = [
    "docker",
    "compose",
    "--env-file",
    "/etc/tank/compose.env",
    "-p",
    "tank",
    "-f",
    "/srv/tank/compose.yaml",
    "-f",
    "/etc/tank/compose.server.yaml",
    "-f",
    str(OUT / "pg.resources.yaml"),
    "-f",
    str(OUT / "tank.image.yaml"),
]
for name in ("tank", "oma"):
    container = "tank-recovery-proof-" + name
    command = COMPOSE + [
        "run",
        "--rm",
        "--no-deps",
        "--no-build",
        "--pull",
        "never",
        "-T",
        "--name",
        container,
        "--user",
        "0:0",
        "--volume",
        str(OUT) + ":/qa",
        "tank",
        "python",
        "/qa/verify_restore.py",
        "--dump",
        f"/backup/recovered-20261020/tank-2026-10-20/{name}.dump",
        "--report",
        f"/qa/proof/{name}.json",
        "--verify",
    ]
    if name == "oma":
        command.append("--oma")
    with (OUT / f"{name}.proof.private.log").open("wb") as log:
        Path(log.name).chmod(0o600)
        start = time.monotonic()
        process = subprocess.Popen(command, stdout=log, stderr=log)
        while process.poll() is None:
            time.sleep(10)
            free = shutil.disk_usage("/var/lib/docker").free
            available = (
                int(
                    next(
                        line.split()[1]
                        for line in Path("/proc/meminfo").read_text().splitlines()
                        if line.startswith("MemAvailable:")
                    )
                )
                * 1024
            )
            if free < 8 * 1024**3 or available < 2 * 1024**3:
                subprocess.run(["docker", "pause", container], check=True, capture_output=True)
                raise RuntimeError("Owned read-only proof paused for capacity")
            print(
                json.dumps(
                    {
                        "database": name,
                        "state": "READ_ONLY_PROOF",
                        "elapsed_seconds": round(time.monotonic() - start),
                        "free_bytes": free,
                        "mem_available_bytes": available,
                    }
                ),
                flush=True,
            )
        assert process.wait() == 0, "Proof failed; private log retained"
    report = json.loads((OUT / "proof" / f"{name}.json").read_text())
    assert report["passed"] and report["mismatches"] == [], "Semantic proof mismatch"
    print(
        json.dumps(
            {
                "database": name,
                "tables": len(report["tables"]),
                "sequences": len(report["sequences"]),
                "passed": True,
            }
        ),
        flush=True,
    )
(OUT / "semantic-proof-complete.json").write_text(
    json.dumps({"tank": True, "oma": True, "migrations_not_started": True}) + "\n"
)
