"""Owned one-off recovery only; root GO 2026-10-20. Never starts Tank/OpenMA."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

OUT = Path("/srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20")
BACKUP = Path("/var/backups/tank/recovered-20261020/tank-2026-10-20")
IMAGE = "postgres@sha256:0ea6700a3b4f0ae6ce746519073558aed4d88a79d8d07622a9a644946c7319c4"
FLOOR = 8 * 1024**3
RESOURCE = OUT / "pg.resources.yaml"
RESOURCE.write_text("services:\n  postgres:\n    cpus: '2.0'\n    mem_limit: 2g\n")
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
    str(RESOURCE),
]


def call(args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, **kwargs).stdout.strip()


def memory():
    return (
        int(
            next(
                line.split()[1]
                for line in Path("/proc/meminfo").read_text().splitlines()
                if line.startswith("MemAvailable:")
            )
        )
        * 1024
    )


def save(name, record):
    p = OUT / name
    p.write_text(json.dumps(record, indent=2) + "\n")
    p.chmod(0o600)


def query(sql, db="postgres"):
    return call(
        [
            "docker",
            "exec",
            "-u",
            "postgres",
            "tank-postgres-1",
            "psql",
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            db,
            "-c",
            sql,
        ]
    )


def guard_capacity(initial=False):
    free = shutil.disk_usage("/var/lib/docker").free
    available = memory()
    assert free >= (14 * 1024**3 if initial else FLOOR), "Disk capacity guard"
    assert available >= 2 * 1024**3, "Memory capacity guard"
    return {"free_bytes": free, "mem_available_bytes": available}


capacity = guard_capacity(True)
assert subprocess.run(["docker", "volume", "inspect", "tank_pg-data"], capture_output=True).returncode != 0, (
    "PG volume already exists"
)
assert (
    subprocess.run(["docker", "container", "inspect", "tank-postgres-1"], capture_output=True).returncode != 0
), "PG container already exists"
configuration = json.loads(call(COMPOSE + ["config", "--format", "json"]))
pg = configuration["services"]["postgres"]
assert pg["image"] == IMAGE and not pg.get("ports"), "Image/port guard"
assert int(pg["mem_limit"]) == 2 * 1024**3 and float(pg["cpus"]) == 2.0, "Resource limit guard"
secret_names = (
    "postgres_password",
    "tank_db_password",
    "oma_db_password",
    "tank_agent_db_password",
    "tank_replay_agent_db_password",
)
secret_metadata = {}
probe = [
    "docker",
    "run",
    "--rm",
    "--pull=never",
    "--network=none",
    "--read-only",
    "--user",
    "999:999",
    "--memory=64m",
    "--cpus=0.25",
]
for name in secret_names:
    path = Path(configuration["secrets"][name]["file"])
    stat = path.stat()
    assert stat.st_uid == 999 and stat.st_mode & 0o077 == (0 if name == "postgres_password" else 0o040), (
        "Secret ownership/mode guard"
    )
    secret_metadata[name] = {
        "path": str(path),
        "uid": stat.st_uid,
        "gid": stat.st_gid,
        "mode": oct(stat.st_mode & 0o777),
    }
    probe += ["--mount", f"type=bind,src={path},dst=/run/secrets/{name},readonly"]
probe += [
    "--entrypoint",
    "sh",
    IMAGE,
    "-c",
    "for name in postgres_password tank_db_password oma_db_password tank_agent_db_password tank_replay_agent_db_password; do test -r /run/secrets/$name || exit 1; done",
]
call(probe)
save(
    "pg-preflight.json",
    {
        "capacity": capacity,
        "image": IMAGE,
        "secrets_readable_by_uid999": True,
        "secret_source_identities": secret_metadata,
        "new_volume_required": True,
        "no_public_ports": True,
        "compose_files": [str(RESOURCE), "/srv/tank/compose.yaml", "/etc/tank/compose.server.yaml"],
    },
)
call(COMPOSE + ["up", "-d", "--no-deps", "--no-build", "--pull", "never", "postgres"])
for _attempt in range(60):
    status = subprocess.run(
        [
            "docker",
            "exec",
            "-u",
            "postgres",
            "tank-postgres-1",
            "pg_isready",
            "-U",
            "postgres",
            "-d",
            "postgres",
        ],
        capture_output=True,
    )
    final = subprocess.run(
        ["docker", "exec", "tank-postgres-1", "sh", "-c", 'test "$(cat /proc/1/comm)" = postgres'],
        capture_output=True,
    )
    if status.returncode == 0 and final.returncode == 0:
        break
    time.sleep(1)
else:
    raise RuntimeError("PG readiness failed")
inspection = json.loads(call(["docker", "inspect", "tank-postgres-1"]))[0]
assert inspection["Config"]["Image"] == IMAGE, "Actual image guard"
assert (
    inspection["HostConfig"]["Memory"] == 2 * 1024**3
    and inspection["HostConfig"]["NanoCpus"] == 2_000_000_000
), "Actual cgroup guard"
assert not inspection["NetworkSettings"]["Ports"].get("5432/tcp"), "Published port guard"
assert query("SHOW max_locks_per_transaction") == "64", "Default64 guard"
for name in ("tank", "oma"):
    assert (
        query(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S','f')",
            name,
        )
        == "0"
    ), "Target not empty"
save(
    "pg-started-empty.json",
    {
        "image_id": inspection["Image"],
        "container_id": inspection["Id"],
        "volume": next(
            m["Name"] for m in inspection["Mounts"] if m["Destination"] == "/var/lib/postgresql/data"
        ),
        "max_locks_per_transaction": 64,
        "resources": {"cpus": 2, "memory_bytes": 2 * 1024**3},
        "empty_databases": ["tank", "oma"],
        "network": inspection["NetworkSettings"]["Networks"],
        "public_port": False,
    },
)
for name in ("tank", "oma"):
    guard_capacity()
    start = time.monotonic()
    with (
        (BACKUP / f"{name}.dump").open("rb") as source,
        (OUT / f"{name}.restore.private.log").open("wb") as log,
    ):
        os.chmod(log.name, 0o600)
        process = subprocess.Popen(
            [
                "docker",
                "exec",
                "-i",
                "-u",
                "postgres",
                "tank-postgres-1",
                "pg_restore",
                "--exit-on-error",
                "--dbname",
                name,
            ],
            stdin=source,
            stdout=log,
            stderr=log,
        )
        while process.poll() is None:
            time.sleep(5)
            try:
                capacity = guard_capacity()
            except AssertionError:
                call(["docker", "pause", "tank-postgres-1"])
                save(
                    "capacity-paused.json",
                    {
                        "database": name,
                        "client_pid": process.pid,
                        "container_paused": True,
                        "free_bytes": shutil.disk_usage("/var/lib/docker").free,
                        "mem_available_bytes": memory(),
                    },
                )
                raise RuntimeError("Owned PG paused for capacity; originals and partial restore preserved")
            print(
                json.dumps(
                    {
                        "database": name,
                        "state": "RESTORING",
                        "elapsed_seconds": round(time.monotonic() - start),
                        "capacity": capacity,
                    }
                ),
                flush=True,
            )
        code = process.wait()
    receipt = {
        "database": name,
        "exit_code": code,
        "elapsed_seconds": round(time.monotonic() - start, 2),
        "capacity": guard_capacity(),
        "owners_acl_preserved": True,
        "clean_create_reset_used": False,
    }
    save(f"{name}.restore-result.json", receipt)
    assert code == 0, "Restore failed; private log and partial database preserved"
    print(json.dumps(receipt), flush=True)
save(
    "restore-stage-complete.json",
    {
        "tank_restored": True,
        "oma_restored": True,
        "semantic_proof_pending": True,
        "migrations_not_started": True,
        "runtime_not_started": True,
    },
)
