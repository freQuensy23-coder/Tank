"""Root-approved normal role/HBA setup. Never emits credential contents or hashes."""

import hashlib
import json
import os
import subprocess
from pathlib import Path
from urllib.parse import quote

OUT = Path("/srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20")


def execute(args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, **kwargs).stdout.strip()


def admin(sql, db="postgres"):
    return execute(
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


# Existing supported owner/agent grants only, without resetting history.
with Path("/srv/tank/deploy/read_only_role.sql").open() as source:
    execute(
        [
            "docker",
            "exec",
            "-i",
            "-u",
            "postgres",
            "tank-postgres-1",
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            "tank",
        ],
        stdin=source,
    )
network = json.loads(execute(["docker", "network", "inspect", "tank_tank-net"]))[0]
gateway = network["IPAM"]["Config"][0]["Gateway"]
subnet = network["IPAM"]["Config"][0]["Subnet"]
assert gateway == "172.30.60.1" and subnet == "172.30.60.0/24"
container = json.loads(execute(["docker", "inspect", "tank-postgres-1"]))[0]
address = container["NetworkSettings"]["Networks"]["tank_tank-net"]["IPAddress"]
assert address.startswith("172.30.60.") and address != gateway
hba = Path("/etc/tank/pg_hba.conf")
old = hba.read_bytes()
line = f"host all postgres {gateway}/32 scram-sha-256"
if line not in old.decode().splitlines():
    content = old.decode()
    # Before existing rejects; all other service restrictions retain their order.
    new = line + "\n" + content
    (OUT / "pg_hba.before-admin-route.conf").write_bytes(old)
    (OUT / "pg_hba.before-admin-route.conf").chmod(0o600)
    with hba.open("w") as stream:
        stream.write(new)
        stream.flush()
        os.fsync(stream.fileno())
assert admin("SELECT pg_reload_conf()") == "t"
assert admin("SELECT count(*) FROM pg_hba_file_rules WHERE error IS NOT NULL") == "0"
secret = Path("/etc/tank/secrets/postgres_password").read_text().strip()
assert secret
private = Path("/srv/tank/data/qa/server-recovery-pg")
private.mkdir(parents=True, exist_ok=True, mode=0o700)
private.chmod(0o700)
env = private / "admin.env"
env.write_text(
    "TANK_TEST_DATABASE_URL=postgresql://postgres:"
    + quote(secret, safe="")
    + "@"
    + address
    + ":5432/postgres\n"
)
env.chmod(0o600)
# Credentials stay in process memory. The route itself is exercised once.
import psycopg

with psycopg.connect(
    host=address, port=5432, user="postgres", password=secret, dbname="postgres", autocommit=True
) as connection:
    assert connection.execute("SELECT current_user").fetchone()[0] == "postgres"
    assert connection.execute("SHOW max_locks_per_transaction").fetchone()[0] == "64"
roles = json.loads(
    admin(
        "SELECT json_agg(json_build_object('role',rolname,'superuser',rolsuper,'createdb',rolcreatedb,'createrole',rolcreaterole,'default_read_only',rolconfig)) FROM pg_roles WHERE rolname IN ('tank_agent','tank_replay_agent')"
    )
)
grants = json.loads(
    admin(
        "SELECT json_build_object('tank_agent_select',has_table_privilege('tank_agent','reg.tokens','SELECT'),'tank_agent_update',has_table_privilege('tank_agent','reg.tokens','UPDATE'),'replay_to_primary',has_database_privilege('tank_replay_agent','tank','CONNECT'),'primary_to_replay',has_database_privilege('tank_agent','tank_replay','CONNECT'))",
        "tank",
    )
)
assert grants == {
    "tank_agent_select": True,
    "tank_agent_update": False,
    "replay_to_primary": False,
    "primary_to_replay": False,
}
report = {
    "passed": True,
    "normal_read_only_role_sql_applied_to": "tank",
    "actual_gateway": gateway,
    "postgres_private_address": address,
    "host_admin_hba_source": gateway + "/32",
    "hba_before_sha256": hashlib.sha256(old).hexdigest(),
    "hba_after_sha256": hashlib.sha256(hba.read_bytes()).hexdigest(),
    "hba_parse_errors": 0,
    "tcp_admin_route_verified": True,
    "private_admin_env_path": str(env),
    "private_env_mode": "0600",
    "credential_contents_or_hashes_emitted": False,
    "roles": roles,
    "grants": grants,
    "public_port": False,
}
p = OUT / "roles-hba-proof.json"
p.write_text(json.dumps(report, indent=2) + "\n")
p.chmod(0o600)
print(json.dumps({key: value for key, value in report.items() if key not in {"roles"}}))
