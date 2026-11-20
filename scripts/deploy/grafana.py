import base64
import http.client
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

KEYS = ("GRAFANA_ADMIN_PASSWORD", "GRAFANA_SECRET_KEY", "GRAFANA_STORE_PASSWORD", "GRAFANA_READER_PASSWORD")
ENV = Path(".env")


def settings():
    info = ENV.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.geteuid():
        raise SystemExit("Deploy requires an owner-only .env owned by the deploy user")
    if any(key in os.environ for key in KEYS):
        raise SystemExit(
            "Configure persistent Grafana credentials in .env, not the deploy process environment"
        )
    missing = secrets.token_hex(32)
    projection = {key: "${" + key + "-" + missing + "}" for key in KEYS}
    model = {"services": {"settings": {"image": "scratch", "environment": projection}}}
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ENV),
            "-p",
            "tank-settings",
            "-f",
            "-",
            "config",
            "--format",
            "json",
        ],
        input=json.dumps(model),
        text=True,
        capture_output=True,
    )
    if result.returncode:
        raise SystemExit("Could not resolve the protected Compose environment")
    env = json.loads(result.stdout)["services"]["settings"]["environment"]
    if any(not env[key] for key in KEYS):
        raise SystemExit("Refusing an explicitly empty Grafana credential")
    return {k: v for k, v in env.items() if v != missing}


def prepare(env):
    absent = [key for key in KEYS if key not in env]
    if not absent:
        return
    original = ENV.read_bytes()
    with tempfile.NamedTemporaryFile(dir=ENV.parent, prefix=".env.next.", delete=False) as tmp:
        path = Path(tmp.name)
        try:
            tmp.write(original + (b"" if original.endswith(b"\n") else b"\n"))
            for key in absent:
                tmp.write(f"{key}={secrets.token_hex(32)}\n".encode())
            tmp.flush()
            os.fsync(tmp.fileno())
            os.replace(path, ENV)
        finally:
            path.unlink(missing_ok=True)


def install(env, directory, container):
    process = subprocess.Popen(
        [
            "tar",
            "-c",
            "-C",
            directory,
            "bootstrap.sql",
            "install-views.sql",
            "queue-views.sql",
            "pipeline-views.sql",
        ],
        stdout=subprocess.PIPE,
    )
    command = """set -eu
        dir=$(mktemp -d)
        trap 'rm -rf "$dir"' EXIT
        cd "$dir"
        tar -x
        psql -Xq -d tank -f bootstrap.sql
        psql -Xq -d tank -f install-views.sql"""
    db_keys = KEYS[2:]
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "-u",
                "postgres",
                "-e",
                db_keys[0],
                "-e",
                db_keys[1],
                container,
                "sh",
                "-c",
                command,
            ],
            env={**os.environ, **{key: env[key] for key in db_keys}},
            stdin=process.stdout,
        )
    finally:
        process.stdout.close()
        tar_status = process.wait()
    if tar_status or result.returncode:
        raise SystemExit("Grafana PostgreSQL provisioning failed")


def ready(env, address):
    if not re.fullmatch(r"127\.0\.0\.1:[0-9]+", address):
        raise SystemExit("Refusing to send Grafana credentials to a non-loopback listener")
    auth = base64.b64encode(("admin:" + env[KEYS[0]]).encode()).decode()

    def get(path, anonymous=False):
        conn = http.client.HTTPConnection(address, timeout=5)
        try:
            conn.request("GET", path, headers={} if anonymous else {"Authorization": "Basic " + auth})
            response = conn.getresponse()
            if response.status != (401 if anonymous else 200):
                raise SystemExit(f"Grafana {path}: HTTP {response.status}")
            return None if anonymous else json.load(response)
        finally:
            conn.close()

    datasource = "/api/datasources/uid/tank-postgres"
    get(datasource, anonymous=True)
    ds = get(datasource)
    if (
        ds.get("user") != "tank_grafana_reader"
        or not ds.get("readOnly")
        or get(datasource + "/health").get("status") != "OK"
    ):
        raise SystemExit("Grafana provisioned datasource is not ready")
    for uid in ("tank-queue", "tank-pipeline"):
        path = "/api/dashboards/uid/" + uid
        get(path, anonymous=True)
        doc = get(path)
        if (
            doc["dashboard"]["uid"] != uid
            or not doc["meta"]["provisioned"]
            or doc["meta"]["folderUid"] != "tank"
        ):
            raise SystemExit(f"Grafana {uid} is not provisioned in the Tank folder")
    print(
        "Grafana verified: tank-queue, tank-pipeline and healthy tank-postgres datasource (authenticated loopback HTTP)"
    )


if __name__ == "__main__":
    action = sys.argv[1]
    values = settings()
    if action == "prepare":
        prepare(values)
    elif action == "install":
        install(values, sys.argv[2], sys.argv[3])
    elif action == "ready":
        ready(values, sys.argv[2])
    else:
        raise SystemExit("Unknown Grafana deploy operation")
