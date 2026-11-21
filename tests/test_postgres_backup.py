import os
import shutil
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tank.scheduler import Scheduler, verify_backup
from tank.settings import Settings
from tank.storage import MemoryRepository

ROOT = Path(__file__).resolve().parents[1]
CREDENTIALS = {
    "POSTGRES_PASSWORD": "fixture-admin",
    "TANK_DB_PASSWORD": "fixture-tank",
    "OMA_DB_PASSWORD": "fixture-oma",
    "TANK_AGENT_DB_PASSWORD": "fixture-agent",
    "TANK_REPLAY_AGENT_DB_PASSWORD": "fixture-replay",
}


@pytest.mark.integration
@pytest.mark.postgres
async def test_peer_backup_restores_grafana_and_both_application_databases(tmp_path, monkeypatch):
    docker = shutil.which("docker")
    assert docker is not None

    def run(*args, **kwargs):
        return subprocess.run([docker, *args], capture_output=True, timeout=90, **kwargs)

    build = run("build", "--quiet", str(ROOT / "deploy/postgres"), text=True)
    assert build.returncode == 0, build.stderr
    image = build.stdout.strip().splitlines()[-1]
    owner = uuid.uuid4().hex
    created = run(
        "create",
        "--network",
        "none",
        "--label",
        f"tank.test.backup={owner}",
        "--shm-size",
        "1g",
        "--tmpfs",
        "/var/lib/postgresql/data:rw,nosuid,nodev",
        *(arg for name, value in CREDENTIALS.items() for arg in ("-e", f"{name}={value}")),
        image,
        text=True,
    )
    assert created.returncode == 0, created.stderr
    container = created.stdout.strip()

    def exec_pg(*args, user="postgres", check=True, stdin=None):
        result = run("exec", "-i", "-u", user, container, *args, input=stdin)
        if check:
            assert result.returncode == 0, result.stderr.decode()
        return result

    def sql(database, statement, *, role="postgres", user="postgres", check=True):
        return exec_pg(
            "psql",
            "-XwAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            role,
            "-d",
            database,
            "-c",
            statement,
            user=user,
            check=check,
        )

    try:
        started = run("start", container)
        assert started.returncode == 0, started.stderr.decode()
        ready = exec_pg(
            "sh",
            "-ec",
            "for i in $(seq 1 100); do "
            'if [ "$(cat /proc/1/comm)" = postgres ] && pg_isready -q -U postgres; '
            "then exit 0; fi; sleep .1; done; exit 1",
            check=False,
        )
        assert ready.returncode == 0, run("logs", container).stderr.decode()[-4000:]
        assert sql("tank", "SELECT current_user", role="tank_backup", user="tank").stdout == b"tank_backup\n"
        for db in ("tank", "oma"):
            exec_pg("pg_dump", "-w", "-Fc", "-U", "tank_backup", "-d", db, user="tank")
        sql("postgres", "REVOKE CONNECT ON DATABASE tank, oma FROM tank_backup; DROP ROLE tank_backup")
        bootstrap = (ROOT / "deploy/grafana/bootstrap.sql").read_bytes()
        exec_pg(
            "env",
            "GRAFANA_STORE_PASSWORD=fixture-store",
            "GRAFANA_READER_PASSWORD=fixture-reader",
            "psql",
            "-Xw",
            "-d",
            "tank",
            stdin=bootstrap,
        )
        sql(
            "tank",
            """
            SET ROLE tank;
            CREATE TABLE public.backup_marker (id text PRIMARY KEY);
            INSERT INTO public.backup_marker VALUES ('tank-input');
            SET ROLE tank_grafana_store;
            CREATE TABLE grafana.dashboard (
                id bigserial PRIMARY KEY, uid text UNIQUE NOT NULL, title text NOT NULL, data jsonb NOT NULL
            );
            CREATE TABLE grafana.data_source (
                id bigserial PRIMARY KEY, uid text UNIQUE NOT NULL, secure_json_data jsonb NOT NULL
            );
            CREATE TABLE grafana.preferences (user_id bigint PRIMARY KEY, home_dashboard_id bigint NOT NULL);
            INSERT INTO grafana.dashboard(uid, title, data)
                VALUES ('ops', 'Tank Operations', '{"panels":[{"id":7,"title":"Pending jobs"}]}');
            INSERT INTO grafana.data_source(uid, secure_json_data)
                VALUES ('metrics', '{"password":"ZmFrZS1jaXBoZXJ0ZXh0"}');
            INSERT INTO grafana.preferences VALUES (42, 1);
        """,
        )
        sql(
            "oma",
            "SET ROLE oma; CREATE TABLE public.backup_marker (id text); "
            "INSERT INTO public.backup_marker VALUES ('oma-input')",
        )
        app_dump = exec_pg("pg_dump", "-w", "-Fc", "--role=tank", "-d", "tank", check=False)
        assert app_dump.returncode != 0 and b"permission denied for schema grafana" in app_dump.stderr
        exec_pg("pg_dump", "-w", "-Fc", "--role=oma", "-d", "oma")
        assert sql("tank", "SELECT 1", role="tank_backup", user="tank", check=False).returncode != 0
        sql("postgres", "CREATE ROLE tank_backup")
        refused = exec_pg("provision-tank-backup", check=False)
        assert refused.returncode != 0 and b"Refusing to adopt" in refused.stderr
        sql("postgres", "DROP ROLE tank_backup")
        exec_pg("provision-tank-backup")
        exec_pg("provision-tank-backup")

        assert (
            sql("tank", "SELECT title FROM grafana.dashboard", role="tank_backup", user="tank").stdout
            == b"Tank Operations\n"
        )
        assert (
            b"Peer authentication failed" in sql("tank", "SELECT 1", role="tank_backup", check=False).stderr
        )
        assert (
            b"rejects connection"
            in sql("tank_replay", "SELECT 1", role="tank_backup", user="tank", check=False).stderr
        )
        assert (
            b"rejects connection"
            in exec_pg(
                "psql", "-Xw", "-h", "127.0.0.1", "-U", "tank_backup", "-d", "tank", user="tank", check=False
            ).stderr
        )
        write = sql(
            "tank",
            "BEGIN READ WRITE; UPDATE grafana.dashboard SET title = 'changed'",
            role="tank_backup",
            user="tank",
            check=False,
        )
        assert write.returncode != 0 and b"permission denied for table dashboard" in write.stderr
        assert (
            b"permission denied to set role"
            in sql(
                "tank",
                "SET ROLE tank_backup; SET ROLE tank_grafana_store",
                role="tank_backup",
                user="tank",
                check=False,
            ).stderr
        )
        assert (
            sql(
                "postgres",
                "SELECT NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND NOT rolcreaterole "
                "AND NOT rolreplication AND rolpassword IS NULL FROM pg_authid WHERE rolname = 'tank_backup'",
            ).stdout
            == b"t\n"
        )
        assert (
            sql(
                "tank",
                "SELECT has_schema_privilege('tank', 'grafana', 'USAGE'), "
                "pg_has_role('tank_agent', 'tank_backup', 'MEMBER'), pg_has_role('oma', 'tank_backup', 'MEMBER')",
            ).stdout
            == b"f|f|f\n"
        )

        wrapper = tmp_path / "pg_dump"
        wrapper.write_text(
            '#!/bin/sh\nset -eu\n[ "$1 $2 $3" = "--no-password --format=custom --file" ]\n'
            f'exec "{docker}" exec -u tank -e PGHOST -e PGUSER -e PGDATABASE "{container}" '
            'pg_dump --no-password --format=custom > "$4"\n'
        )
        wrapper.chmod(0o755)
        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
        monkeypatch.setenv("PGHOSTADDR", "192.0.2.100")
        monkeypatch.setenv("PGPASSWORD", "not-the-backup-credential")
        data, backup = tmp_path / "data", tmp_path / "backup"
        (data / "bundles").mkdir(parents=True)
        (data / "bundles" / "event.json").write_text('{"event":"fixture"}')
        backup.mkdir()
        settings = Settings(
            data_dir=data,
            backup_dir=backup,
            backup_postgres_socket=Path("/var/run/postgresql"),
            database_url="postgresql://tank:unused@wrong/tank",
            openma_database_url="postgresql://oma:unused@wrong/oma",
        )
        result = await Scheduler(MemoryRepository(), settings, None, None).maintenance(
            datetime(2026, 11, 22, 2, tzinfo=UTC), prune=False
        )
        assert result["status"] == "created"
        snapshot = Path(result["path"])
        assert set(verify_backup(snapshot)["files"]) == {"tank.dump", "oma.dump", "bundles/event.json"}
        for db in ("tank", "oma"):
            target = f"tank_test_restore_{db}"
            sql("postgres", f"CREATE DATABASE {target} TEMPLATE template0")
            exec_pg(
                "pg_restore", "--exit-on-error", "-d", target, stdin=(snapshot / f"{db}.dump").read_bytes()
            )
            assert sql(target, "SELECT id FROM public.backup_marker").stdout == f"{db}-input\n".encode()
        restored = "tank_test_restore_tank"
        assert sql(
            restored,
            "SELECT d.uid, d.title, d.data #>> '{panels,0,title}', p.user_id "
            "FROM grafana.dashboard d JOIN grafana.preferences p ON p.home_dashboard_id = d.id",
        ).stdout == (b"ops|Tank Operations|Pending jobs|42\n")
        assert sql(
            restored, "SELECT uid, secure_json_data ->> 'password' FROM grafana.data_source"
        ).stdout == (b"metrics|ZmFrZS1jaXBoZXJ0ZXh0\n")
        assert sql(
            restored,
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'grafana.dashboard'::regclass",
        ).stdout == (b"tank_grafana_store\n")
        assert (
            b"permission denied for schema grafana"
            in sql(restored, "SET ROLE tank; SELECT * FROM grafana.dashboard", check=False).stderr
        )
        assert (
            sql(
                restored,
                "SET ROLE tank_grafana_store; INSERT INTO grafana.dashboard(uid, title, data) "
                "VALUES ('restored', 'New dashboard', '{}') RETURNING id",
            ).stdout
            == b"SET\n2\nINSERT 0 1\n"
        )
    finally:
        identity = run(
            "inspect", "--format", '{{index .Config.Labels "tank.test.backup"}}', container, text=True
        )
        assert identity.returncode == 0 and identity.stdout.strip() == owner
        removed = run("rm", "-f", "-v", container)
        assert removed.returncode == 0, removed.stderr.decode()
