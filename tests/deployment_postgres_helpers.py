"""Actual deployment SQL in an owned, isolated PostgreSQL 16 container."""

import asyncio
import json
import os
import re
import secrets
import shutil
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import psycopg
from postgres_helpers import _finish
from psycopg.conninfo import make_conninfo

from tank.storage import PostgresRepository

IMAGE = "postgres:16-bookworm"
OWNER_LABEL = "tank.test.deployment-postgres"
ROOT = Path(__file__).resolve().parents[1]


async def _docker(*arguments):
    process = await asyncio.create_subprocess_exec(
        "docker", *arguments, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        output, error = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode:
        raise RuntimeError(f"Deployment PostgreSQL Docker {arguments[0]} failed: {error.decode().strip()}")
    if arguments[0] == "logs":
        return (output + error).decode()
    return output.decode().strip()


async def _owned_container(container_id, owner):
    state = json.loads(await _docker("inspect", container_id))[0]
    if state["Id"] != container_id or state["Config"].get("Labels", {}).get(OWNER_LABEL) != owner:
        raise RuntimeError("Refusing to operate on a PostgreSQL container without exact test ownership")
    return state


async def _ready(container_id, owner, dsn) -> None:

    async with asyncio.timeout(60):
        while True:
            state = await _owned_container(container_id, owner)
            if not state["State"]["Running"]:
                raise RuntimeError("Owned deployment PostgreSQL exited before readiness")
            if await _docker("exec", container_id, "cat", "/proc/1/comm") != "postgres":
                continue
            try:
                connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True, connect_timeout=1)
            except psycopg.OperationalError:
                continue
            try:
                row = await (await connection.execute("SELECT current_database()")).fetchone()
                if row[0] != "tank" or not 160000 <= connection.info.server_version < 170000:
                    raise RuntimeError("Deployment fixture requires actual PostgreSQL 16 database tank")
            finally:
                await connection.close()
            return


@asynccontextmanager
async def deployment_repository():
    if shutil.which("docker") is None:
        raise RuntimeError("Selected deployment PostgreSQL integrations require Docker")

    image = json.loads(await _docker("image", "inspect", IMAGE))[0]
    image_id = image["Id"]
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", image_id) or image["Os"] != "linux":
        raise RuntimeError("Deployment PostgreSQL requires an immutable local Linux image")
    owner = uuid.uuid4().hex
    container_id = None
    repo = None
    with tempfile.TemporaryDirectory(
        prefix="tank-deployment-pg-", dir=os.environ.get("TMPDIR", "/tmp")
    ) as directory:
        temporary = Path(directory)
        socket = temporary / "socket"
        socket.mkdir()
        socket.chmod(0o777)
        passwords = {
            name: secrets.token_urlsafe(32)
            for name in (
                "TANK_DB_PASSWORD",
                "OMA_DB_PASSWORD",
                "TANK_AGENT_DB_PASSWORD",
                "TANK_REPLAY_AGENT_DB_PASSWORD",
            )
        }
        dsn = make_conninfo(user="postgres", dbname="tank", host=str(socket))

        async def allocate() -> None:
            nonlocal container_id
            created = await _docker(
                "create",
                "--name",
                "tank-test-deployment-" + owner,
                "--label",
                OWNER_LABEL + "=" + owner,
                "--network",
                "none",
                "--shm-size",
                "1g",
                "--tmpfs",
                "/var/lib/postgresql/data:rw,nosuid,nodev",
                "--mount",
                f"type=bind,src={socket},dst=/socket",
                "--mount",
                f"type=bind,src={ROOT / 'deploy/postgres/init.sh'},dst=/docker-entrypoint-initdb.d/10-tank.sh,readonly",
                *(argument for name, value in passwords.items() for argument in ("--env", f"{name}={value}")),
                "--env",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                image_id,
                "postgres",
                "-c",
                "listen_addresses=",
                "-c",
                "unix_socket_directories=/var/run/postgresql,/socket",
                "-c",
                "max_locks_per_transaction=1024",
                "-c",
                "max_connections=300",
            )
            if not re.fullmatch(r"[a-f0-9]{64}", created):
                raise RuntimeError("Docker did not return an exact owned container ID")
            container_id = created

        async def cleanup() -> None:
            try:
                if repo is not None:
                    await repo.close()
            finally:
                if container_id is not None:
                    await _owned_container(container_id, owner)
                    await _docker("rm", "--force", "--volumes", container_id)

        try:
            await _finish(allocate())
            state = await _owned_container(container_id, owner)
            if state["Image"] != image_id or state["HostConfig"]["NetworkMode"] != "none":
                raise RuntimeError("Owned deployment PostgreSQL image/network configuration changed")
            if state["HostConfig"].get("PortBindings"):
                raise RuntimeError("Deployment PostgreSQL must not publish a host port")
            await _docker("start", container_id)
            try:
                await _ready(container_id, owner, dsn)
            except Exception as error:
                logs = await _docker("logs", container_id)
                for password in passwords.values():
                    logs = logs.replace(password, "<disposable-secret>")
                raise RuntimeError("Owned deployment PostgreSQL startup failed:\n" + logs[-8000:]) from error
            repo = await PostgresRepository(dsn).open()
            await repo.migrate()
            yield repo
        finally:
            await _finish(cleanup())
