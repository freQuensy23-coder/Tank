"""Observe actual CLI-owned resources at controlled public dependency failures."""

import os
import sys
from pathlib import Path
from typing import Literal
from unittest.mock import patch

import click
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from pydantic import JsonValue

import tank.cli as cli
from tank import clickhouse_market
from tank.clickhouse_market import ClickHouse, MarketClientOwner
from tank.contracts.json import encode_json
from tank.market_codec import StoredRow
from tank.settings import Settings
from tank.storage import PostgresRepository

type ProbePool = AsyncConnectionPool[AsyncConnection[StoredRow]]


def main() -> None:
    mode: Literal["OPEN", "CLOSE"]
    if sys.argv[1] == "OPEN":
        mode = "OPEN"
    elif sys.argv[1] == "CLOSE":
        mode = "CLOSE"
    else:
        raise ValueError("Resource probe requires a declared failure phase")
    pools: list[ProbePool] = []
    clients: list[ClickHouse] = []
    pids: list[int] = []
    callbacks: list[str] = []
    real_wait = AsyncConnectionPool.wait
    real_close = PostgresRepository.close
    real_install = clickhouse_market.install

    def settings() -> Settings:
        return Settings(
            _env_file=None,
            database_url=os.environ["TANK_CLI_RESOURCE_DATABASE_URL"],
            clickhouse_url=os.environ["CLICKHOUSE_TEST_URL"],
            data_dir=Path(os.environ["TANK_CLI_RESOURCE_DATA_DIR"]),
            replay=False,
        )

    async def wait(pool: ProbePool) -> None:
        await real_wait(pool)
        async with pool.connection() as connection:
            row = await (await connection.execute("SELECT pg_backend_pid() AS owner_pid")).fetchone()
            assert row is not None
            pid = row["owner_pid"]
            assert isinstance(pid, int) and not isinstance(pid, bool) and pid > 0
            pids.append(pid)
        pools.append(pool)
        if mode == "OPEN":
            raise RuntimeError("synthetic failure after actual pool allocation")

    async def close(repository: PostgresRepository) -> None:
        await real_close(repository)
        if mode == "CLOSE":
            raise RuntimeError("synthetic failure after actual pool closure")

    def install(repository: MarketClientOwner, configured: Settings) -> ClickHouse | None:
        client = real_install(repository, configured)
        assert client is not None
        clients.append(client)
        return client

    async def command(repository: PostgresRepository, configured: Settings) -> dict[str, JsonValue]:
        row = await repository.get("reg.tokens", "cli-resource-control")
        assert row is not None and row["token_id"] == "cli-resource-control"
        callbacks.append("called")
        return {"completed": True}

    failed = False
    with (
        patch.object(cli, "Settings", settings),
        patch.object(AsyncConnectionPool, "wait", wait),
        patch.object(PostgresRepository, "close", close),
        patch.object(clickhouse_market, "install", install),
    ):
        try:
            cli.run_db(command)
        except click.ClickException:
            failed = True
    report: dict[str, JsonValue] = {
        "mode": mode,
        "failed": failed,
        "callback_count": len(callbacks),
        "pool_closed": [pool.closed for pool in pools],
        "client_closed": [client.http.is_closed for client in clients],
        "owner_pids": [pid for pid in pids],
    }
    print(encode_json(report).decode(), flush=True)
    sys.stdin.readline()


if __name__ == "__main__":
    main()
