"""Real allocated resources remain observable until the owned probe child is released."""

import asyncio
import os
import sys
from pathlib import Path
from typing import Literal

import pytest
from foundation_helpers import array_field
from postgres_helpers import PostgresTemplate
from psycopg import AsyncConnection

from tank.contracts.json import decode_unique_json
from tank.contracts.scalars import require_integer


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["OPEN", "CLOSE"])
async def test_cli_failure_closes_actual_owned_pool_and_constructed_http_client(
    postgres_template: PostgresTemplate, tmp_path: Path, mode: Literal["OPEN", "CLOSE"]
) -> None:
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.put("reg.tokens", "cli-resource-control", {"token_id": "cli-resource-control"})
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).with_name("cli_resource_probe.py")),
            mode,
            env={
                **os.environ,
                "TANK_CLI_RESOURCE_DATABASE_URL": repo.database_url,
                "TANK_CLI_RESOURCE_DATA_DIR": str(tmp_path),
            },
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert process.stdout is not None and process.stdin is not None
        try:
            report = decode_unique_json(await process.stdout.readline())
            assert isinstance(report, dict) and report["mode"] == mode and report["failed"] is True
            callbacks = require_integer(report["callback_count"])
            assert callbacks == (0 if mode == "OPEN" else 1)
            pids = [require_integer(pid) for pid in array_field(report, "owner_pids")]
            assert len(pids) == 1 and pids[0] > 0
            async with await AsyncConnection.connect(repo.database_url) as observer:

                async def session_count() -> int:
                    row = await (
                        await observer.execute(
                            "SELECT count(*) FROM pg_stat_activity WHERE pid = ANY(%s) AND datname = current_database()",
                            (pids,),
                        )
                    ).fetchone()
                    assert row is not None and isinstance(row[0], int) and not isinstance(row[0], bool)
                    return row[0]

                remaining = await session_count()
                pools = array_field(report, "pool_closed")
                clients = array_field(report, "client_closed")
                assert len(pools) == len(clients) == 1
                assert pools[0] is True and clients[0] is True, {
                    "pool_closed": pools,
                    "constructed_http_client_closed": clients,
                    "owned_pg_sessions": remaining,
                    "callback_count": callbacks,
                }
                async with asyncio.timeout(5):
                    while remaining:
                        remaining = await session_count()
                assert remaining == 0
        finally:
            try:
                process.stdin.write(b"release\n")
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            _, errors = await process.communicate()
            (tmp_path / "owned-probe-stderr.log").write_bytes(errors)
        assert process.returncode == 0, "Owned resource probe did not finish its declared observation"
